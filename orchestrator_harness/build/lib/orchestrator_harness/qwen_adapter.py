"""Implemented Qwen Code host profile and deterministic safe-boundary delivery.

Qwen Code has no runner-owned in-session item-injection API.  This adapter
therefore carries only the sparse delivery notice at documented lifecycle
boundaries and records resume/finalization decisions through a transport seam.
The synthetic transport is the only transport exercised by this package; a
project hook installation is not evidence of a live Qwen session.
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping

from .host_adapters import (
    DELIVERY_NOTICE_SCHEMA,
    AdapterCapabilities,
    DeliveryCoordinator,
    DeliveryNotice,
    DeliveryReceipt,
    FutureHostFixture,
    HostAdapter,
    HostProfile,
)
from .models import iso_utc, utc_now
from .notifications import ManagerEventRouter


QWEN_ADAPTER_SCHEMA = "orchestrator-qwen-adapter/v1"
QWEN_ADAPTER_VERSION = "qwen-v1"


class QwenAdapterError(ValueError):
    """A Qwen boundary or delivery transaction failed closed."""


def qwen_capabilities() -> AdapterCapabilities:
    """Return the Qwen safe-boundary capabilities represented by this adapter."""

    return AdapterCapabilities(
        active_turn_notice=True,
        idle_wake=True,
        next_input_injection=False,
        finalization_gate=True,
    )


class QwenTransport(ABC):
    """Synthetic-friendly surface for Qwen's documented hook boundaries."""

    @abstractmethod
    def deliver_context(self, notice: Mapping[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def wake_idle(self, session_id: str, notice: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def finalization_continue(self, notice: Mapping[str, Any]) -> bool:
        raise NotImplementedError


class SyntheticQwenTransport(QwenTransport):
    """In-memory transport used by deterministic tests and self-checks only."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.resume_invocations: list[dict[str, Any]] = []

    @staticmethod
    def _assert_sparse_notice(notice: Mapping[str, Any]) -> None:
        if notice.get("schema") != DELIVERY_NOTICE_SCHEMA:
            raise QwenAdapterError("synthetic transport received a non-notice payload")
        forbidden = {
            "event_id",
            "event_ids",
            "data",
            "payload",
            "raw_output",
            "source_event",
            "queue_records",
        }
        if forbidden.intersection(notice):
            raise QwenAdapterError("delivery notice contains event payload material")

    def deliver_context(self, notice: Mapping[str, Any]) -> None:
        self._assert_sparse_notice(notice)
        self.calls.append({"method": "PostToolUse.context", "notice": dict(notice)})

    def wake_idle(self, session_id: str, notice: Mapping[str, Any]) -> bool:
        self._assert_sparse_notice(notice)
        if not isinstance(session_id, str) or not session_id:
            raise QwenAdapterError("idle wake requires a non-empty session id")
        self.calls.append(
            {"method": "resume", "session_id": session_id, "notice": dict(notice)}
        )
        self.resume_invocations.append(
            {
                "session_id": session_id,
                "argv": ["qwen", "exec", "--resume", session_id],
            }
        )
        return True

    def finalization_continue(self, notice: Mapping[str, Any]) -> bool:
        self._assert_sparse_notice(notice)
        self.calls.append({"method": "Stop.continue", "continue": True})
        return True


class QwenAdapter(HostAdapter):
    """The implemented Qwen host profile."""

    def __init__(
        self,
        transport: QwenTransport,
        coordinator: DeliveryCoordinator,
        *,
        session_id: str | None = None,
    ) -> None:
        if not isinstance(transport, QwenTransport):
            raise QwenAdapterError("QwenAdapter requires a QwenTransport")
        if coordinator.adapter is not self:
            coordinator.adapter = self
        self.transport = transport
        self.coordinator = coordinator
        self.session_id = session_id
        self._profile = HostProfile(
            kind="qwen",
            version=QWEN_ADAPTER_VERSION,
            capabilities=qwen_capabilities(),
            implemented=True,
        )

    @property
    def profile(self) -> HostProfile:
        return self._profile

    def deliver_notice(
        self, notice: DeliveryNotice, *, boundary: str
    ) -> DeliveryReceipt:
        if notice.adapter_profile != self.profile.profile_id:
            raise QwenAdapterError("notice was created for another adapter profile")
        if boundary in {"post_tool_use", "tool_result"}:
            self.transport.deliver_context(notice.as_record())
        elif boundary in {"turn_completed", "idle"}:
            sid = self.session_id or notice.registration_id
            self.transport.wake_idle(sid, notice.as_record())
        elif boundary == "finalization":
            self.transport.finalization_continue(notice.as_record())
        else:
            raise QwenAdapterError(f"unsupported Qwen safe boundary: {boundary}")
        return DeliveryReceipt(
            receipt_id="qwen-receipt-" + uuid.uuid4().hex,
            notice_id=notice.notice_id,
            run_id=notice.run_id,
            queue_id=notice.queue_id,
            manager_session_id=notice.manager_session_id,
            manager_thread_id=notice.manager_thread_id,
            registration_id=notice.registration_id,
            registration_generation=notice.registration_generation,
            observed_queue_revision=notice.observed_queue_revision,
            boundary=boundary,
            outcome="DELIVERED",
            delivered_utc=iso_utc(utc_now()) or notice.observed_utc,
            adapter_profile=self.profile.profile_id,
        )

    def post_tool_use(self, *, task_label: str = "tool") -> DeliveryReceipt | None:
        if self.coordinator.task_active:
            self.coordinator.complete_bounded_task(task_label)
        notice = self.coordinator.notice_for_wake()
        return self.coordinator.deliver_at_boundary(notice, boundary="post_tool_use")

    on_post_tool_use = post_tool_use
    tool_result_boundary = post_tool_use

    def idle_wake(self, *, task_label: str = "idle") -> DeliveryReceipt | None:
        if self.coordinator.task_active:
            self.coordinator.complete_bounded_task(task_label)
        notice = self.coordinator.notice_for_wake()
        return self.coordinator.deliver_at_boundary(notice, boundary="idle")

    on_idle_wake = idle_wake
    turn_completed_boundary = idle_wake

    def stop_boundary(self) -> bool:
        if self.coordinator.task_active:
            return False
        notice = self.coordinator.notice_for_wake()
        if notice is None:
            return False
        receipt = self.coordinator.deliver_at_boundary(notice, boundary="finalization")
        if receipt is None or receipt.outcome != "DELIVERED":
            return False
        return self.transport.finalization_continue(notice.as_record())

    on_stop = stop_boundary
    finalization_gate = stop_boundary

    def synthetic_self_test(self) -> dict[str, Any]:
        return run_synthetic_wake_self_test(self.coordinator)


def qwen_profile() -> HostProfile:
    return HostProfile(
        kind="qwen",
        version=QWEN_ADAPTER_VERSION,
        capabilities=qwen_capabilities(),
        implemented=True,
    )


def create_qwen_adapter(
    router: ManagerEventRouter,
    *,
    transport: QwenTransport | None = None,
    state_root: str | Path | None = None,
    registration_generation: int | None = None,
    session_id: str | None = None,
) -> QwenAdapter:
    placeholder = FutureHostFixture("qwen-bootstrap")
    coordinator = DeliveryCoordinator(
        router=router,
        adapter=placeholder,
        state_root=Path(state_root) if state_root is not None else None,
        registration_generation=registration_generation,
    )
    adapter = QwenAdapter(
        transport or SyntheticQwenTransport(), coordinator, session_id=session_id
    )
    return adapter


def run_synthetic_wake_self_test(coordinator: DeliveryCoordinator) -> dict[str, Any]:
    """Exercise one sparse queue delivery without acknowledging queue work."""

    if not isinstance(coordinator.adapter, QwenAdapter):
        raise QwenAdapterError("wake self-test requires the implemented Qwen adapter")
    coordinator.register()
    binding = coordinator.binding
    event_id = "qwen-synthetic-wake-" + uuid.uuid4().hex
    event = {
        "event_id": event_id,
        "type": "MANAGER_SIGNAL",
        "identity": "synthetic:qwen:wake",
        "data": {
            "signal_id": event_id,
            "lane_id": "synthetic:qwen",
            "manager_actionable": True,
            "severity": "warning",
        },
        "binding": binding,
    }
    admitted = coordinator.router.admit(event, priority=2, binding=binding)
    if admitted is None:
        raise QwenAdapterError("synthetic wake event was not admitted")
    notice = coordinator.notice_for_wake()
    if notice is None:
        raise QwenAdapterError("synthetic queue revision did not produce a notice")
    notice_record = notice.as_record()
    if any(key in notice_record for key in ("event_id", "event_ids", "payload", "data")):
        raise QwenAdapterError("synthetic notice contains event payload")
    receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
    if receipt is None or receipt.outcome != "DELIVERED":
        raise QwenAdapterError("synthetic Qwen delivery did not complete")
    pending_after = coordinator.router.pending_events()
    if event_id not in {item.get("event_id") for item in pending_after}:
        raise QwenAdapterError("synthetic transport receipt acknowledged queue work")
    return {
        "schema": "orchestrator-qwen-synthetic-wake/v1",
        "adapter_profile": coordinator.adapter.profile.as_record(),
        "notice": notice_record,
        "receipt": receipt.as_record(),
        "pending_event_ids_for_test": [event_id],
        "pending_after_delivery": len(pending_after),
        "acknowledged_by_delivery": False,
        "transport_calls": list(getattr(coordinator.adapter.transport, "calls", [])),
    }


__all__ = [
    "QWEN_ADAPTER_SCHEMA",
    "QWEN_ADAPTER_VERSION",
    "QwenAdapter",
    "QwenAdapterError",
    "QwenTransport",
    "SyntheticQwenTransport",
    "create_qwen_adapter",
    "qwen_capabilities",
    "qwen_profile",
    "run_synthetic_wake_self_test",
]
