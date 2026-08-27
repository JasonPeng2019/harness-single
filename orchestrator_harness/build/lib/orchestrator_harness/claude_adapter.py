"""Implemented Claude host profile and safe-boundary delivery fixtures.

Claude Code exposes no in-session item-injection API, so the adapter
delivers sparse, payload-free context at active-turn boundaries and wakes
an idle session cross-process via ``claude --resume`` (resume-as-wake).
The persistent coordinator owns the wake subscription and replay; the
transport never acknowledges queue work.  This module mirrors the Codex
adapter core only; the installer is a separate later task.
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


CLAUDE_ADAPTER_SCHEMA = "orchestrator-claude-adapter/v1"
CLAUDE_ADAPTER_VERSION = "claude-v1"


class ClaudeAdapterError(ValueError):
    """A Claude boundary or delivery transaction failed closed."""


def claude_capabilities() -> AdapterCapabilities:
    """The safe lifecycle operations the Claude host honestly exposes.

    Claude Code has no in-session item-injection API, so
    ``next_input_injection`` is False.  Idle wake is performed cross-process
    via ``claude --resume``, so ``idle_wake`` is True.
    """
    return AdapterCapabilities(
        active_turn_notice=True,
        idle_wake=True,
        next_input_injection=False,
        finalization_gate=True,
    )


class ClaudeTransport(ABC):
    """Small synthetic-friendly surface for documented Claude boundaries."""

    @abstractmethod
    def deliver_context(self, notice: Mapping[str, Any]) -> None:
        # Active-turn boundary (PostToolUse context replacement).
        raise NotImplementedError

    @abstractmethod
    def wake_idle(self, session_id: str, notice: Mapping[str, Any]) -> bool:
        # Resume-as-wake for a completed/idle session.
        raise NotImplementedError

    @abstractmethod
    def finalization_continue(self, notice: Mapping[str, Any]) -> bool:
        # Stop boundary backstop: request continuation to drain queue work.
        raise NotImplementedError


class SyntheticClaudeTransport(ClaudeTransport):
    """In-memory, deterministic, resume-as-wake transport used by tests only."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.resume_invocations: list[dict[str, Any]] = []

    @staticmethod
    def _assert_sparse_notice(notice: Mapping[str, Any]) -> None:
        if notice.get("schema") != DELIVERY_NOTICE_SCHEMA:
            raise ClaudeAdapterError("synthetic transport received a non-notice payload")
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
            raise ClaudeAdapterError("delivery notice contains event payload material")

    def deliver_context(self, notice: Mapping[str, Any]) -> None:
        self._assert_sparse_notice(notice)
        self.calls.append({"method": "PostToolUse.context", "notice": dict(notice)})

    def wake_idle(self, session_id: str, notice: Mapping[str, Any]) -> bool:
        self._assert_sparse_notice(notice)
        if not isinstance(session_id, str) or not session_id:
            raise ClaudeAdapterError("idle wake requires a non-empty session id")
        self.calls.append(
            {"method": "resume", "session_id": session_id, "notice": dict(notice)}
        )
        self.resume_invocations.append(
            {"session_id": session_id, "argv": ["claude", "--resume", session_id]}
        )
        return True

    def finalization_continue(self, notice: Mapping[str, Any]) -> bool:
        self._assert_sparse_notice(notice)
        self.calls.append({"method": "Stop.continue", "continue": True})
        return True


class ClaudeAdapter(HostAdapter):
    """The implemented Claude host profile."""

    def __init__(
        self,
        transport: ClaudeTransport,
        coordinator: DeliveryCoordinator,
        *,
        session_id: str | None = None,
    ) -> None:
        if not isinstance(transport, ClaudeTransport):
            raise ClaudeAdapterError("ClaudeAdapter requires a ClaudeTransport")
        if coordinator.adapter is not self:
            # The coordinator may be constructed before the adapter.  The
            # identity check is intentionally relaxed for that construction
            # order; binding and profile checks still happen at delivery time.
            coordinator.adapter = self
        self.transport = transport
        self.coordinator = coordinator
        self.session_id = session_id
        self._profile = HostProfile(
            kind="claude",
            version=CLAUDE_ADAPTER_VERSION,
            capabilities=claude_capabilities(),
            implemented=True,
        )

    @property
    def profile(self) -> HostProfile:
        return self._profile

    def deliver_notice(
        self, notice: DeliveryNotice, *, boundary: str
    ) -> DeliveryReceipt:
        if notice.adapter_profile != self.profile.profile_id:
            raise ClaudeAdapterError("notice was created for another adapter profile")
        if boundary in {"post_tool_use", "tool_result"}:
            self.transport.deliver_context(notice.as_record())
        elif boundary in {"turn_completed", "idle"}:
            sid = self.session_id or notice.registration_id
            self.transport.wake_idle(sid, notice.as_record())
        elif boundary == "finalization":
            self.transport.finalization_continue(notice.as_record())
        else:
            raise ClaudeAdapterError(f"unsupported Claude safe boundary: {boundary}")
        return DeliveryReceipt(
            receipt_id="claude-receipt-" + uuid.uuid4().hex,
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
        """Deliver one bounded next-request notice only at an active-turn boundary."""
        if self.coordinator.task_active:
            self.coordinator.complete_bounded_task(task_label)
        notice = self.coordinator.notice_for_wake()
        return self.coordinator.deliver_at_boundary(notice, boundary="post_tool_use")

    on_post_tool_use = post_tool_use
    tool_result_boundary = post_tool_use

    def idle_wake(self, *, task_label: str = "idle") -> DeliveryReceipt | None:
        """Wake an idle session cross-process via ``claude --resume``."""
        if self.coordinator.task_active:
            self.coordinator.complete_bounded_task(task_label)
        notice = self.coordinator.notice_for_wake()
        return self.coordinator.deliver_at_boundary(notice, boundary="turn_completed")

    on_idle_wake = idle_wake
    turn_completed_boundary = idle_wake

    def stop_boundary(self) -> bool:
        """Use the Stop boundary only as a finalization continuation backstop."""
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


def claude_profile() -> HostProfile:
    return HostProfile(
        kind="claude",
        version=CLAUDE_ADAPTER_VERSION,
        capabilities=claude_capabilities(),
        implemented=True,
    )


def create_claude_adapter(
    router: ManagerEventRouter,
    *,
    transport: ClaudeTransport | None = None,
    state_root: str | Path | None = None,
    registration_generation: int | None = None,
    session_id: str | None = None,
) -> ClaudeAdapter:
    """Compose the implemented Claude profile with one exact binding."""

    placeholder = FutureHostFixture("claude-bootstrap")
    coordinator = DeliveryCoordinator(
        router=router,
        adapter=placeholder,
        state_root=Path(state_root) if state_root is not None else None,
        registration_generation=registration_generation,
    )
    adapter = ClaudeAdapter(
        transport or SyntheticClaudeTransport(), coordinator, session_id=session_id
    )
    return adapter


def run_synthetic_wake_self_test(coordinator: DeliveryCoordinator) -> dict[str, Any]:
    """Exercise one queue revision using only a synthetic Claude transport."""
    if not isinstance(coordinator.adapter, ClaudeAdapter):
        raise ClaudeAdapterError(
            "wake self-test requires the implemented Claude adapter"
        )
    coordinator.register()
    binding = coordinator.binding
    event_id = "s4-synthetic-wake-" + uuid.uuid4().hex
    event = {
        "event_id": event_id,
        "type": "MANAGER_SIGNAL",
        "identity": "synthetic:s4:claude-wake",
        "data": {
            "signal_id": event_id,
            "lane_id": "synthetic:s4",
            "manager_actionable": True,
            "severity": "warning",
        },
        "binding": binding,
    }
    admitted = coordinator.router.admit(event, priority=2, binding=binding)
    if admitted is None:
        raise ClaudeAdapterError("synthetic wake event was not admitted")
    notice = coordinator.notice_for_wake()
    if notice is None:
        raise ClaudeAdapterError("synthetic queue revision did not produce a notice")
    notice_record = notice.as_record()
    if any(
        key in notice_record for key in ("event_id", "event_ids", "payload", "data")
    ):
        raise ClaudeAdapterError("synthetic notice contains event payload")
    receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
    if receipt is None or receipt.outcome != "DELIVERED":
        raise ClaudeAdapterError("synthetic Claude delivery did not complete")
    pending_after = coordinator.router.pending_events()
    if event_id not in {item.get("event_id") for item in pending_after}:
        raise ClaudeAdapterError("synthetic transport receipt acknowledged queue work")
    return {
        "schema": "orchestrator-claude-synthetic-wake/v1",
        "adapter_profile": coordinator.adapter.profile.as_record(),
        "notice": notice_record,
        "receipt": receipt.as_record(),
        "pending_event_ids_for_test": [event_id],
        "pending_after_delivery": len(pending_after),
        "acknowledged_by_delivery": False,
        "transport_calls": list(getattr(coordinator.adapter.transport, "calls", [])),
    }


__all__ = [
    "CLAUDE_ADAPTER_SCHEMA",
    "CLAUDE_ADAPTER_VERSION",
    "ClaudeAdapter",
    "ClaudeAdapterError",
    "ClaudeTransport",
    "SyntheticClaudeTransport",
    "claude_capabilities",
    "claude_profile",
    "create_claude_adapter",
    "run_synthetic_wake_self_test",
]
