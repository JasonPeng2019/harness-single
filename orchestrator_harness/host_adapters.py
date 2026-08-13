"""Capability-selected host delivery and the harness-owned delivery coordinator.

The coordinator is deliberately provider-neutral.  It consumes the canonical S3
``ManagerEventRouter`` queue and wake edge and exposes a small, payload-free notice
to one host adapter.  Host adapters decide how a notice reaches a safe lifecycle
boundary; they never acknowledge manager events.  This keeps delivery evidence and
manager action separate and makes replay safe after either side restarts.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .models import iso_utc, parse_utc, utc_now
from .notifications import ManagerBindingError, ManagerEventRouter
from .stable_io import PreparedOutputTransaction, PathSafetyError, canonical_json


HOST_ADAPTER_SCHEMA = "orchestrator-host-adapter/v1"
DELIVERY_NOTICE_SCHEMA = "orchestrator-delivery-notice/v1"
DELIVERY_RECEIPT_SCHEMA = "orchestrator-delivery-receipt/v1"
MANAGER_EVENT_ACK_SCHEMA = "orchestrator-manager-event-ack/v1"
DELIVERY_COORDINATOR_SCHEMA = "orchestrator-delivery-coordinator/v1"

# Notification stop-policy vocabulary.  The active queue authority is the S3
# ManagerEventRouter; this coordinator keeps only the typed notification
# decoration (OPEN vs EXTERNALLY_BLOCKED and the exact external actor/action)
# in its own per-binding durable state.  CLOSED is never a retained state:
# handled items are mechanically acknowledged and removed from the router.
NOTIFICATION_ITEM_SCHEMA = "orchestrator-notification-item/v1"
NOTIFICATION_OPEN = "OPEN"
NOTIFICATION_EXTERNALLY_BLOCKED = "EXTERNALLY_BLOCKED"
NOTIFICATION_STOP_OPEN_ITEMS_REMAIN = "OPEN_ITEMS_REMAIN"
NOTIFICATION_STOP_QUEUE_EMPTY = "QUEUE_EMPTY"
NOTIFICATION_STOP_EXTERNAL_RESPONSE_REQUIRED = "EXTERNALLY_BLOCKED_FINAL_RESPONSE_REQUIRED"
NOTIFICATION_STOP_EXTERNAL_DECLARED = "EXTERNALLY_BLOCKED_DECLARED"

_CAPABILITY_NAMES = (
    "active_turn_notice",
    "idle_wake",
    "next_input_injection",
    "finalization_gate",
)
_RECEIPT_OUTCOMES = {
    "DELIVERED",
    "DELIVERY_DEFERRED",
    "DELIVERY_DEGRADED",
    "DELIVERY_REJECTED",
    "DELIVERY_FAILED",
}
_SAFE_BOUNDARIES = {
    "post_tool_use",
    "tool_result",
    "turn_completed",
    "idle",
    "finalization",
}
_RETRY_BACKOFF_SECONDS = (0.0, 0.25, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0)


class HostAdapterError(ValueError):
    """The adapter contract or a safe-boundary operation is invalid."""


class UnsupportedHostAdapterError(HostAdapterError):
    """A fixture describes a future host but does not implement it."""


class DeliveryBindingError(HostAdapterError):
    """A notice, wake, or persisted coordinator belongs to another binding."""


class DeliveryRetryExhausted(HostAdapterError):
    """The bounded profile retry budget has been consumed; queue work remains."""


def _text(value: object, field_name: str, *, limit: int = 512) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise HostAdapterError(f"{field_name} must be a non-empty bounded string")
    return value.strip()


def _nonnegative_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise HostAdapterError(f"{field_name} must be a non-negative integer")
    return value


def _timestamp(value: datetime | None = None) -> str:
    result = iso_utc(value or utc_now())
    if result is None:
        raise HostAdapterError("timestamp could not be established")
    return result


@dataclass(frozen=True)
class AdapterCapabilities:
    """The safe lifecycle operations a host honestly exposes."""

    active_turn_notice: bool
    idle_wake: bool
    next_input_injection: bool
    finalization_gate: bool

    def __post_init__(self) -> None:
        for name in _CAPABILITY_NAMES:
            if not isinstance(getattr(self, name), bool):
                raise HostAdapterError(f"adapter capability {name} must be boolean")

    def as_record(self) -> dict[str, bool]:
        return {name: bool(getattr(self, name)) for name in _CAPABILITY_NAMES}

    @classmethod
    def codex(cls) -> "AdapterCapabilities":
        return cls(
            active_turn_notice=True,
            idle_wake=True,
            next_input_injection=True,
            finalization_gate=True,
        )

    @classmethod
    def future_fixture(cls) -> "AdapterCapabilities":
        return cls(
            active_turn_notice=False,
            idle_wake=False,
            next_input_injection=False,
            finalization_gate=False,
        )


@dataclass(frozen=True)
class HostProfile:
    kind: str
    version: str
    capabilities: AdapterCapabilities
    implemented: bool

    def __post_init__(self) -> None:
        _text(self.kind, "adapter kind", limit=64)
        _text(self.version, "adapter version", limit=128)
        if not isinstance(self.implemented, bool):
            raise HostAdapterError("adapter implementation state must be boolean")

    @property
    def profile_id(self) -> str:
        return f"{self.kind}/{self.version}"

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": HOST_ADAPTER_SCHEMA,
            "kind": self.kind,
            "version": self.version,
            "profile_id": self.profile_id,
            "capabilities": self.capabilities.as_record(),
            "implemented": self.implemented,
        }


@dataclass(frozen=True)
class DeliveryNotice:
    """A bounded wake edge; it intentionally contains no event payload or ID."""

    notice_id: str
    run_id: str
    queue_id: str
    manager_session_id: str
    manager_thread_id: str
    registration_id: str
    registration_generation: int
    observed_queue_revision: int
    pending_count: int
    highest_class: str
    highest_severity: str
    observed_utc: str
    adapter_profile: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.notice_id, "notice_id"),
            (self.run_id, "run_id"),
            (self.queue_id, "queue_id"),
            (self.manager_session_id, "manager_session_id"),
            (self.manager_thread_id, "manager_thread_id"),
            (self.registration_id, "registration_id"),
            (self.highest_class, "highest_class"),
            (self.highest_severity, "highest_severity"),
            (self.observed_utc, "observed_utc"),
            (self.adapter_profile, "adapter_profile"),
        ):
            _text(value, name)
        _nonnegative_int(self.registration_generation, "registration_generation")
        _nonnegative_int(self.observed_queue_revision, "observed_queue_revision")
        _nonnegative_int(self.pending_count, "pending_count")
        if parse_utc(self.observed_utc) is None:
            raise HostAdapterError("notice observed_utc must be UTC")
        if self.pending_count == 0:
            raise HostAdapterError("a delivery notice requires pending work")

    @property
    def binding(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "queue_id": self.queue_id,
            "manager_session_id": self.manager_session_id,
            "manager_thread_id": self.manager_thread_id,
            "registration_id": self.registration_id,
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": DELIVERY_NOTICE_SCHEMA,
            "notice_id": self.notice_id,
            **self.binding,
            "registration_generation": self.registration_generation,
            "observed_queue_revision": self.observed_queue_revision,
            "pending_count": self.pending_count,
            "highest_class": self.highest_class,
            "highest_severity": self.highest_severity,
            "observed_utc": self.observed_utc,
            "adapter_profile": self.adapter_profile,
        }

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> "DeliveryNotice":
        if value.get("schema") != DELIVERY_NOTICE_SCHEMA:
            raise HostAdapterError("invalid delivery notice schema")
        allowed = {
            "schema", "notice_id", "run_id", "queue_id", "manager_session_id",
            "manager_thread_id", "registration_id", "registration_generation",
            "observed_queue_revision", "pending_count", "highest_class",
            "highest_severity", "observed_utc", "adapter_profile",
        }
        if set(value) != allowed:
            raise HostAdapterError("delivery notice has an invalid closed shape")
        return cls(
            notice_id=value["notice_id"],
            run_id=value["run_id"],
            queue_id=value["queue_id"],
            manager_session_id=value["manager_session_id"],
            manager_thread_id=value["manager_thread_id"],
            registration_id=value["registration_id"],
            registration_generation=value["registration_generation"],
            observed_queue_revision=value["observed_queue_revision"],
            pending_count=value["pending_count"],
            highest_class=value["highest_class"],
            highest_severity=value["highest_severity"],
            observed_utc=value["observed_utc"],
            adapter_profile=value["adapter_profile"],
        )


@dataclass(frozen=True)
class DeliveryReceipt:
    """Transport evidence only.  A receipt can never acknowledge a queue event."""

    receipt_id: str
    notice_id: str
    run_id: str
    queue_id: str
    manager_session_id: str
    manager_thread_id: str
    registration_id: str
    registration_generation: int
    observed_queue_revision: int
    boundary: str
    outcome: str
    delivered_utc: str
    adapter_profile: str
    attempt: int = 1
    error_class: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.receipt_id, "receipt_id"),
            (self.notice_id, "notice_id"),
            (self.run_id, "run_id"),
            (self.queue_id, "queue_id"),
            (self.manager_session_id, "manager_session_id"),
            (self.manager_thread_id, "manager_thread_id"),
            (self.registration_id, "registration_id"),
            (self.boundary, "boundary"),
            (self.outcome, "outcome"),
            (self.delivered_utc, "delivered_utc"),
            (self.adapter_profile, "adapter_profile"),
        ):
            _text(value, name)
        if self.outcome not in _RECEIPT_OUTCOMES:
            raise HostAdapterError(f"unsupported delivery receipt outcome: {self.outcome}")
        if self.boundary not in _SAFE_BOUNDARIES:
            raise HostAdapterError(f"unsupported delivery receipt boundary: {self.boundary}")
        _nonnegative_int(self.registration_generation, "registration_generation")
        _nonnegative_int(self.observed_queue_revision, "observed_queue_revision")
        if not isinstance(self.attempt, int) or isinstance(self.attempt, bool) or self.attempt < 1:
            raise HostAdapterError("receipt attempt must be a positive integer")
        if self.error_class is not None:
            _text(self.error_class, "error_class", limit=128)
        if parse_utc(self.delivered_utc) is None:
            raise HostAdapterError("receipt delivered_utc must be UTC")

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> "DeliveryReceipt":
        """Parse the closed transport-evidence shape without widening it."""

        if value.get("schema") != DELIVERY_RECEIPT_SCHEMA:
            raise HostAdapterError("invalid delivery receipt schema")
        allowed = {
            "schema", "receipt_id", "notice_id", "run_id", "queue_id",
            "manager_session_id", "manager_thread_id", "registration_id",
            "registration_generation", "observed_queue_revision", "boundary",
            "outcome", "delivered_utc", "adapter_profile", "attempt",
            "error_class",
        }
        if set(value) - allowed or "error_class" in value and value["error_class"] is None:
            raise HostAdapterError("delivery receipt has an invalid closed shape")
        required = allowed - {"schema", "error_class"}
        if set(value) & required != required:
            raise HostAdapterError("delivery receipt is missing a required coordinate")
        return cls(
            receipt_id=value["receipt_id"],
            notice_id=value["notice_id"],
            run_id=value["run_id"],
            queue_id=value["queue_id"],
            manager_session_id=value["manager_session_id"],
            manager_thread_id=value["manager_thread_id"],
            registration_id=value["registration_id"],
            registration_generation=value["registration_generation"],
            observed_queue_revision=value["observed_queue_revision"],
            boundary=value["boundary"],
            outcome=value["outcome"],
            delivered_utc=value["delivered_utc"],
            adapter_profile=value["adapter_profile"],
            attempt=value["attempt"],
            error_class=value.get("error_class"),
        )

    def as_record(self) -> dict[str, Any]:
        record = {
            "schema": DELIVERY_RECEIPT_SCHEMA,
            "receipt_id": self.receipt_id,
            "notice_id": self.notice_id,
            "run_id": self.run_id,
            "queue_id": self.queue_id,
            "manager_session_id": self.manager_session_id,
            "manager_thread_id": self.manager_thread_id,
            "registration_id": self.registration_id,
            "registration_generation": self.registration_generation,
            "observed_queue_revision": self.observed_queue_revision,
            "boundary": self.boundary,
            "outcome": self.outcome,
            "delivered_utc": self.delivered_utc,
            "adapter_profile": self.adapter_profile,
            "attempt": self.attempt,
        }
        if self.error_class is not None:
            record["error_class"] = self.error_class
        return record


@dataclass(frozen=True)
class ManagerEventAck:
    """The only typed object that represents a manager acknowledgement."""

    event_id: str
    action: str = "ACKNOWLEDGED"

    def __post_init__(self) -> None:
        _text(self.event_id, "event_id")
        if self.action != "ACKNOWLEDGED":
            raise HostAdapterError("manager event action must be ACKNOWLEDGED")

    def as_record(self) -> dict[str, str]:
        return {
            "schema": MANAGER_EVENT_ACK_SCHEMA,
            "event_id": self.event_id,
            "action": self.action,
        }


@dataclass(frozen=True)
class NotificationStopDecision:
    """The typed stop decision for one exact binding's active notification queue.

    Rejection paths never disclose notification IDs or payload.  The accepted
    all-EXTERNALLY_BLOCKED path carries only the minimum structured
    final-response declarations the hook itself supplied.
    """

    permitted: bool
    reason: str
    open_count: int = 0
    externally_blocked_count: int = 0
    declarations: tuple[dict[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.permitted, bool):
            raise HostAdapterError("stop decision permitted must be boolean")
        _text(self.reason, "stop decision reason")
        _nonnegative_int(self.open_count, "stop decision open_count")
        _nonnegative_int(self.externally_blocked_count, "stop decision externally_blocked_count")
        if not isinstance(self.declarations, tuple) or not all(
            isinstance(item, dict) and set(item) == {"notification_id", "required_actor", "required_action"}
            for item in self.declarations
        ):
            raise HostAdapterError("stop decision declarations have an invalid closed shape")

    def as_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "permitted": self.permitted,
            "reason": self.reason,
            "open_count": self.open_count,
            "externally_blocked_count": self.externally_blocked_count,
        }
        if self.declarations:
            record["declarations"] = [dict(item) for item in self.declarations]
        return record


class HostAdapter(ABC):
    """Common host contract consumed by ``DeliveryCoordinator``."""

    @property
    @abstractmethod
    def profile(self) -> HostProfile:
        raise NotImplementedError

    @property
    def capabilities(self) -> AdapterCapabilities:
        return self.profile.capabilities

    @abstractmethod
    def deliver_notice(self, notice: DeliveryNotice, *, boundary: str) -> DeliveryReceipt:
        """Deliver one bounded notice at a host-confirmed safe boundary."""

    def finalization_backstop(self, notice: DeliveryNotice | None) -> bool:
        """Return whether a host continuation was requested at finalization."""
        del notice
        return False


class FutureHostFixture(HostAdapter):
    """Contract-only fixture for an unsupported future host profile."""

    def __init__(self, kind: str, *, version: str = "fixture-v1") -> None:
        self._profile = HostProfile(
            kind=kind,
            version=version,
            capabilities=AdapterCapabilities.future_fixture(),
            implemented=False,
        )

    @property
    def profile(self) -> HostProfile:
        return self._profile

    def deliver_notice(self, notice: DeliveryNotice, *, boundary: str) -> DeliveryReceipt:
        del notice, boundary
        raise UnsupportedHostAdapterError(
            f"future host fixture {self.profile.kind!r} has no installed implementation"
        )


def future_host_fixture(kind: str, *, version: str = "fixture-v1") -> FutureHostFixture:
    return FutureHostFixture(kind, version=version)


def _binding_record(router: ManagerEventRouter) -> dict[str, str | None]:
    return router.binding.as_record()


def _binding_digest(value: Mapping[str, Any]) -> str:
    core = {
        key: value.get(key)
        for key in (
            "run_id",
            "queue_id",
            "manager_session_id",
            "manager_thread_id",
            "manager_invocation_id",
        )
    }
    return hashlib.sha256(canonical_json(core).encode("utf-8")).hexdigest()


def _severity_for(record: Mapping[str, Any]) -> tuple[int, str]:
    facts = record.get("facts") if isinstance(record.get("facts"), Mapping) else {}
    value = facts.get("severity")
    if isinstance(value, str) and value.strip():
        rank = {"critical": 0, "error": 1, "warning": 2, "info": 3}.get(value.lower(), 4)
        return rank, value.lower()
    priority = record.get("priority")
    try:
        numeric = float(priority)
    except (TypeError, ValueError):
        numeric = 3.0
    if numeric <= 1:
        return 1, "error"
    if numeric <= 3:
        return 2, "warning"
    return 3, "info"


def _highest_pending(pending: list[dict[str, Any]]) -> tuple[str, str]:
    if not pending:
        raise DeliveryBindingError("cannot summarize an empty manager queue")
    ordered = sorted(
        pending,
        key=lambda item: (
            float(item.get("priority", 3)),
            _severity_for(item)[0],
            int(item.get("admission_seq", 0)),
            str(item.get("event_type", "")),
        ),
    )
    selected = ordered[0]
    return _text(selected.get("event_type"), "highest_class", limit=128), _severity_for(selected)[1]


def _adapter_profile_text(adapter: HostAdapter) -> str:
    return _text(adapter.profile.profile_id, "adapter_profile", limit=192)


@dataclass
class DeliveryCoordinator:
    """Persistent, exact-binding delivery coordination above the S3 queue."""

    router: ManagerEventRouter
    adapter: HostAdapter
    state_root: Path | None = None
    registration_generation: int | None = None
    max_attempts: int = 3
    now: Any = utc_now
    _transaction: PreparedOutputTransaction = field(init=False, repr=False)
    _task_active: bool = field(init=False, default=False, repr=False)
    _markers: list[str] = field(init=False, default_factory=list, repr=False)
    _binding_closed: bool = field(init=False, default=False, repr=False)
    _helper_supervisor: Any = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.adapter, HostAdapter):
            raise HostAdapterError("coordinator requires one HostAdapter instance")
        if self.max_attempts < 1 or self.max_attempts > 8:
            raise HostAdapterError("max_attempts must be between one and eight")
        self.state_root = Path(self.state_root or (self.router.root / "coordinator")).absolute()
        self._transaction = PreparedOutputTransaction(
            self.state_root,
            allowed_roots=(self.state_root.parent,),
        )
        self._transaction.prepare()
        self._transaction.admit(self.state_path)
        if self.registration_generation is not None:
            _nonnegative_int(self.registration_generation, "registration_generation")

    @property
    def state_path(self) -> Path:
        assert self.state_root is not None
        return self.state_root / "DELIVERY_COORDINATOR.json"

    @property
    def binding(self) -> dict[str, Any]:
        return dict(_binding_record(self.router))

    @property
    def binding_identity(self) -> dict[str, Any]:
        state = self.load_state()
        binding = state["binding"]
        return {
            **{key: binding[key] for key in (
                "run_id", "queue_id", "manager_session_id", "manager_thread_id",
                "registration_id", "manager_invocation_id", "binding_digest",
            )},
            "registration_generation": state["registration_generation"],
        }

    def _timestamp(self) -> str:
        value = self.now() if callable(self.now) else self.now
        if not isinstance(value, datetime):
            value = utc_now()
        return _timestamp(value)

    def _default_generation(self, existing: Mapping[str, Any] | None) -> int:
        if self.registration_generation is not None:
            return self.registration_generation
        if isinstance(existing, Mapping):
            prior = existing.get("registration_generation")
            if isinstance(prior, int) and not isinstance(prior, bool) and prior >= 0:
                return prior
        return 1

    def _new_state(self, *, generation: int, status: str = "REGISTERED") -> dict[str, Any]:
        return {
            "schema": DELIVERY_COORDINATOR_SCHEMA,
            "binding": self.binding,
            "binding_digest": _binding_digest(self.binding),
            "registration_generation": generation,
            "adapter": self.adapter.profile.as_record(),
            "subscription": self._subscription_record(generation),
            "retry_profile": {
                "max_attempts": self.max_attempts,
                "backoff_seconds": list(_RETRY_BACKOFF_SECONDS[: self.max_attempts]),
                "mode": "bounded-coordinator-profile",
            },
            "status": status,
            "registered_utc": self._timestamp(),
            "last_seen_wake_revision": 0,
            "outstanding_notice": None,
            "attempt_count": 0,
            "retry_backoff_seconds": 0.0,
            "next_retry_utc": None,
            "last_receipt": None,
            "markers": [],
            "notification_policy": {"items": {}},
        }

    def _subscription_record(self, generation: int) -> dict[str, Any]:
        return {
            "owner": "harness-delivery-coordinator",
            "mode": "in_process_binding_subscription",
            "wake_path": str(self.router.wake_path),
            "binding": self.binding,
            "registration_generation": generation,
            "status": "ACTIVE",
        }

    def _write_state(self, state: Mapping[str, Any]) -> None:
        self._transaction.revalidate(self.state_path)
        self._transaction.atomic_json(self.state_path, dict(state))

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._new_state(generation=self._default_generation(None), status="UNREGISTERED")
        self._transaction.revalidate(self.state_path)
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HostAdapterError(f"cannot read coordinator state: {exc}") from exc
        if not isinstance(value, dict) or value.get("schema") != DELIVERY_COORDINATOR_SCHEMA:
            raise HostAdapterError("invalid delivery coordinator state")
        binding = value.get("binding")
        if not isinstance(binding, Mapping) or _binding_digest(binding) != value.get("binding_digest"):
            raise HostAdapterError("coordinator binding digest is invalid")
        return dict(value)

    def _assert_state_binding(self, state: Mapping[str, Any]) -> None:
        expected = self.binding
        actual = state.get("binding")
        if not isinstance(actual, Mapping):
            raise DeliveryBindingError("coordinator has no binding")
        for key in (
            "run_id", "queue_id", "manager_session_id", "manager_thread_id",
            "registration_id", "binding_digest",
        ):
            expected_value = expected.get(key) if key != "binding_digest" else _binding_digest(expected)
            if actual.get(key) != expected_value:
                raise DeliveryBindingError(f"coordinator binding mismatch in {key}")
        generation = state.get("registration_generation")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
            raise DeliveryBindingError("coordinator registration generation is invalid")
        if state.get("adapter") != self.adapter.profile.as_record():
            raise DeliveryBindingError("coordinator adapter profile mismatch")

    def register(self) -> dict[str, Any]:
        registration = self.router.registration
        self.router.validate_binding(registration)
        prior = self.load_state() if self.state_path.exists() else None
        if prior is not None:
            self._assert_state_binding(prior)
        generation = self._default_generation(prior)
        if prior is not None:
            prior_generation = prior.get("registration_generation")
            if prior_generation != generation and prior.get("status") != "RELEASED":
                raise DeliveryBindingError("registration generation changed while coordinator was active")
        state = dict(prior or self._new_state(generation=generation))
        state["status"] = "REGISTERED"
        state["registration_generation"] = generation
        state["adapter"] = self.adapter.profile.as_record()
        state["subscription"] = self._subscription_record(generation)
        state["retry_profile"] = {
            "max_attempts": self.max_attempts,
            "backoff_seconds": list(_RETRY_BACKOFF_SECONDS[: self.max_attempts]),
            "mode": "bounded-coordinator-profile",
        }
        state["restored_utc"] = self._timestamp() if prior is not None else None
        self._write_state(state)
        return state

    restore = register
    subscribe = register

    def _state_or_register(self) -> dict[str, Any]:
        state = self.load_state()
        self._assert_state_binding(state)
        if state.get("status") == "UNREGISTERED":
            return self.register()
        if state.get("status") == "RELEASED":
            # A closed binding may be restored by the harness-owned boundary
            # when a new durable S3 event arrives.  No manager poll or manual
            # re-arm is involved; the queue itself is the wake authority.
            if self.router.pending_events():
                return self.register()
            return state
        return state

    def _validate_wake(self, wake: Mapping[str, Any] | None) -> int:
        state = self._state_or_register()
        if wake is None:
            revision = self.router.wake_revision
        else:
            expected = self.binding
            for key in (
                "run_id", "queue_id", "manager_session_id", "manager_thread_id",
                "registration_id", "binding_digest",
            ):
                if wake.get(key) != (expected.get(key) if key != "binding_digest" else _binding_digest(expected)):
                    raise DeliveryBindingError(f"wake binding mismatch in {key}")
            revision = wake.get("wake_revision")
            if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
                raise DeliveryBindingError("wake revision is invalid")
            if revision > self.router.wake_revision:
                raise DeliveryBindingError("wake revision was not published by this queue")
            wake_generation = wake.get("registration_generation")
            if wake_generation is not None and wake_generation != state.get("registration_generation"):
                raise DeliveryBindingError("wake registration generation is stale")
        return revision

    def pending_events(self) -> list[dict[str, Any]]:
        self._state_or_register()
        return self.router.pending_events()

    def notice_for_wake(self, wake: Mapping[str, Any] | None = None) -> DeliveryNotice | None:
        state = self._state_or_register()
        revision = self._validate_wake(wake)
        pending = self.router.pending_events()
        state_revision = state.get("last_seen_wake_revision", 0)
        if not isinstance(state_revision, int) or state_revision < 0:
            raise HostAdapterError("coordinator last wake revision is invalid")
        if revision < state_revision:
            raise DeliveryBindingError("stale wake revision cannot replay across coordinator state")
        if not pending:
            state["last_seen_wake_revision"] = revision
            state["outstanding_notice"] = None
            state["status"] = "QUIESCENT"
            self._write_state(state)
            return None
        highest_class, highest_severity = _highest_pending(pending)
        binding = self.binding
        generation = int(state["registration_generation"])
        existing = state.get("outstanding_notice")
        if isinstance(existing, Mapping):
            try:
                prior_notice = DeliveryNotice.from_record(existing)
            except HostAdapterError:
                prior_notice = None
            if prior_notice is not None:
                notice_id = prior_notice.notice_id
            else:
                notice_id = "notice-" + uuid.uuid4().hex
        else:
            notice_id = "notice-" + hashlib.sha256(
                canonical_json({**binding, "generation": generation}).encode("utf-8")
            ).hexdigest()[:32]
        notice = DeliveryNotice(
            notice_id=notice_id,
            run_id=_text(binding["run_id"], "run_id"),
            queue_id=_text(binding["queue_id"], "queue_id"),
            manager_session_id=_text(binding["manager_session_id"], "manager_session_id"),
            manager_thread_id=_text(binding["manager_thread_id"], "manager_thread_id"),
            registration_id=_text(binding["registration_id"], "registration_id"),
            registration_generation=generation,
            observed_queue_revision=revision,
            pending_count=len(pending),
            highest_class=highest_class,
            highest_severity=highest_severity,
            observed_utc=self._timestamp(),
            adapter_profile=_adapter_profile_text(self.adapter),
        )
        state["last_seen_wake_revision"] = revision
        state["outstanding_notice"] = notice.as_record()
        state["status"] = "NOTICE_READY"
        self._write_state(state)
        return notice

    on_wake = notice_for_wake
    replay_pending = notice_for_wake

    def begin_bounded_task(self, label: str) -> None:
        _text(label, "task label", limit=200)
        if self._task_active:
            raise HostAdapterError("a bounded task is already active")
        self._task_active = True
        self._markers.append(f"task-started:{label}")

    def complete_bounded_task(self, label: str) -> None:
        _text(label, "task label", limit=200)
        if not self._task_active:
            raise HostAdapterError("no bounded task is active")
        self._task_active = False
        self._markers.append(f"task-completed:{label}")
        state = self._state_or_register()
        state["markers"] = list(self._markers[-64:])
        self._write_state(state)

    @property
    def task_active(self) -> bool:
        return self._task_active

    @property
    def markers(self) -> tuple[str, ...]:
        return tuple(self._markers)

    def _receipt(
        self,
        notice: DeliveryNotice,
        *,
        boundary: str,
        outcome: str,
        attempt: int,
        error_class: str | None = None,
    ) -> DeliveryReceipt:
        return DeliveryReceipt(
            receipt_id="receipt-" + uuid.uuid4().hex,
            notice_id=notice.notice_id,
            run_id=notice.run_id,
            queue_id=notice.queue_id,
            manager_session_id=notice.manager_session_id,
            manager_thread_id=notice.manager_thread_id,
            registration_id=notice.registration_id,
            registration_generation=notice.registration_generation,
            observed_queue_revision=notice.observed_queue_revision,
            boundary=boundary,
            outcome=outcome,
            delivered_utc=self._timestamp(),
            adapter_profile=notice.adapter_profile,
            attempt=attempt,
            error_class=error_class,
        )

    def deliver_at_boundary(
        self,
        notice: DeliveryNotice | None = None,
        *,
        boundary: str = "post_tool_use",
    ) -> DeliveryReceipt | None:
        state = self._state_or_register()
        if notice is None:
            stored = state.get("outstanding_notice")
            notice = DeliveryNotice.from_record(stored) if isinstance(stored, Mapping) else self.notice_for_wake()
        if notice is None:
            return None
        self._assert_notice_binding(notice, state)
        if self._task_active:
            receipt = self._receipt(
                notice,
                boundary=boundary,
                outcome="DELIVERY_DEFERRED",
                attempt=int(state.get("attempt_count", 0)) + 1,
                error_class="ACTIVE_BOUNDED_TASK",
            )
            state["status"] = "WAITING_BOUNDARY"
            state["last_receipt"] = receipt.as_record()
            self._write_state(state)
            return receipt
        attempts = state.get("attempt_count", 0)
        if not isinstance(attempts, int) or attempts < 0:
            attempts = 0
        if attempts >= self.max_attempts:
            receipt = self._receipt(
                notice,
                boundary=boundary,
                outcome="DELIVERY_DEGRADED",
                attempt=attempts,
                error_class="RETRY_BUDGET_EXHAUSTED",
            )
            state["status"] = "DELIVERY_DEGRADED"
            state["last_receipt"] = receipt.as_record()
            self._write_state(state)
            return receipt
        attempt = attempts + 1
        try:
            receipt = self.adapter.deliver_notice(notice, boundary=boundary)
            if not isinstance(receipt, DeliveryReceipt):
                raise HostAdapterError("adapter returned an invalid delivery receipt")
            self._validate_receipt(receipt, notice, state=state, boundary=boundary)
        except Exception as exc:
            receipt = self._receipt(
                notice,
                boundary=boundary,
                outcome="DELIVERY_REJECTED" if isinstance(exc, DeliveryBindingError) else (
                    "DELIVERY_FAILED" if attempt < self.max_attempts else "DELIVERY_DEGRADED"
                ),
                attempt=attempt,
                error_class=("RECEIPT_BINDING_MISMATCH" if isinstance(exc, DeliveryBindingError) else type(exc).__name__),
            )
        # S3's delivery journal is transport evidence.  Passing an empty event
        # list is intentional: no transport receipt can acknowledge queue work.
        try:
            self.router.record_delivery(
                delivery_id=receipt.receipt_id,
                wake_revision=notice.observed_queue_revision,
                event_ids=[],
                outcome=receipt.outcome,
                delivered_at=parse_utc(receipt.delivered_utc),
                binding=self.binding,
            )
        except Exception as exc:
            receipt = self._receipt(
                notice,
                boundary=boundary,
                outcome="DELIVERY_DEGRADED",
                attempt=attempt,
                error_class=type(exc).__name__,
            )
        state["attempt_count"] = attempt
        if receipt.outcome == "DELIVERED":
            state["retry_backoff_seconds"] = 0.0
            state["next_retry_utc"] = None
        else:
            delay_index = min(attempt, len(_RETRY_BACKOFF_SECONDS) - 1)
            delay = _RETRY_BACKOFF_SECONDS[delay_index]
            state["retry_backoff_seconds"] = delay
            parsed = parse_utc(receipt.delivered_utc)
            state["next_retry_utc"] = (
                _timestamp(parsed + timedelta(seconds=delay))
                if parsed is not None and delay > 0
                else None
            )
        state["last_receipt"] = receipt.as_record()
        state["status"] = "DELIVERED" if receipt.outcome == "DELIVERED" else receipt.outcome
        self._write_state(state)
        return receipt

    deliver = deliver_at_boundary

    def _assert_notice_binding(self, notice: DeliveryNotice, state: Mapping[str, Any]) -> None:
        expected = self.binding_identity
        actual = notice.binding
        for key in ("run_id", "queue_id", "manager_session_id", "manager_thread_id", "registration_id"):
            if actual.get(key) != expected.get(key):
                raise DeliveryBindingError(f"notice binding mismatch in {key}")
        if notice.adapter_profile != _adapter_profile_text(self.adapter):
            raise DeliveryBindingError("notice adapter profile mismatch")
        if notice.registration_generation != expected.get("registration_generation") or notice.registration_generation != state.get("registration_generation"):
            raise DeliveryBindingError("notice registration generation mismatch")

    def _validate_receipt(
        self,
        receipt: DeliveryReceipt,
        notice: DeliveryNotice,
        *,
        state: Mapping[str, Any],
        boundary: str,
    ) -> None:
        """Validate every transport coordinate before successful journaling.

        A HostAdapter is an untrusted boundary even when it is implemented by
        this package.  A receipt with only a matching notice ID is not evidence
        for this queue; every binding, revision, boundary, outcome, and
        timestamp must agree before S3 receives a successful delivery record.
        """

        if not isinstance(receipt, DeliveryReceipt):
            raise DeliveryBindingError("adapter receipt is not a DeliveryReceipt")
        expected = self.binding_identity
        coordinates = (
            ("notice_id", receipt.notice_id, notice.notice_id),
            ("run_id", receipt.run_id, notice.run_id),
            ("queue_id", receipt.queue_id, notice.queue_id),
            ("manager_session_id", receipt.manager_session_id, notice.manager_session_id),
            ("manager_thread_id", receipt.manager_thread_id, notice.manager_thread_id),
            ("registration_id", receipt.registration_id, notice.registration_id),
            ("registration_generation", receipt.registration_generation, notice.registration_generation),
            ("observed_queue_revision", receipt.observed_queue_revision, notice.observed_queue_revision),
            ("adapter_profile", receipt.adapter_profile, notice.adapter_profile),
            ("boundary", receipt.boundary, boundary),
        )
        for name, actual, expected_value in coordinates:
            if actual != expected_value:
                raise DeliveryBindingError(f"adapter receipt mismatch in {name}")
        if receipt.registration_generation != expected.get("registration_generation"):
            raise DeliveryBindingError("adapter receipt registration generation is stale")
        if receipt.run_id != expected.get("run_id"):
            raise DeliveryBindingError("adapter receipt run binding is stale")
        if receipt.queue_id != expected.get("queue_id"):
            raise DeliveryBindingError("adapter receipt queue binding is stale")
        if receipt.manager_session_id != expected.get("manager_session_id"):
            raise DeliveryBindingError("adapter receipt manager session is stale")
        if receipt.manager_thread_id != expected.get("manager_thread_id"):
            raise DeliveryBindingError("adapter receipt manager thread is stale")
        if receipt.registration_id != expected.get("registration_id"):
            raise DeliveryBindingError("adapter receipt registration ID is stale")
        if receipt.observed_queue_revision > self.router.wake_revision:
            raise DeliveryBindingError("adapter receipt references an unpublished wake revision")
        if not isinstance(state.get("registration_generation"), int) or receipt.registration_generation != state["registration_generation"]:
            raise DeliveryBindingError("adapter receipt state generation is stale")
        if receipt.outcome != "DELIVERED":
            raise DeliveryBindingError("successful journal requires a delivered receipt")
        if parse_utc(receipt.delivered_utc) is None:
            raise DeliveryBindingError("adapter receipt timestamp is invalid")

    def acknowledge_event(self, event_id: str, *, action: str = "ACKNOWLEDGED") -> ManagerEventAck:
        if isinstance(event_id, DeliveryReceipt):
            raise HostAdapterError("a delivery receipt is not a manager event acknowledgement")
        event_id = _text(event_id, "event_id")
        state = self._state_or_register()
        policy = dict(state.get("notification_policy") or {})
        items_map = dict(policy.get("items") or {})
        entry = items_map.get(event_id)
        if isinstance(entry, Mapping) and entry.get("state") == NOTIFICATION_EXTERNALLY_BLOCKED:
            raise HostAdapterError(
                "EXTERNALLY_BLOCKED notification items cannot be acknowledged by the worker"
            )
        ack = ManagerEventAck(event_id=event_id, action=action)
        self.router.acknowledge(ack.event_id, action=ack.action, binding=self.binding)
        # Prune decoration for items the mechanical acknowledgement removed.
        pending_ids = {item.get("event_id") for item in self.router.pending_events()}
        for key in [key for key in items_map if key not in pending_ids]:
            items_map.pop(key, None)
        policy["items"] = items_map
        state["notification_policy"] = policy
        state["attempt_count"] = 0
        state["retry_backoff_seconds"] = 0.0
        state["next_retry_utc"] = None
        state["outstanding_notice"] = None if not self.router.pending_events() else state.get("outstanding_notice")
        state["status"] = "ACKNOWLEDGED" if not self.router.pending_events() else "NOTICE_READY"
        self._write_state(state)
        return ack

    acknowledge = acknowledge_event
    manager_acknowledge = acknowledge_event

    def admit_notification(
        self,
        *,
        notification_id: str,
        payload_ref: object = None,
        required_actor: str | None = None,
        required_action: str | None = None,
        externally_blocked: bool = False,
        priority: object = 3,
        **facts: Any,
    ) -> dict[str, Any] | None:
        """Admit one typed notification item through the existing manager-actionable transition.

        The item is durably admitted to the one per-binding ManagerEventRouter
        queue (the active-queue authority).  EXTERNALLY_BLOCKED items keep
        their exact required external actor/action as typed decoration in this
        coordinator's per-binding state; they remain active until the external
        actor acts and the manager acknowledges them.
        """
        notification_id = _text(notification_id, "notification_id")
        data: dict[str, Any] = {
            "signal_id": notification_id,
            "lane_id": self.binding.get("manager_thread_id") or "notification",
            "manager_actionable": True,
            "severity": "warning",
        }
        data.update(facts)
        event = {
            "event_id": notification_id,
            "type": "MANAGER_SIGNAL",
            "identity": f"notification:{notification_id}",
            "data": data,
            "binding": self.binding,
        }
        admitted = self.router.admit(event, priority=priority, payload_ref=payload_ref, binding=self.binding)
        if admitted is not None and externally_blocked:
            if required_actor is None or required_action is None:
                raise HostAdapterError(
                    "EXTERNALLY_BLOCKED admission requires required_actor and required_action"
                )
            self.mark_externally_blocked(
                notification_id,
                required_actor=required_actor,
                required_action=required_action,
            )
        return admitted

    def notification_items(self) -> list[dict[str, Any]]:
        """Active notification items derived from the router queue plus decoration.

        Every pending router event is an active item.  OPEN is the default;
        EXTERNALLY_BLOCKED items carry their exact required external
        actor/action.  CLOSED is never retained as an active state.
        """
        state = self._state_or_register()
        policy = state.get("notification_policy")
        items_map = policy.get("items") if isinstance(policy, Mapping) else {}
        items: list[dict[str, Any]] = []
        for record in self.router.pending_events():
            event_id = record.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                continue
            decoration = items_map.get(event_id) if isinstance(items_map, Mapping) else None
            if isinstance(decoration, Mapping) and decoration.get("state") == NOTIFICATION_EXTERNALLY_BLOCKED:
                items.append({
                    "schema": NOTIFICATION_ITEM_SCHEMA,
                    "notification_id": event_id,
                    "state": NOTIFICATION_EXTERNALLY_BLOCKED,
                    "required_actor": decoration.get("required_actor"),
                    "required_action": decoration.get("required_action"),
                    "external_block_reason": decoration.get("reason"),
                })
            else:
                items.append({
                    "schema": NOTIFICATION_ITEM_SCHEMA,
                    "notification_id": event_id,
                    "state": NOTIFICATION_OPEN,
                    "required_actor": None,
                    "required_action": None,
                    "external_block_reason": None,
                })
        return items

    def mark_externally_blocked(
        self,
        event_id: str,
        *,
        required_actor: str,
        required_action: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Durably decorate one active item as EXTERNALLY_BLOCKED."""
        event_id = _text(event_id, "event_id")
        required_actor = _text(required_actor, "required_actor")
        required_action = _text(required_action, "required_action")
        state = self._state_or_register()
        pending_ids = {item.get("event_id") for item in self.router.pending_events()}
        if event_id not in pending_ids:
            raise HostAdapterError("external block references an item that is not active")
        policy = dict(state.get("notification_policy") or {})
        items_map = dict(policy.get("items") or {})
        entry = {
            "state": NOTIFICATION_EXTERNALLY_BLOCKED,
            "required_actor": required_actor,
            "required_action": required_action,
            "reason": reason if isinstance(reason, str) and reason.strip() else None,
            "marked_utc": self._timestamp(),
        }
        items_map[event_id] = entry
        policy["items"] = items_map
        state["notification_policy"] = policy
        self._write_state(state)
        return dict(entry)

    def notification_stop_request(self) -> NotificationStopDecision:
        """Enforce the stop matrix without exposing payload or notification IDs.

        OPEN items reject stop; an empty active queue permits stop; all
        EXTERNALLY_BLOCKED items permit stop only after a final response named
        every ID and its exact required external actor/action.
        """
        items = self.notification_items()
        open_items = [item for item in items if item.get("state") == NOTIFICATION_OPEN]
        blocked = [item for item in items if item.get("state") == NOTIFICATION_EXTERNALLY_BLOCKED]
        if open_items:
            return NotificationStopDecision(
                permitted=False,
                reason=NOTIFICATION_STOP_OPEN_ITEMS_REMAIN,
                open_count=len(open_items),
                externally_blocked_count=len(blocked),
            )
        if not blocked:
            return NotificationStopDecision(
                permitted=True,
                reason=NOTIFICATION_STOP_QUEUE_EMPTY,
                open_count=0,
                externally_blocked_count=0,
            )
        state = self._state_or_register()
        policy = state.get("notification_policy")
        items_map = policy.get("items") if isinstance(policy, Mapping) else {}
        complete = all(
            isinstance(items_map.get(item["notification_id"]), Mapping)
            and items_map[item["notification_id"]].get("final_response_declared") is True
            for item in blocked
        )
        if complete:
            declarations = tuple(
                {
                    "notification_id": item["notification_id"],
                    "required_actor": item["required_actor"],
                    "required_action": item["required_action"],
                }
                for item in blocked
            )
            return NotificationStopDecision(
                permitted=True,
                reason=NOTIFICATION_STOP_EXTERNAL_DECLARED,
                open_count=0,
                externally_blocked_count=len(blocked),
                declarations=declarations,
            )
        return NotificationStopDecision(
            permitted=False,
            reason=NOTIFICATION_STOP_EXTERNAL_RESPONSE_REQUIRED,
            open_count=0,
            externally_blocked_count=len(blocked),
        )

    def record_notification_final_response(
        self, declarations: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]
    ) -> list[dict[str, str]]:
        """Record the exact final response for every active EXTERNALLY_BLOCKED item.

        The declarations must name every active notification ID with its exact
        required external actor/action.  Items remain active and are never
        marked CLOSED.
        """
        if not isinstance(declarations, (list, tuple)) or not all(
            isinstance(item, Mapping) for item in declarations
        ):
            raise HostAdapterError("final response must be a list of declarations")
        items = self.notification_items()
        blocked = [item for item in items if item.get("state") == NOTIFICATION_EXTERNALLY_BLOCKED]
        expected = {
            (item["notification_id"], item["required_actor"], item["required_action"])
            for item in blocked
        }
        provided: set[tuple[str, str, str]] = set()
        for declaration in declarations:
            notification_id = declaration.get("notification_id")
            required_actor = declaration.get("required_actor")
            required_action = declaration.get("required_action")
            if (
                not isinstance(notification_id, str) or not notification_id.strip()
                or not isinstance(required_actor, str) or not required_actor.strip()
                or not isinstance(required_action, str) or not required_action.strip()
            ):
                raise HostAdapterError("final response declaration is incomplete")
            provided.add((notification_id.strip(), required_actor.strip(), required_action.strip()))
        if provided != expected:
            raise HostAdapterError(
                "final response must name every active EXTERNALLY_BLOCKED notification ID "
                "with its exact required actor/action"
            )
        state = self._state_or_register()
        policy = dict(state.get("notification_policy") or {})
        items_map = dict(policy.get("items") or {})
        for notification_id, required_actor, required_action in provided:
            entry = dict(items_map.get(notification_id) or {})
            entry["final_response_declared"] = True
            entry["declared_utc"] = self._timestamp()
            items_map[notification_id] = entry
        policy["items"] = items_map
        state["notification_policy"] = policy
        self._write_state(state)
        return [
            {
                "notification_id": notification_id,
                "required_actor": required_actor,
                "required_action": required_action,
            }
            for notification_id, required_actor, required_action in sorted(provided)
        ]

    def close_binding(self) -> bool:
        self._binding_closed = True
        state = self._state_or_register()
        pending = self.router.pending_events()
        helper = state.get("helper")
        helper_reaped = helper is None or (
            isinstance(helper, Mapping) and helper.get("state") == "REAPED"
        )
        if pending or not self._binding_closed or not helper_reaped:
            return False
        state["status"] = "RELEASED"
        state["released_utc"] = self._timestamp()
        state["outstanding_notice"] = None
        self._write_state(state)
        return True

    release_if_quiescent = close_binding

    def finalization_state(self) -> dict[str, Any]:
        state = self._state_or_register()
        return {
            "schema": DELIVERY_COORDINATOR_SCHEMA,
            "binding": self.binding_identity,
            "pending_count": len(self.router.pending_events()),
            "registration_restored": state.get("status") != "UNREGISTERED",
            "status": state.get("status"),
            "task_active": self._task_active,
            "markers": list(self._markers),
        }

    def own_helper(
        self,
        process: Any,
        identity: Any,
        *,
        graceful_timeout_seconds: float = 5.0,
        observer: Any = None,
        parent_pid: int | None = None,
    ) -> dict[str, Any]:
        """Bind an optional already-launched helper to the exact supervisor.

        The coordinator never starts a helper.  Reaping delegates to the
        existing identity-checking process supervisor and binding release is
        not considered safe until its final reap proof succeeds.
        """

        from .process_supervisor import ProcessSupervisor

        if self._helper_supervisor is not None:
            raise HostAdapterError("a coordinator helper is already owned")
        supervisor_kwargs: dict[str, Any] = {
            "graceful_timeout_seconds": graceful_timeout_seconds,
        }
        if observer is not None:
            supervisor_kwargs["observer"] = observer
        if parent_pid is not None:
            supervisor_kwargs["parent_pid"] = parent_pid
        self._helper_supervisor = ProcessSupervisor(process, identity, **supervisor_kwargs)
        state = self._state_or_register()
        state["helper"] = {
            "state": "OWNED",
            "pid": self._helper_supervisor.pid,
            "created_utc": self._helper_supervisor.creation_identity,
        }
        self._write_state(state)
        return dict(state["helper"])

    def reap_helper(self) -> dict[str, Any] | None:
        if self._helper_supervisor is None:
            return None
        result = self._helper_supervisor.cleanup()
        state = self._state_or_register()
        state["helper"] = {
            "state": "REAPED" if result.proved_reap else "REAP_UNPROVEN",
            **result.to_record(),
        }
        self._write_state(state)
        return dict(state["helper"])


__all__ = [
    "AdapterCapabilities",
    "DELIVERY_COORDINATOR_SCHEMA",
    "DELIVERY_NOTICE_SCHEMA",
    "DELIVERY_RECEIPT_SCHEMA",
    "DeliveryBindingError",
    "DeliveryCoordinator",
    "DeliveryNotice",
    "DeliveryReceipt",
    "DeliveryRetryExhausted",
    "FutureHostFixture",
    "HostAdapter",
    "HostAdapterError",
    "HostProfile",
    "MANAGER_EVENT_ACK_SCHEMA",
    "ManagerEventAck",
    "UnsupportedHostAdapterError",
    "future_host_fixture",
]
