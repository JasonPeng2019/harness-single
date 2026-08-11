from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .models import iso_utc, parse_utc
from .stable_io import (
    PathKeyedAppendLock,
    PreparedOutputTransaction,
    _append_jsonl_locked,
    _jsonl_bytes,
    canonical_json,
    read_stable,
)


_TRANSITION_ONLY_TYPES = {
    "CHECKPOINT_UPDATED",
    "HELPER_EXITED",
    "HELPER_STATE_UNKNOWN",
    "MCP_EXITED",
    "MCP_STATE_UNKNOWN",
    "RESULT_AVAILABLE",
}

_DEFERRED_HANDOFF_TYPES = {
    "MANAGER_SIGNAL",
    "CHECKPOINT_UPDATED",
    "RESULT_AVAILABLE",
}
_MUTABLE_HANDOFF_TYPES = {"CHECKPOINT_UPDATED", "RESULT_AVAILABLE"}


# S3 owns the disposition table.  It is deliberately data rather than a second
# selector: future producers must either be added here or become an explicit
# OBSERVATION_UNCERTAIN event.  The legacy selector below remains available
# until the S4 compatibility removal.
EVENT_DISPOSITION_WAKING = "WAKING_MANAGER_EVENT"
EVENT_DISPOSITION_OBSERVED = "OBSERVED_STATE"
EVENT_DISPOSITION_SUPERSEDED = "SUPERSESSION"

_WAKING_EVENT_TYPES = frozenset({
    "HARNESS_WATCHER_ALERT",
    "RELAY_READY",
    "REQUEST_AMBIGUOUS",
    "RELAY_UNBOUND",
    "REQUEST_EXPIRY_WARNING",
    "DUPLICATE_CONTROLLER",
    "DUPLICATE_CODING_BRANCH",
    "DUPLICATE_CODING_WORKTREE",
    "CODING_RESULT_INVALID",
    "COORDINATION_FAILED",
    "RESOURCE_CLAIM_STALE",
    "RESOURCE_CONFLICT",
    "RESOURCE_WAIT",
    "RESOURCE_AMBIGUOUS",
    "STALE_STATUS",
    "PROCESS_STATE_UNKNOWN",
    "PROCESS_INVENTORY_INCOMPLETE",
    "OBSERVATION_ERROR",
    "MANAGER_SIGNAL",
    "CONTROLLER_EXITED",
    "HELPER_EXITED",
    "MCP_EXITED",
    "HELPER_STATE_UNKNOWN",
    "MCP_STATE_UNKNOWN",
    "PROVIDER_WAIT",
    "LANE_STATE_UNKNOWN",
    "MANAGER_REVIEW_DUE",
    "LANE_NO_PROGRESS",
    "LANE_STAGE_REPEAT",
    "CHECKPOINT_UPDATED",
    "RESULT_AVAILABLE",
})

_OBSERVED_EVENT_TYPES = frozenset({
    "CONTROLLER_ACTIVE",
    "LANE_WAITING_RESOURCE",
    "LANE_WAITING_RELAY",
    "RESULT_ACCEPTANCE_PENDING",
    "RESOURCE_RELEASE_POSSIBLE",
    "REQUEST_STALE",
    "RELAYED",
    "RELAYED_INACTIVE",
    "RELAYED_AMBIGUOUS",
    "HELPER_ACTIVE",
    "MCP_ACTIVE",
    "CONDITION_CLEARED",
    # These are intentionally explicit: raw worker/provider material is not a
    # manager obligation and must never be copied into QUEUE.jsonl.
    "RAW_OUTPUT",
    "WORKER_OUTPUT",
    "LOG",
    "DIAGNOSTIC",
    "PROVIDER_TELEMETRY",
    "HEARTBEAT",
    "HARNESS_SIGNAL_OBSERVED",
    "HARNESS_EVENT_INELIGIBLE",
    "HARNESS_EVENT_ACTIONABLE",
    "HARNESS_EVENT_PENDING",
    "HARNESS_EVENT_DEFERRED",
    "HARNESS_ACK_ATTEMPTED",
    "HARNESS_ACK_SUCCEEDED",
    "HARNESS_WAKE_ATTEMPTED",
    "HARNESS_WAKE_DELIVERED",
    "HARNESS_WAKE_FAILED",
    "HARNESS_SCAN_COMMITTED",
    "MANAGER_WAKE_RECEIVED",
    "MANAGER_WAIT_FINISHED",
    "WATCH_TIMEOUT",
    "RUNNING_CODEX",
    "RUNNING_PROVIDER",
    "WAITING_RELAY",
    "WAITING_RESOURCE",
    "HELPER_RUNNING",
    "MCP_RUNNING",
    "CODEX_STARTED",
    "CODEX_EXITED",
    "PROVIDER_STARTED",
    "PROVIDER_EXITED",
    "LAUNCH_FAILED",
    "CONTROLLER_FAILED",
    "CONTROLLER_INTERRUPTED",
    "MANAGER_WAKE_ATTEMPTED",
    "MANAGER_WAKE_DELIVERED",
    "MANAGER_WAKE_FAILED",
    "FORMAL_REVIEW_BASELINE_ADVANCED",
})

EVENT_DISPOSITIONS: dict[str, str] = {
    **{kind: EVENT_DISPOSITION_WAKING for kind in _WAKING_EVENT_TYPES},
    **{kind: EVENT_DISPOSITION_OBSERVED for kind in _OBSERVED_EVENT_TYPES},
    "CONDITION_CLEARED": EVENT_DISPOSITION_SUPERSEDED,
}
# Public aliases make the exhaustive table easy to bind in table-driven tests.
CURRENT_EVENT_DISPOSITIONS = EVENT_DISPOSITIONS
EVENT_WIRING = EVENT_DISPOSITIONS

# These source facts are only actionable when the current reconciled snapshot
# corroborates their liveness/actionability.  A direct manager producer may
# supply the explicit manager_actionable assertion for the adapter-neutral
# synthetic boundary when no snapshot is available.
_SNAPSHOT_REQUIRED_TYPES = frozenset({
    "RELAY_READY",
    "REQUEST_AMBIGUOUS",
    "RELAY_UNBOUND",
    "REQUEST_EXPIRY_WARNING",
    "RESOURCE_WAIT",
    "RESOURCE_AMBIGUOUS",
    "PROCESS_STATE_UNKNOWN",
    "OBSERVATION_ERROR",
    "CONTROLLER_EXITED",
    "HELPER_EXITED",
    "MCP_EXITED",
    "HELPER_STATE_UNKNOWN",
    "MCP_STATE_UNKNOWN",
})

MANAGER_REGISTRATION_SCHEMA = "orchestrator-manager-registration/v1"
MANAGER_QUEUE_SCHEMA = "orchestrator-manager-queue-record/v1"
MANAGER_STATE_SCHEMA = "orchestrator-manager-queue-state/v1"
MANAGER_WAKE_SCHEMA = "orchestrator-manager-wake/v1"
MANAGER_DELIVERY_SCHEMA = "orchestrator-manager-delivery/v1"


class ManagerRoutingError(ValueError):
    """The manager binding or a durable manager record is invalid."""


class ManagerBindingError(ManagerRoutingError):
    pass


class ManagerRecordError(ManagerRoutingError):
    pass


@dataclass(frozen=True)
class ManagerBinding:
    run_id: str
    queue_id: str
    manager_session_id: str
    manager_thread_id: str
    registration_id: str
    manager_invocation_id: str | None = None

    @property
    def core(self) -> dict[str, str | None]:
        return {
            "run_id": self.run_id,
            "queue_id": self.queue_id,
            "manager_session_id": self.manager_session_id,
            "manager_thread_id": self.manager_thread_id,
            "manager_invocation_id": self.manager_invocation_id,
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical_json(self.core).encode("utf-8")).hexdigest()

    def as_record(self) -> dict[str, Any]:
        return {
            **self.core,
            "registration_id": self.registration_id,
            "binding_digest": self.digest,
        }


@dataclass(frozen=True)
class ManagerRouteDecision:
    disposition: str
    source_type: str
    event_type: str
    event_id: str | None
    identity: str
    reason: str


def _nonempty_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManagerBindingError(f"{field} must be a non-empty string")
    return value.strip()


def _finite_priority(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)):
        raise ManagerRecordError("priority must be a finite number")
    return float(value)


def _safe_reference(data: Mapping[str, Any], explicit: object = None) -> dict[str, str] | None:
    candidate = explicit
    if candidate is None:
        candidate = data.get("payload_ref")
    if candidate is None:
        path = data.get("path") or data.get("source_path") or data.get("checkpoint_path") or data.get("result_path")
        digest = (
            data.get("sha256")
            or data.get("content_sha256")
            or data.get("checkpoint_sha256")
            or data.get("result_sha256")
        )
        if isinstance(path, str) and isinstance(digest, str):
            candidate = {"path": path, "sha256": digest}
    if candidate is None:
        return None
    if not isinstance(candidate, Mapping):
        raise ManagerRecordError("payload_ref must be an object")
    path = _nonempty_text(candidate.get("path"), "payload_ref.path")
    digest = _nonempty_text(
        candidate.get("sha256") or candidate.get("content_sha256"),
        "payload_ref.sha256",
    )
    if len(digest) != 64 or any(char not in "0123456789abcdefABCDEF" for char in digest):
        raise ManagerRecordError("payload_ref.sha256 must be a SHA-256 digest")
    return {"path": path, "sha256": digest.lower()}


_FACT_KEYS = frozenset({
    "lane_id", "declared_lane_id", "run_id", "request_id", "signal_id", "kind", "severity",
    "state", "reason", "phase", "expiry_bucket", "manager_actionable", "actionable",
    "agent_blocked", "deadline_utc", "delivery_deadline_utc", "generation", "fingerprint",
    "unchanged_since_utc", "worker_invocation_id", "session_id", "thread_id", "resource",
    "resources", "owners", "attempts", "code", "provider_id", "event_id",
})


def _typed_facts(data: Mapping[str, Any]) -> dict[str, Any]:
    """Copy typed routing facts, validating list members like scalar facts.

    The list cardinality is intentionally not capped here.  Recordability is
    enforced against the manager journal's actual read limit immediately
    before append, so this boundary does not invent a smaller adversarial-input
    limit than the durable protocol itself.
    """

    def scalar(value: object, key: str) -> object:
        if isinstance(value, str):
            if len(value) > 4096:
                raise ManagerRecordError(f"routing fact {key} is too large")
        elif isinstance(value, float) and not math.isfinite(value):
            raise ManagerRecordError(f"routing fact {key} must be finite")
        return value

    result: dict[str, Any] = {}
    for key in sorted(_FACT_KEYS):
        value = data.get(key)
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            result[key] = scalar(value, key)
        elif isinstance(value, list) and all(isinstance(item, (str, int, float, bool)) for item in value):
            result[key] = [scalar(item, key) for item in value]
    return result


class ManagerEventRouter:
    """The sole writer for one exact manager binding's durable event channel."""

    _MAX_RECORD_BYTES = 2_000_000
    # This is the existing bound consumed by _queue_records/read_stable.  It
    # is the recordability boundary, rather than a new per-fact heuristic.
    _QUEUE_READ_MAX_BYTES = 20_000_000
    def __init__(
        self,
        root: Path,
        *,
        run_id: str | None = None,
        queue_id: str | None = None,
        manager_session_id: str | None = None,
        manager_thread_id: str | None = None,
        registration_id: str | None = None,
        manager_invocation_id: str | None = None,
        binding: Mapping[str, Any] | None = None,
        now: Any = None,
    ) -> None:
        supplied = dict(binding or {})
        run_id = run_id or supplied.get("run_id")
        queue_id = queue_id or supplied.get("queue_id")
        manager_session_id = manager_session_id or supplied.get("manager_session_id") or supplied.get("session_id")
        manager_thread_id = manager_thread_id or supplied.get("manager_thread_id") or supplied.get("thread_id")
        registration_id = registration_id or supplied.get("registration_id")
        manager_invocation_id = manager_invocation_id or supplied.get("manager_invocation_id") or supplied.get("invocation_id")
        run_text = _nonempty_text(run_id, "run_id")
        session_text = _nonempty_text(manager_session_id, "manager_session_id")
        thread_text = _nonempty_text(manager_thread_id, "manager_thread_id")
        provisional = {
            "run_id": run_text,
            "manager_session_id": session_text,
            "manager_thread_id": thread_text,
            "manager_invocation_id": (
                _nonempty_text(manager_invocation_id, "manager_invocation_id")
                if manager_invocation_id is not None else None
            ),
        }
        if queue_id is None:
            queue_id = "queue-" + hashlib.sha256(canonical_json(provisional).encode("utf-8")).hexdigest()[:32]
        queue_text = _nonempty_text(queue_id, "queue_id")
        if registration_id is None:
            registration_id = "registration-" + hashlib.sha256(
                canonical_json({**provisional, "queue_id": queue_text}).encode("utf-8")
            ).hexdigest()[:32]
        registration_text = _nonempty_text(registration_id, "registration_id")
        self.binding = ManagerBinding(
            run_text, queue_text, session_text, thread_text, registration_text,
            provisional["manager_invocation_id"],
        )
        self.root = Path(root).absolute()
        self._now = now
        self._transaction = PreparedOutputTransaction(self.root, allowed_roots=(self.root.parent,))
        self._transaction.prepare()
        for path in (
            self.registration_path,
            self.queue_path,
            self.state_path,
            self.wake_path,
            self.delivery_path,
        ):
            self._transaction.admit(path)
        self._initialize()

    @property
    def registration_path(self) -> Path:
        return self.root / "REGISTRATION.json"

    @property
    def queue_path(self) -> Path:
        return self.root / "QUEUE.jsonl"

    @property
    def state_path(self) -> Path:
        return self.root / "STATE.json"

    @property
    def wake_path(self) -> Path:
        return self.root / "WAKE.json"

    @property
    def delivery_path(self) -> Path:
        return self.root / "DELIVERY.jsonl"

    @property
    def registration(self) -> dict[str, Any]:
        value = self._read_json(self.registration_path, schema=MANAGER_REGISTRATION_SCHEMA)
        if value is None:
            raise ManagerRecordError("manager registration is missing")
        return dict(value)

    def _timestamp(self) -> str:
        value = self._now() if callable(self._now) else self._now
        if isinstance(value, datetime):
            return iso_utc(value) or ""
        return iso_utc(datetime.now().astimezone()) or ""

    def _binding_record(self) -> dict[str, Any]:
        return self.binding.as_record()

    def _assert_binding(self, record: Mapping[str, Any]) -> None:
        expected = self._binding_record()
        fields = (
            "run_id", "queue_id", "manager_session_id", "manager_thread_id",
            "binding_digest", "registration_id",
        )
        for field in fields:
            if record.get(field) != expected[field]:
                raise ManagerBindingError(f"manager binding mismatch in {field}")
        if "manager_invocation_id" in record and record.get("manager_invocation_id") != expected.get("manager_invocation_id"):
            raise ManagerBindingError("manager binding mismatch in manager_invocation_id")

    def _read_json(self, path: Path, *, schema: str) -> dict[str, Any] | None:
        if not path.exists():
            return None
        self._transaction.revalidate(path)
        try:
            stable = read_stable(path, max_bytes=self._MAX_RECORD_BYTES, retries=3, delay_seconds=0.01)
            value = json.loads(stable.data.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ManagerRecordError(f"cannot read {path.name}: {exc}") from exc
        if not isinstance(value, dict) or value.get("schema") != schema:
            raise ManagerRecordError(f"invalid {path.name} schema")
        self._assert_binding(value)
        return value

    def _write_registration(self) -> None:
        current = self._read_json(self.registration_path, schema=MANAGER_REGISTRATION_SCHEMA)
        if current is not None:
            return
        record = {
            "schema": MANAGER_REGISTRATION_SCHEMA,
            **self._binding_record(),
            "registered_utc": self._timestamp(),
            "channel": ["REGISTRATION.json", "QUEUE.jsonl", "STATE.json", "WAKE.json", "DELIVERY.jsonl"],
        }
        self._transaction.atomic_json(self.registration_path, record)

    def _empty_state(self) -> dict[str, Any]:
        return {
            "schema": MANAGER_STATE_SCHEMA,
            **self._binding_record(),
            "next_admission_seq": 1,
            "next_journal_seq": 1,
            "wake_revision": 0,
            "events": [],
            "acknowledged_event_ids": [],
            "superseded": {},
            "observed": {},
        }

    def _initialize(self) -> None:
        self._write_registration()
        if not self.state_path.exists():
            self._transaction.atomic_json(self.state_path, self._empty_state())
        else:
            try:
                state = self._read_json(self.state_path, schema=MANAGER_STATE_SCHEMA)
            except ManagerRecordError:
                state = None
            if state is None:
                self._transaction.atomic_json(self.state_path, self._empty_state())
        if not self.wake_path.exists():
            self._transaction.atomic_json(self.wake_path, {
                "schema": MANAGER_WAKE_SCHEMA,
                **self._binding_record(),
                "wake_revision": 0,
                "published_utc": self._timestamp(),
            })
        else:
            try:
                self._read_json(self.wake_path, schema=MANAGER_WAKE_SCHEMA)
            except ManagerRecordError:
                self._transaction.atomic_json(self.wake_path, {
                    "schema": MANAGER_WAKE_SCHEMA,
                    **self._binding_record(),
                    "wake_revision": 0,
                    "published_utc": self._timestamp(),
                })
        for path in (self.queue_path, self.delivery_path):
            if not path.exists():
                self._transaction.write_bytes(path, b"")
        # A prior crash may have durable queue records but no state/wake edge.
        if self.queue_path.stat().st_size:
            self.rebuild_state(publish_wake=True)

    def _queue_records(self) -> list[dict[str, Any]]:
        if not self.queue_path.exists():
            return []
        self._transaction.revalidate(self.queue_path)
        try:
            data = read_stable(self.queue_path, max_bytes=self._QUEUE_READ_MAX_BYTES, retries=3, delay_seconds=0.01).data
        except OSError as exc:
            raise ManagerRecordError(f"cannot read QUEUE.jsonl: {exc}") from exc
        records: list[dict[str, Any]] = []
        prior_journal = 0
        prior_admission = 0
        common_fields = set(self._binding_record()) | {
            "schema", "record_kind", "journal_seq", "event_id",
        }
        for line_number, line in enumerate(data.splitlines(), 1):
            try:
                raw = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ManagerRecordError(f"invalid queue record {line_number}: {exc}") from exc
            if not isinstance(raw, dict) or raw.get("schema") != MANAGER_QUEUE_SCHEMA:
                raise ManagerRecordError(f"invalid queue record {line_number} schema")
            self._assert_binding(raw)
            journal = raw.get("journal_seq")
            if not isinstance(journal, int) or isinstance(journal, bool) or journal <= prior_journal:
                raise ManagerRecordError(f"queue journal sequence is not strictly increasing at {line_number}")
            prior_journal = journal
            kind = raw.get("record_kind")
            if kind not in {"EVENT", "ACK", "SUPERSESSION"}:
                raise ManagerRecordError(f"unknown queue record kind at {line_number}")
            allowed = common_fields | {
                "admission_seq", "source_event_id", "event_type", "source_type",
                "identity", "disposition", "priority", "admitted_utc", "facts",
                "payload_ref", "action", "acknowledged_utc", "superseded_by",
                "superseded_utc",
            }
            if set(raw) - allowed:
                raise ManagerRecordError(f"queue record {line_number} has unknown fields")
            event_id = raw.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise ManagerRecordError(f"queue record {line_number} has no event_id")
            if kind == "EVENT":
                admission = raw.get("admission_seq")
                if (
                    not isinstance(admission, int)
                    or isinstance(admission, bool)
                    or admission <= prior_admission
                ):
                    raise ManagerRecordError(f"event record {line_number} has invalid admission_seq")
                prior_admission = admission
                if (
                    not isinstance(raw.get("event_type"), str)
                    or not raw["event_type"].strip()
                    or not isinstance(raw.get("source_type"), str)
                    or not raw["source_type"].strip()
                    or not isinstance(raw.get("identity"), str)
                    or not raw["identity"].strip()
                    or raw.get("disposition") != EVENT_DISPOSITION_WAKING
                    or not isinstance(raw.get("facts"), dict)
                ):
                    raise ManagerRecordError(f"event record {line_number} has invalid identity")
                _finite_priority(raw.get("priority"))
                if not isinstance(raw.get("admitted_utc"), str) or parse_utc(raw["admitted_utc"]) is None:
                    raise ManagerRecordError(f"event record {line_number} has invalid timestamp")
                source_event_id = raw.get("source_event_id")
                if source_event_id is not None and (not isinstance(source_event_id, str) or not source_event_id):
                    raise ManagerRecordError(f"event record {line_number} has invalid source_event_id")
                if "payload_ref" in raw:
                    try:
                        _safe_reference({}, raw["payload_ref"])
                    except ManagerRoutingError as exc:
                        raise ManagerRecordError(f"event record {line_number} has invalid payload_ref") from exc
            elif kind == "ACK":
                if (
                    raw.get("action") != "ACKNOWLEDGED"
                    or not isinstance(raw.get("acknowledged_utc"), str)
                    or parse_utc(raw["acknowledged_utc"]) is None
                ):
                    raise ManagerRecordError(f"ack record {line_number} is invalid")
            else:
                if (
                    not isinstance(raw.get("superseded_by"), str)
                    or not raw.get("superseded_by")
                    or raw.get("action") != "SUPERSEDED"
                    or not isinstance(raw.get("superseded_utc"), str)
                    or parse_utc(raw["superseded_utc"]) is None
                ):
                    raise ManagerRecordError(f"supersession record {line_number} is invalid")
            records.append(raw)
        return records

    def _ledger(self) -> tuple[list[dict[str, Any]], set[str], dict[str, str], int, int]:
        events: dict[str, dict[str, Any]] = {}
        acknowledged: set[str] = set()
        superseded: dict[str, str] = {}
        next_admission = 1
        next_journal = 1
        for record in self._queue_records():
            next_journal = max(next_journal, int(record["journal_seq"]) + 1)
            if record["record_kind"] == "EVENT":
                prior = events.get(record["event_id"])
                if prior is not None and canonical_json(prior) != canonical_json(record):
                    raise ManagerRecordError("event ID collision in QUEUE.jsonl")
                events[record["event_id"]] = record
                next_admission = max(next_admission, int(record["admission_seq"]) + 1)
            elif record["record_kind"] == "ACK":
                acknowledged.add(record["event_id"])
            else:
                superseded[record["event_id"]] = record["superseded_by"]
        pending = [
            record for event_id, record in events.items()
            if event_id not in acknowledged and event_id not in superseded
        ]
        pending.sort(key=lambda item: (float(item["priority"]), int(item["admission_seq"]), item["event_id"]))
        return pending, acknowledged, superseded, next_admission, next_journal

    def _state_for_ledger(self, *, observed: Mapping[str, Any] | None = None, wake_revision: int | None = None) -> dict[str, Any]:
        pending, acknowledged, superseded, next_admission, next_journal = self._ledger()
        try:
            prior = self._read_json(self.state_path, schema=MANAGER_STATE_SCHEMA)
        except ManagerRecordError:
            prior = None
        prior_observed = prior.get("observed", {}) if isinstance(prior, dict) and isinstance(prior.get("observed"), dict) else {}
        merged_observed = dict(prior_observed)
        if observed:
            merged_observed.update(observed)
        prior_revision = prior.get("wake_revision", 0) if isinstance(prior, dict) else 0
        revision = wake_revision if wake_revision is not None else prior_revision
        if not isinstance(revision, int) or revision < 0:
            revision = 0
        return {
            "schema": MANAGER_STATE_SCHEMA,
            **self._binding_record(),
            "next_admission_seq": next_admission,
            "next_journal_seq": next_journal,
            "wake_revision": revision,
            "events": pending,
            "acknowledged_event_ids": sorted(acknowledged),
            "superseded": dict(sorted(superseded.items())),
            "observed": merged_observed,
        }

    def rebuild_state(self, *, publish_wake: bool = True) -> dict[str, Any]:
        """Rebuild the cache solely from the append-only queue journal."""
        try:
            current_wake = self._read_json(self.wake_path, schema=MANAGER_WAKE_SCHEMA)
        except ManagerRecordError:
            current_wake = None
        revision = current_wake.get("wake_revision", 0) if current_wake else 0
        state = self._state_for_ledger(wake_revision=revision)
        self._transaction.atomic_json(self.state_path, state)
        if publish_wake and state["events"]:
            self._publish_wake()
            state = self._state_for_ledger(wake_revision=self.wake_revision)
            self._transaction.atomic_json(self.state_path, state)
        return state

    def load_state(self) -> dict[str, Any]:
        try:
            state = self._read_json(self.state_path, schema=MANAGER_STATE_SCHEMA)
        except ManagerRecordError:
            state = None
        if state is None:
            return self.rebuild_state()
        # A valid but stale cache is still rebuilt; queue remains authoritative.
        rebuilt = self._state_for_ledger(wake_revision=state.get("wake_revision", 0))
        if canonical_json(rebuilt) != canonical_json(state):
            self._transaction.atomic_json(self.state_path, rebuilt)
            state = rebuilt
        return state

    @property
    def wake_revision(self) -> int:
        wake = self._read_json(self.wake_path, schema=MANAGER_WAKE_SCHEMA)
        revision = wake.get("wake_revision", 0) if wake else 0
        if not isinstance(revision, int) or revision < 0:
            raise ManagerRecordError("invalid wake revision")
        return revision

    def _publish_wake(self) -> dict[str, Any]:
        revision = self.wake_revision + 1
        wake = {
            "schema": MANAGER_WAKE_SCHEMA,
            **self._binding_record(),
            "wake_revision": revision,
            "published_utc": self._timestamp(),
        }
        # No event IDs, source data, or payload are permitted in this file.
        self._transaction.atomic_json(self.wake_path, wake)
        return wake

    def classify(self, event: Mapping[str, Any]) -> ManagerRouteDecision:
        if not isinstance(event, Mapping):
            raise ManagerRoutingError("manager source event must be an object")
        source_type = _nonempty_text(event.get("type"), "event.type")
        data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
        source_event_id = event.get("event_id")
        if source_event_id is not None:
            source_event_id = _nonempty_text(source_event_id, "event.event_id")
        identity = event.get("identity") or data.get("signal_id") or data.get("path") or source_event_id
        identity_text = _nonempty_text(identity, "event.identity")
        disposition = EVENT_DISPOSITIONS.get(source_type)
        if disposition is None:
            return ManagerRouteDecision(EVENT_DISPOSITION_WAKING, source_type, "OBSERVATION_UNCERTAIN", source_event_id, identity_text, "unknown source type")
        if data.get("manager_actionable") is False or data.get("actionable") is False:
            return ManagerRouteDecision(EVENT_DISPOSITION_OBSERVED, source_type, source_type, source_event_id, identity_text, "source marked non-actionable")
        if disposition == EVENT_DISPOSITION_SUPERSEDED:
            return ManagerRouteDecision(disposition, source_type, source_type, source_event_id, identity_text, "source explicitly supersedes prior state")
        return ManagerRouteDecision(disposition, source_type, source_type, source_event_id, identity_text, "declared event wiring")

    def _validate_input_binding(self, event: Mapping[str, Any]) -> None:
        candidate: dict[str, Any] = {}
        for key in ("run_id", "queue_id", "manager_session_id", "manager_thread_id", "manager_invocation_id", "registration_id", "binding_digest"):
            if key in event:
                candidate[key] = event[key]
        for container_name in ("binding", "manager_binding"):
            value = event.get(container_name)
            if isinstance(value, Mapping):
                candidate.update({str(key): value[key] for key in value})
        expected = self._binding_record()
        for key, value in candidate.items():
            normalized = "manager_session_id" if key == "session_id" else "manager_thread_id" if key == "thread_id" else key
            if normalized in expected and value != expected[normalized]:
                raise ManagerBindingError(f"source event binding mismatch in {normalized}")

    def validate_binding(self, record: Mapping[str, Any]) -> None:
        """Validate all durable binding coordinates, including registration."""
        if not isinstance(record, Mapping):
            raise ManagerBindingError("binding record must be an object")
        self._assert_binding(record)

    def _event_record(
        self,
        event: Mapping[str, Any],
        decision: ManagerRouteDecision,
        *,
        admission_seq: int,
        journal_seq: int,
        priority: object,
        payload_ref: object = None,
        observed_at: datetime | None = None,
    ) -> dict[str, Any]:
        data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
        facts = _typed_facts(data)
        reference = _safe_reference(data, payload_ref)
        source_event_id = decision.event_id
        stable_identity = {
            "binding_digest": self.binding.digest,
            "source_event_id": source_event_id,
            "event_type": decision.event_type,
            "identity": decision.identity,
            "facts": facts,
            "payload_ref": reference,
        }
        event_id = source_event_id or "event-" + hashlib.sha256(canonical_json(stable_identity).encode("utf-8")).hexdigest()
        timestamp = iso_utc(observed_at) if observed_at is not None else self._timestamp()
        record: dict[str, Any] = {
            "schema": MANAGER_QUEUE_SCHEMA,
            **self._binding_record(),
            "record_kind": "EVENT",
            "journal_seq": journal_seq,
            "admission_seq": admission_seq,
            "event_id": event_id,
            "source_event_id": source_event_id,
            "event_type": decision.event_type,
            "source_type": decision.source_type,
            "identity": decision.identity,
            "disposition": EVENT_DISPOSITION_WAKING,
            "priority": _finite_priority(priority),
            "admitted_utc": timestamp,
            "facts": facts,
        }
        if reference is not None:
            record["payload_ref"] = reference
        return record

    def _priority_for(
        self,
        event: Mapping[str, Any],
        decision: ManagerRouteDecision,
        explicit: object,
        snapshot: Mapping[str, Any] | None,
        observed_at: datetime,
    ) -> float | None:
        data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
        if decision.event_type == "MANAGER_SIGNAL" and snapshot is None:
            if data.get("manager_actionable") is not True:
                return None
        if decision.event_type in _SNAPSHOT_REQUIRED_TYPES and snapshot is None:
            return None
        if snapshot is not None:
            selected = _priority(dict(event), dict(snapshot), observed_at)
            return float(selected) if selected is not None else None
        if explicit is not None:
            return _finite_priority(explicit)
        defaults = {
            "HARNESS_WATCHER_ALERT": 0,
            "RELAY_READY": 1,
            "REQUEST_AMBIGUOUS": 1,
            "RELAY_UNBOUND": 1,
            "REQUEST_EXPIRY_WARNING": 1,
            "COORDINATION_FAILED": 2,
            "RESOURCE_CONFLICT": 2,
            "MANAGER_SIGNAL": 3,
            "CONTROLLER_EXITED": 4,
            "HELPER_EXITED": 4,
            "MCP_EXITED": 4,
            "PROVIDER_WAIT": 4,
            "CHECKPOINT_UPDATED": 5,
            "RESULT_AVAILABLE": 5,
            "OBSERVATION_UNCERTAIN": 2,
        }
        return float(defaults.get(decision.event_type, 3))

    def _append_journal(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        data = _jsonl_bytes(records)
        with PathKeyedAppendLock(self.queue_path):
            self._append_journal_locked(data)

    def _append_journal_locked(self, data: bytes) -> None:
        """Append after the caller has entered the queue's path-keyed lock."""
        self._transaction.revalidate(self.queue_path)
        current_size = self.queue_path.stat().st_size if self.queue_path.exists() else 0
        if current_size + len(data) > self._QUEUE_READ_MAX_BYTES:
            raise ManagerRecordError(
                "manager queue append exceeds the journal reader recordability limit"
            )
        _append_jsonl_locked(self.queue_path, data)

    def _supersede_pending_locked(
        self,
        event_id: str,
        *,
        superseded_by: str,
        identity: str | None,
        event_type: str | None = None,
        source_type: str | None = None,
    ) -> None:
        """Validate one pending condition and append its supersession atomically."""
        pending, acknowledged, superseded, _, next_journal = self._ledger()
        events = {
            record["event_id"]: record
            for record in self._queue_records()
            if record.get("record_kind") == "EVENT"
        }
        target = events.get(event_id)
        if target is None:
            raise ManagerRecordError("supersession references an unknown event")
        if event_id in acknowledged:
            raise ManagerRecordError("supersession references an acknowledged event")
        if event_id in superseded:
            raise ManagerRecordError("supersession references an already-superseded event")
        if not any(item.get("event_id") == event_id for item in pending):
            raise ManagerRecordError("supersession reference is not pending")
        if identity is None:
            raise ManagerRecordError("supersession requires the condition identity")
        if target.get("identity") != identity:
            raise ManagerRecordError("supersession condition identity does not match")
        if event_type is not None and target.get("event_type") != event_type:
            raise ManagerRecordError("supersession condition type does not match")
        if source_type is not None and target.get("source_type") != source_type:
            raise ManagerRecordError("supersession condition source type does not match")
        record = self._supersession_record(event_id, superseded_by, next_journal)
        self._append_journal_locked(_jsonl_bytes([record]))

    def _supersession_record(self, old_event_id: str, new_event_id: str, journal_seq: int) -> dict[str, Any]:
        return {
            "schema": MANAGER_QUEUE_SCHEMA,
            **self._binding_record(),
            "record_kind": "SUPERSESSION",
            "journal_seq": journal_seq,
            "event_id": old_event_id,
            "superseded_by": new_event_id,
            "action": "SUPERSEDED",
            "superseded_utc": self._timestamp(),
        }

    def _ack_record(self, event_id: str, journal_seq: int, *, action: str = "ACKNOWLEDGED") -> dict[str, Any]:
        return {
            "schema": MANAGER_QUEUE_SCHEMA,
            **self._binding_record(),
            "record_kind": "ACK",
            "journal_seq": journal_seq,
            "event_id": event_id,
            "action": action,
            "acknowledged_utc": self._timestamp(),
        }

    def admit(
        self,
        event: Mapping[str, Any],
        *,
        priority: object = None,
        payload_ref: object = None,
        snapshot: Mapping[str, Any] | None = None,
        observed_at: datetime | None = None,
        binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Classify and durably admit one typed manager-targeted source fact."""
        if binding is not None:
            self._validate_input_binding({**event, "binding": dict(binding)})
        else:
            self._validate_input_binding(event)
        decision = self.classify(event)
        timestamp = observed_at or (self._now() if callable(self._now) else datetime.now().astimezone())
        if decision.disposition == EVENT_DISPOSITION_WAKING:
            data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
            # Validate all whitelisted members before any cache repair or
            # other publication.  _event_record repeats this pure validation
            # while constructing the durable record.
            _typed_facts(data)
        if decision.disposition == EVENT_DISPOSITION_OBSERVED:
            state = self.load_state()
            observed = {
                decision.identity: {
                    "source_type": decision.source_type,
                    "event_type": decision.event_type,
                    "event_id": decision.event_id,
                    "observed_utc": iso_utc(timestamp),
                    "reason": decision.reason,
                }
            }
            self._transaction.atomic_json(self.state_path, self._state_for_ledger(observed=observed, wake_revision=state.get("wake_revision", 0)))
            return None
        if decision.disposition == EVENT_DISPOSITION_SUPERSEDED:
            data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
            old_id = data.get("cleared_event_id") or data.get("supersedes_event_id")
            if not isinstance(old_id, str) or not old_id:
                raise ManagerRecordError("condition clear has no superseded event ID")
            cleared_type = data.get("cleared_type")
            if not isinstance(cleared_type, str) or not cleared_type.strip():
                raise ManagerRecordError("condition clear has no cleared event type")
            cleared_source_type = data.get("cleared_source_type")
            if cleared_source_type is not None and (
                not isinstance(cleared_source_type, str) or not cleared_source_type.strip()
            ):
                raise ManagerRecordError("condition clear has an invalid cleared source type")
            with PathKeyedAppendLock(self.queue_path):
                self._supersede_pending_locked(
                    old_id,
                    superseded_by=decision.event_id or decision.identity,
                    identity=decision.identity,
                    event_type=cleared_type,
                    source_type=cleared_source_type,
                )
            self.rebuild_state(publish_wake=False)
            return None
        pending, acknowledged, superseded, next_admission, next_journal = self._ledger()
        del pending
        existing_records = self._queue_records()
        if decision.event_id is not None:
            for prior in existing_records:
                if prior.get("record_kind") == "EVENT" and prior.get("event_id") == decision.event_id:
                    return prior
        selected_priority = self._priority_for(event, decision, priority, snapshot, timestamp)
        if selected_priority is None:
            state = self.load_state()
            observed = {
                decision.identity: {
                    "source_type": decision.source_type,
                    "event_type": decision.event_type,
                    "event_id": decision.event_id,
                    "observed_utc": iso_utc(timestamp),
                    "reason": "source failed current liveness or actionability checks",
                }
            }
            self._transaction.atomic_json(
                self.state_path,
                self._state_for_ledger(observed=observed, wake_revision=state.get("wake_revision", 0)),
            )
            return None
        record = self._event_record(
            event, decision, admission_seq=next_admission, journal_seq=next_journal,
            priority=selected_priority, payload_ref=payload_ref, observed_at=timestamp,
        )
        event_id = record["event_id"]
        for prior in existing_records:
            if prior.get("record_kind") == "EVENT" and prior.get("event_id") == event_id:
                if (
                    prior.get("event_type") != record.get("event_type")
                    or prior.get("identity") != record.get("identity")
                    or prior.get("facts") != record.get("facts")
                    or prior.get("payload_ref") != record.get("payload_ref")
                ):
                    raise ManagerRecordError("event ID collision for manager binding")
                return prior
        supersede_ids: list[str] = []
        for prior in existing_records:
            if (
                prior.get("record_kind") == "EVENT"
                and prior.get("event_type") == record["event_type"]
                and prior.get("identity") == record["identity"]
                and prior.get("event_id") != event_id
                and prior.get("event_id") not in acknowledged
                and prior.get("event_id") not in superseded
            ):
                supersede_ids.append(str(prior["event_id"]))
        journal = next_journal
        journal_records = [record]
        for old_id in sorted(set(supersede_ids)):
            journal += 1
            journal_records.append(self._supersession_record(old_id, event_id, journal))
        # This append is the crash-consistency boundary: queue first, cache and
        # payload-free wake second.
        prior_wake_revision = self.wake_revision
        self._append_journal(journal_records)
        state_after_queue = self._state_for_ledger(wake_revision=prior_wake_revision)
        self._transaction.atomic_json(self.state_path, state_after_queue)
        wake = self._publish_wake()
        self._transaction.atomic_json(self.state_path, self._state_for_ledger(wake_revision=wake["wake_revision"]))
        return record

    # Names used by producers and by deterministic fixtures are intentionally
    # aliases of the same admission boundary.
    admit_event = admit
    submit = admit
    route = admit

    def pending_events(self) -> list[dict[str, Any]]:
        return list(self.load_state().get("events", []))

    def next_event(self) -> dict[str, Any] | None:
        pending = self.pending_events()
        return pending[0] if pending else None

    def replay(self) -> list[dict[str, Any]]:
        return self.pending_events()

    def acknowledge(
        self,
        event_id: str,
        *,
        action: str = "ACKNOWLEDGED",
        binding: Mapping[str, Any] | None = None,
    ) -> bool:
        event_id = _nonempty_text(event_id, "event_id")
        if binding is not None:
            self.validate_binding(binding)
        if action != "ACKNOWLEDGED":
            raise ManagerRoutingError("only ACKNOWLEDGED is an event action")
        pending = self.pending_events()
        if not any(item.get("event_id") == event_id for item in pending):
            _, acknowledged, _, _, _ = self._ledger()
            if event_id in acknowledged:
                return True
            raise ManagerRoutingError("event ID is not pending for this exact manager binding")
        _, _, _, next_admission, next_journal = self._ledger()
        del next_admission
        self._append_journal([self._ack_record(event_id, next_journal, action=action)])
        state = self.rebuild_state(publish_wake=False)
        if state["events"]:
            wake = self._publish_wake()
            self._transaction.atomic_json(self.state_path, self._state_for_ledger(wake_revision=wake["wake_revision"]))
        return True

    acknowledge_event = acknowledge
    ack = acknowledge

    def supersede(
        self,
        event_id: str,
        *,
        superseded_by: str | None = None,
        identity: str | None = None,
        event_type: str | None = None,
        source_type: str | None = None,
    ) -> bool:
        event_id = _nonempty_text(event_id, "event_id")
        replacement = _nonempty_text(superseded_by, "superseded_by") if superseded_by is not None else "explicit-supersession"
        with PathKeyedAppendLock(self.queue_path):
            self._supersede_pending_locked(
                event_id,
                superseded_by=replacement,
                identity=identity,
                event_type=event_type,
                source_type=source_type,
            )
        self.rebuild_state(publish_wake=False)
        return True

    def record_delivery(
        self,
        *,
        delivery_id: str,
        wake_revision: int,
        event_ids: list[str] | None = None,
        outcome: str = "DELIVERED",
        delivered_at: datetime | None = None,
        binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if binding is not None:
            self.validate_binding(binding)
        delivery_id = _nonempty_text(delivery_id, "delivery_id")
        if not isinstance(wake_revision, int) or isinstance(wake_revision, bool) or wake_revision < 0:
            raise ManagerRecordError("delivery wake_revision must be a non-negative integer")
        if wake_revision > self.wake_revision:
            raise ManagerBindingError("delivery references a wake revision not published by this binding")
        ids = list(event_ids or [])
        if any(not isinstance(item, str) or not item for item in ids):
            raise ManagerRecordError("delivery event_ids must be non-empty strings")
        known_event_ids = {
            item["event_id"] for item in self._queue_records() if item.get("record_kind") == "EVENT"
        }
        if any(item not in known_event_ids for item in ids):
            raise ManagerRecordError("delivery references an event outside this manager queue")
        delivered_utc = iso_utc(delivered_at) if delivered_at else self._timestamp()
        if delivered_utc is None or parse_utc(delivered_utc) is None:
            raise ManagerRecordError("delivery timestamp is invalid")
        record = {
            "schema": MANAGER_DELIVERY_SCHEMA,
            **self._binding_record(),
            "delivery_id": delivery_id,
            "wake_revision": wake_revision,
            "event_ids": sorted(set(ids)),
            "outcome": _nonempty_text(outcome, "outcome"),
            "delivered_utc": delivered_utc,
        }
        # Delivery is transport evidence only.  It intentionally cannot call
        # acknowledge() or change the queue/state cache.
        for prior in self.read_deliveries():
            if prior["delivery_id"] == delivery_id:
                if canonical_json(prior) != canonical_json(record):
                    raise ManagerRecordError("delivery ID collision for manager binding")
                return prior
        self._transaction.append_jsonl(self.delivery_path, [record])
        return record

    delivery_receipt = record_delivery
    deliver = record_delivery

    def read_queue(self) -> list[dict[str, Any]]:
        return self._queue_records()

    def read_deliveries(self) -> list[dict[str, Any]]:
        if not self.delivery_path.exists():
            return []
        self._transaction.revalidate(self.delivery_path)
        data = read_stable(self.delivery_path, max_bytes=self._QUEUE_READ_MAX_BYTES, retries=3, delay_seconds=0.01).data
        result: list[dict[str, Any]] = []
        common_fields = set(self._binding_record()) | {
            "schema", "delivery_id", "wake_revision", "event_ids", "outcome", "delivered_utc",
        }
        for index, line in enumerate(data.splitlines(), 1):
            try:
                value = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ManagerRecordError(f"invalid delivery record {index}: {exc}") from exc
            if not isinstance(value, dict) or value.get("schema") != MANAGER_DELIVERY_SCHEMA:
                raise ManagerRecordError(f"invalid delivery record {index} schema")
            self._assert_binding(value)
            if set(value) - common_fields:
                raise ManagerRecordError(f"delivery record {index} has unknown fields")
            if not isinstance(value.get("delivery_id"), str) or not value["delivery_id"]:
                raise ManagerRecordError(f"delivery record {index} has invalid delivery_id")
            revision = value.get("wake_revision")
            if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
                raise ManagerRecordError(f"delivery record {index} has invalid wake_revision")
            if revision > self.wake_revision:
                raise ManagerBindingError(f"delivery record {index} references an unpublished wake revision")
            ids = value.get("event_ids")
            if (
                not isinstance(ids, list)
                or any(not isinstance(item, str) or not item for item in ids)
                or ids != sorted(set(ids))
            ):
                raise ManagerRecordError(f"delivery record {index} has invalid event_ids")
            if not isinstance(value.get("outcome"), str) or not value["outcome"].strip():
                raise ManagerRecordError(f"delivery record {index} has invalid outcome")
            if not isinstance(value.get("delivered_utc"), str) or parse_utc(value["delivered_utc"]) is None:
                raise ManagerRecordError(f"delivery record {index} has invalid timestamp")
            result.append(value)
        return result


def _active_lanes(snapshot: dict[str, Any]) -> bool:
    return any(
        lane.get("process_state", lane.get("operational_state"))
        in {"RUNNING_CODEX", "WAITING_RESOURCE", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
        for lane in snapshot.get("lanes", [])
    )


def _request_is_live(data: dict[str, Any]) -> bool:
    return data.get("lifetime_state") == "LIVE"


def _event_manager_actionable(data: dict[str, Any]) -> bool:
    """New snapshots carry an explicit authority bit; old fixtures fail closed."""
    value = data.get("manager_actionable")
    if isinstance(value, bool):
        return value
    return _request_is_live(data) and data.get("relay_state") != "BOUND_EXPIRED"


def _manager_request_is_actionable(
    request: dict[str, Any], lane_id: str, observed_at: datetime
) -> bool:
    if request.get("declared_lane_id") != lane_id:
        return False
    if request.get("lifetime_state") == "LIVE":
        return True
    if request.get("lifetime_state") != "UNKNOWN":
        return False
    if request.get("expiry_bucket") == "EXPIRED":
        return False
    if not request.get("expiry_bucket"):
        deadline = parse_utc(request.get("deadline_utc"))
        if deadline is not None and deadline <= observed_at:
            return False
    return True


def _manager_signal_ineligibility_reason(
    data: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> str | None:
    """Return the sole passive reason a retained manager signal cannot wake.

    The native selection decision remains exactly this liveness check.  The
    returned value is diagnostic-only and is emitted after observation; it does
    not alter admission, pending work, or wake delivery.
    """
    if data.get("correlated_request_answered") is True:
        return "ALREADY_ANSWERED"
    lane_id = data.get("lane_id")
    if not isinstance(lane_id, str) or not lane_id:
        return "INVALID_LANE_ID"
    lane_current = any(
        lane.get("lane_id") == lane_id
        and lane.get("process_state", lane.get("operational_state"))
        in {"RUNNING_CODEX", "WAITING_RESOURCE", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
        for lane in snapshot.get("lanes", [])
    )
    if lane_current:
        return None
    request_current = any(
        _manager_request_is_actionable(item, lane_id, observed_at)
        for item in snapshot.get("requests", [])
    )
    if request_current:
        return None
    helper_current = any(
        item.get("declared_lane_id") == lane_id
        and item.get("operational_state")
        in {"HELPER_RUNNING", "MCP_RUNNING"}
        for group in ("helpers", "mcps")
        for item in snapshot.get(group, [])
    )
    return None if helper_current else "LANE_NOT_LIVE"


def _manager_signal_is_live(
    data: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> bool:
    """Historical signals remain observable, but do not wake a fresh epoch."""
    return _manager_signal_ineligibility_reason(data, snapshot, observed_at) is None


def _resource_ambiguity_is_current(
    data: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> bool:
    lane_id = data.get("lane_id")
    if not isinstance(lane_id, str) or not lane_id:
        return False
    if any(
        lane.get("lane_id") == lane_id
        and lane.get("operational_state", lane.get("process_state"))
        in {
            "RUNNING_CODEX",
            "WAITING_RESOURCE",
            "WAITING_RELAY",
            "HELPER_RUNNING",
            "PROCESS_STATE_UNKNOWN",
            "UNKNOWN",
        }
        for lane in snapshot.get("lanes", [])
    ):
        return True
    if any(
        _manager_request_is_actionable(item, lane_id, observed_at)
        for item in snapshot.get("requests", [])
    ):
        return True
    return any(
        item.get("declared_lane_id") == lane_id
        and item.get("operational_state") in {"HELPER_RUNNING", "MCP_RUNNING"}
        for group in ("helpers", "mcps")
        for item in snapshot.get(group, [])
    )


def _manager_signal_has_delivery_urgency(data: dict[str, Any]) -> bool:
    return (
        data.get("kind") == "HELP"
        and data.get("agent_blocked") is True
        and parse_utc(data.get("delivery_deadline_utc")) is not None
    )


def _notification_deadline(data: dict[str, Any]) -> datetime | None:
    """Use a HELP delivery deadline for manager ordering when one was declared."""
    return parse_utc(data.get("delivery_deadline_utc")) or parse_utc(data.get("deadline_utc"))


def _priority(
    event: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> float | None:
    kind = event.get("type")
    data = event.get("data", {})
    if kind == "HARNESS_WATCHER_ALERT":
        return 0
    if kind in {"RELAY_READY", "REQUEST_AMBIGUOUS", "RELAY_UNBOUND"}:
        return 1 if _event_manager_actionable(data) else None
    if kind == "REQUEST_EXPIRY_WARNING":
        return 1 if _event_manager_actionable(data) and data.get("expiry_bucket") in {"WARNING", "CRITICAL"} else None
    if kind in {
        "DUPLICATE_CONTROLLER",
        "DUPLICATE_CODING_BRANCH",
        "DUPLICATE_CODING_WORKTREE",
        "CODING_RESULT_INVALID",
        "COORDINATION_FAILED",
        "RESOURCE_CLAIM_STALE",
        "RESOURCE_CONFLICT",
        "LANE_STATE_UNKNOWN",
    }:
        return 2
    if kind == "RESOURCE_WAIT":
        return 2 if data.get("actionable") is True else None
    if kind == "RESOURCE_AMBIGUOUS":
        return 2 if _resource_ambiguity_is_current(data, snapshot, observed_at) else None
    if kind == "STALE_STATUS":
        return 2
    if kind == "PROCESS_STATE_UNKNOWN":
        return 2 if _active_lanes(snapshot) else None
    if kind == "PROCESS_INVENTORY_INCOMPLETE":
        return 2
    if kind == "OBSERVATION_ERROR":
        return 2 if _active_lanes(snapshot) or any(item.get("lifetime_state") == "LIVE" for item in snapshot.get("requests", [])) else None
    if kind == "MANAGER_SIGNAL":
        if not _manager_signal_is_live(data, snapshot, observed_at):
            return None
        return 1.5 if _manager_signal_has_delivery_urgency(data) else 3
    if kind == "CONTROLLER_EXITED":
        lane = next(
            (item for item in snapshot.get("lanes", []) if item.get("lane_id") == data.get("lane_id")),
            {},
        )
        if lane.get("checkpoint_path") or lane.get("result_path"):
            return None
        declared = str(lane.get("declared_state", "")).lower()
        if declared in {"controller_failed", "launch_failed", "error", "failed", "terminated", "killed", "cancelled"}:
            return 4
        ended = parse_utc(lane.get("ended_utc"))
        return 4 if ended is not None and (observed_at - ended).total_seconds() <= 2 else None
    if kind in {"HELPER_EXITED", "MCP_EXITED", "HELPER_STATE_UNKNOWN", "MCP_STATE_UNKNOWN", "PROVIDER_WAIT"}:
        if kind == "PROVIDER_WAIT":
            return 4
        lane_id = data.get("declared_lane_id")
        session_id = data.get("session_id")
        if isinstance(lane_id, str) and lane_id:
            return 4 if any(
                lane.get("lane_id") == lane_id
                and lane.get("process_state", lane.get("operational_state"))
                in {"RUNNING_CODEX", "WAITING_RESOURCE", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
                for lane in snapshot.get("lanes", [])
            ) else None
        if isinstance(session_id, str) and session_id:
            return 4 if any(
                lane.get("thread_id") == session_id
                and lane.get("process_state", lane.get("operational_state"))
                in {"RUNNING_CODEX", "WAITING_RESOURCE", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
                for lane in snapshot.get("lanes", [])
            ) else None
        return 4 if _active_lanes(snapshot) else None
    if kind == "LANE_STAGE_REPEAT":
        return 4.1
    if kind == "LANE_NO_PROGRESS":
        return 4.2
    if kind == "MANAGER_REVIEW_DUE":
        return 4.3
    if kind in {"CHECKPOINT_UPDATED", "RESULT_AVAILABLE"}:
        return 5
    return None


def select_actionable(
    conditions: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
    acknowledged_event_ids: set[str],
    newly_observed_event_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    """Select one durable manager notification from current conditions."""
    candidates: list[tuple[float, datetime, str, dict[str, Any]]] = []
    for condition in conditions.values():
        if condition["event_id"] in acknowledged_event_ids:
            continue
        if (
            newly_observed_event_ids is not None
            and condition.get("type") in _TRANSITION_ONLY_TYPES
            and condition["event_id"] not in newly_observed_event_ids
        ):
            continue
        priority = _priority(condition, snapshot, observed_at)
        if priority is None:
            continue
        deadline = _notification_deadline(condition.get("data", {}))
        candidates.append((priority, deadline or datetime.max.replace(tzinfo=observed_at.tzinfo), condition["identity"], condition))
    if not candidates:
        return None
    priority, _, _, condition = min(candidates, key=lambda item: (item[0], item[1], item[2]))
    return {
        **condition,
        "admitted_priority": priority,
        "observed_utc": iso_utc(observed_at),
        "notification": "MANAGER_ACTION_REQUIRED",
    }


def _order_key(condition: dict[str, Any], observed_at: datetime) -> tuple[str, str]:
    """The stable deadline/identity portion of notification selection ordering."""
    deadline = _notification_deadline(condition.get("data", {}))
    return (iso_utc(deadline) or "9999-12-31T23:59:59Z", str(condition["identity"]))


def admit_deferred_handoffs(
    conditions: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
    acknowledged_event_ids: set[str],
    newly_observed_event_ids: set[str] | None,
    existing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Capture only newly eligible file-backed handoffs at their admission rank.

    Entries deliberately retain the original condition and ranking facts.  A later
    selection must not re-evaluate liveness for a deferred handoff.
    """
    by_event = {
        item.get("event", {}).get("event_id")
        for item in existing
        if isinstance(item, dict) and isinstance(item.get("event"), dict)
    }
    admitted = list(existing)
    for condition in conditions.values():
        if condition.get("type") not in _DEFERRED_HANDOFF_TYPES:
            continue
        event_id = condition.get("event_id")
        if not isinstance(event_id, str) or event_id in acknowledged_event_ids or event_id in by_event:
            continue
        if (
            newly_observed_event_ids is not None
            and condition.get("type") in _TRANSITION_ONLY_TYPES
            and event_id not in newly_observed_event_ids
        ):
            continue
        priority = _priority(condition, snapshot, observed_at)
        if priority is None:
            continue
        deadline_key, identity_key = _order_key(condition, observed_at)
        admitted.append({
            "event": condition,
            "priority": priority,
            "deadline_order": deadline_key,
            "identity": identity_key,
            "admitted_utc": iso_utc(observed_at),
        })
        by_event.add(event_id)
    return sorted(admitted, key=lambda item: (item["priority"], item["deadline_order"], item["identity"]))


def _prune_deferred(
    deferred: list[dict[str, Any]], conditions: dict[str, dict[str, Any]], acknowledged: set[str]
) -> list[dict[str, Any]]:
    retained: list[dict[str, Any]] = []
    for item in deferred:
        event = item.get("event") if isinstance(item, dict) else None
        if (
            not isinstance(event, dict)
            or (
                event.get("type") not in _DEFERRED_HANDOFF_TYPES
                and item.get("preempted_pending") is not True
            )
            or event.get("event_id") in acknowledged
        ):
            continue
        # Only the exact signal identity may prove this stored signal superseded.
        current = conditions.get(event.get("identity"))
        if (
            event.get("type") == "MANAGER_SIGNAL"
            and isinstance(current, dict)
            and current.get("data", {}).get("correlated_request_answered") is True
        ):
            continue
        retained.append(item)
    return retained


def select_actionable_with_deferred(
    conditions: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
    acknowledged_event_ids: set[str],
    newly_observed_event_ids: set[str] | None,
    deferred: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Select current work or an admitted handoff without re-gating stored work."""
    retained = _prune_deferred(deferred, conditions, acknowledged_event_ids)
    current = select_actionable(
        conditions, snapshot, observed_at=observed_at,
        acknowledged_event_ids=acknowledged_event_ids,
        newly_observed_event_ids=newly_observed_event_ids,
    )
    choices: list[tuple[float, str, str, dict[str, Any], bool]] = []
    if current is not None:
        priority = _priority(current, snapshot, observed_at)
        assert priority is not None
        deadline, identity = _order_key(current, observed_at)
        choices.append((priority, deadline, identity, current, False))
    for item in retained:
        event = item["event"]
        choices.append((item["priority"], item["deadline_order"], item["identity"], event, True))
    if not choices:
        return None, retained
    _, _, _, selected, stored = min(choices, key=lambda item: item[:3])
    if stored:
        retained = [item for item in retained if item.get("event", {}).get("event_id") != selected.get("event_id")]
        source = next(item for item in deferred if item.get("event", {}).get("event_id") == selected.get("event_id"))
        selected = {**selected, "admitted_priority": source["priority"], "admitted_utc": source["admitted_utc"], "observed_utc": iso_utc(observed_at), "notification": "MANAGER_ACTION_REQUIRED"}
    elif selected.get("type") in _MUTABLE_HANDOFF_TYPES:
        source = next((item for item in retained if item.get("event", {}).get("event_id") == selected.get("event_id")), None)
        if source is not None:
            retained = [item for item in retained if item is not source]
            selected = {**selected, "admitted_priority": source["priority"], "admitted_utc": source["admitted_utc"]}
    return selected, retained


def preempt_pending_with_higher_priority(
    pending: dict[str, Any] | None,
    deferred: list[dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], bool]:
    """Select strictly higher-priority deferred work without losing pending work."""
    if not isinstance(pending, dict) or not deferred:
        return pending, deferred, False
    stored_priority = pending.get("admitted_priority")
    pending_priority = (
        float(stored_priority)
        if isinstance(stored_priority, (int, float)) and not isinstance(stored_priority, bool)
        else _priority(pending, snapshot, observed_at)
    )
    if pending_priority is None:
        return pending, deferred, False
    ranked = sorted(
        deferred,
        key=lambda item: (item["priority"], item["deadline_order"], item["identity"]),
    )
    selected_item = ranked[0]
    if selected_item["priority"] >= pending_priority:
        return pending, deferred, False

    selected_event = selected_item["event"]
    selected_id = selected_event.get("event_id")
    pending_deadline, pending_identity = _order_key(pending, observed_at)
    admitted_utc = pending.get("admitted_utc") or pending.get("observed_utc") or iso_utc(observed_at)
    displaced = {
        "event": pending,
        "priority": pending_priority,
        "deadline_order": pending_deadline,
        "identity": pending_identity,
        "admitted_utc": admitted_utc,
        "preempted_pending": True,
    }
    remaining = [
        item for item in ranked
        if item.get("event", {}).get("event_id") not in {selected_id, pending.get("event_id")}
    ]
    remaining.append(displaced)
    remaining.sort(
        key=lambda item: (item["priority"], item["deadline_order"], item["identity"])
    )
    selected = {
        **selected_event,
        "admitted_priority": selected_item["priority"],
        "admitted_utc": selected_item["admitted_utc"],
        "observed_utc": iso_utc(observed_at),
        "notification": "MANAGER_ACTION_REQUIRED",
    }
    return selected, remaining, True


def coalesce_mutable_handoffs(
    pending: dict[str, Any] | None,
    deferred: list[dict[str, Any]],
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Keep exactly the newest mutable file version per (type, identity)."""
    entries: list[tuple[dict[str, Any], dict[str, Any] | None, str]] = []
    if isinstance(pending, dict) and pending.get("type") in _MUTABLE_HANDOFF_TYPES:
        # Missing admission is legacy state and deterministically ranks before explicit facts.
        entries.append((pending, None, str(pending.get("admitted_utc") or "")))
    for item in deferred:
        event = item.get("event") if isinstance(item, dict) else None
        if isinstance(event, dict) and event.get("type") in _MUTABLE_HANDOFF_TYPES:
            entries.append((event, item, str(item.get("admitted_utc") or "")))
    groups: dict[tuple[str, str], list[tuple[dict[str, Any], dict[str, Any] | None, str]]] = {}
    for entry in entries:
        event = entry[0]
        groups.setdefault((str(event.get("type")), str(event.get("identity"))), []).append(entry)
    kept_deferred = list(deferred)
    result_pending = pending
    for _, group in groups.items():
        if len(group) < 2:
            continue
        winner, winner_item, _ = max(group, key=lambda item: (item[2], str(item[0].get("event_id") or "")))
        superseded = sorted(
            str(event.get("event_id")) for event, _, _ in group if event.get("event_id") != winner.get("event_id")
        )
        pending_member = next((event for event, item, _ in group if item is None), None)
        group_ids = {event.get("event_id") for event, _, _ in group}
        kept_deferred = [
            item for item in kept_deferred
            if item.get("event", {}).get("event_id") not in group_ids
        ]
        if pending_member is not None:
            data = dict(winner.get("data", {}))
            prior = data.get("superseded_event_ids", [])
            data["superseded_event_ids"] = sorted(set(superseded + [str(item) for item in prior]))
            admission = winner_item.get("admitted_utc") if winner_item is not None else winner.get("admitted_utc")
            result_pending = {**winner, "data": data, "admitted_utc": admission, "observed_utc": iso_utc(observed_at), "notification": "MANAGER_ACTION_REQUIRED"}
            if winner_item is not None:
                result_pending["admitted_priority"] = winner_item["priority"]
        elif winner_item is not None:
            kept_deferred.append(winner_item)
    return result_pending, sorted(kept_deferred, key=lambda item: (item.get("priority", 99), item.get("deadline_order", ""), item.get("identity", "")))

