"""Versioned, conservative manager-attention record contract.

This module has no harness or hardware dependency.  It owns the v1 wire shape so
producers and the optional watcher can exchange records without inferring state
from log silence.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
import base64

SCHEMA = "manager-attention-timeline/v1"
REPORT_SCHEMA = "manager-attention-report/v1"
SOURCE_ROLES = frozenset({"orchestrator", "subagent", "harness", "supervisor"})
PRODUCER_ROLES = frozenset({"orchestrator", "subagent"})
KINDS = frozenset({
    "AGENT_SIGNAL_CREATED", "AGENT_SIGNAL_PUBLISHED", "AGENT_WAIT_STARTED", "AGENT_WAIT_FINISHED",
    "AGENT_RESPONSE_RECEIVED", "AGENT_WORK_RESUMED", "AGENT_GATE_EXPIRED", "WATCHER_NOTIFICATION_SENT", "HARNESS_SIGNAL_OBSERVED",
    "HARNESS_SCAN_COMMITTED", "HARNESS_EVENT_ACTIONABLE", "HARNESS_EVENT_DEFERRED",
    "HARNESS_EVENT_INELIGIBLE", "HARNESS_EVENT_PENDING", "HARNESS_ACK_ATTEMPTED", "HARNESS_ACK_SUCCEEDED",
    "FORMAL_REVIEW_BASELINE_ADVANCED", "MANAGER_INVOCATION_STARTED",
    "MANAGER_INVOCATION_FINISHED", "MANAGER_WAIT_STARTED", "MANAGER_WAIT_FINISHED",
    "MANAGER_WAKE_ATTEMPTED", "MANAGER_WAKE_DELIVERED", "MANAGER_WAKE_FAILED",
    "MANAGER_WAKE_RECEIVED",
    "MANAGER_ACTIVITY_HEARTBEAT", "MANAGER_EVENT_CLAIMED", "MANAGER_REVIEW_STARTED",
    "MANAGER_TOOL_STARTED", "MANAGER_TOOL_FINISHED", "MANAGER_DECISION_RECORDED",
    "MANAGER_RESPONSE_PUBLISHED", "MANAGER_CHECKPOINT_COMPLETED",
    "MANAGER_ABSENCE_STARTED", "MANAGER_ABSENCE_FINISHED",
})
WAKE_ATTEMPT_KINDS = frozenset({"MANAGER_WAKE_ATTEMPTED", "MANAGER_WAKE_DELIVERED", "MANAGER_WAKE_FAILED"})
WAKE_KINDS = WAKE_ATTEMPT_KINDS | frozenset({"MANAGER_WAKE_RECEIVED"})
ROLE_KINDS = {
    "orchestrator": frozenset({*(k for k in KINDS if k.startswith("MANAGER_") and "ABSENCE" not in k and k not in WAKE_ATTEMPT_KINDS), "FORMAL_REVIEW_BASELINE_ADVANCED"}),
    "subagent": frozenset({*(k for k in KINDS if k.startswith("AGENT_")), "WATCHER_NOTIFICATION_SENT"}),
    "harness": frozenset({*(k for k in KINDS if k.startswith("HARNESS_")), *WAKE_ATTEMPT_KINDS, "FORMAL_REVIEW_BASELINE_ADVANCED"}),
    "supervisor": frozenset({"MANAGER_ABSENCE_STARTED", "MANAGER_ABSENCE_FINISHED"}),
}
MANAGER_KINDS = frozenset(k for k in KINDS if k.startswith("MANAGER_"))
LANE_KINDS = frozenset(k for k in KINDS if k.startswith("AGENT_"))
ACTIVITY_STATES = frozenset({"ABSENT", "READING_EVENT", "REASONING", "RUNNING_TOOL", "WAITING_ON_TOOL", "HANDLING_OTHER_EVENT", "CHECKPOINTING", "UNKNOWN"})
CLASSIFICATIONS = frozenset({"NO_BLOCKING_IMPACT", "HARNESS_DELIVERY_DELAY", "ACKNOWLEDGEMENT_ONLY_DELAY", "IDLE_OR_ABSENT_MANAGER_DELAY", "BUSY_MANAGER_DELAY", "INSUFFICIENT_EVIDENCE"})
INELIGIBILITY_REASONS = frozenset({"ALREADY_ANSWERED", "INVALID_LANE_ID", "LANE_NOT_LIVE"})
PENDING_SNAPSHOT_KINDS = frozenset({"MANAGER_INVOCATION_STARTED", "MANAGER_INVOCATION_FINISHED", "HARNESS_EVENT_PENDING", "FORMAL_REVIEW_BASELINE_ADVANCED"})
MAX_PENDING_SNAPSHOT_EVENTS = 128
_SAFE_ID = re.compile(r"[^A-Za-z0-9_.-]+")

class AttentionValidationError(ValueError): pass
class AttentionConflictError(ValueError): pass

def utc_now() -> str: return datetime.now(timezone.utc).isoformat()
def safe_source_id(value: str) -> str:
    result = _SAFE_ID.sub("_", value).strip("._")
    if not result: raise AttentionValidationError("source_id is unsafe")
    return result[:128]
def _timestamp(value: object, field: str) -> None:
    if not isinstance(value, str): raise AttentionValidationError(f"{field} must be an ISO-8601 UTC timestamp")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise AttentionValidationError(f"{field} must be an ISO-8601 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0): raise AttentionValidationError(f"{field} must be UTC (+00:00)")
def _utc_timestamp(value: str) -> datetime:
    """Parse a timestamp already validated as UTC without changing equality semantics."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
def _string(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value: raise AttentionValidationError(f"{field} must be a non-empty string")
    return value

def _validate_pending_snapshot(record: Mapping[str, Any]) -> None:
    snapshot=record.get("pending_work_snapshot")
    if not isinstance(snapshot, Mapping): raise AttentionValidationError("pending_work_snapshot must be an object")
    if set(snapshot)-{"complete","events","selected_event_id","selection_reason"}: raise AttentionValidationError("pending_work_snapshot contains unsupported fields")
    complete=snapshot.get("complete"); events=snapshot.get("events")
    if not isinstance(complete,bool) or not isinstance(events,list) or len(events)>MAX_PENDING_SNAPSHOT_EVENTS: raise AttentionValidationError("pending_work_snapshot is malformed or exceeds its event limit")
    selected=snapshot.get("selected_event_id"); reason=snapshot.get("selection_reason")
    if selected is not None and (not isinstance(selected,str) or not selected): raise AttentionValidationError("pending_work_snapshot selected_event_id is invalid")
    if not isinstance(reason,str) or not reason: raise AttentionValidationError("pending_work_snapshot selection_reason is invalid")
    if not complete:
        if events or selected is not None or reason != "UNKNOWN": raise AttentionValidationError("incomplete pending_work_snapshot must explicitly be UNKNOWN")
        return
    seen=set()
    for event in events:
        if not isinstance(event,Mapping) or set(event)-{"event_id","type","priority","age_seconds","agent_blocked","response_deadline_utc","lease_deadline_utc"}: raise AttentionValidationError("pending snapshot event contains unsupported fields")
        for field in ("event_id","type"):_string(event,field)
        if event["event_id"] in seen: raise AttentionValidationError("pending snapshot event IDs must be unique")
        seen.add(event["event_id"])
        if isinstance(event.get("priority"),bool) or not isinstance(event.get("priority"),(int,float)) or event["priority"]<0: raise AttentionValidationError("pending snapshot priority is invalid")
        if isinstance(event.get("age_seconds"),bool) or not isinstance(event.get("age_seconds"),(int,float)) or event["age_seconds"]<0: raise AttentionValidationError("pending snapshot age_seconds is invalid")
        if not isinstance(event.get("agent_blocked"),bool): raise AttentionValidationError("pending snapshot agent_blocked is invalid")
        for field in ("response_deadline_utc","lease_deadline_utc"):
            if field in event: _timestamp(event[field],field)
    if selected is not None and selected not in seen: raise AttentionValidationError("selected pending event is absent from snapshot")

def canonical_bytes(record: Mapping[str, Any]) -> bytes:
    return json.dumps(dict(record), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
def source_digest(record: Mapping[str, Any]) -> str: return hashlib.sha256(canonical_bytes(record)).hexdigest()

def validate_record(record: Mapping[str, Any], *, canonical: bool = False) -> dict[str, Any]:
    if not isinstance(record, Mapping): raise AttentionValidationError("record must be an object")
    result = dict(record)
    if result.get("schema") != SCHEMA: raise AttentionValidationError("schema must be manager-attention-timeline/v1")
    if not canonical and {"observed_timestamp_utc","source_path","source_role","source_id","source_generation","byte_start","byte_end","source_record_sha256"}.intersection(result): raise AttentionValidationError("source record contains reserved canonical provenance field")
    try: parsed_id = uuid.UUID(_string(result, "record_id"))
    except ValueError as exc: raise AttentionValidationError("record_id must be UUIDv4") from exc
    if parsed_id.version != 4: raise AttentionValidationError("record_id must be UUIDv4")
    _timestamp(result.get("source_timestamp_utc"), "source_timestamp_utc")
    for field in ("epoch_id", "event_id", "kind", "recorder"): _string(result, field)
    if "signal_id" in result:
        if _string(result, "signal_id") != result["event_id"]:
            raise AttentionValidationError("signal_id must exactly match event_id")
    if result["kind"] not in KINDS: raise AttentionValidationError("kind is not allowed")
    if "observed_timestamp_utc" in result:
        if not canonical: raise AttentionValidationError("source record must not supply observed_timestamp_utc")
        _timestamp(result["observed_timestamp_utc"], "observed_timestamp_utc")
    elif canonical: raise AttentionValidationError("canonical record requires observed_timestamp_utc")
    for field in ("delivery_deadline_utc", "response_deadline_utc", "ack_deadline_utc", "formal_review_due_utc"):
        if field in result and result[field] is not None: _timestamp(result[field], field)
    # A signal cannot declare a deadline that had already passed when it was
    # created.  This applies only at the signal origin, not to later records
    # that faithfully repeat the signal's deadline.
    if result["kind"] == "AGENT_SIGNAL_CREATED":
        created = _utc_timestamp(result["source_timestamp_utc"])
        for field in ("delivery_deadline_utc", "response_deadline_utc"):
            if result.get(field) is not None and _utc_timestamp(result[field]) < created:
                raise AttentionValidationError(f"{field} precedes AGENT_SIGNAL_CREATED source_timestamp_utc")
    if result["kind"] in LANE_KINDS:
        _string(result, "lane_id")
        if result["kind"] in {"AGENT_SIGNAL_CREATED", "AGENT_SIGNAL_PUBLISHED", "AGENT_WAIT_STARTED"} and not isinstance(result.get("agent_blocked"), bool):
            raise AttentionValidationError("agent_blocked must be boolean for agent signal/publication/wait records")
        if result["kind"] == "AGENT_SIGNAL_PUBLISHED":
            _string(result, "signal_id")
    if result["kind"] == "AGENT_GATE_EXPIRED" and result.get("terminal_gate_expired") is not True:
        raise AttentionValidationError("terminal gate expiration requires terminal_gate_expired true")
    if result["kind"] == "WATCHER_NOTIFICATION_SENT":
        if result.get("wake_transport") != "collaboration.send_message" or result.get("delivery_succeeded") is not True:
            raise AttentionValidationError("watcher notification requires successful collaboration.send_message delivery")
    if result["kind"] in MANAGER_KINDS and result["kind"] not in {"MANAGER_ABSENCE_STARTED", "MANAGER_ABSENCE_FINISHED"}:
        _string(result, "manager_session_id")
        _string(result, "manager_invocation_id")
    if "wake_id" in result:
        try: wake_id = uuid.UUID(_string(result, "wake_id"))
        except ValueError as exc: raise AttentionValidationError("wake_id must be UUIDv4") from exc
        if wake_id.version != 4: raise AttentionValidationError("wake_id must be UUIDv4")
    if result["kind"] in WAKE_KINDS:
        _string(result, "wake_id")
        _string(result, "wake_transport")
        if result["kind"] != "MANAGER_WAKE_RECEIVED": _string(result, "wake_component")
    if result["kind"] == "MANAGER_WAKE_ATTEMPTED":
        if "delivery_succeeded" in result or "failure_kind" in result:
            raise AttentionValidationError("wake attempt must not declare an outcome")
    if result["kind"] == "MANAGER_WAKE_DELIVERED":
        if result.get("delivery_succeeded") is not True:
            raise AttentionValidationError("wake delivery requires delivery_succeeded true")
        if "failure_kind" in result:
            raise AttentionValidationError("wake delivery must not declare failure_kind")
    if result["kind"] == "MANAGER_WAKE_FAILED":
        if result.get("delivery_succeeded") is not False: raise AttentionValidationError("wake failure requires delivery_succeeded false")
        _string(result, "failure_kind")
    if result["kind"] == "MANAGER_WAIT_FINISHED":
        if "wake_id" in result: _string(result, "wake_transport")
        if result.get("wake_transport") == "blocking_harness_wait_stdout" and "wake_id" not in result:
            raise AttentionValidationError("blocking wake finish requires wake_id")
    if result["kind"] == "HARNESS_EVENT_INELIGIBLE":
        _string(result, "harness_event_id")
        _string(result, "ineligibility_reason")
    if result["kind"] in PENDING_SNAPSHOT_KINDS: _validate_pending_snapshot(result)
    if result["kind"] in {"MANAGER_ACTIVITY_HEARTBEAT", "MANAGER_EVENT_CLAIMED", "MANAGER_REVIEW_STARTED", "MANAGER_TOOL_STARTED", "MANAGER_TOOL_FINISHED", "MANAGER_DECISION_RECORDED", "MANAGER_RESPONSE_PUBLISHED", "MANAGER_CHECKPOINT_COMPLETED"}:
        state = _string(result, "manager_state")
        if state not in ACTIVITY_STATES: raise AttentionValidationError("manager_state is not allowed")
    if result.get("manager_state") == "HANDLING_OTHER_EVENT": _string(result, "related_event_id")
    if result["kind"] == "MANAGER_REVIEW_STARTED": _timestamp(result.get("formal_review_due_utc"), "formal_review_due_utc")
    if "terminal_for_activity" in result and result["terminal_for_activity"] is not True: raise AttentionValidationError("terminal_for_activity must be true when present")
    if "continuous_from_record_id" in result:
        if result["kind"] not in {"MANAGER_EVENT_CLAIMED", "MANAGER_REVIEW_STARTED", "MANAGER_WAIT_STARTED", "MANAGER_TOOL_STARTED"}: raise AttentionValidationError("continuous_from_record_id is only valid on manager activity openings")
        try: linked_id=uuid.UUID(_string(result, "continuous_from_record_id"))
        except ValueError as exc: raise AttentionValidationError("continuous_from_record_id must be UUIDv4") from exc
        if linked_id.version != 4: raise AttentionValidationError("continuous_from_record_id must be UUIDv4")
    if result["kind"] in {"MANAGER_WAIT_STARTED", "MANAGER_WAIT_FINISHED", "MANAGER_TOOL_STARTED", "MANAGER_TOOL_FINISHED", "MANAGER_ABSENCE_STARTED", "MANAGER_ABSENCE_FINISHED"}: _string(result, "activity_id")
    if result["kind"] in {"MANAGER_ABSENCE_STARTED", "MANAGER_ABSENCE_FINISHED"}:
        for field in ("supervisor_id", "session_lock_id", "registry_generation", "process_snapshot_hash", "registered_manager_identities"): _string(result, field)
        registered=result.get("registered_invocation_ids")
        if not isinstance(registered,list) or not registered or not all(isinstance(v,str) and v for v in registered): raise AttentionValidationError("registered_invocation_ids must be a non-empty string list")
        if result.get("process_snapshot_complete") is not True: raise AttentionValidationError("absence records require complete process snapshot")
    if canonical:
        for field in ("source_path", "source_role", "source_id", "source_generation", "source_record_sha256"):_string(result, field)
        if result["source_role"] not in SOURCE_ROLES: raise AttentionValidationError("canonical source_role is not allowed")
        if result["source_role"] in PRODUCER_ROLES and result["recorder"] != result["source_id"]: raise AttentionValidationError("producer recorder must equal canonical source_id")
        if result["kind"] == "WATCHER_NOTIFICATION_SENT" and result["source_role"] != "subagent": raise AttentionValidationError("watcher notification requires subagent provenance")
        if result["kind"] in WAKE_ATTEMPT_KINDS and result["source_role"] != "harness": raise AttentionValidationError("wake attempt records require harness provenance")
        if result["kind"] == "MANAGER_WAKE_RECEIVED" and result["source_role"] != "orchestrator": raise AttentionValidationError("wake receipt requires orchestrator provenance")
        if result["kind"] == "MANAGER_WAIT_FINISHED" and "wake_id" in result and result["source_role"] != "orchestrator": raise AttentionValidationError("wake-bearing wait finish requires orchestrator provenance")
        if result["kind"] == "MANAGER_WAIT_FINISHED" and result.get("wake_transport") == "collaboration.send_message" and result["source_role"] != "orchestrator": raise AttentionValidationError("wake receipt requires orchestrator provenance")
        if result["kind"] in {"MANAGER_ABSENCE_STARTED", "MANAGER_ABSENCE_FINISHED"} and (result["source_role"] != "supervisor" or result["source_id"] != result["supervisor_id"]): raise AttentionValidationError("absence records require supervisor provenance")
        if not isinstance(result.get("byte_start"),int) or not isinstance(result.get("byte_end"),int) or result["byte_start"]<0 or result["byte_end"]<result["byte_start"]: raise AttentionValidationError("canonical record requires valid byte range")
        source_form={k:v for k,v in result.items() if k not in {"observed_timestamp_utc","source_path","source_role","source_id","source_generation","byte_start","byte_end","source_record_sha256"}}
        if result["source_record_sha256"] != source_digest(source_form): raise AttentionValidationError("canonical source_record_sha256 does not bind source bytes")
    return result

def make_source_record(*, recorder: str, epoch_id: str, event_id: str, kind: str, metadata: Mapping[str, Any] | None = None, record_id: str | None = None, source_timestamp_utc: str | None = None) -> dict[str, Any]:
    result = {"schema": SCHEMA, "record_id": record_id or str(uuid.uuid4()), "source_timestamp_utc": source_timestamp_utc or utc_now(), "epoch_id": epoch_id, "event_id": event_id, "kind": kind, "recorder": recorder}
    supplied=dict(metadata or {})
    reserved={"observed_timestamp_utc","source_path","source_role","source_id","source_generation","byte_start","byte_end","source_record_sha256"}
    if reserved.intersection(supplied): raise AttentionValidationError("source metadata contains reserved canonical provenance field")
    result.update(supplied); validate_record(result); return result

def canonicalize(record: Mapping[str, Any], *, observed_timestamp_utc: str, source_path: str, source_role: str, source_id: str, source_generation: str, byte_start: int, byte_end: int) -> dict[str, Any]:
    source = validate_record(record)
    canonical = dict(source)
    canonical.update({"observed_timestamp_utc": observed_timestamp_utc, "source_path": source_path, "source_role": source_role, "source_id": source_id, "source_generation": source_generation, "byte_start": byte_start, "byte_end": byte_end, "source_record_sha256": source_digest(source)})
    return validate_record(canonical, canonical=True)

def _observed(record: Mapping[str, Any]) -> datetime:
    return datetime.fromisoformat(str(record.get("observed_timestamp_utc") or record["source_timestamp_utc"]).replace("Z", "+00:00"))
def _deadline(record: Mapping[str, Any], name: str) -> datetime | None:
    value=record.get(name)
    return _utc_timestamp(value) if isinstance(value,str) else None

def _duration(a: Mapping[str, Any] | None, b: Mapping[str, Any] | None) -> float | None:
    return (_source_time(b)-_source_time(a)).total_seconds() if a and b else None

def _deferral_duration(deferrals: Iterable[Mapping[str, Any]], pending: Mapping[str, Any] | None) -> float | None:
    """Measure only an explicit deferral that causally precedes pending work."""
    if pending is None:
        return None
    eligible=[record for record in deferrals if _source_time(record) <= _source_time(pending)]
    return _duration(max(eligible,key=_source_time),pending) if eligible else None

def _publication_evidence(
    publications: Iterable[Mapping[str, Any]],
    signal: Mapping[str, Any] | None,
    observed: Mapping[str, Any] | None,
) -> tuple[Mapping[str, Any] | None, str | None, str | None]:
    """Return the one causally usable final-publication record, or fail closed.

    A worker creation record says that a request exists; it does not prove the
    final manager-signal file was visible to the harness.  This narrow boundary
    prevents the watcher from attributing worker publication time to the
    harness.
    """
    if signal is None:
        return None, "missing AGENT_SIGNAL_CREATED", None
    candidates=list(publications)
    if len(candidates) != 1:
        return None, "unique AGENT_SIGNAL_PUBLISHED record", None
    publication=candidates[0]
    if publication.get("signal_id") != signal["event_id"]:
        return None, None, "publication contradicts signal signal_id"
    for field in ("lane_id", "agent_blocked"):
        if publication.get(field) != signal.get(field):
            return None, None, f"publication contradicts signal {field}"
    for field in ("delivery_deadline_utc", "response_deadline_utc"):
        if publication.get(field) != signal.get(field):
            return None, None, f"publication contradicts signal {field}"
    published_at=_source_time(publication)
    if published_at < _source_time(signal):
        return None, None, "publication precedes signal creation"
    if observed is not None and published_at > _source_time(observed):
        return None, None, "publication follows harness observation"
    delivery_deadline=_deadline(signal, "delivery_deadline_utc")
    if delivery_deadline is not None and published_at > delivery_deadline:
        return None, None, "publication follows delivery deadline"
    return publication, None, None

def _paired_manager_intervals(items: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return only explicit, unambiguous manager activity intervals.

    An interval is evidence, not a state estimate: both endpoints must be
    canonical and share the manager session/invocation and their prescribed
    correlation key.  Callers use this to prove continuous coverage; gaps are
    deliberately left unknown.
    """
    intervals: list[dict[str, Any]] = []
    pair_specs = (
        ("MANAGER_WAIT_STARTED", "MANAGER_WAIT_FINISHED", "idle_or_absent", "activity"),
        ("MANAGER_TOOL_STARTED", "MANAGER_TOOL_FINISHED", "busy", "activity"),
    )
    for opened_kind, closed_kind, cause, _ in pair_specs:
        for opened in items:
            if opened["kind"] != opened_kind:
                continue
            activity = opened.get("activity_id")
            session, invocation = opened.get("manager_session_id"), opened.get("manager_invocation_id")
            if not all(isinstance(v, str) and v for v in (activity, session, invocation)):
                continue
            matches = [closed for closed in items if closed["kind"] == closed_kind and closed.get("activity_id") == activity and closed.get("manager_session_id") == session and closed.get("manager_invocation_id") == invocation and _observed(closed) >= _observed(opened)]
            if len(matches) == 1:
                closed = matches[0]
                intervals.append({"start": _observed(opened), "end": _observed(closed), "start_record_id": opened["record_id"], "end_record_id": closed["record_id"], "activity_id": activity, "related_event_id": opened.get("related_event_id"), "manager_session_id": session, "manager_invocation_id": invocation, "cause": cause})
    # Real CLI recorder calls have separate timestamps.  An opening may make
    # that transition explicit by naming the exact prior terminal it continues
    # from; this is the only non-touching bridge the analyzer accepts.
    openings = [r for r in items if r["kind"] in {"MANAGER_EVENT_CLAIMED", "MANAGER_REVIEW_STARTED", "MANAGER_WAIT_STARTED", "MANAGER_TOOL_STARTED"} and isinstance(r.get("continuous_from_record_id"), str)]
    references: dict[str, list[Mapping[str, Any]]] = {}
    for opening in openings:
        references.setdefault(str(opening["continuous_from_record_id"]), []).append(opening)
    terminals = {"MANAGER_WAIT_FINISHED", "MANAGER_TOOL_FINISHED", "MANAGER_RESPONSE_PUBLISHED"}
    for linked_id, successors in references.items():
        # A terminal can lead into precisely one next activity.  A fork is not
        # a continuous single-manager timeline and is intentionally unknown.
        if len(successors) != 1:
            continue
        opening = successors[0]
        terminal_matches = [r for r in items if r["record_id"] == linked_id and (r["kind"] in terminals or (r["kind"] in {"MANAGER_DECISION_RECORDED", "MANAGER_CHECKPOINT_COMPLETED"} and r.get("terminal_for_activity") is True))]
        if len(terminal_matches) != 1:
            continue
        terminal = terminal_matches[0]
        session, invocation = opening.get("manager_session_id"), opening.get("manager_invocation_id")
        # Host observation time must be strictly ordered.  Equal timestamps do
        # not prove which JSONL record was ingested first, so they cannot close
        # a cycle or fabricate continuity.
        if not all(isinstance(v, str) and v for v in (session, invocation)) or terminal.get("manager_session_id") != session or terminal.get("manager_invocation_id") != invocation or _observed(terminal) >= _observed(opening):
            continue
        cause = "idle_or_absent" if opening["kind"] == "MANAGER_WAIT_STARTED" else "busy"
        intervals.append({"start": _observed(terminal), "end": _observed(opening), "start_record_id": terminal["record_id"], "end_record_id": opening["record_id"], "activity_id": opening.get("activity_id") or opening.get("event_id"), "related_event_id": opening.get("event_id"), "manager_session_id": session, "manager_invocation_id": invocation, "cause": cause})
    # Claim-to-response is a complete busy interval for the exact event.  A
    # decision/checkpoint may close it only when the producer explicitly marks
    # that terminal boundary, avoiding an invented endpoint.
    for claim in items:
        if claim["kind"] != "MANAGER_EVENT_CLAIMED":
            continue
        session, invocation = claim.get("manager_session_id"), claim.get("manager_invocation_id")
        if not all(isinstance(v, str) and v for v in (session, invocation)):
            continue
        event = claim.get("event_id")
        responses = [r for r in items if r.get("event_id") == event and r.get("manager_session_id") == session and r.get("manager_invocation_id") == invocation and _observed(r) >= _observed(claim) and r["kind"] == "MANAGER_RESPONSE_PUBLISHED"]
        terminals = [r for r in items if r.get("event_id") == event and r.get("manager_session_id") == session and r.get("manager_invocation_id") == invocation and _observed(r) >= _observed(claim) and r["kind"] in {"MANAGER_DECISION_RECORDED", "MANAGER_CHECKPOINT_COMPLETED"} and r.get("terminal_for_activity") is True]
        matches = responses if responses else terminals
        if len(matches) == 1:
            closed = matches[0]
            intervals.append({"start": _observed(claim), "end": _observed(closed), "start_record_id": claim["record_id"], "end_record_id": closed["record_id"], "activity_id": event, "related_event_id": event, "manager_session_id": session, "manager_invocation_id": invocation, "cause": "busy"})
    # Formal review has the same explicit completion rule, but its start is not
    # a claim and carries the review event identity directly.
    for opened in items:
        if opened["kind"] != "MANAGER_REVIEW_STARTED":
            continue
        session, invocation = opened.get("manager_session_id"), opened.get("manager_invocation_id")
        if not all(isinstance(v, str) and v for v in (session, invocation)):
            continue
        event = opened.get("event_id")
        responses = [r for r in items if r.get("event_id") == event and r.get("manager_session_id") == session and r.get("manager_invocation_id") == invocation and _observed(r) >= _observed(opened) and r["kind"] == "MANAGER_RESPONSE_PUBLISHED"]
        terminals = [r for r in items if r.get("event_id") == event and r.get("manager_session_id") == session and r.get("manager_invocation_id") == invocation and _observed(r) >= _observed(opened) and r["kind"] == "MANAGER_CHECKPOINT_COMPLETED" and r.get("terminal_for_activity") is True]
        matches = responses if responses else terminals
        if len(matches) == 1:
            closed = matches[0]
            intervals.append({"start": _observed(opened), "end": _observed(closed), "start_record_id": opened["record_id"], "end_record_id": closed["record_id"], "activity_id": event, "related_event_id": event, "manager_session_id": session, "manager_invocation_id": invocation, "cause": "busy"})
    # Validated supervisor absence is intentionally narrow and uses its existing
    # authority checks before becoming an idle/absence interval.
    for opened in items:
        if opened["kind"] != "MANAGER_ABSENCE_STARTED":
            continue
        for closed in items:
            if closed["kind"] != "MANAGER_ABSENCE_FINISHED" or closed.get("activity_id") != opened.get("activity_id") or _observed(closed) < _observed(opened):
                continue
            if _valid_absence(items, _observed(opened), _observed(closed)):
                intervals.append({"start": _observed(opened), "end": _observed(closed), "start_record_id": opened["record_id"], "end_record_id": closed["record_id"], "activity_id": opened.get("activity_id"), "related_event_id": None, "manager_session_id": None, "manager_invocation_id": None, "cause": "idle_or_absent"})
    return intervals


def _causal_interval_cause(items: list[Mapping[str, Any]], start: datetime, end: datetime, *, manager_identity: tuple[str, str] | None = None) -> str | None:
    """Prove one gap-free causal chain over [start, end], else return None."""
    if end < start:
        return None
    intervals = _paired_manager_intervals(items)
    # Group by exact persistent invocation.  Absence is a distinct supervisor
    # proof and must cover the entire window by itself.
    groups: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for interval in intervals:
        groups.setdefault((interval["manager_session_id"], interval["manager_invocation_id"]), []).append(interval)
    for identity, group in groups.items():
        # A late claim/review belongs to one exact root invocation.  Never use
        # an unrelated invocation's earlier activity as its explanation.
        if manager_identity is not None and identity != manager_identity:
            continue
        # Supervisor absence is not an active invocation and is usable only
        # when there is no active late endpoint identity to contradict it.
        if manager_identity is not None and identity == (None, None):
            continue
        # Overlapping independent activity makes causal attribution ambiguous.
        ordered = sorted(group, key=lambda interval: (interval["start"], interval["end"]))
        if any(next_interval["start"] < interval["end"] for interval, next_interval in zip(ordered, ordered[1:])):
            continue
        cursor = start
        causes: set[str] = set()
        selected: list[dict[str, Any]] = []
        for interval in ordered:
            if interval["end"] < cursor or interval["start"] > cursor:
                continue
            selected.append(interval); causes.add(interval["cause"])
            if interval["end"] > cursor:
                cursor = interval["end"]
            if cursor >= end:
                return "BUSY_MANAGER_DELAY" if "busy" in causes else "IDLE_OR_ABSENT_MANAGER_DELAY"
    return None

def _wait_spans_deadline_then_exact_claim(
    items: list[Mapping[str, Any]], *, event_id: str, deadline: datetime
) -> bool:
    """Prove the sealed wake sequence without requiring the wait to outlive claim."""
    for opened in items:
        if opened["kind"] != "MANAGER_WAIT_STARTED" or _observed(opened) > deadline:
            continue
        activity = opened.get("activity_id")
        session = opened.get("manager_session_id")
        invocation = opened.get("manager_invocation_id")
        if not all(isinstance(value, str) and value for value in (activity, session, invocation)):
            continue
        for closed in items:
            if (
                closed["kind"] != "MANAGER_WAIT_FINISHED"
                or closed.get("activity_id") != activity
                or closed.get("manager_session_id") != session
                or closed.get("manager_invocation_id") != invocation
                or closed.get("wake_transport") != "collaboration.send_message"
                or _observed(closed) < deadline
            ):
                continue
            finished = _observed(closed)
            for claim in items:
                if (
                    claim["kind"] != "MANAGER_EVENT_CLAIMED"
                    or claim.get("event_id") != event_id
                    or claim.get("manager_session_id") != session
                    or claim.get("manager_invocation_id") != invocation
                ):
                    continue
                claimed = _observed(claim)
                if claimed < finished or (claimed - finished).total_seconds() > 10:
                    continue
                contradictory = any(
                    _observed(other) >= _observed(opened)
                    and _observed(other) <= claimed
                    and (
                        (
                            other.get("kind") in {"MANAGER_INVOCATION_STARTED", "MANAGER_INVOCATION_FINISHED"}
                            and other.get("manager_invocation_id") != invocation
                        )
                        or (
                            other.get("kind") in {"MANAGER_WAIT_STARTED", "MANAGER_TOOL_STARTED"}
                            and other.get("activity_id") != activity
                        )
                    )
                    for other in items
                )
                if not contradictory:
                    return True
    return False

def _late_actionability_attribution(items: list[Mapping[str, Any]], *, late_window_start: datetime, actionable: Mapping[str, Any]) -> str:
    """Attribute a late actionable endpoint without excusing late observation.

    Only a gap-free explicit manager interval covering the actual late window
    can move the delay away from the harness. Time before the delivery target
    is not part of the overrun. A manager still waiting for the native harness
    result is not manager work, so its complete wait interval deliberately
    remains a harness-delivery delay.
    """
    start, end = late_window_start, _observed(actionable)
    cause = _causal_interval_cause(items, start, end)
    if cause == "BUSY_MANAGER_DELAY":
        return cause
    if cause is not None:
        return "HARNESS_DELIVERY_DELAY"
    if any(
        opened["kind"] == "MANAGER_WAIT_STARTED"
        and opened.get("activity_id") == closed.get("activity_id")
        and opened.get("manager_session_id") == closed.get("manager_session_id")
        and opened.get("manager_invocation_id") == closed.get("manager_invocation_id")
        and closed["kind"] == "MANAGER_WAIT_FINISHED"
        and closed.get("wake_transport") == "blocking_harness_wait_stdout"
        and isinstance(closed.get("wake_id"), str)
        and _observed(opened) <= start
        and end <= _observed(closed)
        for opened in items for closed in items
    ):
        return "HARNESS_DELIVERY_DELAY"
    activity_kinds = {
        "MANAGER_WAIT_STARTED", "MANAGER_WAIT_FINISHED", "MANAGER_TOOL_STARTED", "MANAGER_TOOL_FINISHED",
        "MANAGER_EVENT_CLAIMED", "MANAGER_REVIEW_STARTED", "MANAGER_DECISION_RECORDED",
        "MANAGER_CHECKPOINT_COMPLETED", "MANAGER_RESPONSE_PUBLISHED",
    }
    intervals = _paired_manager_intervals(items)
    if any(interval["start"] <= end and start <= interval["end"] for interval in intervals):
        return "INSUFFICIENT_EVIDENCE"
    paired_openings = {interval["start_record_id"] for interval in intervals}
    opening_kinds = {"MANAGER_WAIT_STARTED", "MANAGER_TOOL_STARTED", "MANAGER_EVENT_CLAIMED", "MANAGER_REVIEW_STARTED"}
    if any(record["kind"] in opening_kinds and _observed(record) <= end and record["record_id"] not in paired_openings for record in items):
        return "INSUFFICIENT_EVIDENCE"
    if any(record["kind"] in activity_kinds and start <= _observed(record) <= end for record in items):
        return "INSUFFICIENT_EVIDENCE"
    return "HARNESS_DELIVERY_DELAY"

def _valid_absence(items: list[Mapping[str, Any]], start: datetime, end: datetime) -> bool:
    for opened in items:
        if opened["kind"] != "MANAGER_ABSENCE_STARTED" or _observed(opened)>start: continue
        activity=opened.get("activity_id"); registered=set(opened.get("registered_invocation_ids",[])); identities=opened.get("registered_manager_identities")
        if not isinstance(activity,str) or not activity or not registered: continue
        if opened.get("source_role") != "supervisor" or opened.get("source_id") != opened.get("supervisor_id"): continue
        # Absence only follows a recorded terminal boundary for each registered turn.
        if not all(any(r["kind"]=="MANAGER_INVOCATION_FINISHED" and r.get("manager_invocation_id")==inv and _observed(r)<=_observed(opened) for r in items) for inv in registered): continue
        for closed in items:
            if closed["kind"]!="MANAGER_ABSENCE_FINISHED" or closed.get("activity_id")!=activity or _observed(closed)<end: continue
            exact=("supervisor_id","session_lock_id","registry_generation","registered_manager_identities")
            if closed.get("source_role") != "supervisor" or closed.get("source_id") != closed.get("supervisor_id"): continue
            if any(closed.get(k)!=opened.get(k) for k in exact) or set(closed.get("registered_invocation_ids",[]))!=registered: continue
            # A new/unregistered invocation during the alleged absence contradicts it; closure must precede the next registered start.
            starts=[r for r in items if r["kind"]=="MANAGER_INVOCATION_STARTED" and _observed(opened)<=_observed(r)<=_observed(closed)]
            if starts: continue
            if not isinstance(identities,str) or not identities: continue
            return True
    return False

def _complete_delivery_coverage(items: list[Mapping[str, Any]], signal: Mapping[str, Any], deadline: datetime, trusted_coverage: Mapping[str, Mapping[str, Any]] | None) -> bool:
    scans=sorted((r for r in items if r["kind"]=="HARNESS_SCAN_COMMITTED"),key=lambda r: _deadline(r,"coverage_start_utc") or _observed(r))
    if not scans: return False
    previous=None; expected=None; process=None; root=None; source=None; cursor=_observed(signal)
    for scan in scans:
        try:
            begin=_deadline(scan,"coverage_start_utc"); end=_deadline(scan,"coverage_end_utc"); sequence=scan["scan_sequence"]; pid=scan["harness_pid"]; created=scan["harness_created_utc"]; output=scan["output_root_identity"]
        except (KeyError, TypeError): return False
        if begin is None or end is None or end < begin or _observed(scan) < end or not isinstance(sequence,int) or not isinstance(pid,int) or not isinstance(created,str) or not isinstance(output,str): return False
        if scan.get("cursor_complete") is not True or scan.get("source_complete") is not True or scan.get("integrity_error") is True: return False
        # Harness self-claims are not enough: watcher cursor state must prove this
        # exact source generation is drained through the canonical scan bytes.
        if trusted_coverage is None: return False
        coverage=trusted_coverage.get(str(scan.get("source_path")))
        if not isinstance(coverage, Mapping) or coverage.get("generation") != scan.get("source_generation") or coverage.get("partial_bytes") != 0 or coverage.get("error") is True or int(coverage.get("offset", -1)) < int(scan.get("byte_end", -1)): return False
        identity=(pid,created); source_identity=(scan.get("source_generation"),scan.get("source_path"))
        if expected is not None and (sequence!=expected or scan.get("previous_committed_scan_record_id")!=previous): return False
        if process is not None and process!=identity or root is not None and root!=output or source is not None and source!=source_identity: return False
        expected=sequence+1; previous=scan["record_id"]; process=identity; root=output; source=source_identity
        # Ordered windows must touch/overlap the already proven interval; no temporal gap.
        if end < cursor: continue
        if begin > cursor: return False
        cursor=max(cursor,end)
        if cursor >= deadline: return True
    return False

def _stage_deadline(records: list[Mapping[str, Any]], field: str, contradictions: list[str]) -> datetime | None:
    values=[_deadline(r,field) for r in records if _deadline(r,field) is not None]
    if not values: return None
    if any(v != values[0] for v in values[1:]): contradictions.append(f"contradictory {field}"); return None
    return values[0]

def _manager_available_at(records: Iterable[Mapping[str, Any]]) -> datetime | None:
    """Return proven persistent-root notification time, not source-observation time."""
    notifications=[record for record in records if record["kind"] == "WATCHER_NOTIFICATION_SENT" and record.get("wake_transport") == "collaboration.send_message" and record.get("delivery_succeeded") is True and record.get("source_role") == "subagent"]
    if not notifications:
        return None
    # A matching root wait receipt corroborates a wake sequence elsewhere; it
    # cannot predate or replace the successful watcher-to-root notification.
    return min(_observed(record) for record in notifications)

def _source_time(record: Mapping[str, Any]) -> datetime:
    return _utc_timestamp(str(record["source_timestamp_utc"]))

def _wake_evidence(records: Iterable[Mapping[str, Any]], *, event_id: str) -> dict[str, Any]:
    """Validate production blocking-wake attempts without inferring missing facts."""
    items=list(records); attempts=[r for r in items if r["event_id"] == event_id and r["kind"] == "MANAGER_WAKE_ATTEMPTED"]
    if not attempts: return {"status":"NOT_APPLICABLE"}
    signal=next((r for r in items if r["event_id"] == event_id and r["kind"] == "AGENT_SIGNAL_CREATED"),None)
    observed=next((r for r in items if r["event_id"] == event_id and r["kind"] == "HARNESS_SIGNAL_OBSERVED"),None)
    if signal is None or observed is None: return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake source stages are missing"}
    seen=set(); successful=[]; failure_kinds=[]
    for attempt in attempts:
        wake_id=attempt["wake_id"]
        if wake_id in seen: return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake_id reused"}
        seen.add(wake_id)
        outcomes=[r for r in items if r["event_id"] == event_id and r.get("wake_id") == wake_id and r["kind"] in {"MANAGER_WAKE_DELIVERED","MANAGER_WAKE_FAILED"}]
        if len(outcomes) != 1: return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake attempt requires exactly one outcome"}
        outcome=outcomes[0]
        fields=("wake_component","wake_transport","manager_session_id","manager_invocation_id")
        if any(outcome.get(field) != attempt.get(field) for field in fields) or not (_source_time(signal) <= _source_time(observed) <= _source_time(attempt) <= _source_time(outcome)): return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake outcome contradicts attempt"}
        receipts=[r for r in items if r["event_id"] == event_id and r["kind"] == "MANAGER_WAKE_RECEIVED" and r.get("wake_id") == wake_id]
        if outcome["kind"] == "MANAGER_WAKE_FAILED":
            if receipts: return {"status":"INSUFFICIENT_EVIDENCE","reason":"failed wake has receipt"}
            failure_kinds.append(outcome["failure_kind"]); continue
        successful.append((attempt,outcome,receipts))
    if not successful: return {"status":"FAILED","failure_kind":failure_kinds[-1]}
    if len(successful) != 1: return {"status":"INSUFFICIENT_EVIDENCE","reason":"multiple successful wake attempts"}
    attempt, delivered, receipts=successful[0]
    if len(receipts) != 1 or any(receipts[0].get(field) != attempt.get(field) for field in ("wake_transport","manager_session_id","manager_invocation_id")): return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake receipt is missing or mismatched"}
    receipt=receipts[0]
    finishes=[r for r in items if r["event_id"] == event_id and r["kind"] == "MANAGER_WAIT_FINISHED" and r.get("wake_id") == attempt["wake_id"] and r.get("wake_transport") == attempt["wake_transport"] and r.get("manager_session_id") == attempt["manager_session_id"] and r.get("manager_invocation_id") == attempt["manager_invocation_id"]]
    claims=[r for r in items if r["event_id"] == event_id and r["kind"] == "MANAGER_EVENT_CLAIMED" and r.get("manager_session_id") == attempt["manager_session_id"] and r.get("manager_invocation_id") == attempt["manager_invocation_id"]]
    if len(finishes) != 1 or len(claims) != 1: return {"status":"INSUFFICIENT_EVIDENCE","reason":"matching wait finish or claim is missing"}
    claim=claims[0]; finish=finishes[0]
    if not (_source_time(signal) <= _source_time(observed) <= _source_time(attempt) <= _source_time(delivered) <= _source_time(receipt) <= _source_time(claim)):
        return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake source timestamps are out of order"}
    if not (_source_time(delivered) <= _source_time(finish) <= _source_time(claim)):
        return {"status":"INSUFFICIENT_EVIDENCE","reason":"wake wait finish is out of order"}
    actions=[r for r in items if r["kind"].startswith("MANAGER_") and r.get("manager_session_id") == attempt["manager_session_id"] and r.get("manager_invocation_id") == attempt["manager_invocation_id"] and _source_time(delivered) < _source_time(r) < _source_time(receipt)]
    if actions: return {"status":"INSUFFICIENT_EVIDENCE","reason":"manager acted before wake receipt"}
    return {"status":"COMPLETE","wake_id":attempt["wake_id"],"manager_session_id":attempt["manager_session_id"],"manager_invocation_id":attempt["manager_invocation_id"],"attempted_to_delivered_seconds":(_source_time(delivered)-_source_time(attempt)).total_seconds(),"delivered_to_received_seconds":(_source_time(receipt)-_source_time(delivered)).total_seconds(),"received_to_claim_seconds":(_source_time(claim)-_source_time(receipt)).total_seconds()}

def _invocation_is_active_for_response(items: Iterable[Mapping[str, Any]], claim: Mapping[str, Any], response: Mapping[str, Any]) -> bool:
    """Require a real invocation lifetime, rather than trusting equal ID strings."""
    session=claim.get("manager_session_id"); invocation=claim.get("manager_invocation_id")
    if not isinstance(session,str) or not session or not isinstance(invocation,str) or not invocation:
        return False
    if response.get("manager_session_id") != session or response.get("manager_invocation_id") != invocation:
        return False
    claimed_at=_observed(claim); responded_at=_observed(response)
    if not any(record["kind"] == "MANAGER_INVOCATION_STARTED" and record.get("manager_session_id") == session and record.get("manager_invocation_id") == invocation and _observed(record) < claimed_at for record in items):
        return False
    return not any(record["kind"] == "MANAGER_INVOCATION_FINISHED" and record.get("manager_session_id") == session and record.get("manager_invocation_id") == invocation and _observed(record) < responded_at for record in items)

def _valid_ineligibility_evidence(
    records: Iterable[Mapping[str, Any]], observed: Mapping[str, Any] | None
) -> str | None:
    """Accept one exact post-observation non-live explanation, else ignore it.

    An explanation is diagnostic evidence only.  It cannot erase a proven
    actionable transition or make malformed, stale, duplicate, or unrelated
    evidence suppress an independently established delivery delay.
    """
    if observed is None or not isinstance(observed.get("harness_event_id"), str):
        return None
    candidates = [r for r in records if r["kind"] == "HARNESS_EVENT_INELIGIBLE"]
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    reason = candidate.get("ineligibility_reason")
    if not isinstance(reason, str) or reason not in INELIGIBILITY_REASONS:
        return None
    if (
        candidate.get("source_role") != "harness"
        or candidate.get("harness_event_id") != observed["harness_event_id"]
        or _observed(candidate) < _observed(observed)
        or any(r["kind"] == "HARNESS_EVENT_ACTIONABLE" for r in records)
    ):
        return None
    return reason


def analyze_event(records: Iterable[Mapping[str, Any]], *, epoch_id: str, event_id: str, trusted_coverage: Mapping[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Pure, evidence-bound v1 diagnosis.  Manager activity is epoch-wide by design."""
    all_items=[]; contradictions=[]
    for raw in records:
        if raw.get("epoch_id") != epoch_id: continue
        if "observed_timestamp_utc" not in raw:
            contradictions.append("diagnosis requires canonical host-observed record"); continue
        try: item=validate_record(raw, canonical=True)
        except AttentionValidationError as exc: contradictions.append(str(exc)); continue
        if item["event_id"]==event_id or item["kind"].startswith("MANAGER_") or item["kind"]=="HARNESS_SCAN_COMMITTED": all_items.append(item)
    all_items.sort(key=_observed); target=[r for r in all_items if r["event_id"]==event_id]
    by={}
    for r in target: by.setdefault(r["kind"],[]).append(r)
    first=lambda kind:by.get(kind,[None])[0]
    signal, observed, actionable, pending = first("AGENT_SIGNAL_CREATED"),first("HARNESS_SIGNAL_OBSERVED"),first("HARNESS_EVENT_ACTIONABLE"),first("HARNESS_EVENT_PENDING")
    publication, publication_missing, publication_contradiction = _publication_evidence(by.get("AGENT_SIGNAL_PUBLISHED", []), signal, observed)
    claim,decision,response,receipt,resumed,ack=(first("MANAGER_EVENT_CLAIMED"),first("MANAGER_DECISION_RECORDED"),first("MANAGER_RESPONSE_PUBLISHED"),first("AGENT_RESPONSE_RECEIVED"),first("AGENT_WORK_RESUMED"),first("HARNESS_ACK_SUCCEEDED"))
    baseline=first("FORMAL_REVIEW_BASELINE_ADVANCED"); review=first("MANAGER_REVIEW_STARTED")
    metrics={"creation_to_publication_seconds":_duration(signal,publication),"signal_to_observation_seconds":_duration(publication,observed),"publication_to_observation_seconds":_duration(publication,observed),"observation_to_actionable_seconds":_duration(observed,actionable),"pending_to_manager_claim_seconds":_duration(pending,claim),"claim_to_decision_seconds":_duration(claim,decision),"response_to_agent_receipt_seconds":_duration(response,receipt),"receipt_to_resume_seconds":_duration(receipt,resumed),"explicit_blocked_seconds":_duration(signal,resumed),"deadline_lateness_seconds":None,"response_to_ack_seconds":_duration(response,ack),"explicit_deferral_seconds":_deferral_duration(by.get("HARNESS_EVENT_DEFERRED",[]),pending)}
    for metric_name, metric_value in metrics.items():
        if metric_value is not None and metric_value < 0: contradictions.append(f"negative causal metric: {metric_name}")
    missing=[]; classification="INSUFFICIENT_EVIDENCE"
    all_deadlines=("delivery_deadline_utc","response_deadline_utc","ack_deadline_utc","formal_review_due_utc","safety_deadline_utc","lease_deadline_utc")
    blocked_signal = signal is not None and signal.get("agent_blocked") is True and not any(r.get("non_blocking") is True or r.get("continue_without_manager") is True for r in target)
    response_deadline = _stage_deadline(target,"response_deadline_utc",contradictions)
    availability = _manager_available_at(target)
    production_wake=_wake_evidence(all_items,event_id=event_id)
    ineligible = _valid_ineligibility_evidence(target, observed)
    if ineligible is not None:
        missing.append(f"manager signal was explicitly ineligible: {ineligible}")
    if production_wake.get("status") == "COMPLETE":
        received=next((record for record in target if record["kind"] == "MANAGER_WAKE_RECEIVED" and record.get("wake_id") == production_wake.get("wake_id")),None)
        if received is not None: availability=_source_time(received)
    if any(r.get("non_blocking") is True and r.get("agent_blocked") is False for r in target) and not any(_deadline(r,k) for r in target for k in all_deadlines): classification="NO_BLOCKING_IMPACT"
    # A delivery-stage miss predates, and therefore takes precedence over, a
    # healthy manager response under a later response deadline.
    elif signal and (delivery_deadline:=_deadline(signal,"delivery_deadline_utc")):
        endpoints=[r for r in target if r["kind"] == "HARNESS_EVENT_ACTIONABLE"]
        endpoints.extend(r for r in target if r["kind"] == "WATCHER_NOTIFICATION_SENT" and r.get("wake_transport") == "collaboration.send_message" and r.get("delivery_succeeded") is True and r.get("source_role") == "subagent")
        late=[r for r in endpoints if _observed(r)>delivery_deadline]
        if late:
            endpoint=min(late,key=_observed)
            if endpoint["kind"] == "HARNESS_EVENT_ACTIONABLE" and observed is not None and _observed(observed) <= delivery_deadline:
                classification=_late_actionability_attribution(all_items,late_window_start=delivery_deadline,actionable=endpoint)
                if classification == "INSUFFICIENT_EVIDENCE": missing.append("complete explicit manager interval chain")
            else: classification="HARNESS_DELIVERY_DELAY"
            metrics["deadline_lateness_seconds"]=(_observed(endpoint)-delivery_deadline).total_seconds()
        elif actionable is None and _complete_delivery_coverage(all_items,signal,delivery_deadline,trusted_coverage):
            classification="HARNESS_DELIVERY_DELAY"
        elif blocked_signal and response_deadline is not None:
            timely_notifications=[r for r in target if r["kind"] == "WATCHER_NOTIFICATION_SENT" and r.get("wake_transport") == "collaboration.send_message" and r.get("delivery_succeeded") is True and r.get("source_role") == "subagent" and _observed(r) <= response_deadline]
            timely_claims=[r for r in by.get("MANAGER_EVENT_CLAIMED",[]) if _observed(r) <= response_deadline]
            timely_responses=[r for r in by.get("MANAGER_RESPONSE_PUBLISHED",[]) if _observed(r) <= response_deadline]
            healthy=(bool(timely_notifications) or (production_wake.get("status") == "COMPLETE" and availability is not None and availability <= response_deadline)) and any(_observed(claim_record) <= _observed(response_record) and _invocation_is_active_for_response(all_items,claim_record,response_record) and (production_wake.get("status") != "COMPLETE" or (claim_record.get("manager_session_id") == production_wake.get("manager_session_id") and claim_record.get("manager_invocation_id") == production_wake.get("manager_invocation_id"))) for claim_record in timely_claims for response_record in timely_responses)
            if healthy: classification="NO_BLOCKING_IMPACT"
            elif availability is not None and availability > response_deadline:
                classification="HARNESS_DELIVERY_DELAY";metrics["deadline_lateness_seconds"]=(availability-response_deadline).total_seconds()
            elif claim is None or _observed(claim)>response_deadline:
                if availability is not None:
                    end=_observed(claim) if claim else response_deadline
                    cause=("IDLE_OR_ABSENT_MANAGER_DELAY" if _wait_spans_deadline_then_exact_claim(all_items,event_id=event_id,deadline=response_deadline) else _causal_interval_cause(all_items,availability,end,manager_identity=((str(claim.get("manager_session_id")),str(claim.get("manager_invocation_id"))) if claim is not None else None)))
                    if cause is not None: classification=cause;metrics["deadline_lateness_seconds"]=(end-response_deadline).total_seconds() if claim is not None else None
                    else: missing.append("complete explicit manager interval chain")
                else: missing.append("canonical manager-availability evidence")
            else: missing.append("matching on-time manager response identity")
        else: missing.append("matching stage deadline and late endpoint")
    # A fully on-time blocking handoff is complete at manager response.  Agent
    # receipt/resume are retained as separate metrics and do not retroactively
    # make an on-time manager response late.
    elif blocked_signal and response_deadline is not None:
        timely_notifications=[r for r in target if r["kind"] == "WATCHER_NOTIFICATION_SENT" and r.get("wake_transport") == "collaboration.send_message" and r.get("delivery_succeeded") is True and r.get("source_role") == "subagent" and _observed(r) <= response_deadline]
        timely_claims=[r for r in by.get("MANAGER_EVENT_CLAIMED",[]) if _observed(r) <= response_deadline]
        timely_responses=[r for r in by.get("MANAGER_RESPONSE_PUBLISHED",[]) if _observed(r) <= response_deadline]
        healthy = any(
            _observed(claim_record) <= _observed(response_record)
            and _invocation_is_active_for_response(all_items,claim_record,response_record)
            and (production_wake.get("status") != "COMPLETE" or (claim_record.get("manager_session_id") == production_wake.get("manager_session_id") and claim_record.get("manager_invocation_id") == production_wake.get("manager_invocation_id")))
            for claim_record in timely_claims for response_record in timely_responses
        )
        if (timely_notifications or (production_wake.get("status") == "COMPLETE" and availability is not None and availability <= response_deadline)) and healthy:
            classification="NO_BLOCKING_IMPACT"
        elif availability is not None and availability > response_deadline:
            classification="HARNESS_DELIVERY_DELAY";metrics["deadline_lateness_seconds"]=(availability-response_deadline).total_seconds()
        # A response deadline is causal only after the canonical timeline proves
        # this exact event was manager-visible.  Earlier harness delivery
        # lateness must not mask a late claim once that availability proof exists.
        elif claim is None or _observed(claim)>response_deadline:
            if availability is not None:
                end=_observed(claim) if claim else response_deadline
                cause = ("IDLE_OR_ABSENT_MANAGER_DELAY" if _wait_spans_deadline_then_exact_claim(all_items,event_id=event_id,deadline=response_deadline) else _causal_interval_cause(all_items, availability, end, manager_identity=((str(claim.get("manager_session_id")), str(claim.get("manager_invocation_id"))) if claim is not None else None)))
                if cause is not None:
                    classification=cause;metrics["deadline_lateness_seconds"]=(end-response_deadline).total_seconds() if claim is not None else None
                else: missing.append("complete explicit manager interval chain")
            else: missing.append("canonical manager-availability evidence")
        else: missing.append("matching on-time manager response identity")
    elif review is not None and (d:=_stage_deadline(target,"formal_review_due_utc",contradictions)) is not None and _observed(review)>d:
        # A scheduled review has no watcher notification; the self-scheduled due
        # time is its causal window start.  A late start takes precedence over a
        # later baseline acknowledgement delay.
        cause = _causal_interval_cause(all_items, d, _observed(review), manager_identity=(str(review.get("manager_session_id")), str(review.get("manager_invocation_id"))))
        if cause is not None:
            classification=cause;metrics["deadline_lateness_seconds"]=(_observed(review)-d).total_seconds()
        else: missing.append("complete explicit manager interval chain")
    elif ack and (d:=_stage_deadline(target,"ack_deadline_utc",contradictions)) and _observed(ack)>d:
        rd=_stage_deadline(target,"response_deadline_utc",contradictions)
        if response is not None and rd is not None and _observed(response)<=rd and not contradictions: classification="ACKNOWLEDGEMENT_ONLY_DELAY";metrics["deadline_lateness_seconds"]=(_observed(ack)-d).total_seconds()
        else: missing.append("on-time response evidence")
    elif baseline and (d:=_stage_deadline(target,"formal_review_due_utc",contradictions)) and _observed(baseline)>d:
        # A late baseline is acknowledgement-only only after exact-event review work completed on time.
        review_deadline=_stage_deadline(target,"response_deadline_utc",contradictions) or d
        if review is not None and decision is not None and response is not None and _observed(review)<=review_deadline and _observed(decision)<=review_deadline and _observed(response)<=review_deadline and not contradictions:
            classification="ACKNOWLEDGEMENT_ONLY_DELAY";metrics["deadline_lateness_seconds"]=(_observed(baseline)-d).total_seconds()
        else: missing.append("on-time exact-event manager review/decision/response evidence")
    elif review is not None and (d:=_stage_deadline(target,"formal_review_due_utc",contradictions)) is not None and _observed(review)<=d and not contradictions:
        classification="NO_BLOCKING_IMPACT"
    elif pending and (d:=_deadline(pending,"response_deadline_utc")) and (claim is None or _observed(claim)>d):
        if availability is None:
            missing.append("successful watcher notification")
        elif availability > d:
            classification="HARNESS_DELIVERY_DELAY";metrics["deadline_lateness_seconds"]=(availability-d).total_seconds()
        else:
            end=_observed(claim) if claim else d
            cause = _causal_interval_cause(all_items, availability, end, manager_identity=((str(claim.get("manager_session_id")), str(claim.get("manager_invocation_id"))) if claim is not None else None))
            if cause is not None:
                classification=cause;metrics["deadline_lateness_seconds"]=(end-d).total_seconds() if claim is not None else None
            else: missing.append("complete explicit manager interval chain")
    else: missing.append("matching stage deadline and late endpoint")
    wake_evidence=_wake_evidence(all_items, event_id=event_id)
    if wake_evidence["status"] == "INSUFFICIENT_EVIDENCE":
        classification="INSUFFICIENT_EVIDENCE"; missing.append(wake_evidence["reason"])
    elif wake_evidence["status"] == "FAILED": classification="HARNESS_DELIVERY_DELAY"
    elif wake_evidence["status"] == "COMPLETE":
        metrics.update({key:value for key,value in wake_evidence.items() if key.endswith("_seconds")})
    correlated_response=None
    correlated_claims=by.get("MANAGER_EVENT_CLAIMED",[])
    if production_wake.get("status") == "COMPLETE":
        correlated_claims=[candidate for candidate in correlated_claims if candidate.get("manager_session_id") == production_wake.get("manager_session_id") and candidate.get("manager_invocation_id") == production_wake.get("manager_invocation_id")]
    correlated_response=next((candidate for candidate in by.get("MANAGER_RESPONSE_PUBLISHED",[]) if any(_observed(claim_candidate) <= _observed(candidate) and _invocation_is_active_for_response(all_items,claim_candidate,candidate) for claim_candidate in correlated_claims)),None)
    if response_deadline is not None and correlated_response is not None and metrics["deadline_lateness_seconds"] is None:
        metrics["deadline_lateness_seconds"]=max(0.0,(_observed(correlated_response)-response_deadline).total_seconds())
    # A HARNESS_DELIVERY_DELAY is a component-causal claim.  For a blocked
    # signal it is valid only after a single, ordered final publication proves
    # the final JSON was actually available to the harness during that window.
    if classification == "HARNESS_DELIVERY_DELAY" and blocked_signal:
        if publication_missing is not None:
            classification="INSUFFICIENT_EVIDENCE"; missing.append(publication_missing)
        elif publication_contradiction is not None:
            classification="INSUFFICIENT_EVIDENCE"; contradictions.append(publication_contradiction)
    if ineligible is not None:
        classification="INSUFFICIENT_EVIDENCE"
    if contradictions: classification="INSUFFICIENT_EVIDENCE"
    return {"schema":REPORT_SCHEMA,"epoch_id":epoch_id,"event_id":event_id,"classification":classification,"evidence_record_ids":[r["record_id"] for r in target],"missing_evidence":missing,"contradictory_evidence":contradictions,"metrics":metrics,"wake_evidence":wake_evidence}

def _path_safe(root: Path, target: Path) -> None:
    root=root.resolve(); target=target.absolute()
    try: target.resolve(strict=False).relative_to(root)
    except ValueError as exc: raise AttentionValidationError("derived input path escapes runtime root") from exc
    current=root
    for part in target.relative_to(root).parts:
        if current.exists() and current.is_symlink(): raise AttentionValidationError("reparse/symlink component rejected")
        current=current / part
        if current.exists() and current.is_symlink(): raise AttentionValidationError("reparse/symlink component rejected")

def producer_path(runtime_root: Path, role: str, source_id: str) -> Path:
    if role not in PRODUCER_ROLES: raise AttentionValidationError("producer role must be orchestrator or subagent")
    root=runtime_root.resolve(); safe=safe_source_id(source_id); segment=safe+"-"+hashlib.sha256(source_id.encode()).hexdigest()[:16]
    path=root/"inputs"/role/segment/"attention.jsonl"; _path_safe(root,path); return path

@contextmanager
def _advisory_lock(path: Path, timeout: float):
    """A real OS advisory lock; the file is never unlinked while another writer may hold it."""
    fd=os.open(path,os.O_RDWR|os.O_CREAT,0o600)
    deadline=time.monotonic()+timeout
    try:
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(fd,0,os.SEEK_SET)
                    if os.fstat(fd).st_size == 0: os.write(fd,b"0")
                    os.lseek(fd,0,os.SEEK_SET); msvcrt.locking(fd,msvcrt.LK_NBLCK,1)
                else:
                    import fcntl
                    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic()>=deadline: raise TimeoutError("attention input lock timeout")
                time.sleep(.01)
        yield
    finally:
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd,0,os.SEEK_SET); msvcrt.locking(fd,msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(fd,fcntl.LOCK_UN)
        finally: os.close(fd)

def append_producer_record(runtime_root: Path, *, role: str, source_id: str, record: Mapping[str, Any], lock_timeout_seconds: float=5.0) -> Path:
    validate_record(record)
    if record.get("recorder") != source_id: raise AttentionValidationError("record recorder must equal source_id")
    if record.get("kind") not in ROLE_KINDS[role]: raise AttentionValidationError("record kind is not permitted for producer role")
    # Check the supplied root itself before resolve, then every derived component before each open.
    supplied=runtime_root.absolute()
    if supplied.exists() and supplied.is_symlink(): raise AttentionValidationError("runtime root may not be a symlink/reparse point")
    supplied.mkdir(parents=True,exist_ok=True)
    root=supplied.resolve(); identity=(root.stat().st_dev,root.stat().st_ino); path=producer_path(root,role,source_id); path.parent.mkdir(parents=True,exist_ok=True)
    encoded=canonical_bytes(record)+b"\n"; lock=path.with_suffix(".lock")
    _path_safe(root,lock)
    if (root.stat().st_dev,root.stat().st_ino)!=identity: raise AttentionValidationError("runtime root identity changed")
    _path_safe(root,lock)
    with _advisory_lock(lock,lock_timeout_seconds):
        _path_safe(root,lock); _path_safe(root,path)
        if (root.stat().st_dev,root.stat().st_ino)!=identity: raise AttentionValidationError("runtime root identity changed")
        if path.exists():
            _path_safe(root,path)
            with path.open("rb") as handle:
                for line in handle:
                    try: existing=json.loads(line)
                    except json.JSONDecodeError: continue
                    if existing.get("record_id")==record["record_id"]:
                        if canonical_bytes(existing)!=canonical_bytes(record): raise AttentionConflictError("record_id collision with different bytes")
                        return path
        _path_safe(root,path)
        if (root.stat().st_dev,root.stat().st_ino)!=identity: raise AttentionValidationError("runtime root identity changed")
        fd=os.open(path,os.O_APPEND|os.O_CREAT|os.O_WRONLY,0o600)
        try: os.write(fd,encoded); os.fsync(fd)
        finally: os.close(fd)
        return path


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".attention-", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(value), sort_keys=True, separators=(",", ":")))
        handle.flush(); os.fsync(handle.fileno())
    os.replace(name, path)

def _attention_paths(config: Any) -> list[tuple[Path, str, str]]:
    """Configured harness sources plus watcher-owned allowlisted producer inputs."""
    paths=[]
    for source in getattr(config, "observed_sources", ()):
        if getattr(source, "role", "") == "harness" and source.path.name == "attention-events.jsonl":
            paths.append((source.path, "harness", source.source_id))
    root=Path(config.runtime_root)
    for role, source_id in getattr(config, "attention_producers", ()):
        paths.append((producer_path(root, role, source_id), role, source_id))
    return paths

def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Return each independently decodable JSON object from an append-only file."""
    records=[]
    try:
        with path.open("rb") as handle:
            for line in handle:
                try: item=json.loads(line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError): continue
                if isinstance(item,dict): records.append(item)
    except OSError: pass
    return records

def _read_timeline(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read every valid canonical record and report each corrupt timeline line."""
    records=[]; corruptions=[]; offset=0
    try:
        with path.open("rb") as handle:
            for line in handle:
                start=offset; offset += len(line)
                try:
                    raw=json.loads(line.decode("utf-8")); record=validate_record(raw, canonical=True)
                except (UnicodeDecodeError, json.JSONDecodeError, AttentionValidationError) as exc:
                    affected_event=None; affected_epoch=None
                    try:
                        candidate=json.loads(line.decode("utf-8"))
                        if isinstance(candidate,dict):
                            affected_event=candidate.get("event_id")
                            affected_epoch=candidate.get("epoch_id")
                    except (UnicodeDecodeError,json.JSONDecodeError):
                        pass
                    corruptions.append({"schema":"manager-attention-observation-error/v1","source_path":str(path),"source_role":"watcher","source_id":"watcher","source_generation":"timeline","byte_start":start,"byte_end":offset,"error":"canonical timeline corruption: "+str(exc),"timeline_line_sha256":hashlib.sha256(line).hexdigest(),"affected_event_id":affected_event,"affected_epoch_id":affected_epoch,"observed_timestamp_utc":utc_now()})
                    continue
                records.append(record)
    except OSError: pass
    return records, corruptions

def _timeline_dedup(records: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    """Rebuild the durable id->source-byte index; reject contradictory history."""
    dedup: dict[str, str] = {}
    for raw in records:
        try:
            record = validate_record(raw, canonical=True)
        except AttentionValidationError:
            continue
        record_id, digest = record["record_id"], record["source_record_sha256"]
        prior = dedup.get(record_id)
        if prior is not None and prior != digest:
            raise AttentionConflictError("durable timeline has record_id collision with different source bytes")
        dedup[record_id] = digest
    return dedup

def ingest_attention(config: Any) -> dict[str, Any]:
    """Consume attention JSONL incrementally; valid timeline durable before cursor advance."""
    if not getattr(config, "attention_logging_enabled", False): return {"disabled": True, "findings": []}
    root=Path(config.runtime_root); watcher=root / "watcher"; cursor_path=watcher / "attention-cursor.json"; timeline_path=watcher / "attention-timeline.jsonl"; errors_path=watcher / "attention-observation-errors.jsonl"
    try: cursor=json.loads(cursor_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError): cursor={"schema":"manager-attention-cursor/v1","sources":{},"dedup":{}}
    sources=cursor.get("sources",{}) if isinstance(cursor.get("sources"),dict) else {}
    # The timeline is authoritative.  A crash after its fsync but before cursor
    # replacement must replay without manufacturing a second canonical record or
    # changing its first host-observed timestamp.
    persisted_before, timeline_errors = _read_timeline(timeline_path)
    durable_errors_before=_read_jsonl(errors_path)
    known_timeline_corruptions={str(item.get("timeline_line_sha256")) for item in durable_errors_before if isinstance(item.get("timeline_line_sha256"),str)}
    timeline_errors=[item for item in timeline_errors if item["timeline_line_sha256"] not in known_timeline_corruptions]
    try:
        dedup = _timeline_dedup(persisted_before)
    except AttentionConflictError as exc:
        return {"disabled": False, "records": 0, "errors": 1, "findings": [], "error": str(exc)}
    chunk=max(1024, int(getattr(config, "max_tail_bytes", 65536))); new_records=[]; errors=list(timeline_errors)
    for path, role, source_id in _attention_paths(config):
        key=str(path); old=sources.get(key,{})
        try: st=path.stat()
        except FileNotFoundError: continue
        fid=f"{st.st_dev}:{st.st_ino}"; generation=str(old.get("generation", "0"))
        replaced = old.get("file_id") != fid or int(old.get("offset",0)) > st.st_size
        carry=b""; carry_start=0
        if replaced:
            offset=0; generation=str(int(generation) + 1)
            # A partial record belongs to the old byte stream and must never be
            # prepended to the new generation after rotation/truncation.
        else: offset=int(old.get("offset",0))
        if not replaced:
            carry=base64.b64decode(old.get("partial_b64", "")) if old.get("partial_b64") else b""
            carry_start=int(old.get("partial_start", offset))
        with path.open("rb") as handle:
            handle.seek(offset); data=handle.read(chunk)
        combined=carry+data; complete=combined.rfind(b"\n")
        complete_data=combined[:complete+1] if complete >= 0 else b""
        remainder=combined[complete+1:] if complete >= 0 else combined
        # Each poll advances over newly read bytes and retains an exact partial prefix.
        for begin, line in ((carry_start + pos, raw) for pos, raw in _line_spans(complete_data)):
            end=begin + len(line) + 1
            if not line.strip(): continue
            try:
                raw=json.loads(line.decode("utf-8")); record=validate_record(raw)
                if record["kind"] not in ROLE_KINDS[role]: raise AttentionValidationError("record kind is not permitted for source role")
                canonical=canonicalize(record, observed_timestamp_utc=utc_now(), source_path=key, source_role=role, source_id=source_id, source_generation=generation, byte_start=begin, byte_end=end)
                prior=dedup.get(canonical["record_id"]); digest=canonical["source_record_sha256"]
                if prior is not None:
                    if prior != digest: raise AttentionConflictError("record_id collision with different source bytes")
                    continue
                dedup[canonical["record_id"]]=digest; new_records.append(canonical)
            except (UnicodeDecodeError, json.JSONDecodeError, AttentionValidationError, AttentionConflictError) as exc:
                affected=None; affected_epoch=None
                try:
                    candidate=json.loads(line.decode("utf-8")); affected=candidate.get("event_id") if isinstance(candidate,dict) else None; affected_epoch=candidate.get("epoch_id") if isinstance(candidate,dict) else None
                except (UnicodeDecodeError,json.JSONDecodeError): pass
                errors.append({"schema":"manager-attention-observation-error/v1","source_path":key,"source_role":role,"source_id":source_id,"source_generation":generation,"byte_start":begin,"byte_end":end,"error":str(exc),"affected_event_id":affected,"affected_epoch_id":affected_epoch,"observed_timestamp_utc":utc_now()})
        max_record=chunk*16
        if len(remainder)>max_record:
            errors.append({"schema":"manager-attention-observation-error/v1","source_path":key,"source_role":role,"source_id":source_id,"source_generation":generation,"byte_start":carry_start,"byte_end":offset+len(data),"error":"attention record exceeds configured maximum", "observed_timestamp_utc":utc_now()})
            remainder=b""; carry_start=offset+len(data)
        elif complete >= 0: carry_start=offset+len(data)-len(remainder)
        sources[key]={"file_id":fid,"offset":offset+len(data),"generation":generation,"partial_bytes":len(remainder),"partial_start":carry_start,"partial_b64":base64.b64encode(remainder).decode("ascii")}
    # One append/fsync for records/errors, then atomically advance source state.
    for destination, values in ((timeline_path,new_records),(errors_path,errors)):
        if values:
            destination.parent.mkdir(parents=True,exist_ok=True)
            with destination.open("ab") as handle:
                for value in values: handle.write(canonical_bytes(value)+b"\n")
                handle.flush(); os.fsync(handle.fileno())
    _atomic_json(cursor_path,{"schema":"manager-attention-cursor/v1","sources":sources,"dedup":dedup})
    persisted, post_write_timeline_errors=_read_timeline(timeline_path)
    # Observation errors are durable evidence too: subsequent polls and process
    # restarts may not silently reclassify their affected event as sufficient.
    durable_errors=_read_jsonl(errors_path)
    configured_epoch=getattr(config,"attention_epoch_id",None)
    events={(x.get("epoch_id"),x.get("event_id")) for x in persisted if isinstance(x,dict) and x.get("kind") != "HARNESS_SCAN_COMMITTED" and (configured_epoch is None or x.get("epoch_id")==configured_epoch)}
    # The durable files retain every epoch for audit and replay, but a configured
    # sprint report must not turn historical observation failures into current
    # causal evidence.
    report_errors = durable_errors if configured_epoch is None else [
        error for error in durable_errors
        if error.get("affected_epoch_id") == configured_epoch
    ]
    # An invalid source record is not canonicalized, but its declared epoch and
    # event remain durable enough to report an event-level fail-closed finding.
    events.update((error.get("affected_epoch_id"),error.get("affected_event_id")) for error in report_errors if isinstance(error.get("affected_epoch_id"),str) and isinstance(error.get("affected_event_id"),str))
    events=sorted(events)
    error_sources={(str(e.get("source_path")), str(e.get("source_generation", ""))) for e in report_errors}
    trusted={path:{"generation":entry.get("generation"),"offset":entry.get("offset",0),"partial_bytes":entry.get("partial_bytes",0),"error":(path,str(entry.get("generation",""))) in error_sources} for path,entry in sources.items()}
    findings=[analyze_event(persisted,epoch_id=epoch,event_id=event,trusted_coverage=trusted) for epoch,event in events if isinstance(epoch,str) and isinstance(event,str)]
    affected={(str(e.get("affected_epoch_id")), str(e.get("affected_event_id"))) for e in report_errors if isinstance(e.get("affected_epoch_id"),str) and isinstance(e.get("affected_event_id"),str)}
    for finding in findings:
        if (str(finding.get("epoch_id")), str(finding.get("event_id"))) in affected:
            matching=[str(error.get("error")) for error in report_errors if error.get("affected_epoch_id") == finding.get("epoch_id") and error.get("affected_event_id") == finding.get("event_id")]
            finding["classification"]="INSUFFICIENT_EVIDENCE"; finding.setdefault("missing_evidence",[]).append("attention observation error")
            finding.setdefault("contradictory_evidence",[]).extend("attention observation error: "+error for error in matching)
    report={"schema":REPORT_SCHEMA,"generated_utc":utc_now(),"events":findings,"observation_errors":report_errors,"trusted_coverage":trusted,"cursor_drained":all(int(v.get("partial_bytes",0))==0 for v in sources.values()),"classification_counts":{name:sum(x["classification"]==name for x in findings) for name in sorted(CLASSIFICATIONS)}}
    _atomic_json(watcher / "attention-report.json",report)
    return {"disabled":False,"records":len(new_records),"errors":len(errors),"findings":findings,"report":report}

def _line_spans(data: bytes) -> Iterable[tuple[int, bytes]]:
    pos=0
    for line in data.splitlines():
        yield pos,line; pos += len(line)+1
