"""Read-only legacy evidence helpers for removed attention-sprint settings.

S4 no longer starts or renews an attention sprint.  The compatibility
validators remain importable only so historical evidence/config readers fail
closed with explicit diagnostics; they create no runtime, heartbeat, timeline,
or integrity state.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from .models import parse_utc


class AttentionSprintError(ValueError):
    pass


def validate_sprint_boundary(*, epoch_id: str, heartbeat_timeout_seconds: float, formal_review_interval_seconds: float, bounded_lifetime_seconds: float) -> dict[str, object]:
    """Validate one fresh, bounded coverage contract without emitting records."""
    if not isinstance(epoch_id, str) or not epoch_id:
        raise AttentionSprintError("attention epoch_id must be non-empty")
    for name, value in (("heartbeat_timeout_seconds", heartbeat_timeout_seconds), ("formal_review_interval_seconds", formal_review_interval_seconds), ("bounded_lifetime_seconds", bounded_lifetime_seconds)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise AttentionSprintError(f"{name} must be positive")
    if heartbeat_timeout_seconds <= formal_review_interval_seconds:
        raise AttentionSprintError("heartbeat timeout must exceed formal review interval")
    if heartbeat_timeout_seconds <= bounded_lifetime_seconds:
        raise AttentionSprintError("heartbeat timeout must exceed bounded sprint lifetime")
    return {"epoch_id": epoch_id, "heartbeat_timeout_seconds": float(heartbeat_timeout_seconds), "formal_review_interval_seconds": float(formal_review_interval_seconds), "bounded_lifetime_seconds": float(bounded_lifetime_seconds)}


def complete_pending_snapshot(events: Iterable[Mapping[str, Any]], *, selected_event_id: str | None, selection_reason: str) -> dict[str, object]:
    """Render the exact bounded pending-event metadata; never substitute UNKNOWN."""
    if not isinstance(selection_reason, str) or not selection_reason or selection_reason == "UNKNOWN":
        raise AttentionSprintError("selection_reason must be a real non-UNKNOWN reason")
    rendered=[]; seen=set()
    for event in events:
        if not isinstance(event, Mapping) or set(event) - {"event_id", "type", "priority", "age_seconds", "agent_blocked", "response_deadline_utc", "lease_deadline_utc"}:
            raise AttentionSprintError("pending event metadata is incomplete or unsupported")
        event_id=event.get("event_id"); kind=event.get("type")
        if not isinstance(event_id,str) or not event_id or event_id in seen or not isinstance(kind,str) or not kind:
            raise AttentionSprintError("pending event identity is invalid")
        if isinstance(event.get("priority"),bool) or not isinstance(event.get("priority"),(int,float)) or event["priority"] < 0:
            raise AttentionSprintError("pending event priority is invalid")
        if isinstance(event.get("age_seconds"),bool) or not isinstance(event.get("age_seconds"),(int,float)) or event["age_seconds"] < 0 or not isinstance(event.get("agent_blocked"),bool):
            raise AttentionSprintError("pending event age or blocked state is invalid")
        row=dict(event)
        for deadline in ("response_deadline_utc", "lease_deadline_utc"):
            if deadline in row and parse_utc(row[deadline]) is None:
                raise AttentionSprintError(f"{deadline} is invalid")
        seen.add(event_id); rendered.append(row)
    rendered.sort(key=lambda item: str(item["event_id"]))
    if selected_event_id is not None and selected_event_id not in seen:
        raise AttentionSprintError("selected event is absent from complete snapshot")
    return {"complete": True, "events": rendered, "selected_event_id": selected_event_id, "selection_reason": selection_reason}


def invocation_snapshot(events: Iterable[Mapping[str, Any]]) -> dict[str, object]:
    return complete_pending_snapshot(events, selected_event_id=None, selection_reason="BOUNDARY_INVENTORY")


def formal_baseline_snapshot(events: Iterable[Mapping[str, Any]]) -> dict[str, object]:
    return complete_pending_snapshot(events, selected_event_id=None, selection_reason="FORMAL_REVIEW_BASELINE")


def event_selection_snapshot(events: Iterable[Mapping[str, Any]], *, event_id: str) -> dict[str, object]:
    return complete_pending_snapshot(events, selected_event_id=event_id, selection_reason="SELECT_ACTIONABLE")


def _complete_snapshot(record: Mapping[str, Any]) -> bool:
    snapshot=record.get("pending_work_snapshot")
    return isinstance(snapshot, Mapping) and snapshot.get("complete") is True and isinstance(snapshot.get("events"), list) and isinstance(snapshot.get("selection_reason"), str) and snapshot["selection_reason"] != "UNKNOWN"


def validate_sprint_finalize(records: Iterable[Mapping[str, Any]], *, epoch_id: str) -> None:
    """Reject incomplete boundaries or a blocking gate ending only in controller exit."""
    rows=[record for record in records if record.get("epoch_id") == epoch_id]
    kinds={kind:[row for row in rows if row.get("kind") == kind] for kind in {"MANAGER_INVOCATION_STARTED","MANAGER_INVOCATION_FINISHED","FORMAL_REVIEW_BASELINE_ADVANCED","MANAGER_EVENT_CLAIMED","AGENT_SIGNAL_CREATED","AGENT_RESPONSE_RECEIVED","AGENT_WORK_RESUMED","AGENT_GATE_EXPIRED"}}
    for kind in ("MANAGER_INVOCATION_STARTED","MANAGER_INVOCATION_FINISHED"):
        if not kinds[kind] or any(not _complete_snapshot(row) for row in kinds[kind]):
            raise AttentionSprintError(f"required complete {kind} boundary is absent")
    for claim in kinds["MANAGER_EVENT_CLAIMED"]:
        snapshot=claim.get("pending_work_snapshot")
        if not _complete_snapshot(claim) or not isinstance(snapshot, Mapping) or snapshot.get("selected_event_id") != claim.get("event_id") or snapshot.get("selection_reason") != "SELECT_ACTIONABLE":
            raise AttentionSprintError("manager claim requires its own exact complete event-selection snapshot")
    # Input is the durable timeline order.  There must be one activation baseline
    # after start and a fresh later baseline after every review, not merely one
    # baseline somewhere in the sprint.
    def formal_baseline(row: Mapping[str, Any]) -> bool:
        snapshot=row.get("pending_work_snapshot")
        return row.get("kind") == "FORMAL_REVIEW_BASELINE_ADVANCED" and _complete_snapshot(row) and row.get("source_role") == "orchestrator" and isinstance(snapshot, Mapping) and snapshot.get("selection_reason") == "FORMAL_REVIEW_BASELINE"
    start_index=next((index for index,row in enumerate(rows) if row.get("kind") == "MANAGER_INVOCATION_STARTED"), None)
    if start_index is None or not any(index > start_index and formal_baseline(row) for index,row in enumerate(rows)):
        raise AttentionSprintError("activation formal-review baseline is absent")
    for index,row in enumerate(rows):
        if row.get("kind") == "MANAGER_REVIEW_STARTED" and not any(later > index and formal_baseline(candidate) for later,candidate in enumerate(rows)):
            raise AttentionSprintError("formal review lacks a later baseline")
    received={str(row.get("event_id")) for row in kinds["AGENT_RESPONSE_RECEIVED"]}
    resumed={str(row.get("event_id")) for row in kinds["AGENT_WORK_RESUMED"]}
    expired={str(row.get("event_id")) for row in kinds["AGENT_GATE_EXPIRED"] if row.get("terminal_gate_expired") is True}
    for signal in kinds["AGENT_SIGNAL_CREATED"]:
        if signal.get("agent_blocked") is True:
            event_id=str(signal.get("event_id"))
            if event_id not in expired and not (event_id in received and event_id in resumed):
                raise AttentionSprintError(f"blocking gate {event_id} lacks receipt/resume or terminal expiration")
