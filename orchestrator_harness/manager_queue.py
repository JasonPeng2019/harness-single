"""The manager queue (managed) and the worker inbox.

Three components write the manager queue, each under the same short queue
lock: the monitor (sole producer of events), ROOT (advances event state via
the manager commands), and the managed PostToolUse hook (appends a
delivery-history receipt only).  The controller and worker never write it.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .core import iso_utc, new_id, read_json, require_schema
from .epochs import (
    MANAGER_QUEUE_SCHEMA,
    current_epoch_path,
    lane_record_dir,
    manager_queue_path,
    read_current_epoch,
)
from .records import RecordLock, atomic_write_json, read_record

LANE_INBOX_SCHEMA = "lane-inbox/v1"

EVENT_TYPES = frozenset(
    {
        "COMPLETION_REVIEW_REQUIRED",
        "LANE_RESULT_INVALID",
        "LANE_RESUME_REQUIRED",
        "LANE_STATUS_CHANGED",
    }
)
EVENT_STATES = frozenset({"PENDING", "ACKNOWLEDGED", "COMPLETE", "BLOCKED"})
ASSIGNMENT_STATES = frozenset({"PENDING", "ACKNOWLEDGED", "COMPLETE", "BLOCKED"})

MANAGER_ACK_EVENT_NOT_FOUND = "MANAGER_ACK_EVENT_NOT_FOUND"
MANAGER_ACK_ALREADY_ACKNOWLEDGED = "MANAGER_ACK_ALREADY_ACKNOWLEDGED"
MANAGER_ACK_NOT_ROOT_EVENT = "MANAGER_ACK_NOT_ROOT_EVENT"
MANAGER_CLOSE_NOT_ACKNOWLEDGED = "MANAGER_CLOSE_NOT_ACKNOWLEDGED"
MANAGER_CLOSE_ALREADY_CLOSED = "MANAGER_CLOSE_ALREADY_CLOSED"
MANAGER_CLOSE_NOT_ROOT_EVENT = "MANAGER_CLOSE_NOT_ROOT_EVENT"
MANAGER_CLOSE_INVALID_OUTCOME = "MANAGER_CLOSE_INVALID_OUTCOME"
MANAGER_CLOSE_SUMMARY_REQUIRED = "MANAGER_CLOSE_SUMMARY_REQUIRED"
SEND_LANE_NOT_FOUND = "SEND_LANE_NOT_FOUND"
SEND_LANE_NOT_MANAGED = "SEND_LANE_NOT_MANAGED"
SEND_LANE_NOT_RUNNING = "SEND_LANE_NOT_RUNNING"
SEND_LANE_WRITE_FAILED = "SEND_LANE_WRITE_FAILED"


class ManagerQueueError(RuntimeError):
    """A manager-queue or worker-inbox operation failed."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _queue_header(rt: Path) -> dict[str, Any]:
    marker = read_current_epoch(rt)
    if marker is None:
        raise ManagerQueueError("MANAGER_QUEUE_NO_EPOCH", "no active epoch")
    return {
        "epoch_id": str(marker["epoch_id"]),
        "queue_id": str(marker.get("queue_id") or ""),
    }


def read_manager_queue(rt: Path) -> dict[str, Any]:
    """Read the manager queue, confirming its header matches the epoch marker."""
    path = manager_queue_path(rt)
    if not path.is_file():
        raise ManagerQueueError("MANAGER_QUEUE_MISSING", f"manager queue missing: {path}")
    try:
        record = read_record(path, MANAGER_QUEUE_SCHEMA)
    except (OSError, ValueError) as exc:
        raise ManagerQueueError("MANAGER_QUEUE_INVALID", str(exc)) from exc
    header = _queue_header(rt)
    if record.get("epoch_id") != header["epoch_id"] or record.get("queue_id") != header["queue_id"]:
        raise ManagerQueueError("QUEUE_REPLACED", "manager queue header does not match the active epoch")
    return record


def _write_manager_queue(rt: Path, record: dict[str, Any]) -> None:
    """Replace the queue while the caller owns its queue lock."""
    path = manager_queue_path(rt)
    atomic_write_json(path, record)


def _update_manager_queue(
    rt: Path,
    mutate: Callable[[dict[str, Any]], tuple[Any, bool]],
) -> Any:
    """Validate, mutate, and replace the manager queue in one transaction."""
    path = manager_queue_path(rt)
    with RecordLock(path):
        record = read_manager_queue(rt)
        result, changed = mutate(record)
        if changed:
            _write_manager_queue(rt, record)
        return result


def promote_event(
    rt: Path,
    *,
    event_type: str,
    lane_id: str,
    run_id: str,
    summary: str,
    actionable_status: str | None = None,
) -> dict[str, Any]:
    """The monitor's sole-producer admission of one PENDING event."""
    if event_type not in EVENT_TYPES:
        raise ManagerQueueError("MANAGER_QUEUE_INVALID_EVENT_TYPE", event_type)

    def mutate(record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        event = {
            "event_id": new_id(),
            "type": event_type,
            "lane_id": lane_id,
            "run_id": run_id,
            "summary": summary,
            "state": "PENDING",
            "history": [{"state": "PENDING", "at": iso_utc()}],
            "delivery_history": [],
        }
        if actionable_status is not None:
            event["actionable_status"] = actionable_status
        record["events"].append(event)
        return event, True

    return _update_manager_queue(rt, mutate)


def acknowledge_event(rt: Path, event_id: str) -> dict[str, Any]:
    """ROOT advances one event PENDING -> ACKNOWLEDGED."""
    def mutate(record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        event = _find_event(record, event_id)
        if event is None:
            raise ManagerQueueError(
                MANAGER_ACK_EVENT_NOT_FOUND, f"event not found: {event_id}"
            )
        if event["state"] != "PENDING":
            raise ManagerQueueError(
                MANAGER_ACK_ALREADY_ACKNOWLEDGED,
                f"event {event_id} is already {event['state']}",
            )
        event["state"] = "ACKNOWLEDGED"
        event["history"].append({"state": "ACKNOWLEDGED", "at": iso_utc()})
        return event, True

    return _update_manager_queue(rt, mutate)


def close_event(
    rt: Path, event_id: str, outcome: str, summary: str | None = None
) -> dict[str, Any]:
    """ROOT closes an acknowledged event with COMPLETE or BLOCKED."""
    if outcome not in ("COMPLETE", "BLOCKED"):
        raise ManagerQueueError(MANAGER_CLOSE_INVALID_OUTCOME, f"invalid outcome: {outcome}")
    normalized_summary = summary.strip() if isinstance(summary, str) else ""
    if not normalized_summary:
        raise ManagerQueueError(
            MANAGER_CLOSE_SUMMARY_REQUIRED, "close summary must be nonblank"
        )
    def mutate(record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        event = _find_event(record, event_id)
        if event is None:
            raise ManagerQueueError(
                MANAGER_ACK_EVENT_NOT_FOUND, f"event not found: {event_id}"
            )
        if event["state"] == "PENDING":
            raise ManagerQueueError(
                MANAGER_CLOSE_NOT_ACKNOWLEDGED,
                f"event {event_id} is not acknowledged",
            )
        if event["state"] in ("COMPLETE", "BLOCKED"):
            raise ManagerQueueError(
                MANAGER_CLOSE_ALREADY_CLOSED,
                f"event {event_id} is already {event['state']}",
            )
        event["state"] = outcome
        event["history"].append(
            {"state": outcome, "at": iso_utc(), "summary": normalized_summary}
        )
        event["summary"] = normalized_summary
        return event, True

    return _update_manager_queue(rt, mutate)


def append_delivery_history(rt: Path, event_id: str) -> None:
    """The managed PostToolUse hook appends a DELIVERED receipt only."""
    def mutate(record: dict[str, Any]) -> tuple[None, bool]:
        event = _find_event(record, event_id)
        if event is None:
            return None, False
        event["delivery_history"].append(
            {"outcome": "DELIVERED", "at": iso_utc()}
        )
        return None, True

    _update_manager_queue(rt, mutate)


def _find_event(record: dict[str, Any], event_id: str) -> dict[str, Any] | None:
    for event in record.get("events", []):
        if event.get("event_id") == event_id:
            return event
    return None


def read_lane_inbox(worktree: Path) -> dict[str, Any]:
    path = worktree / ".agent-workspace" / "QUEUE.json"
    if not path.is_file():
        raise ManagerQueueError("LANE_INBOX_MISSING", f"lane inbox missing: {path}")
    try:
        return read_record(path, LANE_INBOX_SCHEMA)
    except (OSError, ValueError) as exc:
        raise ManagerQueueError("LANE_INBOX_INVALID", str(exc)) from exc


def write_lane_inbox(worktree: Path, record: dict[str, Any]) -> None:
    path = worktree / ".agent-workspace" / "QUEUE.json"
    with RecordLock(path):
        atomic_write_json(path, record)


def append_assignment(
    rt: Path, lane: dict[str, Any], prompt: str
) -> dict[str, Any]:
    """ROOT appends one PENDING assignment to a running managed lane's inbox."""
    worktree = Path(lane["worktree_path"])
    inbox = read_lane_inbox(worktree)
    if inbox.get("run_id") != lane.get("run_id"):
        raise ManagerQueueError(SEND_LANE_WRITE_FAILED, "lane inbox run_id is stale")
    assignment = {
        "event_id": new_id(),
        "lane_id": lane["lane_id"],
        "run_id": lane["run_id"],
        "prompt": prompt,
        "created_at": iso_utc(),
        "state": "PENDING",
        "history": [{"state": "PENDING", "at": iso_utc()}],
    }
    inbox["assignments"].append(assignment)
    write_lane_inbox(worktree, inbox)
    return assignment
