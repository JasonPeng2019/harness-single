"""Lane record helpers: the authoritative outside-worktree ``lane.json``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .core import content_hash, read_json
from .epochs import (
    lane_record_dir,
    lane_record_path,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
)
from .records import RecordLock, atomic_write_json, read_record

LANE_SCHEMA = "lane/v1"
LANE_LIFECYCLES = frozenset(
    {
        "prepared",
        "resuming",
        "running",
        "review_pending",
        "result_invalid",
        "accepted",
        "retired",
        "blocked",
        "abandoned",
    }
)


class LaneError(RuntimeError):
    """A lane record operation failed."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def read_lane(rt: Path, epoch_id: str, lane_id: str) -> dict[str, Any]:
    path = lane_record_path(rt, epoch_id, lane_id)
    if not path.is_file():
        raise LaneError("LANE_NOT_FOUND", f"lane record missing: {path}")
    try:
        return read_record(path, LANE_SCHEMA)
    except (OSError, ValueError) as exc:
        raise LaneError("LANE_RECORD_INVALID", str(exc)) from exc


def write_lane(rt: Path, epoch_id: str, lane_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    record = dict(fields)
    record["schema"] = LANE_SCHEMA
    record["lane_id"] = lane_id
    path = lane_record_path(rt, epoch_id, lane_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with RecordLock(path):
        atomic_write_json(path, record)
    return record


def update_lane(
    rt: Path,
    epoch_id: str,
    lane_id: str,
    mutate: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    path = lane_record_path(rt, epoch_id, lane_id)
    with RecordLock(path):
        try:
            current = read_json(path)
        except (OSError, ValueError):
            current = {}
        record = dict(mutate(current))
        record["schema"] = LANE_SCHEMA
        record["lane_id"] = lane_id
        atomic_write_json(path, record)
    return record


def find_active_lane(rt: Path, lane_id: str) -> tuple[str, dict[str, Any]]:
    """Locate one lane in the active epoch by its operator-chosen id."""
    marker = read_current_epoch(rt)
    if marker is None:
        raise LaneError("LANE_NOT_FOUND", f"no active epoch; lane {lane_id} not found")
    epoch_id = str(marker["epoch_id"])
    entries = read_active_lanes(rt, epoch_id)
    matching = [entry for entry in entries if entry.get("lane_id") == lane_id]
    if len(matching) != 1:
        raise LaneError("LANE_NOT_FOUND", f"active lane not found: {lane_id}")
    try:
        lane = read_lane(rt, epoch_id, lane_id)
    except LaneError:
        raise LaneError("LANE_NOT_FOUND", f"lane not found: {lane_id}")
    entry = matching[0]
    expected_path = lane_record_path(rt, epoch_id, lane_id)
    pending = lane.get("pending_resume")
    pending_index_transition = bool(
        lane.get("lifecycle") == "resuming"
        and isinstance(pending, dict)
        and pending.get("schema") == "pending-resume/v1"
        and pending.get("content_hash") == content_hash(pending)
        and lane.get("run_id") == pending.get("prior_run_id")
        and entry.get("run_id") == pending.get("new_run_id")
    )
    if (
        (
            entry.get("run_id") != lane.get("run_id")
            and not pending_index_transition
        )
        or Path(str(entry.get("lane_record_path") or "")).resolve()
        != expected_path.resolve()
        or lane.get("publication_state") == "staged"
    ):
        raise LaneError(
            "LANE_NOT_FOUND",
            f"lane {lane_id} is not consistently published in the active index",
        )
    return epoch_id, lane


def active_lane_entries(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    return read_active_lanes(rt, epoch_id)


def lane_worktree(lane: dict[str, Any]) -> Path:
    return Path(lane["worktree_path"])


def lane_agent_workspace(lane: dict[str, Any]) -> Path:
    return lane_worktree(lane) / ".agent-workspace"
