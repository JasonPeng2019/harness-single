"""Lane record helpers: the authoritative outside-worktree ``lane.json``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .core import read_json
from .epochs import (
    lane_record_dir,
    epoch_dir,
    lane_record_path,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
)
from .records import RecordLock, atomic_write_json, read_record, runtime_admission_lock

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
        record = read_record(path, LANE_SCHEMA)
        if record.get("lane_id") != lane_id:
            raise ValueError(f"lane record identity differs from its directory: {path}")
        return record
    except (OSError, ValueError) as exc:
        raise LaneError("LANE_RECORD_INVALID", str(exc)) from exc


def write_lane(
    rt: Path, epoch_id: str, lane_id: str, fields: dict[str, Any], *, create_only: bool = False,
) -> dict[str, Any]:
    record = dict(fields)
    record["schema"] = LANE_SCHEMA
    record["lane_id"] = lane_id
    path = lane_record_path(rt, epoch_id, lane_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with RecordLock(path):
        if create_only:
            try:
                path.lstat()
            except FileNotFoundError:
                pass
            else:
                raise LaneError("LANE_ID_IN_USE", f"lane record already exists: {path}")
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
        current = read_lane(rt, epoch_id, lane_id)
        record = dict(mutate(current))
        if record.get("lane_id", lane_id) != lane_id:
            raise LaneError("LANE_OWNERSHIP_CHANGED", "mutation cannot change lane identity")
        record["schema"] = LANE_SCHEMA
        record["lane_id"] = lane_id
        atomic_write_json(path, record)
    return record


def retained_lanes(rt: Path, epoch_id: str) -> dict[str, dict[str, Any]]:
    """Read all durable owners; unreadable entries are never dropped as absent."""
    directory = epoch_dir(rt, epoch_id) / "lanes"
    try:
        metadata = directory.lstat()
    except FileNotFoundError:
        return {}
    if not directory.is_dir() or directory.is_symlink() or getattr(metadata, "st_file_attributes", 0) & 0x400:
        raise LaneError("LANE_RECORD_INVALID", f"lane directory is not a plain directory: {directory}")
    result = {}
    for folder in sorted(directory.iterdir()):
        metadata = folder.lstat()
        if not folder.is_dir() or folder.is_symlink() or getattr(metadata, "st_file_attributes", 0) & 0x400:
            raise LaneError("LANE_RECORD_INVALID", f"unexpected durable lane entry: {folder}")
        lane = read_lane(rt, epoch_id, folder.name)
        if lane.get("lifecycle") not in LANE_LIFECYCLES or not isinstance(lane.get("run_id"), str) or not lane["run_id"]:
            raise LaneError("LANE_RECORD_INVALID", f"lane lifecycle or run identity is invalid: {folder}")
        result[folder.name] = lane
    return result


def reconcile_active_lanes(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    """Publish one complete durable inventory under runtime admission."""
    from .epochs import write_active_lanes

    with runtime_admission_lock(rt):
        lanes = retained_lanes(rt, epoch_id)
        entries = [
            {"lane_id": lane_id, "run_id": lane["run_id"],
             "lane_record_path": str(lane_record_path(rt, epoch_id, lane_id))}
            for lane_id, lane in lanes.items()
            if lane["lifecycle"] not in {"retired", "abandoned"}
        ]
        # Validate the existing index before replacing it. Corruption is evidence,
        # not permission for an observer to silently repair ownership.
        read_active_lanes(rt, epoch_id)
        write_active_lanes(rt, epoch_id, entries)
        return entries


def lane_operation_lock(rt: Path, epoch_id: str, lane_id: str) -> RecordLock:
    """Serialize native launch/resume/cancellation effects, outside controller writes."""
    return RecordLock(lane_record_dir(rt, epoch_id, lane_id) / "lifecycle-operation")


def require_same_run(current: dict[str, Any], captured: dict[str, Any]) -> None:
    if not captured.get("run_id") or any(
        current.get(field) != captured.get(field)
        for field in ("lane_id", "run_id", "worktree_path", "provider", "exclusive_resources",
                      "worktree_identity", "control_dir", "invocation_hash", "git_source",
                      "result_assignment_hash", "source_exclusions")
    ):
        raise LaneError("LANE_OWNERSHIP_CHANGED", "lane invocation changed before the operation's effect")


def find_active_lane(rt: Path, lane_id: str) -> tuple[str, dict[str, Any]]:
    """Locate one lane in the active epoch by its operator-chosen id."""
    marker = read_current_epoch(rt)
    if marker is None:
        raise LaneError("LANE_NOT_FOUND", f"no active epoch; lane {lane_id} not found")
    epoch_id = str(marker["epoch_id"])
    try:
        lane = read_lane(rt, epoch_id, lane_id)
    except LaneError as exc:
        if exc.code != "LANE_NOT_FOUND":
            raise
        raise LaneError("LANE_NOT_FOUND", f"lane not found: {lane_id}")
    return epoch_id, lane


def find_retained_lane(rt: Path, lane_id: str) -> tuple[str, dict[str, Any]]:
    """Find a unique retained owner for explicit resume only.

    Ordinary launch/review/discovery must continue to use the current epoch.
    Reused IDs and unreadable inventories require explicit reconciliation.
    """
    from .portable_paths import component

    component(lane_id)
    directory = rt / "epochs"
    if not directory.exists():
        raise LaneError("LANE_NOT_FOUND", f"retained lane not found: {lane_id}")
    matches = []
    for folder in sorted(directory.iterdir()):
        metadata = folder.lstat()
        if not folder.is_dir() or folder.is_symlink() or getattr(metadata, "st_file_attributes", 0) & 0x400:
            raise LaneError("LANE_RECORD_INVALID", f"unexpected epoch entry: {folder}")
        lane = retained_lanes(rt, folder.name).get(lane_id)
        if lane is not None:
            matches.append((folder.name, lane))
    if len(matches) > 1:
        # A pause after resume preparation retains both the closed original
        # and its replacement. Select only a complete, unbranched native chain;
        # timestamps or directory order never establish ownership.
        owners = dict(matches)
        parents = {lane.get("resume_from_epoch_id") for _, lane in matches}
        leaves = set(owners) - parents
        if len(leaves) == 1:
            selected = next(iter(leaves))
            visited = set()
            cursor = selected
            while cursor in owners and cursor not in visited:
                visited.add(cursor)
                child = owners[cursor]
                parent_id = child.get("resume_from_epoch_id")
                if parent_id is None:
                    break
                parent = owners.get(parent_id)
                if parent is None or child.get("resume_from_run_id") != parent["run_id"] or any(
                    child.get(field) != parent.get(field) for field in (
                        "worktree_path", "worktree_identity", "provider", "session", "git_source",
                        "provider_history_path", "exclusive_resources", "git_write_namespace",
                    )
                ):
                    break
                cursor = parent_id
            if len(visited) == len(owners) and owners[cursor].get("resume_from_epoch_id") is None:
                return selected, owners[selected]
    if len(matches) != 1:
        raise LaneError(
            "LANE_ID_AMBIGUOUS" if matches else "LANE_NOT_FOUND",
            f"retained lane {lane_id} has {len(matches)} matching epoch records; resume requires a unique owner",
        )
    return matches[0]


def active_lane_entries(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    return read_active_lanes(rt, epoch_id)


def lane_worktree(lane: dict[str, Any]) -> Path:
    return Path(lane["worktree_path"])


def lane_agent_workspace(lane: dict[str, Any]) -> Path:
    return lane_worktree(lane) / ".agent-workspace"
