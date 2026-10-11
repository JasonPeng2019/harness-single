"""``harness shutdown``: end the whole runtime with exact process ownership.

Shutdown flips the runtime state OPEN -> SHUTTING_DOWN, lets each lane's own
controller clean its provider/helper processes (cleanup-proof-first), stops
the persistent monitor, clears the active-epoch marker, and closes the
runtime.  It never kills by broad process name and never retries with broad
kills on failure.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from . import processes
from .activity_log import append_activity
from .config import find_harness_root, load_config
from .core import iso_utc, read_json, require_schema
from .epochs import (
    close_epoch,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
    write_active_lanes,
)
from .lanes import lane_operation_lock, read_lane, require_same_run, update_lane
from .lane_cleanup import cleanup_is_proven, observe_controller_status
from .leases import release_leases
from .manager_queue import ManagerQueueError, read_manager_queue
from .records import RecordLock, atomic_write_json, read_record
from .setup import (
    MONITOR_SCHEMA,
    RUNTIME_STATE_SCHEMA,
    monitor_record_path,
    read_monitor_record,
    read_runtime_state,
    runtime_state_path,
)

SHUTDOWN_LANE_CLEANUP_UNPROVEN = "SHUTDOWN_LANE_CLEANUP_UNPROVEN"
SHUTDOWN_MONITOR_UNPROVEN = "SHUTDOWN_MONITOR_UNPROVEN"
SHUTDOWN_RUNTIME_AMBIGUOUS = "SHUTDOWN_RUNTIME_AMBIGUOUS"
SHUTDOWN_UNRESOLVED_MANAGER_OBLIGATIONS = "SHUTDOWN_UNRESOLVED_MANAGER_OBLIGATIONS"

CONTROLLER_STATUS_SCHEMA = "controller-status/v1"

# A bounded wait for a lane controller to observe SHUTTING_DOWN and finish its
# own cleanup; a wedged controller surfaces as SHUTDOWN_LANE_CLEANUP_UNPROVEN.
SHUTDOWN_WAIT_SECONDS = 120.0
MONITOR_WAIT_SECONDS = 30.0


class ShutdownError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _unresolved_manager_obligations(rt: Path) -> list[dict[str, Any]]:
    """Return queue work that must be resolved before an OPEN runtime closes."""

    marker = read_current_epoch(rt)
    if marker is None:
        return []
    try:
        queue = read_manager_queue(rt)
    except (ManagerQueueError, OSError, ValueError) as exc:
        raise ShutdownError(
            SHUTDOWN_RUNTIME_AMBIGUOUS,
            f"cannot prove that the manager queue is resolved: {exc}",
        ) from exc
    events = queue.get("events")
    if not isinstance(events, list) or any(not isinstance(event, dict) for event in events):
        raise ShutdownError(
            SHUTDOWN_RUNTIME_AMBIGUOUS,
            "cannot prove that the manager queue is resolved: events are invalid",
        )
    return [
        event for event in events
        if event.get("state") in {"PENDING", "ACKNOWLEDGED"}
    ]


def _read_controller_status(lane: dict[str, Any]) -> dict[str, Any] | None:
    return observe_controller_status(lane).status


def _provider_identity(lane: dict[str, Any]) -> tuple[int | None, str | None]:
    status = _read_controller_status(lane)
    if status is None:
        return None, None
    provider_state = status.get("provider_state") or {}
    pid = provider_state.get("pid")
    if not isinstance(pid, int):
        return None, None
    return pid, None


def _wait_for_controller_exit(lane: dict[str, Any], timeout_seconds: float) -> bool:
    process = lane.get("process") or {}
    pid = process.get("pid")
    creation = process.get("creation_time")
    if not (isinstance(pid, int) and processes.identity_matches(pid, creation)):
        return True
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not processes.identity_matches(pid, creation):
            return True
        time.sleep(0.5)
    return not processes.identity_matches(pid, creation)


def _lane_clean(rt: Path, lane: dict[str, Any]) -> bool:
    return cleanup_is_proven(lane)


def _retire_lane(rt: Path, epoch_id: str, lane_id: str, lane: dict[str, Any]) -> None:
    def retire(current: dict[str, Any]) -> dict[str, Any]:
        require_same_run(current, lane)
        marker = read_current_epoch(rt)
        if not marker or marker.get("epoch_id") != epoch_id or current.get("lifecycle") == "resuming":
            raise ShutdownError(SHUTDOWN_LANE_CLEANUP_UNPROVEN, "lane or epoch changed before retirement")
        if not _lane_clean(rt, current):
            raise ShutdownError(SHUTDOWN_LANE_CLEANUP_UNPROVEN, "lane cleanup changed before retirement")
        release_leases(rt, lane_id, current["run_id"], strict=True)
        return {**current, "lifecycle": "retired", "launch_pending": False}

    update_lane(rt, epoch_id, lane_id, retire)
    entries = [e for e in read_active_lanes(rt, epoch_id) if e.get("lane_id") != lane_id]
    write_active_lanes(rt, epoch_id, entries)


def _stop_monitor(rt: Path) -> None:
    """Set stop_requested under the monitor-record lock, then wait for STOPPED."""
    record_path = monitor_record_path(rt)
    append_activity(rt, "process.stop_request.started", component="shutdown")
    with RecordLock(record_path):
        record = read_monitor_record(rt)
        if record is None:
            append_activity(
                rt, "process.stop_request.completed", component="shutdown",
                outcome="monitor_record_missing",
            )
            return
        record["stop_requested"] = True
        atomic_write_json(record_path, record)
    deadline = time.monotonic() + MONITOR_WAIT_SECONDS
    while time.monotonic() < deadline:
        current = read_monitor_record(rt)
        if current is None:
            append_activity(
                rt, "process.stop_request.completed", component="shutdown",
                outcome="monitor_record_removed",
            )
            return
        if current.get("health") == "STOPPED":
            append_activity(
                rt, "process.stop_request.completed", component="shutdown",
                outcome="stopped",
            )
            return
        pid = current.get("pid")
        creation = current.get("creation_time")
        if not (isinstance(pid, int) and processes.identity_matches(pid, creation)):
            append_activity(
                rt, "process.stop_request.completed", component="shutdown",
                outcome="process_absent",
            )
            return
        time.sleep(0.5)
    current = read_monitor_record(rt)
    if current is not None and current.get("health") != "STOPPED":
        raise ShutdownError(
            SHUTDOWN_MONITOR_UNPROVEN,
            "the monitor did not stop within the wait window",
        )


def _prune_worktrees(root_workspace: Path) -> None:
    import subprocess

    subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "prune"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def run_shutdown() -> dict[str, Any]:
    """Execute ``harness shutdown`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": SHUTDOWN_RUNTIME_AMBIGUOUS,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        state = read_runtime_state(rt)
        if state is None:
            return {
                "ok": True,
                "code": "SHUTDOWN_OK",
                "summary": "runtime is not open; nothing to shut down",
                "evidence_paths": [],
                "next_action": "none",
            }
        current_state = state.get("state")
        if current_state == "CLOSED":
            return {
                "ok": True,
                "code": "SHUTDOWN_OK",
                "summary": "runtime is already CLOSED",
                "evidence_paths": [str(runtime_state_path(rt))],
                "next_action": "none",
            }
        if current_state not in ("OPEN", "SHUTTING_DOWN"):
            raise ShutdownError(
                SHUTDOWN_RUNTIME_AMBIGUOUS,
                f"unexpected runtime state: {current_state}",
            )
        if current_state == "OPEN":
            if config.profile == "managed":
                unresolved = _unresolved_manager_obligations(rt)
                if unresolved:
                    event_ids = [
                        str(event.get("event_id") or "<missing-id>")
                        for event in unresolved
                    ]
                    raise ShutdownError(
                        SHUTDOWN_UNRESOLVED_MANAGER_OBLIGATIONS,
                        "refusing shutdown with unresolved manager obligation(s): "
                        + ", ".join(event_ids),
                    )
            with RecordLock(runtime_state_path(rt)):
                atomic_write_json(
                    runtime_state_path(rt),
                    {"schema": RUNTIME_STATE_SCHEMA, "state": "SHUTTING_DOWN", "updated_at": iso_utc()},
                )

        marker = read_current_epoch(rt)
        if marker is not None:
            epoch_id = str(marker["epoch_id"])
            try:
                state_record = read_epoch_state(rt, epoch_id)
            except (OSError, ValueError):
                state_record = {}
            if state_record.get("lifecycle") == "active":
                for entry in read_active_lanes(rt, epoch_id):
                    lane_id = entry["lane_id"]
                    with lane_operation_lock(rt, epoch_id, lane_id):
                        try:
                            lane = read_lane(rt, epoch_id, lane_id)
                        except Exception as exc:
                            raise ShutdownError(SHUTDOWN_LANE_CLEANUP_UNPROVEN, f"cannot read lane {lane_id}: {exc}") from exc
                        if lane.get("lifecycle") in ("retired", "abandoned"):
                            continue
                        if not _wait_for_controller_exit(lane, SHUTDOWN_WAIT_SECONDS):
                            raise ShutdownError(
                                SHUTDOWN_LANE_CLEANUP_UNPROVEN,
                                f"lane {lane_id} controller did not exit; force-stop the lane",
                            )
                        if not _lane_clean(rt, lane):
                            raise ShutdownError(
                                SHUTDOWN_LANE_CLEANUP_UNPROVEN,
                                f"lane {lane_id} processes could not be proven gone; force-stop the lane",
                            )
                        _retire_lane(rt, epoch_id, lane_id, lane)
                close_epoch(rt, epoch_id)

        _stop_monitor(rt)
        with RecordLock(runtime_state_path(rt)):
            atomic_write_json(
                runtime_state_path(rt),
                {"schema": RUNTIME_STATE_SCHEMA, "state": "CLOSED", "updated_at": iso_utc()},
            )
        _prune_worktrees(config.root_workspace)
    except ShutdownError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [str(runtime_state_path(rt))],
            "next_action": "on SHUTDOWN_LANE_CLEANUP_UNPROVEN, run `lane force-stop --lane-id <id>` for the offending lane(s), then retry shutdown",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": SHUTDOWN_RUNTIME_AMBIGUOUS,
            "summary": str(exc),
            "evidence_paths": [str(runtime_state_path(rt))],
            "next_action": "resolve the error and retry shutdown",
        }
    return {
        "ok": True,
        "code": "SHUTDOWN_OK",
        "summary": "runtime is CLOSED; all lanes retired and the monitor stopped",
        "evidence_paths": [str(runtime_state_path(rt))],
        "next_action": "run `harness setup` to open a fresh runtime",
    }
