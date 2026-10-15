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
from .lane_cleanup import cleanup_is_proven, observe_controller_status, retired_lane_record
from .leases import release_leases
from .manager_queue import ManagerQueueError, read_manager_queue
from .records import RecordLock, atomic_write_json, read_record
from .records import runtime_admission_lock
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
    try:
        if marker is None:
            from .epochs import manager_queue_path, MANAGER_QUEUE_SCHEMA
            from .manager_queue import validate_queue_record
            path = manager_queue_path(rt)
            try:
                queue = read_record(path, MANAGER_QUEUE_SCHEMA)
            except FileNotFoundError:
                return []
            validate_queue_record(queue)
        else:
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
    process = lane.get("process")
    if process == {}:
        return True  # The separate cleanup check still requires native proof.
    if not isinstance(process, dict):
        return False
    pid = process.get("pid")
    creation = process.get("creation_time")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if processes.identity_exit_proven(pid, creation):
            return True
        time.sleep(0.5)
    return processes.identity_exit_proven(pid, creation)


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
        return retired_lane_record(current)

    update_lane(rt, epoch_id, lane_id, retire)
    entries = [e for e in read_active_lanes(rt, epoch_id) if e.get("lane_id") != lane_id]
    write_active_lanes(rt, epoch_id, entries)


def _stop_monitor(rt: Path) -> dict[str, Any] | None:
    """Request cooperative stop and prove the exact monitor process exited."""
    record_path = monitor_record_path(rt)
    append_activity(rt, "process.stop_request.started", component="shutdown")
    with RecordLock(record_path):
        record = read_monitor_record(rt)
        if record is None:
            try:
                record_path.lstat()
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise ShutdownError(SHUTDOWN_MONITOR_UNPROVEN, f"cannot inspect monitor record: {exc}") from exc
            else:
                raise ShutdownError(SHUTDOWN_MONITOR_UNPROVEN, "monitor record is invalid or unreadable")
            append_activity(
                rt, "process.stop_request.completed", component="shutdown",
                outcome="monitor_record_missing",
            )
            return
        pid, creation = record.get("pid"), record.get("creation_time")
        if (type(pid) is not int or pid <= 0 or not processes.valid_creation_identity(creation)
                or record_path.is_symlink()):
            raise ShutdownError(SHUTDOWN_MONITOR_UNPROVEN, "monitor process identity is invalid")
        record["stop_requested"] = True
        atomic_write_json(record_path, record)
    deadline = time.monotonic() + MONITOR_WAIT_SECONDS
    while time.monotonic() < deadline:
        current = read_monitor_record(rt)
        if (current is None or any(current.get(field) != record.get(field)
                for field in ("pid", "creation_time", "config_identity", "startup_id"))):
            raise ShutdownError(SHUTDOWN_MONITOR_UNPROVEN, "monitor evidence changed during shutdown")
        if processes.identity_exit_proven(pid, creation):
            append_activity(
                rt, "process.stop_request.completed", component="shutdown",
                outcome="exact_process_exited",
            )
            return current
        time.sleep(0.5)
    raise ShutdownError(SHUTDOWN_MONITOR_UNPROVEN, "exact monitor exit was not proven within the wait window")


def _prune_worktrees(root_workspace: Path) -> None:
    import subprocess

    subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "prune"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _shutdown_inventory(rt: Path, marker: dict[str, Any] | None) -> list[tuple[str, dict[str, Any]]]:
    """Reconcile the active index against durable, outside-worktree lane records."""
    directory = rt / "epochs"
    try:
        epoch_paths = sorted(directory.iterdir())
    except FileNotFoundError:
        if marker is not None:
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "active epoch directory is missing")
        return []
    if directory.is_symlink():
        raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "epoch inventory is redirected")
    inventory = []
    active_found = False
    for path in epoch_paths:
        if not path.is_dir():
            continue
        if path.is_symlink():
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "epoch record directory is redirected")
        epoch_id = path.name
        state = read_epoch_state(rt, epoch_id)
        lane_directory = path / "lanes"
        if lane_directory.is_symlink():
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "lane inventory is redirected")
        from .lanes import retained_lanes
        durable = retained_lanes(rt, epoch_id)
        if state["lifecycle"] == "closing":
            close_epoch(rt, epoch_id)
            state = read_epoch_state(rt, epoch_id)
        if state.get("lifecycle") == "closed":
            if any(lane.get("lifecycle") not in {"retired", "abandoned"} for lane in durable.values()):
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "closed epoch retains unretired lane ownership")
            continue
        if not marker or marker.get("epoch_id") != epoch_id or state.get("lifecycle") != "active":
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "durable epoch ownership differs from the active marker")
        active_found = True
        for entry in read_active_lanes(rt, epoch_id):
            lane = durable.get(entry["lane_id"])
            if lane is None or (entry.get("run_id") is not None and entry["run_id"] != lane["run_id"]):
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "indexed lane ownership is missing or stale")
        inventory.extend((epoch_id, lane) for lane in durable.values())
    if marker and not active_found:
        raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "active marker has no matching active epoch")
    return inventory


def _monitor_receipt(record: dict[str, Any] | None) -> dict[str, Any] | None:
    return None if record is None else {field: record.get(field) for field in
                                      ("pid", "creation_time", "config_identity", "startup_id")}


def _service_receipt(record: dict[str, Any] | None) -> dict[str, Any] | None:
    return None if record is None else {field: record.get(field) for field in
                                      ("pid", "creation_time", "root", "harness", "policy_sha256", "startup_id")}


def _background_cleanup_snapshot(rt: Path, root: Path, harness: Path) -> dict[str, Any]:
    monitor = read_monitor_record(rt)
    if monitor is not None and not processes.identity_exit_proven(
            monitor.get("pid"), monitor.get("creation_time")):
        raise ShutdownError(SHUTDOWN_MONITOR_UNPROVEN, "current monitor owner exit is unproven")
    from .public_checks import service_cleanup
    service = service_cleanup(root, harness)
    return {"monitor": _monitor_receipt(monitor), "service": _service_receipt(service)}


def _closed_inventory_clean(rt: Path) -> None:
    """Prove quiescence without changing or repairing records during observation."""
    if read_current_epoch(rt) is not None:
        raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "closed runtime retains an active epoch")
    from .lanes import retained_lanes
    directory = rt / "epochs"
    if directory.exists():
        if directory.resolve() != directory.absolute() or not directory.is_dir():
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "closed epoch inventory is redirected or invalid")
        for epoch in directory.iterdir():
            if not epoch.is_dir() or epoch.resolve() != epoch.absolute():
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "closed epoch inventory contains an invalid entry")
            if read_epoch_state(rt, epoch.name).get("lifecycle") != "closed":
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "runtime retains an unclosed epoch")
            for lane in retained_lanes(rt, epoch.name).values():
                if lane.get("lifecycle") not in {"retired", "abandoned"} or not _lane_clean(rt, lane):
                    raise ShutdownError(SHUTDOWN_LANE_CLEANUP_UNPROVEN, "retained lane cleanup is unproven")
    from .leases import lease_dir
    directory = lease_dir(rt)
    if directory.exists():
        if directory.resolve() != directory.absolute() or not directory.is_dir():
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "closed lease inventory is redirected or invalid")
        for path in directory.iterdir():
            if not (path.name.startswith(".") and path.name.endswith(".lock")
                    and path.is_file() and path.resolve() == path.absolute()):
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "closed runtime retains resource ownership")


def prove_runtime_closed(rt: Path, root: Path, harness: Path) -> None:
    """Check the CLOSED receipt against current exact native owners."""
    with runtime_admission_lock(rt):
        state = read_runtime_state(rt)
        if not state or state.get("state") != "CLOSED" or not isinstance(state.get("cleanup_owners"), dict):
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "runtime has no proven CLOSED cleanup receipt; run harness shutdown")
        _closed_inventory_clean(rt)
        if state["cleanup_owners"] != _background_cleanup_snapshot(rt, root, harness):
            raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "CLOSED cleanup receipt does not match current ownership")


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
        with runtime_admission_lock(rt):
            state = read_runtime_state(rt)
            if state is None:
                from .public_checks import home
                if (runtime_state_path(rt).exists() or (rt / "epochs").exists()
                        or monitor_record_path(rt).exists() or (home(config.root_workspace) / "service.json").exists()):
                    raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "runtime state is missing or invalid while native ownership records remain")
                return {
                    "ok": True,
                    "code": "SHUTDOWN_OK",
                    "summary": "runtime is not open; nothing to shut down",
                    "evidence_paths": [],
                    "next_action": "none",
                }
            current_state = state.get("state")
            if current_state == "CLOSED":
                _closed_inventory_clean(rt)
                owners = _background_cleanup_snapshot(rt, config.root_workspace, harness_root)
                expected = state.get("cleanup_owners")
                if expected is not None and expected != owners:
                    raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "CLOSED background ownership differs from its cleanup receipt")
                if expected is None:
                    # Reconcile an older CLOSED runtime only from retained exact
                    # native ownership, never from the CLOSED label alone.
                    if owners["monitor"] is None:
                        raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "legacy CLOSED runtime has no retained monitor cleanup identity")
                    atomic_write_json(runtime_state_path(rt), {**state, "cleanup_owners": owners, "updated_at": iso_utc()})
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
            marker = read_current_epoch(rt)
            inventory = _shutdown_inventory(rt, marker)
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
                atomic_write_json(
                    runtime_state_path(rt),
                    {"schema": RUNTIME_STATE_SCHEMA, "state": "SHUTTING_DOWN", "updated_at": iso_utc()},
                )

        for epoch_id, captured in inventory:
            lane_id = captured["lane_id"]
            with runtime_admission_lock(rt), lane_operation_lock(rt, epoch_id, lane_id):
                lane = read_lane(rt, epoch_id, lane_id)
                require_same_run(lane, captured)
                if lane.get("lifecycle") in ("retired", "abandoned"):
                    continue
            # Controllers need this operation lock to complete their own startup.
            if not _wait_for_controller_exit(lane, SHUTDOWN_WAIT_SECONDS):
                raise ShutdownError(SHUTDOWN_LANE_CLEANUP_UNPROVEN, f"lane {lane_id} controller exit is unproven; force-stop the lane")
            with runtime_admission_lock(rt), lane_operation_lock(rt, epoch_id, lane_id):
                current = read_lane(rt, epoch_id, lane_id)
                require_same_run(current, lane)
                if not _lane_clean(rt, current):
                    raise ShutdownError(SHUTDOWN_LANE_CLEANUP_UNPROVEN, f"lane {lane_id} cleanup is unproven; force-stop the lane")
                _retire_lane(rt, epoch_id, lane_id, current)
        if marker is not None:
            close_epoch(rt, str(marker["epoch_id"]))

        from .public_checks import stop_service
        service_receipt = stop_service(config.root_workspace, harness_root)
        monitor_receipt = _stop_monitor(rt)
        expected_owners = {"monitor": _monitor_receipt(monitor_receipt),
                           "service": _service_receipt(service_receipt)}
        with runtime_admission_lock(rt):
            state = read_runtime_state(rt)
            if not state or state.get("state") != "SHUTTING_DOWN" or read_current_epoch(rt) is not None:
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "runtime ownership changed before CLOSED")
            if _shutdown_inventory(rt, None):
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "durable active ownership remains before CLOSED")
            _closed_inventory_clean(rt)
            owners = _background_cleanup_snapshot(rt, config.root_workspace, harness_root)
            if owners != expected_owners:
                raise ShutdownError(SHUTDOWN_RUNTIME_AMBIGUOUS, "background ownership changed after shutdown exit proof")
            atomic_write_json(
                runtime_state_path(rt),
                {"schema": RUNTIME_STATE_SCHEMA, "state": "CLOSED", "cleanup_owners": owners, "updated_at": iso_utc()},
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
