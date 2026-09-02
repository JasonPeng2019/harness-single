"""The persistent monitor: exactly one per runtime, the sole producer of
manager-queue events (managed) and the status-derivation engine for both
profiles.

It reads ``CURRENT_EPOCH.json``, then the active epoch's ``epoch-state.json``
and ``active-lanes.json``; it therefore needs no queue-path discovery, provider
binding, or restart when lanes change.
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from typing import Any

from . import processes
from .config import find_harness_root, load_config
from .core import iso_utc, read_json, require_schema, utc_now
from .epochs import (
    ACTIVE_LANES_SCHEMA,
    current_epoch_path,
    epoch_dir,
    lane_record_dir,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
)
from .lanes import LANE_SCHEMA, read_lane, update_lane
from .manager_queue import ManagerQueueError, promote_event
from .records import RecordLock, atomic_write_json, read_record
from .setup import MONITOR_SCHEMA, monitor_record_path, read_monitor_record

CONTROLLER_STATUS_SCHEMA = "controller-status/v1"
LEASE_SCHEMA = "resource-lease/v1"

PASS_INTERVAL_SECONDS = 30.0
HEARTBEAT_STALENESS_SECONDS = 180.0

ACTIONABLE_STATUSES = frozenset(
    {
        "review_pending",
        "result_invalid",
        "controller_exited",
        "provider_exited_no_result",
        "status_transcript_contradiction",
        "cleanup_unproven",
        "orphaned_lease",
        "resume_required",
    }
)

STATUS_TO_EVENT = {
    "review_pending": "COMPLETION_REVIEW_REQUIRED",
    "result_invalid": "LANE_RESULT_INVALID",
    "resume_required": "LANE_RESUME_REQUIRED",
}


def read_controller_status(lane: dict[str, Any]) -> dict[str, Any] | None:
    path = Path(lane["controller_status_path"])
    if not path.is_file():
        return None
    try:
        return read_record(path, CONTROLLER_STATUS_SCHEMA)
    except (OSError, ValueError):
        return None


def _read_acceptance_chain(rt: Path, epoch_id: str, lane_id: str) -> dict[str, Any] | None:
    """Return the acceptance decision when a complete linked pair exists."""
    folder = lane_record_dir(rt, epoch_id, lane_id)
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    if not review_path.is_file() or not acceptance_path.is_file():
        return None
    try:
        review = read_json(review_path)
        acceptance = read_json(acceptance_path)
        require_schema(review, "completion-review/v1", review_path)
        require_schema(acceptance, "orchestrator-acceptance/v1", acceptance_path)
    except (OSError, ValueError):
        return None
    if review.get("run_id") != acceptance.get("run_id"):
        return None
    if review.get("result_hash") != acceptance.get("result_id") and review.get("result_id") != acceptance.get("result_id"):
        return None
    return acceptance


def _lease_files(rt: Path) -> list[dict[str, Any]]:
    leases_dir = rt / "resources" / "leases"
    if not leases_dir.is_dir():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(leases_dir.iterdir()):
        if not path.is_file():
            continue
        try:
            records.append(read_record(path, LEASE_SCHEMA))
        except (OSError, ValueError):
            continue
    return records


def _orphaned_lease_for(rt: Path, lane: dict[str, Any]) -> bool:
    for lease in _lease_files(rt):
        if lease.get("lane_id") != lane.get("lane_id"):
            continue
        if lease.get("run_id") != lane.get("run_id"):
            return True
        pid = lease.get("pid")
        creation = lease.get("creation_time")
        if not (isinstance(pid, int) and processes.identity_matches(pid, creation)):
            return True
    return False


def _transcript_contradicts(lane: dict[str, Any], status: dict[str, Any] | None) -> bool:
    """Recorded status vs the provider transcript's terminal state.

    The controller records the provider's exit code in its status snapshot;
    a terminal recorded status that contradicts the provider's exit is
    surfaced as ``status_transcript_contradiction``.
    """
    if status is None:
        return False
    provider_state = status.get("provider_state") or {}
    exit_code = provider_state.get("exit_code")
    if exit_code is None:
        return False
    recorded = status.get("recorded_status")
    if recorded == "review_pending" and exit_code != 0:
        return True
    if recorded == "result_invalid" and exit_code == 0:
        return True
    return False


def derive_lane_status(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    status: dict[str, Any] | None,
) -> str | None:
    """Derive one lane's current actionable status, or None when none exists."""
    if lane.get("lifecycle") in ("accepted", "retired", "abandoned"):
        return None
    if _orphaned_lease_for(rt, lane):
        return "orphaned_lease"
    acceptance = _read_acceptance_chain(rt, epoch_id, lane["lane_id"])
    if acceptance is not None:
        if acceptance.get("approval") == "ACCEPTED":
            return None
        if acceptance.get("approval") == "REJECTED":
            return "resume_required"
    process = lane.get("process") or {}
    controller_alive = processes.identity_matches(
        process.get("pid"), process.get("creation_time")
    )
    recorded = (status or {}).get("recorded_status")
    if not controller_alive:
        if recorded in ("review_pending", "result_invalid"):
            return recorded
        return "controller_exited"
    if recorded in ("review_pending", "result_invalid"):
        if not (status or {}).get("cleanup_proven", False):
            return "cleanup_unproven"
        if _transcript_contradicts(lane, status):
            return "status_transcript_contradiction"
        return recorded
    provider_state = (status or {}).get("provider_state") or {}
    if provider_state.get("state") == "exited" and (status or {}).get("result_state") == "absent":
        return "provider_exited_no_result"
    if _transcript_contradicts(lane, status):
        return "status_transcript_contradiction"
    return None


def reconcile_active_lanes(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    """Rebuild ``active-lanes.json`` from the epoch's controlled lanes/ dir."""
    lanes_dir = epoch_dir(rt, epoch_id) / "lanes"
    entries: list[dict[str, Any]] = []
    if lanes_dir.is_dir():
        for lane_folder in sorted(lanes_dir.iterdir()):
            lane_path = lane_folder / "lane.json"
            if not lane_path.is_file():
                continue
            try:
                lane = read_record(lane_path, LANE_SCHEMA)
            except (OSError, ValueError):
                continue
            if lane.get("lifecycle") in ("retired", "abandoned"):
                continue
            entries.append(
                {
                    "lane_id": lane["lane_id"],
                    "lane_record_path": str(lane_path),
                    "run_id": lane.get("run_id", ""),
                }
            )
    path = epoch_dir(rt, epoch_id) / "active-lanes.json"
    record = {"schema": ACTIVE_LANES_SCHEMA, "epoch_id": epoch_id, "lanes": entries}
    with RecordLock(path):
        atomic_write_json(path, record)
    return entries


def _consume_outbox(rt: Path, epoch_id: str, lane: dict[str, Any]) -> None:
    """Move worker-outbox files to processed-notifications/ and promote one
    escalation event per file (managed)."""
    workspace = Path(lane["worktree_path"]) / ".agent-workspace"
    outbox = workspace / "manager-notifications"
    processed = workspace / "processed-notifications"
    if not outbox.is_dir():
        return
    processed.mkdir(parents=True, exist_ok=True)
    for path in sorted(outbox.iterdir()):
        if not path.is_file():
            continue
        try:
            notice = read_json(path)
        except (OSError, ValueError):
            continue
        summary = str(notice.get("summary") or notice.get("message") or "worker escalation")
        severity = str(notice.get("severity") or "blocking")
        try:
            promote_event(
                rt,
                event_type="LANE_STATUS_CHANGED",
                lane_id=lane["lane_id"],
                run_id=lane.get("run_id", ""),
                summary=f"[{severity}] {summary}",
                actionable_status="worker_escalation",
            )
        except ManagerQueueError:
            pass
        shutil.move(str(path), str(processed / path.name))


def _promote_status(rt: Path, epoch_id: str, lane: dict[str, Any], status: str) -> None:
    event_type = STATUS_TO_EVENT.get(status, "LANE_STATUS_CHANGED")
    summary = {
        "review_pending": "lane finished with a structurally valid result; review required",
        "result_invalid": "lane result is missing or invalid",
        "resume_required": "lane was rejected; resume with a new task card",
        "controller_exited": "lane controller process exited before a terminal result",
        "provider_exited_no_result": "provider exited without producing a result",
        "status_transcript_contradiction": "recorded status contradicts the provider transcript",
        "cleanup_unproven": "lane result is terminal but cleanup is not proven",
        "orphaned_lease": "a lease is held by a dead or retired lane",
    }.get(status, status)
    promote_event(
        rt,
        event_type=event_type,
        lane_id=lane["lane_id"],
        run_id=lane.get("run_id", ""),
        summary=summary,
        actionable_status=status if event_type == "LANE_STATUS_CHANGED" else None,
    )


def _monitor_pass(rt: Path, config_identity: str) -> None:
    marker = read_current_epoch(rt)
    if marker is None:
        return
    epoch_id = str(marker["epoch_id"])
    try:
        state = read_epoch_state(rt, epoch_id)
    except (OSError, ValueError):
        return
    if state.get("lifecycle") != "active":
        return
    managed = state.get("lane_mode") == "managed"
    lanes = reconcile_active_lanes(rt, epoch_id)
    for entry in lanes:
        lane_id = entry["lane_id"]
        try:
            lane = read_lane(rt, epoch_id, lane_id)
        except Exception:
            continue
        status = read_controller_status(lane)
        derived = derive_lane_status(rt, epoch_id, lane, status)
        if managed:
            _consume_outbox(rt, epoch_id, lane)
        if derived is None:
            continue
        last = lane.get("last_reported_actionable_status")
        if derived == last:
            continue
        if managed:
            try:
                _promote_status(rt, epoch_id, lane, derived)
            except ManagerQueueError:
                continue
        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current, value=derived: {**current, "last_reported_actionable_status": value},
        )


def _heartbeat(rt: Path, config_identity: str) -> None:
    record_path = monitor_record_path(rt)
    with RecordLock(record_path):
        try:
            record = read_monitor_record(rt)
        except (OSError, ValueError):
            record = None
        if record is None:
            return
        record["health"] = "healthy"
        record["last_heartbeat_at"] = iso_utc()
        atomic_write_json(record_path, record)


def run_monitor_once(rt: Path, config_identity: str) -> None:
    _monitor_pass(rt, config_identity)
    _heartbeat(rt, config_identity)


def main() -> int:
    """The monitor process entry point (started by setup)."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return 1
    rt = config.runtime_root
    while True:
        try:
            run_monitor_once(rt, config.profile)
        except Exception:
            pass
        record = read_monitor_record(rt)
        if record is not None and record.get("stop_requested", False):
            record_path = monitor_record_path(rt)
            with RecordLock(record_path):
                try:
                    current = read_monitor_record(rt)
                except (OSError, ValueError):
                    current = None
                if current is not None and current.get("stop_requested", False):
                    current["health"] = "STOPPED"
                    atomic_write_json(record_path, current)
            return 0
        time.sleep(PASS_INTERVAL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
