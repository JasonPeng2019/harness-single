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
from .config import (
    compute_config_identity,
    find_harness_root,
    load_config,
    load_resource_manifest,
)
from .core import content_hash, iso_utc, read_json, require_schema, utc_now
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
from .manager_queue import ManagerQueueError, promote_event, read_manager_queue
from .records import RecordLock, atomic_write_json, read_record
from .review import (
    lane_integrity_contract_error,
    validate_acceptance_chain,
    validate_lane_acceptance_chain,
)
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


def _read_acceptance_chain(
    rt: Path,
    epoch_id: str,
    lane_id: str,
    lane: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
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
    valid = (
        validate_lane_acceptance_chain(review, acceptance, lane)
        if lane is not None
        else validate_acceptance_chain(
            review, acceptance, lane_id=lane_id, run_id=None
        )
    )
    if not valid:
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


def _retained_lanes(rt: Path, epoch_id: str) -> dict[str, dict[str, Any]]:
    """Read retained lane records, including retired records omitted from the index."""
    result: dict[str, dict[str, Any]] = {}
    lanes_dir = epoch_dir(rt, epoch_id) / "lanes"
    if not lanes_dir.is_dir():
        return result
    for folder in sorted(lanes_dir.iterdir()):
        path = folder / "lane.json"
        if not path.is_file():
            continue
        try:
            lane = read_record(path, LANE_SCHEMA)
        except (OSError, ValueError):
            continue
        lane_id = lane.get("lane_id")
        if isinstance(lane_id, str) and lane_id:
            result[lane_id] = lane
    return result


def _discover_orphaned_leases(
    rt: Path, epoch_id: str, lanes: dict[str, dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Scan all lease files once and return resource-specific orphan facts."""
    retained = lanes if lanes is not None else _retained_lanes(rt, epoch_id)
    orphaned: list[dict[str, Any]] = []
    for lease in _lease_files(rt):
        resource_id = lease.get("resource_id")
        lane_id = lease.get("lane_id")
        run_id = lease.get("run_id")
        pid = lease.get("pid")
        creation = lease.get("creation_time")
        if not isinstance(resource_id, str) or not resource_id:
            continue
        # Exact live evidence wins over stale lane metadata.  In particular,
        # a live holder is never reported merely because its retained lane is
        # marked retired or has a newer run.
        if isinstance(pid, int) and processes.identity_matches(pid, creation):
            continue
        lane = retained.get(lane_id) if isinstance(lane_id, str) else None
        if lane is None:
            reason = "lane_missing"
        elif lane.get("lifecycle") in {"retired", "abandoned"}:
            reason = f"lane_{lane.get('lifecycle')}"
        elif lane.get("run_id") != run_id:
            reason = "lease_run_is_not_current"
        else:
            reason = "holder_not_live"
        orphaned.append(
            {
                "resource_id": resource_id,
                "lane_id": lane_id,
                "run_id": run_id,
                "pid": pid,
                "creation_time": creation,
                "reason": reason,
            }
        )
    return orphaned


def _valid_current_result(lane: dict[str, Any]) -> bool:
    """Check the current worktree result without treating prose as a result."""
    path = Path(lane.get("result_path") or Path(lane["worktree_path"]) / "RESULT.json")
    if not path.is_file():
        return False
    try:
        result = read_json(path)
        require_schema(result, "result/v1", path)
    except (OSError, ValueError):
        return False
    return (
        result.get("lane_id") == lane.get("lane_id")
        and result.get("run_id") == lane.get("run_id")
        and result.get("outcome") in {"PASS", "FAIL", "BLOCKED"}
        and isinstance(result.get("summary"), str)
        and bool(result["summary"].strip())
        and isinstance(result.get("evidence"), list)
        and isinstance(result.get("completed_at"), str)
        and bool(result["completed_at"].strip())
        and result.get("content_hash") == content_hash(result)
    )


def _review_pair_is_valid(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> bool:
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    if not review_path.is_file() or not acceptance_path.is_file():
        return False
    try:
        review = read_json(review_path)
        acceptance = read_json(acceptance_path)
        require_schema(review, "completion-review/v1", review_path)
        require_schema(acceptance, "orchestrator-acceptance/v1", acceptance_path)
    except (OSError, ValueError):
        return False
    return validate_lane_acceptance_chain(review, acceptance, lane)


def _recover_broken_review_pair(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> bool:
    """Remove a broken pair and leave the lane awaiting a fresh review."""
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    if review_path.exists() or acceptance_path.exists():
        legacy = lane_integrity_contract_error(lane)
        if legacy is not None:
            # These records may be perfectly valid under the old contract.
            # Preserve them and surface an explicit migration diagnostic; the
            # monitor is not permitted to turn a version mismatch into data
            # deletion.
            raise ValueError(legacy)
    with RecordLock(review_path):
        if not (review_path.exists() or acceptance_path.exists()):
            return False
        if _review_pair_is_valid(rt, epoch_id, lane):
            return False
        for path in (review_path, acceptance_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
    if _valid_current_result(lane):
        update_lane(
            rt,
            epoch_id,
            lane["lane_id"],
            lambda current: {
                **current,
                "lifecycle": "review_pending",
                "last_reported_actionable_status": None,
                "acceptance_advancement": None,
                "review_recovery": {
                    "at": iso_utc(),
                    "reason": "malformed_or_incomplete_pair_removed",
                    "run_id": lane.get("run_id"),
                },
            },
        )
    return True


def _has_open_review_event(rt: Path, lane: dict[str, Any]) -> bool:
    try:
        queue = read_manager_queue(rt)
    except Exception:
        return False
    for event in queue.get("events", []):
        if (
            event.get("type") == "COMPLETION_REVIEW_REQUIRED"
            and event.get("lane_id") == lane.get("lane_id")
            and event.get("run_id") == lane.get("run_id")
            and event.get("state") in {"PENDING", "ACKNOWLEDGED"}
        ):
            return True
    return False


def _recover_lost_review_event(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> list[dict[str, Any]]:
    """Restore one review request when a valid result has no open request."""
    if lane.get("lifecycle") != "review_pending" or not _valid_current_result(lane):
        return []
    if _review_pair_is_valid(rt, epoch_id, lane):
        return []
    if _has_open_review_event(rt, lane):
        update_lane(
            rt,
            epoch_id,
            lane["lane_id"],
            lambda current: {
                **current,
                "last_reported_actionable_status": "review_pending",
            },
        )
        return []
    dedup_key = f"review:{lane['lane_id']}:{lane.get('run_id', '')}:{epoch_id}"
    try:
        queue = read_manager_queue(rt)
    except ManagerQueueError:
        raise
    if any(
        event.get("dedup_key") == dedup_key
        and event.get("state") in {"COMPLETE", "BLOCKED"}
        for event in queue.get("events", [])
    ):
        # A terminal event without its durable pair is a lost review, not a
        # reason to suppress recovery.  The new event remains uniquely
        # associated with the same lane/run/epoch.
        dedup_key = None
    try:
        event = promote_event(
            rt,
            event_type="COMPLETION_REVIEW_REQUIRED",
            lane_id=lane["lane_id"],
            run_id=str(lane.get("run_id") or ""),
            summary="lane has a valid result and requires ROOT completion review",
            event_class="COMPLETION_REVIEW_REQUIRED",
            severity="info",
            data={"lane_id": lane["lane_id"], "run_id": lane.get("run_id")},
            dedup_key=dedup_key,
        )
    except ManagerQueueError:
        raise
    update_lane(
        rt,
        epoch_id,
        lane["lane_id"],
        lambda current, value=event: {
            **current,
            "review_recovery": {
                "at": iso_utc(),
                "reason": "missing_open_review_event",
                "run_id": lane.get("run_id"),
                "event_id": value.get("event_id"),
            },
            "last_reported_actionable_status": "review_pending",
        },
    )
    return [event]


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
        if status.get("result_state") == "valid" or _valid_current_result(lane):
            return False
        return True
    if recorded == "result_invalid" and exit_code == 0:
        return True
    return False


def derive_lane_status(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    status: dict[str, Any] | None,
    *,
    lease_records: list[dict[str, Any]] | None = None,
    orphaned_leases: list[dict[str, Any]] | None = None,
) -> str | None:
    """Derive one lane's current actionable status, or None when none exists."""
    if lane.get("lifecycle") in ("accepted", "retired", "abandoned"):
        return None
    if lane.get("lifecycle") == "resuming":
        return None
    if orphaned_leases is not None:
        has_orphan = any(
            lease.get("lane_id") == lane.get("lane_id")
            for lease in orphaned_leases
        )
    else:
        leases = lease_records if lease_records is not None else _lease_files(rt)
        has_orphan = any(
            lease.get("lane_id") == lane.get("lane_id")
            and (
                lease.get("run_id") != lane.get("run_id")
                or not (
                    isinstance(lease.get("pid"), int)
                    and processes.identity_matches(
                        lease.get("pid"), lease.get("creation_time")
                    )
                )
            )
            for lease in leases
        )
    if has_orphan:
        return "orphaned_lease"
    acceptance = _read_acceptance_chain(rt, epoch_id, lane["lane_id"], lane)
    if acceptance is not None:
        if acceptance.get("approval") == "ACCEPTED":
            return None
        if acceptance.get("approval") == "REJECTED":
            return "resume_required"
    if lane.get("lifecycle") == "prepared":
        return None
    process = lane.get("process") or {}
    controller_alive = processes.identity_matches(
        process.get("pid"), process.get("creation_time")
    )
    if lane.get("launch_pending") is True:
        return None
    recorded = (status or {}).get("recorded_status")
    if recorded == "correction_pending":
        return None if controller_alive else "controller_exited"
    if recorded == "provider_exited_no_result":
        return recorded
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
    if provider_state.get("state") == "exited" and (status or {}).get("result_state") in {"absent", "invalid"}:
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
            if (
                lane.get("lifecycle") in ("retired", "abandoned")
                or lane.get("publication_state") == "staged"
            ):
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


def _consume_outbox(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> list[dict[str, Any]]:
    """Promote outbox files before archiving them, retaining failures."""
    diagnostics: list[dict[str, Any]] = []
    workspace = Path(lane["worktree_path"]) / ".agent-workspace"
    outbox = workspace / "manager-notifications"
    processed = workspace / "processed-notifications"
    if not outbox.is_dir():
        return diagnostics
    for path in sorted(outbox.iterdir()):
        if not path.is_file():
            continue
        try:
            notice = read_json(path)
        except (OSError, ValueError) as exc:
            diagnostics.append(
                {"kind": "outbox_inspection", "path": str(path), "error": str(exc)}
            )
            continue
        if not isinstance(notice, dict):
            diagnostics.append(
                {
                    "kind": "outbox_inspection",
                    "path": str(path),
                    "error": "notification is not a JSON object",
                }
            )
            continue
        summary = str(notice.get("summary") or notice.get("message") or "worker escalation")
        severity = str(notice.get("severity") or "blocking")
        event_class = str(notice.get("event_class") or "WORKER_ESCALATION")
        notice_id = str(notice.get("notice_id") or path.stem)
        signal_id = str(notice.get("signal_id") or notice_id)
        try:
            promote_event(
                rt,
                event_type="LANE_STATUS_CHANGED",
                lane_id=lane["lane_id"],
                run_id=lane.get("run_id", ""),
                summary=summary,
                actionable_status="worker_escalation",
                event_class=event_class,
                severity=severity,
                data={
                    "signal_id": signal_id,
                    "notice_id": notice_id,
                    "lane_id": lane["lane_id"],
                    "run_id": lane.get("run_id", ""),
                },
                dedup_key=f"worker-notice:{lane['lane_id']}:{lane.get('run_id', '')}:{signal_id}",
            )
        except Exception as exc:
            diagnostics.append(
                {"kind": "outbox_promotion", "path": str(path), "error": str(exc)}
            )
            # The source remains in the outbox for the next monitor pass.
            continue
        try:
            processed.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(processed / path.name))
        except OSError as exc:
            diagnostics.append(
                {"kind": "outbox_archive", "path": str(path), "error": str(exc)}
            )
    return diagnostics


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
    severity = {
        "review_pending": "info",
        "resume_required": "warning",
        "result_invalid": "error",
        "provider_exited_no_result": "error",
        "orphaned_lease": "error",
    }.get(status, "warning")
    promote_event(
        rt,
        event_type=event_type,
        lane_id=lane["lane_id"],
        run_id=lane.get("run_id", ""),
        summary=summary,
        actionable_status=status if event_type == "LANE_STATUS_CHANGED" else None,
        event_class=status.upper(),
        severity=severity,
        data={"lane_id": lane["lane_id"], "run_id": lane.get("run_id", "")},
        dedup_key=f"lane-status:{lane['lane_id']}:{lane.get('run_id', '')}:{status}",
    )


def _monitor_pass(
    rt: Path, config_identity: str
) -> tuple[int, list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    marker = read_current_epoch(rt)
    if marker is None:
        if current_epoch_path(rt).exists():
            return 0, [
                {
                    "kind": "epoch_inspection",
                    "path": str(current_epoch_path(rt)),
                    "error": "CURRENT_EPOCH.json is missing, malformed, or has an invalid schema",
                }
            ]
        return 0, diagnostics
    epoch_id = str(marker["epoch_id"])
    try:
        state = read_epoch_state(rt, epoch_id)
    except (OSError, ValueError) as exc:
        return 0, [{"kind": "epoch_inspection", "error": str(exc)}]
    if state.get("lifecycle") != "active":
        return 0, diagnostics
    managed = state.get("lane_mode") == "managed"
    lanes = reconcile_active_lanes(rt, epoch_id)
    retained = _retained_lanes(rt, epoch_id)
    orphaned = _discover_orphaned_leases(rt, epoch_id, retained)
    for entry in lanes:
        lane_id = entry["lane_id"]
        try:
            lane = read_lane(rt, epoch_id, lane_id)
        except Exception as exc:
            diagnostics.append(
                {"kind": "lane_inspection", "lane_id": lane_id, "error": str(exc)}
            )
            continue
        try:
            _recover_broken_review_pair(rt, epoch_id, lane)
            lane = read_lane(rt, epoch_id, lane_id)
            if managed:
                _recover_lost_review_event(rt, epoch_id, lane)
                lane = read_lane(rt, epoch_id, lane_id)
        except Exception as exc:
            diagnostics.append(
                {"kind": "review_recovery", "lane_id": lane_id, "error": str(exc)}
            )
        status = read_controller_status(lane)
        derived = derive_lane_status(
            rt,
            epoch_id,
            lane,
            status,
            orphaned_leases=orphaned,
        )
        if managed:
            diagnostics.extend(_consume_outbox(rt, epoch_id, lane))
        if derived is None:
            continue
        last = lane.get("last_reported_actionable_status")
        if derived == last:
            continue
        if derived == "orphaned_lease":
            # Global lease discovery below emits one event per resource.
            continue
        if managed:
            try:
                _promote_status(rt, epoch_id, lane, derived)
            except Exception as exc:
                diagnostics.append(
                    {"kind": "status_promotion", "lane_id": lane_id, "error": str(exc)}
                )
                continue
        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current, value=derived: {**current, "last_reported_actionable_status": value},
        )
    if managed:
        for lease in orphaned:
            resource_id = str(lease["resource_id"])
            try:
                promote_event(
                    rt,
                    event_type="LANE_STATUS_CHANGED",
                    lane_id=str(lease.get("lane_id") or ""),
                    run_id=str(lease.get("run_id") or ""),
                    summary=f"orphaned lease blocks resource {resource_id}",
                    actionable_status="orphaned_lease",
                    event_class="ORPHANED_LEASE",
                    severity="error",
                    data=lease,
                    dedup_key=f"orphan-lease:{resource_id}",
                )
            except Exception as exc:
                diagnostics.append(
                    {
                        "kind": "orphan_lease_promotion",
                        "resource_id": resource_id,
                        "error": str(exc),
                    }
                )
    return len(lanes), diagnostics


def _heartbeat(
    rt: Path,
    config_identity: str,
    watched_lane_count: int = 0,
    diagnostics: list[dict[str, Any]] | None = None,
) -> None:
    record_path = monitor_record_path(rt)
    with RecordLock(record_path):
        try:
            record = read_monitor_record(rt)
        except (OSError, ValueError):
            record = None
        if record is None:
            return
        if record.get("config_identity") != config_identity:
            return
        record["health"] = "degraded" if diagnostics else "healthy"
        record["last_heartbeat_at"] = iso_utc()
        record["watched_lane_count"] = watched_lane_count
        record["diagnostics"] = list(diagnostics or [])
        atomic_write_json(record_path, record)


def run_monitor_once(rt: Path, config_identity: str) -> None:
    try:
        watched_lane_count, diagnostics = _monitor_pass(rt, config_identity)
    except Exception as exc:
        watched_lane_count, diagnostics = 0, [{"kind": "monitor_pass", "error": str(exc)}]
    _heartbeat(rt, config_identity, watched_lane_count, diagnostics)


def main() -> int:
    """The monitor process entry point (started by setup)."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
        config_identity = compute_config_identity(config, manifest)
    except Exception as exc:
        return 1
    rt = config.runtime_root
    while True:
        try:
            run_monitor_once(rt, config_identity)
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
