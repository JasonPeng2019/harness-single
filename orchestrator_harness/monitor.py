"""The persistent monitor: exactly one per runtime, the sole producer of
manager-queue events (managed) and the status-derivation engine for both
profiles.

It reads ``CURRENT_EPOCH.json``, then the active epoch's ``epoch-state.json``
and ``active-lanes.json``; it therefore needs no queue-path discovery, provider
binding, or restart when lanes change.
"""

from __future__ import annotations

import os
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

from . import processes
from .activity_log import append_activity
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
from .lanes import LANE_SCHEMA, read_lane, update_lane, lane_operation_lock, require_same_run
from .manager_queue import ManagerQueueError, promote_event, read_manager_queue
from .records import RecordLock, atomic_write_json, atomic_write_bytes, read_record
from .records import runtime_admission_lock
from .mutation import TargetState, MutationConflict, delete as anchored_delete
from .stable_io import PreparedOutputTransaction
from .portable_paths import component as portable_component
from .review import validate_acceptance_chain
from .setup import MONITOR_SCHEMA, monitor_record_path, read_monitor_record, read_runtime_state

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
    if not validate_acceptance_chain(
        review,
        acceptance,
        lane_id=lane_id,
        run_id=lane.get("run_id") if lane is not None else None,
    ):
        return None
    return acceptance


def _lease_files(rt: Path) -> list[dict[str, Any]]:
    from .leases import lease_records
    return [record for _path, record in lease_records(rt)]


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
    from .lanes import retained_lanes
    return retained_lanes(rt, epoch_id)


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
    try:
        from .lane_authority import read_invocation
        from .result_validation import read_result
        read_invocation(lane)
        read_result(lane)
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


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
    if not validate_acceptance_chain(review, acceptance, lane_id=lane["lane_id"], run_id=lane.get("run_id")):
        return False
    try:
        from .lane_authority import read_invocation
        from .result_validation import read_result
        read_invocation(lane)
        result = read_result(lane, clean=acceptance["approval"] == "ACCEPTED")
        return (review["source_assignment_hash"] == lane["result_assignment_hash"]
                and review["task_card_hash"] == lane["task_card_hash"]
                and review["result_hash"] == result["content_hash"] and review["commit"] == result["commit"])
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _recover_broken_review_pair(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> bool:
    """Preserve broken ROOT decisions and expose an actionable diagnostic."""
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    with RecordLock(review_path):
        if not (review_path.exists() or acceptance_path.exists()):
            return False
        if _review_pair_is_valid(rt, epoch_id, lane):
            return False
        raise ValueError("ROOT review/acceptance pair is invalid or incomplete; preserve it for explicit ROOT reconciliation")


def _has_open_review_event(rt: Path, lane: dict[str, Any]) -> bool:
    try:
        queue = read_manager_queue(rt)
    except Exception:
        return False
    for event in queue.get("events", []):
        if (
            (
                event.get("type") == "COMPLETION_REVIEW_REQUIRED"
                or (
                    event.get("type") == "LANE_STATUS_CHANGED"
                    and event.get("actionable_status") == "provider_exited_no_result"
                )
            )
            and event.get("lane_id") == lane.get("lane_id")
            and event.get("run_id") == lane.get("run_id")
            and event.get("state") in {"PENDING", "ACKNOWLEDGED"}
        ):
            return True
    return False


def _recover_lost_review_event(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> list[dict[str, Any]]:
    """Restore one review request when valid work has no open request."""
    lifecycle = lane.get("lifecycle")
    if lifecycle not in {"review_pending", "result_invalid"}:
        return []
    if lifecycle == "result_invalid":
        return []
    if not _valid_current_result(lane):
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
    from .lanes import reconcile_active_lanes as reconcile
    return reconcile(rt, epoch_id)


def _consume_outbox(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> list[dict[str, Any]]:
    """Promote exact current-run notices; retain bytes before deleting sources."""
    diagnostics: list[dict[str, Any]] = []
    worktree = Path(lane["worktree_path"]).absolute()
    workspace = worktree / ".agent-workspace"
    outbox = workspace / "manager-notifications"
    processed = workspace / "processed-notifications"
    append_activity(
        rt,
        "outbox.scan",
        epoch_id=epoch_id,
        lane_id=lane.get("lane_id"),
        path=str(outbox),
        exists=outbox.is_dir(),
    )
    with runtime_admission_lock(rt), lane_operation_lock(rt, epoch_id, lane["lane_id"]):
        try:
            current = read_lane(rt, epoch_id, lane["lane_id"])
            require_same_run(current, lane)
            portable_component(lane["run_id"])
            output = PreparedOutputTransaction(worktree, allowed_roots=(worktree.parent,))
            output.prepare()
            if "worktree_identity" in current and tuple(current["worktree_identity"]) != output.root_identity:
                raise ValueError("notice worktree changed after preparation")
            output.revalidate(outbox)
            if not outbox.exists():
                return diagnostics
            paths = sorted(outbox.iterdir())
        except Exception as exc:
            return [{"kind": "outbox_inspection", "path": str(outbox), "error": str(exc)}]
        for path in paths:
            if path.suffix != ".json":
                continue
            phase = "outbox_inspection"
            try:
                raw = output.read_bytes(path, max_bytes=65536)
                notice = json.loads(raw)
                if (not isinstance(notice, dict) or notice.get("schema") != "manager-notice/v1"
                        or notice.get("lane_id") != lane["lane_id"]
                        or notice.get("run_id") != lane["run_id"]
                        or notice.get("severity") not in {"blocking", "error", "warning", "info"}
                        or any(not isinstance(notice.get(field), str) or not notice[field].strip()
                               for field in ("notice_id", "signal_id", "summary", "event_class", "created_at"))):
                    raise ValueError("notice schema, current lane/run identity or required fields are invalid")
                metadata = path.lstat()
                digest = hashlib.sha256(raw).hexdigest()
                source = TargetState(True, "file", (metadata.st_dev, metadata.st_ino), digest, len(raw), raw)
                archived = output.admit(processed / path.name)
                output.ensure_parent(archived)
                dedup = f"worker-notice:{lane['lane_id']}:{lane['run_id']}:{notice['signal_id']}"
                for existing in read_manager_queue(rt)["events"]:
                    if existing.get("dedup_key") == dedup and existing.get("data", {}).get("notice_sha256") != digest:
                        raise ValueError("notice identity was reused with different bytes")
                require_same_run(read_lane(rt, epoch_id, lane["lane_id"]), lane)
                phase = "outbox_promotion"
                event = promote_event(
                    rt, event_type="LANE_STATUS_CHANGED", lane_id=lane["lane_id"], run_id=lane["run_id"],
                    summary=notice["summary"], actionable_status="worker_escalation",
                    event_class=notice["event_class"], severity=notice["severity"],
                    data={"signal_id": notice["signal_id"], "notice_id": notice["notice_id"],
                          "lane_id": lane["lane_id"], "run_id": lane["run_id"], "notice_sha256": digest},
                    dedup_key=dedup,
                )
                append_activity(rt, "outbox.item.promoted", epoch_id=epoch_id,
                                lane_id=lane["lane_id"], signal_id=notice["signal_id"], event_id=event.get("event_id"))
                phase = "outbox_archive"
                retained = lane_record_dir(rt, epoch_id, lane["lane_id"]) / "notice-archive" / lane["run_id"] / f"{digest}.json"
                if retained.exists():
                    if retained.read_bytes() != raw:
                        raise ValueError("protected notice archive conflicts with the original bytes")
                else:
                    atomic_write_bytes(retained, raw)
                if archived.exists():
                    if output.read_bytes(archived, max_bytes=65536) != raw:
                        raise ValueError("processed notice filename already contains different bytes")
                else:
                    output.write_bytes(archived, raw)
                # Never delete a replacement notice or follow a changed parent.
                output.revalidate(path)
                if output.root_identity != (worktree.stat().st_dev, worktree.stat().st_ino):
                    raise MutationConflict("notice root changed before source removal")
                anchored_delete(worktree, path.relative_to(worktree), expected=source,
                                expected_root_identity=output.root_identity)
                append_activity(rt, "outbox.item.archived", epoch_id=epoch_id,
                                lane_id=lane["lane_id"], path=str(archived), retained_path=str(retained))
            except Exception as exc:
                diagnostics.append({"kind": phase, "path": str(path), "error": str(exc)})
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
    event = promote_event(
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
    append_activity(
        rt, "lane.status.promoted", epoch_id=epoch_id,
        lane_id=lane["lane_id"], run_id=lane.get("run_id", ""),
        status=status, event_id=event.get("event_id"),
    )


def _monitor_owner(record: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(record.get(field) for field in ("pid", "creation_time", "config_identity", "startup_id"))


def _require_monitor_owner(rt: Path, owner: tuple[Any, ...]) -> dict[str, Any]:
    record = read_monitor_record(rt)
    if record is None or _monitor_owner(record) != owner:
        raise ValueError("monitor ownership changed")
    return record


def _monitor_pass(
    rt: Path, config_identity: str, *, monitor_owner: tuple[Any, ...] | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    with runtime_admission_lock(rt):
        if monitor_owner is not None:
            _require_monitor_owner(rt, monitor_owner)
        diagnostics: list[dict[str, Any]] = []
        marker = read_current_epoch(rt)
        if marker is None:
            append_activity(
                rt, "epoch.unavailable", marker_exists=current_epoch_path(rt).exists()
            )
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
            append_activity(
                rt, "epoch.inactive", epoch_id=epoch_id,
                lifecycle=state.get("lifecycle"),
            )
            return 0, diagnostics
        managed = state.get("lane_mode") == "managed"
        lanes = reconcile_active_lanes(rt, epoch_id)
        retained = _retained_lanes(rt, epoch_id)
        orphaned = _discover_orphaned_leases(rt, epoch_id, retained)
        append_activity(
            rt, "epoch.scanned", epoch_id=epoch_id, managed=managed,
            active_lanes=len(lanes), retained_lanes=len(retained),
            orphaned_leases=len(orphaned),
        )
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
            append_activity(
                rt, "lane.scanned", epoch_id=epoch_id, lane_id=lane_id,
                run_id=lane.get("run_id", ""), lifecycle=lane.get("lifecycle"),
                controller_status=(status or {}).get("recorded_status"),
                derived_status=derived,
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
                    append_activity(
                        rt, "lease.orphan.promoted", epoch_id=epoch_id,
                        resource_id=resource_id, lane_id=lease.get("lane_id"),
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
    *, monitor_owner: tuple[Any, ...] | None = None,
) -> None:
    record_path = monitor_record_path(rt)
    with runtime_admission_lock(rt), RecordLock(record_path):
        try:
            record = read_monitor_record(rt)
        except (OSError, ValueError):
            record = None
        if record is None:
            append_activity(rt, "heartbeat.skipped", reason="monitor_record_missing")
            return
        if record.get("config_identity") != config_identity:
            append_activity(rt, "heartbeat.skipped", reason="config_identity_mismatch")
            return
        if monitor_owner is None:
            identity = processes.process_identity(os.getpid())
            if identity is None or (record.get("pid"), record.get("creation_time")) != (
                    identity["pid"], identity["creation_time"]):
                append_activity(rt, "heartbeat.skipped", reason="process_identity_mismatch")
                return
        elif _monitor_owner(record) != monitor_owner:
            append_activity(rt, "heartbeat.skipped", reason="owner_mismatch")
            return
        state = read_runtime_state(rt)
        if not state or state.get("state") != "OPEN" or record.get("stop_requested"):
            return
        record["health"] = "degraded" if diagnostics else "healthy"
        record["last_heartbeat_at"] = iso_utc()
        record["watched_lane_count"] = watched_lane_count
        record["diagnostics"] = list(diagnostics or [])
        if not diagnostics and record.get("initialized") is not True:
            record["initialized"] = True
            record["ready_at"] = record["last_heartbeat_at"]
        atomic_write_json(record_path, record)
    append_activity(
        rt, "heartbeat.written", health=record["health"],
        watched_lane_count=watched_lane_count,
        diagnostic_count=len(record["diagnostics"]),
    )


def run_monitor_once(rt: Path, config_identity: str, *, monitor_owner: tuple[Any, ...] | None = None) -> None:
    append_activity(rt, "pass.started", config_identity=config_identity)
    try:
        watched_lane_count, diagnostics = _monitor_pass(rt, config_identity, monitor_owner=monitor_owner)
    except Exception as exc:
        watched_lane_count, diagnostics = 0, [{"kind": "monitor_pass", "error": str(exc)}]
        append_activity(rt, "pass.failed", error_type=type(exc).__name__)
    _heartbeat(rt, config_identity, watched_lane_count, diagnostics, monitor_owner=monitor_owner)
    append_activity(
        rt, "pass.completed", watched_lane_count=watched_lane_count,
        diagnostic_count=len(diagnostics),
    )


def main() -> int:
    """The monitor process entry point (started by setup)."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
        config_identity = compute_config_identity(config, manifest)
    except Exception as exc:
        print(f"monitor configuration initialization failed: {exc}", file=sys.stderr, flush=True)
        return 1
    rt = config.runtime_root
    identity = processes.process_identity(os.getpid())
    if identity is None:
        return 1
    with runtime_admission_lock(rt):
        record = read_monitor_record(rt)
        if (record is None or not os.environ.get("HARNESS_MONITOR_STARTUP_ID")
                or record.get("startup_id") != os.environ["HARNESS_MONITOR_STARTUP_ID"]
                or record.get("config_identity") != config_identity
                or (record.get("pid"), record.get("creation_time")) != (
                    identity["pid"], identity["creation_time"])):
            return 1
        owner = _monitor_owner(record)
    append_activity(
        rt, "process.started", config_identity=config_identity,
        pass_interval_seconds=PASS_INTERVAL_SECONDS,
    )
    next_pass = time.monotonic()
    while True:
        try:
            record = _require_monitor_owner(rt, owner)
        except (OSError, ValueError):
            return 1
        state = read_runtime_state(rt)
        if record.get("stop_requested", False) or not state or state.get("state") != "OPEN":
            append_activity(rt, "process.stop_requested")
            record_path = monitor_record_path(rt)
            with runtime_admission_lock(rt), RecordLock(record_path):
                try:
                    current = _require_monitor_owner(rt, owner)
                except (OSError, ValueError):
                    return 1
                current["health"] = "STOPPED"
                atomic_write_json(record_path, current)
            append_activity(rt, "process.stopped")
            return 0
        if time.monotonic() >= next_pass:
            try:
                run_monitor_once(rt, config_identity, monitor_owner=owner)
            except Exception as exc:
                append_activity(rt, "process.loop.failed", error_type=type(exc).__name__)
            next_pass = time.monotonic() + PASS_INTERVAL_SECONDS
        # Retain the configured scan cadence while observing cooperative stop
        # promptly; sleeping a whole scan interval delays exact shutdown proof.
        time.sleep(min(0.2, max(0.0, next_pass - time.monotonic())))


if __name__ == "__main__":
    raise SystemExit(main())
