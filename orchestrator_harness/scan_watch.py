"""``scan``, ``watch``, and ``health reconcile``: ROOT-side polling.

``scan --no-write`` returns a read-only snapshot; ``watch --until-actionable``
blocks the ROOT session until an actionable condition exists (or a named
manager event appears); ``health reconcile`` is the manual entry point to the
monitor's automatic reconciliation.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .config import ConfigError, find_harness_root, load_config
from .epochs import (
    manager_queue_path,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
)
from .lanes import read_lane
from .manager_queue import append_watch_delivery, read_manager_queue
from .root_session import resolve_root_session
from .records import RecordLock, runtime_admission_lock
from .monitor import (
    ACTIONABLE_STATUSES,
    _discover_orphaned_leases,
    derive_lane_status,
    read_controller_status,
    reconcile_active_lanes,
)

SCAN_NO_ACTIVE_EPOCH = "SCAN_NO_ACTIVE_EPOCH"
WATCH_TIMEOUT = "WATCH_TIMEOUT"
WATCH_EVIDENCE_INVALID = "WATCH_EVIDENCE_INVALID"
HEALTH_RECONCILE_NO_ACTIVE_EPOCH = "HEALTH_RECONCILE_NO_ACTIVE_EPOCH"

WATCH_POLL_SECONDS = 2.0


def _parse_duration(text: str | None) -> float | None:
    """Parse a duration like ``30``, ``30s``, ``5m``, or ``1h`` into seconds."""
    if text is None:
        return None
    value = str(text).strip().lower()
    if not value:
        return None
    if value.isdigit():
        return float(value)
    unit = value[-1]
    number = value[:-1]
    if not number.isdigit():
        raise ValueError(f"invalid duration: {text}")
    multiplier = {"s": 1.0, "m": 60.0, "h": 3600.0}.get(unit)
    if multiplier is None:
        raise ValueError(f"invalid duration unit: {text}")
    return float(number) * multiplier


def _active_epoch(rt: Path) -> tuple[str, dict[str, Any]]:
    marker = read_current_epoch(rt)
    if marker is None:
        raise ValueError("no active epoch")
    epoch_id = str(marker["epoch_id"])
    state = read_epoch_state(rt, epoch_id)
    if state.get("lifecycle") != "active":
        raise ValueError("active epoch is not active")
    return epoch_id, state


def _lane_snapshot(
    rt: Path,
    epoch_id: str,
    orphaned_leases: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for entry in read_active_lanes(rt, epoch_id):
        lane_id = entry["lane_id"]
        lane = read_lane(rt, epoch_id, lane_id)
        status = read_controller_status(lane)
        derived = derive_lane_status(
            rt, epoch_id, lane, status, orphaned_leases=orphaned_leases
        )
        entries.append(
            {
                "lane_id": lane_id,
                "run_id": lane.get("run_id", ""),
                "lifecycle": lane.get("lifecycle"),
                "actionable_status": derived,
            }
        )
    return entries


def build_snapshot(rt: Path) -> dict[str, Any]:
    epoch_id, state = _active_epoch(rt)
    orphaned_leases = _discover_orphaned_leases(rt, epoch_id)
    return {
        "epoch_id": epoch_id,
        "lane_mode": state.get("lane_mode"),
        "lanes": _lane_snapshot(rt, epoch_id, orphaned_leases),
        "orphaned_leases": orphaned_leases,
    }


def run_scan() -> dict[str, Any]:
    """Execute ``scan --no-write`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": SCAN_NO_ACTIVE_EPOCH,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "run `harness setup` first",
        }
    rt = config.runtime_root
    try:
        snapshot = build_snapshot(rt)
    except Exception as exc:
        return {
            "ok": False,
            "code": SCAN_NO_ACTIVE_EPOCH,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "run `harness setup` first",
        }
    lines = [f"epoch {snapshot['epoch_id']} ({snapshot['lane_mode']})"]
    for lane in snapshot["lanes"]:
        status = lane["actionable_status"] or "ok"
        lines.append(f"  {lane['lane_id']}: {lane['lifecycle']} ({status})")
    for lease in snapshot.get("orphaned_leases", []):
        lines.append(
            f"  resource {lease['resource_id']}: orphaned lease "
            f"({lease.get('lane_id') or 'missing lane'})"
        )
    result = {
        "ok": True,
        "code": "SCAN_OK",
        "summary": "\n".join(lines),
        "evidence_paths": [],
        "next_action": "act on any actionable status per the operator-responses table",
    }
    result["snapshot"] = snapshot
    return result


def _find_actionable(
    rt: Path,
    epoch_id: str,
    orphaned_leases: list[dict[str, Any]] | None = None,
    *,
    acknowledged_reviews: set[tuple[str, str]] | None = None,
) -> tuple[str, str] | None:
    if orphaned_leases:
        lease = orphaned_leases[0]
        return f"resource:{lease['resource_id']}", "orphaned_lease"
    for entry in read_active_lanes(rt, epoch_id):
        lane_id = entry["lane_id"]
        lane = read_lane(rt, epoch_id, lane_id)
        status = read_controller_status(lane)
        derived = derive_lane_status(
            rt, epoch_id, lane, status, orphaned_leases=orphaned_leases
        )
        if derived == "review_pending" and (
            lane_id, lane.get("run_id", "")
        ) in (acknowledged_reviews or set()):
            continue
        if derived in ACTIONABLE_STATUSES:
            return lane_id, derived
    return None


def _watch_evidence_failure(rt: Path, exc: Exception) -> dict[str, Any]:
    code = str(exc).partition(":")[0] if isinstance(exc, ConfigError) else WATCH_EVIDENCE_INVALID
    if not code.startswith("ROOT_SESSION_"):
        code = WATCH_EVIDENCE_INVALID
    return {"ok": False, "code": code,
            "summary": f"watch cannot inspect or record native evidence: {exc}",
            "evidence_paths": [str(rt)],
            "next_action": "reconcile the named native record before waiting again"}


def _watch_delivered(
    event: dict[str, Any], *, queue_id: Any, root_session_id: str, binding_id: str,
) -> bool:
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        return False
    return any(
        receipt.get("source") == "watch"
        and receipt.get("queue_id") == queue_id
        and receipt.get("event_id") == event_id
        and receipt.get("session_id") == root_session_id
        and receipt.get("binding_id") == binding_id
        for receipt in event.get("delivery_history", [])
        if isinstance(receipt, dict)
    )


def _watch_event(
    queue: dict[str, Any], *, root_session_id: str, binding_id: str,
    requested_event_id: str | None = None,
) -> dict[str, Any] | None:
    queue_id = queue.get("queue_id")
    for event in queue.get("events", []):
        if event.get("state") not in {"PENDING", "ACKNOWLEDGED"}:
            continue
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            continue
        if requested_event_id is not None and event_id != requested_event_id:
            continue
        scope = {"queue_id": queue_id, "event_id": event_id,
                 "session_id": root_session_id, "binding_id": binding_id,
                 "lane_id": event.get("lane_id"), "run_id": event.get("run_id")}
        if event.get("state") == "ACKNOWLEDGED" and any(
                receipt.get("state") == "ACKNOWLEDGED"
                and all(receipt.get(key) == value for key, value in scope.items())
                for receipt in event.get("history", []) if isinstance(receipt, dict)):
            continue
        if not _watch_delivered(
            event, queue_id=queue_id, root_session_id=root_session_id, binding_id=binding_id,
        ):
            return event
    return None


def run_watch(
    *,
    timeout: str | None = None,
    until_event: str | None = None,
    root_session_id: str | None = None,
    binding_id: str | None = None,
) -> dict[str, Any]:
    """Execute ``watch --until-actionable`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": WATCH_TIMEOUT,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "run `harness setup` first",
        }
    rt = config.runtime_root
    try:
        duration = _parse_duration(timeout)
    except ValueError as exc:
        return {
            "ok": False,
            "code": WATCH_TIMEOUT,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "pass a duration like 30s, 5m, or 1h",
        }
    deadline = time.monotonic() + duration if duration is not None else None
    try:
        epoch_id, _state = _active_epoch(rt)
    except Exception as exc:
        return {
            "ok": False,
            "code": WATCH_TIMEOUT,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "run `harness setup` first",
        }
    managed = config.profile == "managed"
    if _state.get("lane_mode") != config.profile:
        return _watch_evidence_failure(rt, ValueError("active epoch profile differs from configuration"))
    if managed:
        try:
            session_id, watch_binding_id = resolve_root_session(
                config, session_id=root_session_id, binding_id=binding_id)
        except Exception as exc:
            return _watch_evidence_failure(rt, exc)
    while True:
        if managed:
            try:
                with runtime_admission_lock(rt), RecordLock(manager_queue_path(rt)):
                    queue = read_manager_queue(rt)
                event = _watch_event(
                    queue,
                    root_session_id=session_id,
                    binding_id=watch_binding_id,
                    requested_event_id=until_event,
                )
            except Exception as exc:
                return _watch_evidence_failure(rt, exc)
            if event is not None:
                event_id = str(event["event_id"])
                try:
                    delivered = append_watch_delivery(
                        rt,
                        event_id,
                        root_session_id=session_id,
                        binding_id=watch_binding_id,
                    )
                except Exception as exc:
                    return _watch_evidence_failure(rt, exc)
                if delivered:
                    return {
                        "ok": True,
                        "code": "WATCH_EVENT",
                        "summary": f"manager event {event_id} is present",
                        "evidence_paths": [],
                        "next_action": "read, acknowledge, and handle the event",
                        "source": "manager_queue",
                        "wake_reason": "manager_event",
                        "event_id": event_id,
                    }
        else:
            try:
                orphaned_leases = _discover_orphaned_leases(rt, epoch_id)
                actionable = _find_actionable(rt, epoch_id, orphaned_leases)
            except Exception as exc:
                return _watch_evidence_failure(rt, exc)
            if actionable is not None:
                lane_id, status = actionable
                result = {
                    "ok": True, "code": "WATCH_ACTIONABLE",
                    "summary": f"{lane_id} is actionable: {status}",
                    "evidence_paths": [],
                    "next_action": "act on the condition per the operator-responses table",
                    "wake_reason": "lane_status",
                }
                if lane_id.startswith("resource:"):
                    result["resource_id"] = lane_id.split(":", 1)[1]
                return result
        if deadline is not None and time.monotonic() >= deadline:
            return {
                "ok": False,
                "code": WATCH_TIMEOUT,
                "summary": "no actionable condition appeared before the timeout",
                "evidence_paths": [],
                "next_action": "re-run watch or act on the last scan snapshot",
            }
        time.sleep(WATCH_POLL_SECONDS)


def run_health_reconcile() -> dict[str, Any]:
    """Execute ``health reconcile`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": HEALTH_RECONCILE_NO_ACTIVE_EPOCH,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "run `harness setup` first",
        }
    rt = config.runtime_root
    try:
        epoch_id, _state = _active_epoch(rt)
        entries = reconcile_active_lanes(rt, epoch_id)
        lanes = _lane_snapshot(rt, epoch_id)
    except Exception as exc:
        return {
            "ok": False,
            "code": HEALTH_RECONCILE_NO_ACTIVE_EPOCH,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "run `harness setup` first",
        }
    return {
        "ok": True,
        "code": "HEALTH_RECONCILE_OK",
        "summary": f"active-lanes rebuilt ({len(entries)} lanes) and statuses re-derived",
        "evidence_paths": [],
        "next_action": "none in the normal case",
        "lanes": lanes,
    }
