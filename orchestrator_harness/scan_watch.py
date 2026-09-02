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

from .config import find_harness_root, load_config
from .epochs import (
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
)
from .lanes import read_lane
from .manager_queue import read_manager_queue
from .monitor import ACTIONABLE_STATUSES, derive_lane_status, read_controller_status, reconcile_active_lanes
from .records import read_record

SCAN_NO_ACTIVE_EPOCH = "SCAN_NO_ACTIVE_EPOCH"
WATCH_TIMEOUT = "WATCH_TIMEOUT"
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


def _lane_snapshot(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for entry in read_active_lanes(rt, epoch_id):
        lane_id = entry["lane_id"]
        try:
            lane = read_lane(rt, epoch_id, lane_id)
        except Exception:
            continue
        status = read_controller_status(lane)
        derived = derive_lane_status(rt, epoch_id, lane, status)
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
    return {
        "epoch_id": epoch_id,
        "lane_mode": state.get("lane_mode"),
        "lanes": _lane_snapshot(rt, epoch_id),
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
    result = {
        "ok": True,
        "code": "SCAN_OK",
        "summary": "\n".join(lines),
        "evidence_paths": [],
        "next_action": "act on any actionable status per the operator-responses table",
    }
    result["snapshot"] = snapshot
    return result


def _find_actionable(rt: Path, epoch_id: str) -> tuple[str, str] | None:
    for entry in read_active_lanes(rt, epoch_id):
        lane_id = entry["lane_id"]
        try:
            lane = read_lane(rt, epoch_id, lane_id)
        except Exception:
            continue
        status = read_controller_status(lane)
        derived = derive_lane_status(rt, epoch_id, lane, status)
        if derived in ACTIONABLE_STATUSES:
            return lane_id, derived
    return None


def _event_present(rt: Path, event_id: str) -> bool:
    try:
        queue = read_manager_queue(rt)
    except Exception:
        return False
    return any(event.get("event_id") == event_id for event in queue.get("events", []))


def run_watch(
    *,
    timeout: str | None = None,
    until_event: str | None = None,
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
    while True:
        actionable = _find_actionable(rt, epoch_id)
        if actionable is not None:
            lane_id, status = actionable
            return {
                "ok": True,
                "code": "WATCH_ACTIONABLE",
                "summary": f"lane {lane_id} is actionable: {status}",
                "evidence_paths": [],
                "next_action": "act on the condition per the operator-responses table",
            }
        if until_event is not None and _event_present(rt, until_event):
            return {
                "ok": True,
                "code": "WATCH_EVENT",
                "summary": f"manager event {until_event} is present",
                "evidence_paths": [],
                "next_action": "acknowledge and handle the event",
            }
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
