"""Epochs: one active epoch, launcher-owned open/reuse/close.

The public launcher owns the epoch records.  It creates a new epoch only when
none is active and otherwise reuses the active epoch while its immutable
configuration is unchanged.  Retirement clears ``CURRENT_EPOCH.json`` before
the epoch is marked closed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import HarnessConfig, ResourceManifest, compute_config_identity
from .core import iso_utc, new_id, read_json, require_schema, utc_now
from .records import RecordLock, atomic_write_json, read_record, write_record

CURRENT_EPOCH_SCHEMA = "current-epoch/v1"
EPOCH_STATE_SCHEMA = "epoch-state/v1"
ACTIVE_LANES_SCHEMA = "active-lanes/v1"
MANAGER_QUEUE_SCHEMA = "manager-queue/v1"


class EpochError(RuntimeError):
    """An epoch operation failed."""


def current_epoch_path(rt: Path) -> Path:
    return rt / "CURRENT_EPOCH.json"


def manager_queue_path(rt: Path) -> Path:
    return rt / "manager" / "QUEUE.json"


def epoch_dir(rt: Path, epoch_id: str) -> Path:
    return rt / "epochs" / epoch_id


def lane_record_dir(rt: Path, epoch_id: str, lane_id: str) -> Path:
    return epoch_dir(rt, epoch_id) / "lanes" / lane_id


def read_current_epoch(rt: Path) -> dict[str, Any] | None:
    """Return the active-epoch marker, or None when no epoch is active."""
    path = current_epoch_path(rt)
    if not path.is_file():
        return None
    try:
        return read_record(path, CURRENT_EPOCH_SCHEMA)
    except (OSError, ValueError):
        return None


def read_epoch_state(rt: Path, epoch_id: str) -> dict[str, Any]:
    return read_record(epoch_dir(rt, epoch_id) / "epoch-state.json", EPOCH_STATE_SCHEMA)


def read_active_lanes(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    path = epoch_dir(rt, epoch_id) / "active-lanes.json"
    if not path.is_file():
        return []
    try:
        record = read_record(path, ACTIVE_LANES_SCHEMA)
    except (OSError, ValueError):
        return []
    lanes = record.get("lanes")
    return lanes if isinstance(lanes, list) else []


def write_active_lanes(rt: Path, epoch_id: str, lanes: list[dict[str, Any]]) -> None:
    path = epoch_dir(rt, epoch_id) / "active-lanes.json"
    record = {
        "schema": ACTIVE_LANES_SCHEMA,
        "epoch_id": epoch_id,
        "lanes": lanes,
    }
    with RecordLock(path):
        atomic_write_json(path, record)


def _stage_manager_queue(rt: Path, epoch_id: str) -> str:
    """Atomically replace the fixed manager queue with a fresh queue_id."""
    queue_id = new_id()
    path = manager_queue_path(rt)
    record = {
        "schema": MANAGER_QUEUE_SCHEMA,
        "epoch_id": epoch_id,
        "queue_id": queue_id,
        "events": [],
    }
    with RecordLock(path):
        atomic_write_json(path, record)
    return queue_id


def open_epoch(
    rt: Path,
    config: HarnessConfig,
    manifest: ResourceManifest,
) -> dict[str, Any]:
    """Return the active epoch, opening a new one when none is active.

    Reuses the active epoch while its immutable configuration is unchanged;
    a config-identity mismatch is an epoch-breaking change and forces a new
    epoch (the caller must retire the old one first).
    """
    identity = compute_config_identity(config, manifest)
    marker = read_current_epoch(rt)
    if marker is not None:
        epoch_id = str(marker["epoch_id"])
        state = read_epoch_state(rt, epoch_id)
        if state.get("config_identity") != identity:
            raise EpochError(
                "active epoch configuration changed; retire the runtime before "
                "changing root_workspace, profile, or the resource manifest"
            )
        if state.get("lifecycle") != "active":
            raise EpochError(f"active epoch {epoch_id} is not active")
        return state

    epoch_id = new_id()
    lane_mode = config.profile
    queue_id: str | None = None
    if lane_mode == "managed":
        queue_id = _stage_manager_queue(rt, epoch_id)
    directory = epoch_dir(rt, epoch_id)
    directory.mkdir(parents=True, exist_ok=True)
    state = {
        "schema": EPOCH_STATE_SCHEMA,
        "epoch_id": epoch_id,
        "config_identity": identity,
        "lane_mode": lane_mode,
        "lifecycle": "opening",
        "lane_dir": str(directory / "lanes"),
        "ownership": "launcher",
        "opened_at": iso_utc(),
    }
    with RecordLock(directory / "epoch-state.json"):
        atomic_write_json(directory / "epoch-state.json", state)
    marker = {
        "schema": CURRENT_EPOCH_SCHEMA,
        "epoch_id": epoch_id,
        "lane_mode": lane_mode,
        "opened_at": iso_utc(),
    }
    if queue_id is not None:
        marker["queue_id"] = queue_id
    with RecordLock(current_epoch_path(rt)):
        atomic_write_json(current_epoch_path(rt), marker)
    state["lifecycle"] = "active"
    with RecordLock(directory / "epoch-state.json"):
        atomic_write_json(directory / "epoch-state.json", state)
    write_active_lanes(rt, epoch_id, [])
    return state


def close_epoch(rt: Path, epoch_id: str) -> None:
    """Retire one epoch: clear the marker first, then mark the epoch closed."""
    marker_path = current_epoch_path(rt)
    with RecordLock(marker_path):
        try:
            marker_path.unlink()
        except FileNotFoundError:
            pass
    state_path = epoch_dir(rt, epoch_id) / "epoch-state.json"
    with RecordLock(state_path):
        try:
            state = read_json(state_path)
        except (OSError, ValueError):
            state = {}
        state["lifecycle"] = "closed"
        state["closed_at"] = iso_utc()
        atomic_write_json(state_path, state)


def lane_record_path(rt: Path, epoch_id: str, lane_id: str) -> Path:
    return lane_record_dir(rt, epoch_id, lane_id) / "lane.json"
