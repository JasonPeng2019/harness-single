"""Epochs: one active epoch, launcher-owned open/reuse/close.

The public launcher owns the epoch records.  It creates a new epoch only when
none is active and otherwise reuses the active epoch while its immutable
configuration is unchanged. Retirement records a closing intent before
clearing ``CURRENT_EPOCH.json``, so an interrupted close can be completed.
"""

from __future__ import annotations

from pathlib import Path
import stat
from typing import Any

from .config import HarnessConfig, ResourceManifest, compute_config_identity
from .core import iso_utc, new_id, read_json, require_schema, utc_now
from .records import RecordLock, atomic_write_json, read_record, write_record, runtime_admission_lock
from .portable_paths import component as portable_component

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
    try:
        portable_component(epoch_id)
    except ValueError as exc:
        raise EpochError(str(exc)) from exc
    return rt / "epochs" / epoch_id


def lane_record_dir(rt: Path, epoch_id: str, lane_id: str) -> Path:
    try:
        portable_component(lane_id)
    except ValueError as exc:
        raise EpochError(str(exc)) from exc
    return epoch_dir(rt, epoch_id) / "lanes" / lane_id


def read_current_epoch(rt: Path) -> dict[str, Any] | None:
    """Return the active-epoch marker, or None when no epoch is active."""
    path = current_epoch_path(rt)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        raise EpochError(f"active epoch marker is not a regular record: {path}")
    record = read_record(path, CURRENT_EPOCH_SCHEMA)
    if not _record_id(record.get("epoch_id")):
        raise EpochError(f"active epoch identity is invalid: {path}")
    return record


def _record_id(value: object) -> bool:
    try:
        portable_component(value)
    except ValueError:
        return False
    return True


def read_epoch_state(rt: Path, epoch_id: str) -> dict[str, Any]:
    record = read_record(epoch_dir(rt, epoch_id) / "epoch-state.json", EPOCH_STATE_SCHEMA)
    if record.get("epoch_id") != epoch_id or record.get("lifecycle") not in {"opening", "active", "closing", "closed"}:
        raise EpochError(f"epoch state identity or lifecycle is invalid: {epoch_id}")
    return record


def read_active_lanes(rt: Path, epoch_id: str) -> list[dict[str, Any]]:
    path = epoch_dir(rt, epoch_id) / "active-lanes.json"
    record = read_record(path, ACTIVE_LANES_SCHEMA)
    lanes = record.get("lanes")
    if record.get("epoch_id") != epoch_id or not isinstance(lanes, list):
        raise EpochError(f"active lane inventory is invalid: {path}")
    identities = []
    for entry in lanes:
        if not isinstance(entry, dict) or not _record_id(entry.get("lane_id")):
            raise EpochError(f"active lane entry is invalid: {path}")
        if "run_id" in entry and (not isinstance(entry["run_id"], str) or not entry["run_id"]):
            raise EpochError(f"active lane run identity is invalid: {path}")
        identities.append(entry["lane_id"])
    if len(set(identities)) != len(identities):
        raise EpochError(f"active lane inventory contains duplicate identities: {path}")
    return lanes


def write_active_lanes(rt: Path, epoch_id: str, lanes: list[dict[str, Any]]) -> None:
    path = epoch_dir(rt, epoch_id) / "active-lanes.json"
    record = {
        "schema": ACTIVE_LANES_SCHEMA,
        "epoch_id": epoch_id,
        "lanes": lanes,
    }
    with RecordLock(path):
        atomic_write_json(path, record)


def _stage_manager_queue(rt: Path, epoch_id: str, *, queue_id: str | None = None) -> str:
    """Atomically replace the fixed manager queue with a fresh queue_id."""
    queue_id = queue_id or new_id()
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
    with runtime_admission_lock(rt):
        return _open_epoch_locked(rt, config, manifest)


def _open_epoch_locked(rt: Path, config: HarnessConfig, manifest: ResourceManifest) -> dict[str, Any]:
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
        if state.get("lifecycle") == "opening":
            return _finish_open_epoch(rt, state)
        if state.get("lifecycle") != "active":
            raise EpochError(f"active epoch {epoch_id} is not active")
        return state

    # Interrupted ownership transitions retain their durable intent. Do not
    # replace the fixed queue or hide them by opening a different epoch.
    directory = rt / "epochs"
    if directory.exists():
        pending = []
        for previous in sorted(directory.iterdir()):
            if not previous.is_dir() or previous.is_symlink():
                raise EpochError(f"unexpected epoch inventory entry: {previous}")
            prior = read_epoch_state(rt, previous.name)
            if prior["lifecycle"] == "closing":
                close_epoch(rt, previous.name)
            elif prior["lifecycle"] != "closed":
                pending.append(prior)
        if pending:
            if len(pending) != 1 or pending[0]["lifecycle"] != "opening" or pending[0].get("config_identity") != identity:
                raise EpochError("unfinished epoch ownership requires explicit reconciliation")
            return _finish_open_epoch(rt, pending[0])

    epoch_id = new_id()
    lane_mode = config.profile
    queue_id = new_id() if lane_mode == "managed" else None
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
        "queue_id": queue_id,
    }
    with RecordLock(directory / "epoch-state.json"):
        atomic_write_json(directory / "epoch-state.json", state)
    return _finish_open_epoch(rt, state)


def _finish_open_epoch(rt: Path, state: dict[str, Any]) -> dict[str, Any]:
    from .lanes import retained_lanes
    from .manager_queue import validate_queue_record

    epoch_id = state["epoch_id"]
    directory = epoch_dir(rt, epoch_id)
    if retained_lanes(rt, epoch_id) or (rt / "worktrees" / epoch_id).exists():
        raise EpochError("opening epoch has unexpected lane or worktree ownership")
    index = directory / "active-lanes.json"
    if index.exists():
        if read_active_lanes(rt, epoch_id):
            raise EpochError("opening epoch index is not empty")
    else:
        write_active_lanes(rt, epoch_id, [])
    queue_id = state.get("queue_id")
    if state["lane_mode"] == "managed":
        if not _record_id(queue_id):
            raise EpochError("opening epoch has no durable queue identity")
        path = manager_queue_path(rt)
        if path.exists():
            prior_queue = read_record(path, MANAGER_QUEUE_SCHEMA)
            validate_queue_record(prior_queue)
            if any(event["state"] in {"PENDING", "ACKNOWLEDGED"} for event in prior_queue["events"]):
                raise EpochError("cannot replace unresolved manager obligations while opening an epoch")
            same_queue = prior_queue.get("epoch_id") == epoch_id and prior_queue.get("queue_id") == queue_id
        else:
            same_queue = False
        if not same_queue:
            _stage_manager_queue(rt, epoch_id, queue_id=queue_id)
    marker = {
        "schema": CURRENT_EPOCH_SCHEMA,
        "epoch_id": epoch_id,
        "lane_mode": state["lane_mode"],
        "opened_at": state["opened_at"],
    }
    if queue_id is not None:
        marker["queue_id"] = queue_id
    with RecordLock(current_epoch_path(rt)):
        atomic_write_json(current_epoch_path(rt), marker)
    state["lifecycle"] = "active"
    with RecordLock(directory / "epoch-state.json"):
        atomic_write_json(directory / "epoch-state.json", state)
    return state


def close_epoch(rt: Path, epoch_id: str) -> None:
    """Close a proven-empty epoch under admission, with durable closing intent."""
    from .lanes import reconcile_active_lanes
    from .manager_queue import read_manager_queue, validate_queue_record

    with runtime_admission_lock(rt):
        marker_path = current_epoch_path(rt)
        state_path = epoch_dir(rt, epoch_id) / "epoch-state.json"
        with RecordLock(state_path):
            state = read_epoch_state(rt, epoch_id)
            marker = read_current_epoch(rt)
            if marker is not None and marker.get("epoch_id") != epoch_id:
                raise EpochError("cannot clear another epoch's active marker")
            if marker is None and state["lifecycle"] not in {"closing", "closed"}:
                raise EpochError("active epoch marker is missing; closure is ambiguous")
            if reconcile_active_lanes(rt, epoch_id):
                raise EpochError("cannot close an epoch with durable active lanes")
            if state.get("lane_mode") == "managed":
                if marker is not None:
                    queue = read_manager_queue(rt)
                else:
                    queue = read_record(manager_queue_path(rt), MANAGER_QUEUE_SCHEMA)
                    validate_queue_record(queue)
                    if queue.get("queue_id") != state.get("closing_queue_id"):
                        raise EpochError("closing epoch queue identity changed")
                if queue.get("epoch_id") != epoch_id or any(
                    event["state"] in {"PENDING", "ACKNOWLEDGED"} for event in queue["events"]
                ):
                    raise EpochError("cannot close an epoch with unresolved or foreign manager obligations")
            if state["lifecycle"] != "closed":
                state["lifecycle"] = "closing"
                if state.get("lane_mode") == "managed":
                    state["closing_queue_id"] = queue["queue_id"]
                atomic_write_json(state_path, state)
            with RecordLock(marker_path):
                if marker is not None:
                    marker_path.unlink()
            state["lifecycle"] = "closed"
            state["closed_at"] = state.get("closed_at") or iso_utc()
            atomic_write_json(state_path, state)


def lane_record_path(rt: Path, epoch_id: str, lane_id: str) -> Path:
    return lane_record_dir(rt, epoch_id, lane_id) / "lane.json"
