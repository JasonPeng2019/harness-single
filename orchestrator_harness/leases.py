"""Exclusive-resource leases: created at launch, held only while the
controller lives, released only after cleanup proof (or a forced path).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .core import iso_utc, read_json, require_schema
from .records import RecordLock, atomic_write_json, read_record

LEASE_SCHEMA = "resource-lease/v1"

LAUNCH_LEASE_BUSY = "LAUNCH_LEASE_BUSY"


class LeaseError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def lease_dir(rt: Path) -> Path:
    return rt / "resources" / "leases"


def lease_path(rt: Path, resource_id: str) -> Path:
    digest = hashlib.sha256(resource_id.encode("utf-8")).hexdigest()
    return lease_dir(rt) / f"{digest}.lease"


def _leases_lock(rt: Path) -> RecordLock:
    return RecordLock(lease_dir(rt) / ".leases.lock")


def read_lease(rt: Path, resource_id: str) -> dict[str, Any] | None:
    path = lease_path(rt, resource_id)
    if not path.is_file():
        return None
    try:
        return read_record(path, LEASE_SCHEMA)
    except (OSError, ValueError):
        return None


def acquire_leases(
    rt: Path,
    resource_ids: list[str],
    *,
    lane_id: str,
    run_id: str,
    pid: int,
    creation_time: str,
) -> None:
    """Acquire every declared lease or none (fail-fast, all-or-nothing)."""
    if not resource_ids:
        return
    lease_dir(rt).mkdir(parents=True, exist_ok=True)
    acquired: list[Path] = []
    with _leases_lock(rt):
        try:
            for resource_id in resource_ids:
                path = lease_path(rt, resource_id)
                if path.is_file():
                    raise LeaseError(
                        LAUNCH_LEASE_BUSY,
                        f"resource is held: {resource_id}",
                    )
            for resource_id in resource_ids:
                path = lease_path(rt, resource_id)
                record = {
                    "schema": LEASE_SCHEMA,
                    "resource_id": resource_id,
                    "lane_id": lane_id,
                    "run_id": run_id,
                    "pid": pid,
                    "creation_time": creation_time,
                    "acquired_at": iso_utc(),
                }
                atomic_write_json(path, record)
                acquired.append(path)
        except BaseException:
            for path in acquired:
                try:
                    path.unlink()
                except OSError:
                    pass
            raise


def release_leases(rt: Path, lane_id: str, run_id: str) -> None:
    """Remove every lease held by this lane's current run."""
    with _leases_lock(rt):
        for path in sorted(lease_dir(rt).glob("*.lease")):
            try:
                record = read_record(path, LEASE_SCHEMA)
            except (OSError, ValueError):
                continue
            if record.get("lane_id") == lane_id and record.get("run_id") == run_id:
                try:
                    path.unlink()
                except OSError:
                    pass


def force_release_leases(rt: Path, lane_id: str) -> None:
    """Force-release every lease held by one lane (force-stop / orphan clear)."""
    with _leases_lock(rt):
        for path in sorted(lease_dir(rt).glob("*.lease")):
            try:
                record = read_record(path, LEASE_SCHEMA)
            except (OSError, ValueError):
                continue
            if record.get("lane_id") == lane_id:
                try:
                    path.unlink()
                except OSError:
                    pass
