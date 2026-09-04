"""Exclusive-resource leases: created at launch, held only while the
controller lives, released only after cleanup proof (or a forced path).
"""

from __future__ import annotations

from collections.abc import Callable
import hashlib
from pathlib import Path
from typing import Any

from .core import iso_utc, read_json, require_schema
from .processes import identity_matches, process_alive, process_identity
from .records import RecordLock, atomic_write_json, read_record

LEASE_SCHEMA = "resource-lease/v1"

LAUNCH_LEASE_BUSY = "LAUNCH_LEASE_BUSY"

FORCE_RELEASE_CONFIG_INVALID = "FORCE_RELEASE_CONFIG_INVALID"
FORCE_RELEASE_UNDECLARED = "FORCE_RELEASE_UNDECLARED"
FORCE_RELEASE_LEASE_MISSING = "FORCE_RELEASE_LEASE_MISSING"
FORCE_RELEASE_LEASE_INVALID = "FORCE_RELEASE_LEASE_INVALID"
FORCE_RELEASE_HOLDER_LIVE = "FORCE_RELEASE_HOLDER_LIVE"
FORCE_RELEASE_HOLDER_UNPROVEN = "FORCE_RELEASE_HOLDER_UNPROVEN"
FORCE_RELEASE_DELETE_FAILED = "FORCE_RELEASE_DELETE_FAILED"
FORCE_RELEASE_OK = "FORCE_RELEASE_OK"


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


def force_release_lease(
    rt: Path,
    resource_id: str,
    *,
    lane: dict[str, Any] | None = None,
    lane_resolver: Callable[[str], dict[str, Any] | None] | None = None,
) -> None:
    """Force-release one orphaned lease after exact-identity proof.

    The lease is re-read and schema-validated under the leases lock.  The
    exact recorded holder incarnation is never released while live; an
    unprovable holder is released only when the matching lane/run is retired,
    abandoned, or superseded by a different current run.
    """
    path = lease_path(rt, resource_id)
    with _leases_lock(rt):
        if not path.is_file():
            raise LeaseError(
                FORCE_RELEASE_LEASE_MISSING,
                f"lease not found: {resource_id}",
            )
        try:
            record = read_record(path, LEASE_SCHEMA)
        except (OSError, ValueError) as exc:
            raise LeaseError(
                FORCE_RELEASE_LEASE_INVALID,
                f"lease invalid: {resource_id}: {exc}",
            ) from exc
        if record.get("resource_id") != resource_id:
            raise LeaseError(
                FORCE_RELEASE_LEASE_INVALID,
                f"lease resource mismatch: {resource_id}",
            )
        lane_id = record.get("lane_id")
        run_id = record.get("run_id")
        pid = record.get("pid")
        creation_time = record.get("creation_time")
        if (
            not isinstance(lane_id, str)
            or not lane_id
            or not isinstance(run_id, str)
            or not run_id
            or not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(creation_time, str)
            or not creation_time
        ):
            raise LeaseError(
                FORCE_RELEASE_LEASE_INVALID,
                f"lease identity incomplete: {resource_id}",
            )
        current_lane = lane_resolver(lane_id) if lane_resolver is not None else lane
        if identity_matches(pid, creation_time):
            raise LeaseError(
                FORCE_RELEASE_HOLDER_LIVE,
                f"lease holder is live: {resource_id} (pid {pid})",
            )
        holder_dead = not process_alive(pid)
        if not holder_dead:
            current = process_identity(pid)
            if current is not None and current["creation_time"] != creation_time:
                holder_dead = True
        lane_release_proven = False
        if current_lane is not None and current_lane.get("lane_id") == lane_id:
            if current_lane.get("lifecycle") in ("retired", "abandoned"):
                lane_release_proven = True
            else:
                current_run = current_lane.get("run_id")
                if (
                    isinstance(current_run, str)
                    and current_run
                    and current_run != run_id
                ):
                    lane_release_proven = True
        if not holder_dead and not lane_release_proven:
            raise LeaseError(
                FORCE_RELEASE_HOLDER_UNPROVEN,
                f"lease holder identity unproven: {resource_id}",
            )
        try:
            path.unlink()
        except OSError as exc:
            raise LeaseError(
                FORCE_RELEASE_DELETE_FAILED,
                f"lease delete failed: {resource_id}: {exc}",
            ) from exc
        if path.is_file():
            raise LeaseError(
                FORCE_RELEASE_DELETE_FAILED,
                f"lease still present after delete: {resource_id}",
            )
