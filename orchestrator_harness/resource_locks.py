"""Local named-resource claims for coding controller launch coordination."""
from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, cast

from harness_common.process_identity import exact_process_identity

from .models import ProcessInfo, ProcessSnapshot, iso_utc
from .processes import process_snapshot

CLAIM_SCHEMA = "orchestrator-coding-resource-claim/v1"
Claim = dict[str, object]
IdentityProvider = Callable[[int], Mapping[str, object] | None]


class ResourceLockError(RuntimeError):
    pass


@contextmanager
def _kernel_resource_lock(path: Path) -> Iterator[None]:
    """Serialize one resource's reclaim/release transaction in the kernel."""
    # Keep persistent kernel lock files outside the claim directory.  The
    # claim directory therefore remains an exact inventory of owned claims,
    # while a process crash still leaves a reusable inode whose OS lock has
    # already been released by the kernel.
    claim_root = path.parent.absolute()
    lock_root = claim_root.parent / f".{claim_root.name}.resource-locks"
    digest = hashlib.sha256(os.path.normcase(str(path.absolute())).encode("utf-8")).hexdigest()
    lock_path = lock_root / f"{digest}.lock"
    handle: Any | None = None
    windows_locked = False
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+b")
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            windows_locked = True
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        if handle is not None:
            try:
                if os.name == "nt" and windows_locked:
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                elif os.name != "nt":
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


resource_reclaim_lock = _kernel_resource_lock
ResourceReclaimLock = _kernel_resource_lock


def claim_filename(resource: str) -> str:
    """Map an opaque name to a confined, deterministic filename."""
    return hashlib.sha256(resource.encode("utf-8")).hexdigest() + ".json"


def _claim_bytes(claim: Mapping[str, object]) -> bytes:
    return (json.dumps(claim, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_claim_evidence(path: Path) -> tuple[Claim | None, str | None, bytes | None]:
    try:
        data = path.read_bytes()
        if len(data) > 64 * 1024:
            return None, "claim exceeds 64 KiB", data
        value = cast(object, json.loads(data.decode("utf-8-sig")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"cannot read claim: {exc}", None
    if not isinstance(value, dict):
        return None, "claim root is not an object", data
    value = cast(Claim, value)
    owner = value.get("owner")
    typed_owner = cast(Claim, owner) if isinstance(owner, dict) else None
    owner_pid = typed_owner.get("pid") if typed_owner is not None else None
    if (
        value.get("schema") != CLAIM_SCHEMA
        or not isinstance(value.get("resource"), str)
        or not value["resource"]
        or not isinstance(value.get("lane_id"), str)
        or not value["lane_id"]
        or not isinstance(value.get("worker_invocation_id"), str)
        or not value["worker_invocation_id"]
        or typed_owner is None
        or not isinstance(owner_pid, int)
        or isinstance(owner_pid, bool)
        or owner_pid <= 0
        or not isinstance(typed_owner.get("created_utc"), str)
        or not typed_owner["created_utc"]
        or not isinstance(typed_owner.get("creation_identity"), str)
        or not typed_owner["creation_identity"]
    ):
        return None, "claim has an invalid schema or owner identity", data
    return value, None, data


def _read_claim(path: Path) -> tuple[Claim | None, str | None]:
    claim, error, _ = _read_claim_evidence(path)
    return claim, error


def _claim_parent_is_unambiguous(path: Path) -> bool:
    """Reject a substituted claim directory before any claim mutation."""
    try:
        parent = path.parent
        lexical_parent = Path(os.path.abspath(str(parent)))
        if parent.resolve(strict=True) != lexical_parent:
            return False
        root_info = parent.stat()
        if not stat.S_ISDIR(root_info.st_mode):
            return False
        root_attributes = getattr(root_info, "st_file_attributes", 0)
        if root_attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            return False
        return True
    except OSError:
        return False


def _claim_path_is_unambiguous(path: Path) -> bool:
    """Reject symlink/reparse/parent substitution before reclaim or release."""
    try:
        if not _claim_parent_is_unambiguous(path):
            return False
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            return False
        attributes = getattr(info, "st_file_attributes", 0)
        if attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            return False
        return stat.S_ISREG(info.st_mode)
    except OSError:
        return False


def _exact_identity(pid: int) -> Mapping[str, object] | None:
    identity = exact_process_identity(pid)
    return cast(Mapping[str, object], identity) if identity is not None else None


def _owner_state(
    claim: Mapping[str, object],
    processes: ProcessSnapshot,
    identity_provider: IdentityProvider,
) -> tuple[str, str]:
    owner = claim.get("owner")
    if not isinstance(owner, Mapping):
        return "MALFORMED", "claim owner is missing"
    typed_owner = cast(Mapping[str, object], owner)
    if not processes.complete:
        return "INVENTORY_UNKNOWN", "complete process inventory is unavailable"
    pid = typed_owner.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return "MALFORMED", "claim owner PID is invalid"
    process = processes.by_pid.get(pid)
    if process is None:
        try:
            exact = identity_provider(pid)
        except Exception:
            return "OWNER_IDENTITY_UNKNOWN", "fresh exact owner creation identity is unavailable"
        if exact is not None:
            return "OWNER_IDENTITY_UNKNOWN", "process inventory and exact identity provider contradict"
        return "PROVEN_STALE", f"complete process inventory proves owner PID {pid} absent"
    expected = typed_owner.get("created_utc")
    actual = iso_utc(process.created_utc)
    if not isinstance(expected, str) or actual is None:
        return "OWNER_IDENTITY_UNKNOWN", "owner creation identity cannot be compared"
    if expected != actual:
        return "OWNER_IDENTITY_REUSED", "owner PID exists with a different creation identity"
    expected_exact = typed_owner.get("creation_identity")
    try:
        fresh = identity_provider(pid)
    except Exception:
        return "OWNER_IDENTITY_UNKNOWN", "fresh exact owner creation identity is unavailable"
    fresh_exact = fresh.get("created_utc") if fresh is not None else None
    if not isinstance(fresh_exact, str):
        return "OWNER_IDENTITY_UNKNOWN", "fresh exact owner creation identity is unavailable"
    if fresh_exact != expected_exact:
        return "OWNER_IDENTITY_REUSED", "fresh exact owner creation identity does not match the claim"
    return "CONTENDED", "the exact owning process is live"


@dataclass
class ResourceClaims:
    root: Path
    lane_id: str
    worker_invocation_id: str
    controller: ProcessInfo
    wait_excess_seconds: float = 30.0
    poll_seconds: float = 0.1
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot
    identity_provider: IdentityProvider = _exact_identity
    findings: list[Claim] = field(init=False, default_factory=list)
    _owner: Claim = field(init=False, repr=False)
    _held: dict[str, Claim] = field(init=False, default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        identity = self.identity_provider(self.controller.pid)
        creation_identity = (
            cast(object, identity.get("created_utc")) if identity is not None else None
        )
        if not isinstance(creation_identity, str):
            raise ResourceLockError("cannot establish controller creation identity for resource claims")
        self.root.mkdir(parents=True, exist_ok=True)
        self._owner = {
            "pid": self.controller.pid,
            "created_utc": iso_utc(self.controller.created_utc),
            "creation_identity": creation_identity,
        }

    @property
    def held(self) -> list[Claim]:
        return [dict(self._held[name]) for name in sorted(self._held)]

    def _new_claim(self, resource: str) -> Claim:
        return {
            "schema": CLAIM_SCHEMA,
            "resource": resource,
            "lane_id": self.lane_id,
            "worker_invocation_id": self.worker_invocation_id,
            "owner": dict(self._owner),
            "created_utc": iso_utc(self.controller.created_utc),
        }

    def _create(self, resource: str) -> Claim | None:
        claim = self._new_claim(resource)
        path = self.root / claim_filename(resource)
        if not _claim_parent_is_unambiguous(path):
            return None
        with _kernel_resource_lock(path):
            if path.is_symlink() or path.exists():
                return None
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                return None
            try:
                with os.fdopen(fd, "wb", closefd=True) as handle:
                    _ = handle.write(_claim_bytes(claim))
                    handle.flush()
                    _ = os.fsync(handle.fileno())
            except Exception:
                path.unlink(missing_ok=True)
                raise
        claim["path"] = str(path)
        self._held[resource] = claim
        return claim

    def _release_one(self, resource: str) -> bool:
        expected = self._held.get(resource)
        if expected is None:
            return False
        path = self.root / claim_filename(resource)
        if not _claim_path_is_unambiguous(path):
            return False
        try:
            with _kernel_resource_lock(path):
                if not _claim_path_is_unambiguous(path):
                    return False
                current, error = _read_claim(path)
                if error is not None or current is None:
                    return False
                comparable = {key: value for key, value in expected.items() if key != "path"}
                if current != comparable:
                    return False
                path.unlink()
        except (FileNotFoundError, OSError):
            return False
        _ = self._held.pop(resource, None)
        return True

    def release_all(self) -> list[str]:
        failures: list[str] = []
        for resource in sorted(self._held, reverse=True):
            if not self._release_one(resource):
                failures.append(resource)
        return sorted(failures)

    def _reclaim_stale(
        self,
        *,
        resource: str,
        path: Path,
        expected: Claim,
        expected_bytes: bytes,
    ) -> tuple[bool, str]:
        # A legacy marker is observed but never created or removed by S3.  The
        # real serialization is the kernel-held per-resource lock below.
        legacy_guard = path.with_suffix(".reclaim")
        if legacy_guard.exists():
            return False, "another controller is revalidating the stale claim"
        if not _claim_path_is_unambiguous(path):
            return False, "claim path identity is ambiguous"
        try:
            with _kernel_resource_lock(path):
                # Every fact is re-read while the kernel lock is held.  A
                # contender that replaced the file, or an owner that became
                # live/reused/uncertain, therefore remains untouched.
                if not _claim_path_is_unambiguous(path):
                    return False, "claim path identity is ambiguous"
                current, error, current_bytes = _read_claim_evidence(path)
                if (
                    error is not None
                    or current is None
                    or current != expected
                    or current_bytes != expected_bytes
                ):
                    return False, "claim changed before stale reclaim"
                state, reason = _owner_state(
                    current, self.process_provider(), self.identity_provider
                )
                if state != "PROVEN_STALE":
                    return False, f"stale reclaim revalidation changed to {state}: {reason}"
                # Unlink only the exact bytes that were revalidated under the
                # lock.  No reclaim marker or process-name operation is used.
                path.unlink()
                return True, reason
        except OSError as exc:
            return False, f"ownership-safe stale reclaim failed: {exc}"

    def acquire_all(
        self,
        resources: list[str],
        *,
        on_wait: Callable[[Claim], None],
    ) -> None:
        ordered = sorted(set(resources))
        wait_started = time.monotonic()
        while True:
            for resource in ordered:
                if self._create(resource) is not None:
                    continue
                release_failures = self.release_all()
                if release_failures:
                    raise ResourceLockError(
                        "cannot safely release partial resource claims: "
                        + ", ".join(release_failures)
                    )
                path = self.root / claim_filename(resource)
                existing, malformed, existing_bytes = _read_claim_evidence(path)
                if malformed is not None or existing is None:
                    state, reason, actionable = "MALFORMED", malformed or "claim is unreadable", True
                elif existing.get("resource") != resource:
                    state, reason, actionable = "MALFORMED", "hashed claim contains a different resource name", True
                else:
                    state, reason = _owner_state(
                        existing, self.process_provider(), self.identity_provider
                    )
                    actionable = state in {"MALFORMED", "PROVEN_STALE", "OWNER_IDENTITY_REUSED", "OWNER_IDENTITY_UNKNOWN"}
                    if state == "PROVEN_STALE" and existing_bytes is not None:
                        reclaimed, reason = self._reclaim_stale(
                            resource=resource,
                            path=path,
                            expected=existing,
                            expected_bytes=existing_bytes,
                        )
                        if reclaimed:
                            finding: Claim = {
                                "resource": resource,
                                "path": str(path),
                                "state": state,
                                "reason": reason,
                                "actionable": True,
                            }
                            if finding not in self.findings:
                                self.findings.append(finding)
                            break
                elapsed = time.monotonic() - wait_started
                if elapsed >= self.wait_excess_seconds:
                    state, actionable = "EXCESSIVE_WAIT", True
                    reason = f"resource wait exceeded {self.wait_excess_seconds:g} seconds; last state: {reason}"
                on_wait({
                    "resource": resource,
                    "path": str(path),
                    "state": state,
                    "reason": reason,
                    "actionable": actionable,
                    "wait_seconds": round(elapsed, 3),
                    "claim": existing,
                })
                time.sleep(self.poll_seconds)
                break
            else:
                return
