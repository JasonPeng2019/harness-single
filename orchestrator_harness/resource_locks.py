"""Local named-resource claims for coding controller launch coordination."""
from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from harness_common.process_identity import exact_process_identity

from .models import ProcessInfo, ProcessSnapshot, iso_utc
from .processes import process_snapshot

CLAIM_SCHEMA = "orchestrator-coding-resource-claim/v1"
Claim = dict[str, object]


class ResourceLockError(RuntimeError):
    pass


def claim_filename(resource: str) -> str:
    """Map an opaque name to a confined, deterministic filename."""
    return hashlib.sha256(resource.encode("utf-8")).hexdigest() + ".json"


def _claim_bytes(claim: Mapping[str, object]) -> bytes:
    return (json.dumps(claim, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_claim(path: Path) -> tuple[Claim | None, str | None]:
    try:
        data = path.read_bytes()
        if len(data) > 64 * 1024:
            return None, "claim exceeds 64 KiB"
        value = cast(object, json.loads(data.decode("utf-8-sig")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"cannot read claim: {exc}"
    if not isinstance(value, dict):
        return None, "claim root is not an object"
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
        return None, "claim has an invalid schema or owner identity"
    return value, None


def _owner_state(claim: Mapping[str, object], processes: ProcessSnapshot) -> tuple[str, str]:
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
        return "PROVEN_STALE", f"complete process inventory proves owner PID {pid} absent"
    expected = typed_owner.get("created_utc")
    actual = iso_utc(process.created_utc)
    if not isinstance(expected, str) or actual is None:
        return "OWNER_IDENTITY_UNKNOWN", "owner creation identity cannot be compared"
    if expected != actual:
        return "OWNER_IDENTITY_REUSED", "owner PID exists with a different creation identity"
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
    findings: list[Claim] = field(init=False, default_factory=list)
    _owner: Claim = field(init=False, repr=False)
    _held: dict[str, Claim] = field(init=False, default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        identity = exact_process_identity(self.controller.pid)
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
        current, error = _read_claim(path)
        if error is not None or current is None:
            return False
        comparable = {key: value for key, value in expected.items() if key != "path"}
        if current != comparable:
            return False
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        _ = self._held.pop(resource, None)
        return True

    def release_all(self) -> list[str]:
        failures: list[str] = []
        for resource in sorted(self._held, reverse=True):
            if not self._release_one(resource):
                failures.append(resource)
        return sorted(failures)

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
                _ = self.release_all()
                path = self.root / claim_filename(resource)
                existing, malformed = _read_claim(path)
                if malformed is not None or existing is None:
                    state, reason, actionable = "MALFORMED", malformed or "claim is unreadable", True
                elif existing.get("resource") != resource:
                    state, reason, actionable = "MALFORMED", "hashed claim contains a different resource name", True
                else:
                    state, reason = _owner_state(existing, self.process_provider())
                    actionable = state in {"MALFORMED", "PROVEN_STALE", "OWNER_IDENTITY_REUSED", "OWNER_IDENTITY_UNKNOWN"}
                    if state == "PROVEN_STALE":
                        try:
                            path.unlink()
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
                        except OSError as exc:
                            reason = f"{reason}; reclaim failed: {exc}"
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
