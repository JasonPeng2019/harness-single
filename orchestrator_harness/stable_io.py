from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from .models import ProcessInfo, ProcessSnapshot, StableBytes, iso_utc, parse_utc, utc_now
from harness_common.process_identity import exact_process_identity


class UnstableReadError(OSError):
    pass


class PathSafetyError(OSError):
    pass


class ManagedWatcherClaimError(OSError):
    """The output root is already owned by a live managed watcher."""


MANAGED_RUNTIME_SCHEMA = "orchestrator-managed-watch-runtime/v1"
ACTIVE_MANAGEMENT_HISTORY_SCHEMA = "orchestrator-active-management-history/v1"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _signature(info: os.stat_result) -> tuple[int, int, int, int]:
    return (info.st_size, info.st_mtime_ns, info.st_dev, info.st_ino)


def read_stable(
    path: Path,
    *,
    max_bytes: int,
    retries: int,
    delay_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> StableBytes:
    last_reason = "unknown"
    for attempt in range(retries):
        try:
            before = path.stat()
            if before.st_size > max_bytes:
                raise UnstableReadError(
                    f"{path} is {before.st_size} bytes; limit is {max_bytes}"
                )
            with path.open("rb") as handle:
                data = handle.read(max_bytes + 1)
            after = path.stat()
        except FileNotFoundError:
            last_reason = "file disappeared"
        else:
            if len(data) > max_bytes:
                raise UnstableReadError(f"{path} exceeds {max_bytes} bytes")
            if _signature(before) == _signature(after) and len(data) == after.st_size:
                return StableBytes(
                    path=path,
                    data=data,
                    sha256=sha256_bytes(data),
                    size=len(data),
                    mtime_ns=after.st_mtime_ns,
                    file_id=(after.st_dev, after.st_ino),
                )
            last_reason = "stat/read/stat identity changed"
        if attempt + 1 < retries and delay_seconds:
            sleep(delay_seconds)
    raise UnstableReadError(f"unstable read for {path}: {last_reason}")


def read_tail_stable(
    path: Path,
    *,
    max_bytes: int,
    retries: int,
    delay_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> StableBytes:
    last_reason = "unknown"
    for attempt in range(retries):
        try:
            before = path.stat()
            offset = max(0, before.st_size - max_bytes)
            with path.open("rb") as handle:
                handle.seek(offset)
                data = handle.read(max_bytes + 1)
            after = path.stat()
        except FileNotFoundError:
            last_reason = "file disappeared"
        else:
            if len(data) > max_bytes:
                data = data[-max_bytes:]
            if _signature(before) == _signature(after):
                return StableBytes(
                    path=path,
                    data=data,
                    sha256=sha256_bytes(data),
                    size=after.st_size,
                    mtime_ns=after.st_mtime_ns,
                    file_id=(after.st_dev, after.st_ino),
                )
            last_reason = "stat/read/stat identity changed"
        if attempt + 1 < retries and delay_seconds:
            sleep(delay_seconds)
    raise UnstableReadError(f"unstable tail read for {path}: {last_reason}")


def _has_ads_syntax(path: Path) -> bool:
    if os.name != "nt":
        return False
    drive = path.drive
    for part in path.parts:
        if part in {drive, path.anchor, "\\", "/"}:
            continue
        if ":" in part:
            return True
    return False


def _is_reparse(path: Path) -> bool:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(attributes & flag)


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


class SafeOutput:
    def __init__(
        self,
        *,
        harness_root: Path,
        output_root: Path,
        forbidden_roots: tuple[Path, ...],
        allowed_output_roots: tuple[Path, ...] = (),
        fail_after_event_append: bool = False,
        attention_logging_enabled: bool = False,
        attention_epoch_id: str = "harness",
    ) -> None:
        self.harness_root = harness_root.resolve()
        self.output_root = output_root.absolute()
        self.forbidden_roots = tuple(root.resolve() for root in forbidden_roots)
        self.allowed_output_roots = (self.harness_root, *(root.resolve() for root in allowed_output_roots))
        self.fail_after_event_append = fail_after_event_append
        self.attention_logging_enabled = attention_logging_enabled
        self.attention_epoch_id = attention_epoch_id
        self._root_identity: tuple[int, int] | None = None
        self._harness_identity = exact_process_identity(os.getpid())

    def _validate_components(self, target: Path) -> None:
        if _has_ads_syntax(target):
            raise PathSafetyError(f"alternate data stream syntax rejected: {target}")
        lexical = target.absolute()
        resolved = lexical.resolve(strict=False)
        allowed_root = next((root for root in self.allowed_output_roots if _within(resolved, root)), None)
        if allowed_root is None:
            raise PathSafetyError(f"output escapes permitted roots: {target}")
        output_resolved = self.output_root.resolve(strict=False)
        for forbidden in self.forbidden_roots:
            if _within(resolved, forbidden) or (
                resolved == output_resolved and _within(forbidden, resolved)
            ):
                raise PathSafetyError(f"output overlaps observed root: {forbidden}")

        current = allowed_root
        if current.exists() and _is_reparse(current):
            raise PathSafetyError(f"output root is a reparse point: {current}")
        relative = lexical.relative_to(allowed_root)
        for part in relative.parts:
            current = current / part
            if current.exists() and _is_reparse(current):
                raise PathSafetyError(f"reparse output component rejected: {current}")

    def prepare(self) -> None:
        self._validate_components(self.output_root)
        base = next(root for root in self.allowed_output_roots if _within(self.output_root.resolve(strict=False), root))
        current = base
        current.mkdir(parents=True, exist_ok=True)
        self._validate_components(current)
        relative = self.output_root.relative_to(base)
        for part in relative.parts:
            current = current / part
            if not current.exists():
                current.mkdir()
            self._validate_components(current)
        info = self.output_root.stat()
        self._root_identity = (info.st_dev, info.st_ino)

    def _revalidate(self, target: Path) -> None:
        self._validate_components(target)
        if self._root_identity is None:
            raise PathSafetyError("output root was not prepared")
        info = self.output_root.stat()
        if (info.st_dev, info.st_ino) != self._root_identity:
            raise PathSafetyError("output root identity changed after validation")

    @property
    def snapshot_path(self) -> Path:
        return self.output_root / "snapshot.json"

    @property
    def events_path(self) -> Path:
        return self.output_root / "events.jsonl"

    @property
    def attention_events_path(self) -> Path:
        return self.output_root / "attention-events.jsonl"

    def _attention_record_id(self, key: str) -> str:
        # Deterministic UUID with RFC-4122 v4/version bits: stable retries remain valid v4 IDs.
        digest = bytearray(hashlib.sha256((self.attention_epoch_id + "\0" + key).encode("utf-8")).digest()[:16])
        digest[6] = (digest[6] & 0x0F) | 0x40; digest[8] = (digest[8] & 0x3F) | 0x80
        return str(uuid.UUID(bytes=bytes(digest)))

    def append_attention(self, *, kind: str, event_id: str, metadata: Mapping[str, Any] | None = None, timestamp: datetime | None = None) -> None:
        """Locked, fsynced, idempotent committed-boundary append."""
        if not self.attention_logging_enabled: return
        at = timestamp or utc_now()
        record = {"schema":"manager-attention-timeline/v1", "source_timestamp_utc":iso_utc(at), "epoch_id":self.attention_epoch_id, "event_id":event_id, "kind":kind, "recorder":"orchestrator_harness"}
        record.update(dict(metadata or {})); record["record_id"]=self._attention_record_id(canonical_json(record)); encoded=(canonical_json(record)+"\n").encode()
        lock=self.attention_events_path.with_suffix(".lock"); self._revalidate(lock); lock.parent.mkdir(parents=True,exist_ok=True)
        with lock.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt
                handle.write(b"0"); handle.flush(); msvcrt.locking(handle.fileno(),msvcrt.LK_LOCK,1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(),fcntl.LOCK_EX)
            try:
                self._revalidate(self.attention_events_path)
                if self.attention_events_path.exists():
                    for line in self.attention_events_path.read_bytes().splitlines():
                        try: existing=json.loads(line)
                        except (UnicodeDecodeError,json.JSONDecodeError): continue
                        if existing.get("record_id")==record["record_id"]:
                            if canonical_json(existing)!=canonical_json(record): raise PathSafetyError("attention record ID collision")
                            return
                self._revalidate(self.attention_events_path)
                fd=os.open(self.attention_events_path,os.O_APPEND|os.O_CREAT|os.O_WRONLY,0o600)
                try: os.write(fd,encoded); os.fsync(fd)
                finally: os.close(fd)
            finally:
                if os.name == "nt": msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
                else: fcntl.flock(handle.fileno(),fcntl.LOCK_UN)

    @property
    def pending_notification_path(self) -> Path:
        return self.output_root / "pending-notification.json"

    @property
    def managed_runtime_path(self) -> Path:
        return self.output_root / "managed-watch-runtime.json"

    @property
    def active_management_history_path(self) -> Path:
        return self.output_root / "active-management-history.json"

    @property
    def _managed_claim_lock_path(self) -> Path:
        return self.output_root / ".managed-watch-claim.lock"

    def load_notification_state(self) -> dict[str, Any]:
        if not self.pending_notification_path.exists():
            return {"pending": None, "acknowledged_event_ids": [], "deferred": []}
        self._revalidate(self.pending_notification_path)
        stable = read_stable(self.pending_notification_path, max_bytes=2_000_000, retries=3, delay_seconds=0.01)
        raw = json.loads(stable.data.decode("utf-8"))
        if (
            not isinstance(raw, dict)
            or not isinstance(raw.get("acknowledged_event_ids", []), list)
            or not isinstance(raw.get("deferred", []), list)
        ):
            raise PathSafetyError("invalid pending notification state")
        raw.setdefault("deferred", [])
        return raw

    def save_notification_state(
        self, *, pending: dict[str, Any] | None, acknowledged_event_ids: list[str],
        deferred: list[dict[str, Any]] | None = None,
    ) -> None:
        self.atomic_json(self.pending_notification_path, {
            "schema": "orchestrator-manager-notification/v1",
            "pending": pending,
            "acknowledged_event_ids": acknowledged_event_ids[-1000:],
            "deferred": list(deferred or []),
        })

    def acknowledge_notification(
        self, event_id: str, *, heartbeat_at: datetime | None = None
    ) -> bool:
        state = self.load_notification_state()
        acknowledged = [item for item in state.get("acknowledged_event_ids", []) if isinstance(item, str)]
        pending = state.get("pending")
        if isinstance(pending, dict) and pending.get("event_id") == event_id:
            self.save_notification_state(
                pending=None, acknowledged_event_ids=acknowledged + [event_id],
                deferred=state.get("deferred", []),
            )
            self.renew_manager_heartbeat(renewed_at=heartbeat_at)
            return True
        if event_id in acknowledged:
            self.renew_manager_heartbeat(renewed_at=heartbeat_at)
            return True
        return False

    def load_active_management_history(self) -> dict[str, Any]:
        """Load the managed-watch policy history without touching watcher runtime state."""
        if not self.active_management_history_path.exists():
            return {
                "schema": ACTIVE_MANAGEMENT_HISTORY_SCHEMA,
                "review_baseline_utc": None,
                "lanes": {},
            }
        self._revalidate(self.active_management_history_path)
        stable = read_stable(
            self.active_management_history_path,
            max_bytes=2_000_000,
            retries=3,
            delay_seconds=0.01,
        )
        try:
            raw = json.loads(stable.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PathSafetyError("invalid active-management history") from exc
        if (
            not isinstance(raw, dict)
            or raw.get("schema") != ACTIVE_MANAGEMENT_HISTORY_SCHEMA
            or not isinstance(raw.get("lanes"), dict)
        ):
            raise PathSafetyError("invalid active-management history")
        return raw

    def save_active_management_history(self, history: Mapping[str, Any]) -> None:
        if history.get("schema") != ACTIVE_MANAGEMENT_HISTORY_SCHEMA:
            raise PathSafetyError("invalid active-management history schema")
        if not isinstance(history.get("lanes"), Mapping):
            raise PathSafetyError("invalid active-management history lanes")
        self.atomic_json(self.active_management_history_path, dict(history))

    def load_managed_runtime(self) -> dict[str, Any] | None:
        if not self.managed_runtime_path.exists():
            return None
        self._revalidate(self.managed_runtime_path)
        stable = read_stable(
            self.managed_runtime_path,
            max_bytes=256_000,
            retries=3,
            delay_seconds=0.01,
        )
        try:
            raw = json.loads(stable.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PathSafetyError("invalid managed watcher runtime") from exc
        if not isinstance(raw, dict) or raw.get("schema") != MANAGED_RUNTIME_SCHEMA:
            raise PathSafetyError("invalid managed watcher runtime")
        for prefix in ("watcher", "owner"):
            if not isinstance(raw.get(f"{prefix}_pid"), int) or isinstance(
                raw.get(f"{prefix}_pid"), bool
            ):
                raise PathSafetyError("invalid managed watcher runtime identity")
            if parse_utc(raw.get(f"{prefix}_created_utc")) is None:
                raise PathSafetyError("invalid managed watcher runtime identity")
        if parse_utc(raw.get("started_utc")) is None:
            raise PathSafetyError("invalid managed watcher runtime start")
        if (
            parse_utc(raw.get("last_manager_heartbeat_utc")) is None
            or parse_utc(raw.get("lease_expires_utc")) is None
            or not isinstance(raw.get("manager_heartbeat_timeout_seconds"), (int, float))
            or isinstance(raw.get("manager_heartbeat_timeout_seconds"), bool)
            or raw["manager_heartbeat_timeout_seconds"] <= 0
            or not isinstance(raw.get("stop_requested"), bool)
        ):
            raise PathSafetyError("invalid managed watcher runtime lease")
        return raw

    def _save_managed_runtime(self, runtime: Mapping[str, Any]) -> None:
        if runtime.get("schema") != MANAGED_RUNTIME_SCHEMA:
            raise PathSafetyError("invalid managed watcher runtime schema")
        self.atomic_json(self.managed_runtime_path, dict(runtime))

    @staticmethod
    def _process_identity_status(
        runtime: Mapping[str, Any], prefix: str, processes: ProcessSnapshot
    ) -> str:
        """Return live, absent, reused, or unknown for an exact recorded identity."""
        if not processes.complete:
            return "unknown"
        pid = runtime.get(f"{prefix}_pid")
        expected = parse_utc(runtime.get(f"{prefix}_created_utc"))
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or expected is None:
            return "unknown"
        actual = processes.by_pid.get(pid)
        if actual is None:
            return "absent"
        if actual.created_utc is None:
            return "unknown"
        return "live" if actual.created_utc == expected else "reused"

    @staticmethod
    def _process_record(process: ProcessInfo, *, prefix: str) -> dict[str, Any]:
        if process.pid <= 0 or process.created_utc is None:
            raise ManagedWatcherClaimError(
                f"{prefix} process requires a PID and creation time"
            )
        return {
            f"{prefix}_pid": process.pid,
            f"{prefix}_created_utc": iso_utc(process.created_utc),
        }

    @staticmethod
    def _runtime_matches_process(runtime: Mapping[str, Any], process: ProcessInfo, prefix: str) -> bool:
        expected = parse_utc(runtime.get(f"{prefix}_created_utc"))
        return (
            isinstance(runtime.get(f"{prefix}_pid"), int)
            and runtime.get(f"{prefix}_pid") == process.pid
            and expected is not None
            and process.created_utc is not None
            and expected == process.created_utc
        )

    def _claim_lock_owner_status(self, processes: ProcessSnapshot) -> str:
        path = self._managed_claim_lock_path
        try:
            self._revalidate(path)
            stable = read_stable(path, max_bytes=16_000, retries=2, delay_seconds=0.01)
            raw = json.loads(stable.data.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return "unknown"
        if not isinstance(raw, dict):
            return "unknown"
        return self._process_identity_status(raw, "watcher", processes)

    @contextmanager
    def _managed_claim_lock(
        self, *, watcher: ProcessInfo, processes: ProcessSnapshot
    ) -> Iterator[None]:
        """Serialize short claim/reclaim transactions without becoming a daemon lock."""
        path = self._managed_claim_lock_path
        acquired = False
        payload = canonical_json(
            {
                "schema": "orchestrator-managed-watch-claim-lock/v1",
                **self._process_record(watcher, prefix="watcher"),
            }
        ).encode("utf-8")
        for _ in range(20):
            self._revalidate(path)
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                time.sleep(0.01)
                continue
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            acquired = True
            break
        if not acquired:
            status = self._claim_lock_owner_status(processes)
            if status in {"absent", "reused"}:
                self._revalidate(path)
                path.unlink(missing_ok=True)
                with self._managed_claim_lock(watcher=watcher, processes=processes):
                    yield
                return
            raise ManagedWatcherClaimError("managed watcher startup is already in progress")
        try:
            yield
        finally:
            self._revalidate(path)
            path.unlink(missing_ok=True)

    def claim_managed_watcher(
        self,
        *,
        watcher: ProcessInfo,
        owner: ProcessInfo,
        processes: ProcessSnapshot,
        heartbeat_timeout_seconds: float,
        started_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically claim this output root for one exact watcher incarnation."""
        if heartbeat_timeout_seconds <= 0:
            raise ValueError("heartbeat timeout must be positive")
        if not processes.complete:
            raise ManagedWatcherClaimError("cannot claim with an incomplete process inventory")
        watcher_record = self._process_record(watcher, prefix="watcher")
        owner_record = self._process_record(owner, prefix="owner")
        if self._process_identity_status(watcher_record, "watcher", processes) != "live":
            raise ManagedWatcherClaimError("watcher process identity is not live")
        if self._process_identity_status(owner_record, "owner", processes) != "live":
            raise ManagedWatcherClaimError("owner process identity is not live")
        now = (started_at or utc_now()).astimezone(timezone.utc)
        with self._managed_claim_lock(watcher=watcher, processes=processes):
            existing = self.load_managed_runtime()
            if existing is not None:
                existing_state = self._process_identity_status(existing, "watcher", processes)
                if existing_state == "live":
                    raise ManagedWatcherClaimError("a managed watcher already owns this output root")
                if existing_state == "unknown":
                    raise ManagedWatcherClaimError(
                        "cannot safely reclaim managed watcher ownership"
                    )
            runtime = {
                "schema": MANAGED_RUNTIME_SCHEMA,
                **watcher_record,
                **owner_record,
                "started_utc": iso_utc(now),
                "last_manager_heartbeat_utc": iso_utc(now),
                "manager_heartbeat_timeout_seconds": float(
                    heartbeat_timeout_seconds
                ),
                "lease_expires_utc": iso_utc(
                    now + timedelta(seconds=heartbeat_timeout_seconds)
                ),
                "stop_requested": False,
                "stop_requested_utc": None,
                "exit_reason": None,
                "exited_utc": None,
            }
            self._save_managed_runtime(runtime)
            return runtime

    def managed_watcher_ownership_status(
        self,
        *,
        watcher: ProcessInfo,
        owner: ProcessInfo,
        processes: ProcessSnapshot,
    ) -> str:
        """Verify this watcher and its launching manager by exact process identity."""
        runtime = self.load_managed_runtime()
        if runtime is None:
            return "runtime-missing"
        if not self._runtime_matches_process(runtime, watcher, "watcher"):
            return "watcher-mismatch"
        if not self._runtime_matches_process(runtime, owner, "owner"):
            return "owner-mismatch"
        watcher_state = self._process_identity_status(runtime, "watcher", processes)
        if watcher_state != "live":
            return f"watcher-{watcher_state}"
        owner_state = self._process_identity_status(runtime, "owner", processes)
        if owner_state != "live":
            return f"owner-{owner_state}"
        return "live"

    def renew_manager_heartbeat(self, *, renewed_at: datetime | None = None) -> bool:
        """Renew a non-expired managed-watch lease; no process is started or touched."""
        runtime = self.load_managed_runtime()
        if runtime is None or runtime.get("exit_reason") is not None:
            return False
        now = (renewed_at or utc_now()).astimezone(timezone.utc)
        expiry = parse_utc(runtime.get("lease_expires_utc"))
        if expiry is None or now >= expiry:
            return False
        timeout = runtime.get("manager_heartbeat_timeout_seconds")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
        ):
            return False
        runtime["last_manager_heartbeat_utc"] = iso_utc(now)
        runtime["lease_expires_utc"] = iso_utc(now + timedelta(seconds=timeout))
        self._save_managed_runtime(runtime)
        return True

    def request_managed_watcher_stop(
        self, *, requested_at: datetime | None = None
    ) -> bool:
        """Persist a cooperative stop request; this method never signals a process."""
        runtime = self.load_managed_runtime()
        if runtime is None:
            return False
        if not runtime.get("stop_requested", False):
            runtime["stop_requested"] = True
            runtime["stop_requested_utc"] = iso_utc(
                (requested_at or utc_now()).astimezone(timezone.utc)
            )
            self._save_managed_runtime(runtime)
        return True

    def record_managed_watcher_exit(
        self,
        *,
        watcher: ProcessInfo,
        reason: str,
        exited_at: datetime | None = None,
    ) -> bool:
        """Record a clean watcher exit only for the exact owning watcher incarnation."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("managed watcher exit reason must be non-empty")
        runtime = self.load_managed_runtime()
        if runtime is None or not self._runtime_matches_process(runtime, watcher, "watcher"):
            return False
        if runtime.get("exit_reason") is None:
            runtime["exit_reason"] = reason.strip()
            runtime["exited_utc"] = iso_utc(
                (exited_at or utc_now()).astimezone(timezone.utc)
            )
            self._save_managed_runtime(runtime)
        return True

    def atomic_json(self, path: Path, value: Any) -> None:
        self._revalidate(path)
        data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            self._revalidate(path)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def append_events(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        self._revalidate(self.events_path)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        fd = os.open(self.events_path, flags, 0o600)
        try:
            with os.fdopen(fd, "ab", closefd=True) as handle:
                for event in events:
                    handle.write((canonical_json(event) + "\n").encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            pass
        if self.fail_after_event_append:
            raise RuntimeError("injected failure after event append")

    def load_cursor(self) -> dict[str, Any] | None:
        if not self.snapshot_path.exists():
            return None
        self._revalidate(self.snapshot_path)
        stable = read_stable(
            self.snapshot_path,
            max_bytes=20_000_000,
            retries=3,
            delay_seconds=0.01,
        )
        raw = json.loads(stable.data.decode("utf-8"))
        return raw if isinstance(raw, dict) else None

    def commit(
        self,
        *,
        snapshot: dict[str, Any],
        events: list[dict[str, Any]],
        conditions: dict[str, dict[str, Any]],
    ) -> None:
        self.append_events(events)
        cursor = {
            "schema": "orchestrator-watcher-cursor/v1",
            "snapshot": snapshot,
            "conditions": conditions,
        }
        self.atomic_json(self.snapshot_path, cursor)
        # This boundary is emitted only after events and cursor snapshot are durable.
        if self.attention_logging_enabled:
            if not isinstance(self._harness_identity, dict) or self._harness_identity.get("pid") != os.getpid() or not isinstance(self._harness_identity.get("created_utc"), str):
                raise PathSafetyError("cannot establish exact harness process identity for authoritative scan")
            root_identity=self._root_identity
            harness_identity=self._harness_identity
            if root_identity is None or not isinstance(harness_identity.get("pid"), int) or not isinstance(harness_identity.get("created_utc"), str):
                raise PathSafetyError("authoritative scan identity is unavailable")
            scans=[]
            if self.attention_events_path.exists():
                for line in self.attention_events_path.read_bytes().splitlines():
                    try: item=json.loads(line)
                    except (UnicodeDecodeError, json.JSONDecodeError): continue
                    if item.get("kind") == "HARNESS_SCAN_COMMITTED": scans.append(item)
            sequence=len(scans)+1; prior=None
            stamp=snapshot.get("observed_utc") if isinstance(snapshot.get("observed_utc"),str) else iso_utc(utc_now())
            coverage_start=stamp
            if scans:
                prior_scan=scans[-1]
                required=("record_id","coverage_end_utc","scan_sequence","scan_chain_id","output_root_identity","harness_pid","harness_created_utc")
                if any(field not in prior_scan for field in required):
                    raise PathSafetyError("prior committed scan is incomplete")
                if not isinstance(prior_scan["record_id"], str) or not isinstance(prior_scan["scan_sequence"], int) or not isinstance(prior_scan["coverage_end_utc"], str) or not isinstance(prior_scan["harness_pid"], int) or not isinstance(prior_scan["harness_created_utc"], str):
                    raise PathSafetyError("prior committed scan has invalid chain identity")
                if prior_scan["scan_sequence"] != sequence-1 or prior_scan["scan_chain_id"] != self.attention_epoch_id or prior_scan["output_root_identity"] != f"{root_identity[0]}:{root_identity[1]}":
                    raise PathSafetyError("prior committed scan chain contradicts current output")
                if prior_scan["harness_pid"] != harness_identity["pid"] or prior_scan["harness_created_utc"] != harness_identity["created_utc"]:
                    raise PathSafetyError("prior committed scan process identity changed")
                try:
                    prior_end=parse_utc(prior_scan["coverage_end_utc"]); current_end=parse_utc(stamp)
                except (TypeError, ValueError) as exc:
                    raise PathSafetyError("prior committed scan has invalid coverage timestamp") from exc
                if prior_end is None or current_end is None:
                    raise PathSafetyError("prior committed scan has invalid coverage timestamp")
                if current_end < prior_end:
                    raise PathSafetyError("new scan precedes prior committed coverage")
                prior=prior_scan["record_id"]
                coverage_start=prior_scan["coverage_end_utc"]
            event_id=f"harness-scan-{sequence}"
            self.append_attention(kind="HARNESS_SCAN_COMMITTED", event_id=event_id, metadata={"scan_sequence": sequence, "previous_committed_scan_record_id": prior, "coverage_start_utc": coverage_start, "coverage_end_utc": stamp, "harness_pid": harness_identity["pid"], "harness_created_utc": harness_identity["created_utc"], "snapshot_sha256": sha256_bytes(canonical_json(snapshot).encode()), "conditions_sha256": sha256_bytes(canonical_json(conditions).encode()), "scan_chain_id": self.attention_epoch_id, "output_root_identity": f"{root_identity[0]}:{root_identity[1]}", "cursor_complete": True, "source_complete": True, "integrity_error": False})
