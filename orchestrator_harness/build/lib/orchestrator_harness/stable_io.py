from __future__ import annotations

import hashlib
import json
import os
import stat
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import StableBytes
from .mutation import (
    MutationConflict,
    MutationUnsupported,
    AnchoredAppendFile,
    append_bytes as mutation_append,
    capture_target,
    ensure_directory_path,
    open_append_file,
    rename as mutation_rename,
    replace as mutation_replace,
)


class UnstableReadError(OSError):
    pass


class PathSafetyError(OSError):
    pass


class AppendLockError(OSError):
    """A shared append lock could not be established safely."""


def _path_key(path: Path) -> str:
    """Return one stable, case-normalized key for a path across processes."""
    return os.path.normcase(os.path.abspath(str(path)))


class _PathLockEntry:
    """One in-process serialization entry for one canonical path key."""

    __slots__ = ("lock", "users")

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.users = 0


class _PathLockRegistry:
    """Guarded per-path in-process lock registry with usage accounting.

    One entry exists per canonical path key only while that path has active or
    waiting users; the last release removes the entry so the registry cannot
    grow without bound as paths become inactive.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._entries: dict[str, _PathLockEntry] = {}

    def acquire(self, key: str) -> _PathLockEntry:
        with self._guard:
            entry = self._entries.get(key)
            if entry is None:
                entry = _PathLockEntry()
                self._entries[key] = entry
            entry.users += 1
        entry.lock.acquire()
        return entry

    def release(self, key: str, entry: _PathLockEntry) -> None:
        entry.lock.release()
        with self._guard:
            entry.users -= 1
            if entry.users == 0 and self._entries.get(key) is entry:
                del self._entries[key]


_PATH_LOCK_REGISTRY = _PathLockRegistry()


class PathKeyedAppendLock:
    """One kernel-owned lock for one canonical JSONL destination.

    The lock file is intentionally persistent.  Its contents are irrelevant; the
    operating system releases the lock when the owning process dies, which is the
    property a create-once marker cannot provide.

    A per-canonical-path in-process lock is held around the kernel lock
    lifecycle so only one thread in this process attempts or holds that path's
    kernel lock at a time; Windows byte-range locks cannot serialize two
    handles opened in the same process.
    """

    def __init__(self, path: Path, *, lock_root: Path | None = None) -> None:
        self.path = Path(path).absolute()
        self.key = _path_key(self.path)
        root = Path(lock_root).absolute() if lock_root is not None else self.path.parent
        digest = hashlib.sha256(self.key.encode("utf-8")).hexdigest()
        self.lock_path = root / f".{self.path.name or 'append'}.{digest}.lock"
        self._handle: Any | None = None
        self._anchored: AnchoredAppendFile | None = None
        self._windows_locked = False
        self._local_entry: _PathLockEntry | None = None

    def __enter__(self) -> "PathKeyedAppendLock":
        entry = _PATH_LOCK_REGISTRY.acquire(self.key)
        self._local_entry = entry
        handle: Any | None = None
        anchored: AnchoredAppendFile | None = None
        try:
            ensure_directory_path(self.lock_path.parent)
            anchored = open_append_file(self.lock_path.parent, self.lock_path.name)
            handle = anchored.handle
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                self._windows_locked = True
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            self._handle = handle
            self._anchored = anchored
            return self
        except Exception as exc:
            try:
                if anchored is not None:
                    if os.name == "nt" and self._windows_locked and handle is not None:
                        try:
                            import msvcrt

                            handle.seek(0)
                            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                        except Exception:
                            pass
                    anchored.close()
                elif handle is not None:
                    handle.close()
            finally:
                self._handle = None
                self._anchored = None
                self._local_entry = None
                _PATH_LOCK_REGISTRY.release(self.key, entry)
            raise AppendLockError(
                f"cannot acquire append lock for {self.path}: {exc}"
            ) from exc

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        handle = self._handle
        anchored = self._anchored
        entry = self._local_entry
        self._handle = None
        self._anchored = None
        self._local_entry = None
        try:
            if handle is None:
                return
            try:
                if os.name == "nt" and self._windows_locked:
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                elif os.name != "nt":
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                if anchored is not None:
                    anchored.close()
                else:
                    handle.close()
        finally:
            if entry is not None:
                _PATH_LOCK_REGISTRY.release(self.key, entry)


def _jsonl_bytes(records: list[Mapping[str, Any]]) -> bytes:
    return b"".join(
        (canonical_json(dict(record)) + "\n").encode("utf-8") for record in records
    )


def _append_jsonl_locked(path: Path, data: bytes) -> None:
    try:
        mutation_append(path.parent, path.name, data)
    except (MutationConflict, MutationUnsupported) as exc:
        raise PathSafetyError(str(exc)) from exc


def append_jsonl_records(
    path: Path,
    records: list[Mapping[str, Any]],
    *,
    lock_root: Path | None = None,
) -> None:
    """Append complete canonical records while holding the path-keyed OS lock."""
    if not records:
        return
    target = Path(path).absolute()
    data = _jsonl_bytes(records)
    with PathKeyedAppendLock(target, lock_root=lock_root):
        _append_jsonl_locked(target, data)


def append_jsonl_record(
    path: Path,
    record: Mapping[str, Any],
    *,
    lock_root: Path | None = None,
) -> None:
    append_jsonl_records(path, [record], lock_root=lock_root)


class PreparedOutputTransaction:
    """A fixed output root with explicit dynamic-child admission.

    Every mutation rechecks the fixed root and the complete path chain.  The
    transaction deliberately has no discovery or provider authority; it only
    provides safe local file publication.
    """

    def __init__(
        self,
        root: Path,
        *,
        allowed_roots: tuple[Path, ...] = (),
        forbidden_roots: tuple[Path, ...] = (),
    ) -> None:
        self.root = Path(root).absolute()
        default_allowed = self.root.parent.resolve(strict=False)
        self.allowed_roots = tuple(
            Path(item).resolve() for item in (allowed_roots or (default_allowed,))
        )
        self.forbidden_roots = tuple(Path(item).resolve() for item in forbidden_roots)
        self._root_identity: tuple[int, int] | None = None
        self._admitted: set[str] = set()

    @staticmethod
    def _inside(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    @staticmethod
    def _has_ads(value: Path) -> bool:
        drive = value.drive
        for part in value.parts:
            if part in {drive, value.anchor, "\\", "/"}:
                continue
            # A colon in a Windows component is an alternate data stream.  It
            # is rejected on every platform so tests and portable records have
            # one path identity rule.
            if ":" in part:
                return True
        return False

    def _validate_components(
        self, target: Path, *, allow_prepared_parent: bool = False
    ) -> Path:
        lexical = Path(os.path.abspath(str(target)))
        if self._has_ads(lexical):
            raise PathSafetyError(f"alternate data stream syntax rejected: {target}")
        resolved = lexical.resolve(strict=False)
        allowed = next(
            (root for root in self.allowed_roots if self._inside(resolved, root)), None
        )
        if allowed is None:
            raise PathSafetyError(f"output escapes permitted roots: {target}")
        root_resolved = self.root.resolve(strict=False)
        if (
            not allow_prepared_parent
            and not self._inside(resolved, root_resolved)
            and resolved != root_resolved
        ):
            raise PathSafetyError(f"output escapes prepared root: {target}")
        for forbidden in self.forbidden_roots:
            if self._inside(resolved, forbidden) or (
                resolved == root_resolved and self._inside(forbidden, resolved)
            ):
                raise PathSafetyError(f"output overlaps observed root: {forbidden}")
        current = allowed
        if current.exists() and _is_reparse(current):
            raise PathSafetyError(f"output root is a reparse point: {current}")
        try:
            relative = lexical.relative_to(allowed)
        except ValueError as exc:
            raise PathSafetyError(
                f"output path has ambiguous identity: {target}"
            ) from exc
        for part in relative.parts:
            current = current / part
            if current.exists() and _is_reparse(current):
                raise PathSafetyError(f"reparse output component rejected: {current}")
        return lexical

    def prepare(self) -> None:
        self._validate_components(self.root)
        allowed = next(
            root
            for root in self.allowed_roots
            if self._inside(self.root.resolve(strict=False), root)
        )
        try:
            ensure_directory_path(self.root)
        except (MutationConflict, MutationUnsupported) as exc:
            raise PathSafetyError(str(exc)) from exc
        self._validate_components(self.root)
        relative = self.root.relative_to(allowed)
        current = allowed
        for part in relative.parts:
            current = current / part
            self._validate_components(current, allow_prepared_parent=True)
        info = self.root.stat()
        self._root_identity = (info.st_dev, info.st_ino)
        self._admitted.add(_path_key(self.root))

    def revalidate(self, target: Path) -> Path:
        if self._root_identity is None:
            raise PathSafetyError("output root was not prepared")
        lexical = self._validate_components(target)
        info = self.root.stat()
        if (info.st_dev, info.st_ino) != self._root_identity:
            raise PathSafetyError("output root identity changed after validation")
        return lexical

    def admit(self, target: Path | str) -> Path:
        path = Path(target)
        if not path.is_absolute():
            path = self.root / path
        lexical = self.revalidate(path)
        self._admitted.add(_path_key(lexical))
        return lexical

    def child(self, relative: str) -> Path:
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
        ):
            raise PathSafetyError("dynamic output child must be a relative path")
        return self.admit(self.root / relative)

    def _admit_for_write(self, path: Path) -> Path:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.root / candidate
        lexical = self.revalidate(candidate)
        if _path_key(lexical) not in self._admitted:
            raise PathSafetyError(
                f"output child was not admitted before write: {lexical}"
            )
        if not lexical.parent.exists():
            raise PathSafetyError(f"output parent does not exist: {lexical.parent}")
        return lexical

    def atomic_json(self, path: Path, value: Any) -> None:
        target = self._admit_for_write(path)
        data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
        try:
            relative = target.relative_to(self.root)
            expected = capture_target(self.root, relative)
            mutation_replace(self.root, relative, data, expected=expected)
        except (MutationConflict, MutationUnsupported, ValueError) as exc:
            raise PathSafetyError(str(exc)) from exc

    def write_bytes(self, path: Path, data: bytes) -> None:
        target = self._admit_for_write(path)
        try:
            relative = target.relative_to(self.root)
            expected = capture_target(self.root, relative)
            mutation_replace(self.root, relative, data, expected=expected)
        except (MutationConflict, MutationUnsupported, ValueError) as exc:
            raise PathSafetyError(str(exc)) from exc

    def replace(self, source: Path, target: Path) -> None:
        source_path = self._admit_for_write(source)
        target_path = self._admit_for_write(target)
        try:
            source_relative = source_path.relative_to(self.root)
            target_relative = target_path.relative_to(self.root)
            mutation_rename(
                self.root,
                source_relative,
                target_relative,
                expected_source=capture_target(self.root, source_relative),
                expected_target=capture_target(self.root, target_relative),
            )
        except (MutationConflict, MutationUnsupported, ValueError) as exc:
            raise PathSafetyError(str(exc)) from exc

    def append_jsonl(self, path: Path, records: list[Mapping[str, Any]]) -> None:
        if not records:
            return
        target = self._admit_for_write(path)
        data = _jsonl_bytes(records)
        with PathKeyedAppendLock(target):
            try:
                relative = target.relative_to(self.root)
                mutation_append(self.root, relative, data)
            except (MutationConflict, MutationUnsupported, ValueError) as exc:
                raise PathSafetyError(str(exc)) from exc


# Short names are kept as discoverable aliases for callers that think of the
# object as a prepared root rather than a write transaction.
PreparedOutput = PreparedOutputTransaction
OutputTransaction = PreparedOutputTransaction
PreparedOutputRoot = PreparedOutputTransaction


def append_jsonl(
    path: Path,
    record_or_records: Mapping[str, Any] | list[Mapping[str, Any]],
    *,
    lock_root: Path | None = None,
) -> None:
    """Compatibility spelling for the single shared append primitive."""
    records = (
        record_or_records
        if isinstance(record_or_records, list)
        else [record_or_records]
    )
    append_jsonl_records(path, records, lock_root=lock_root)


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
    ) -> None:
        self.harness_root = harness_root.resolve()
        self.output_root = output_root.absolute()
        self.forbidden_roots = tuple(root.resolve() for root in forbidden_roots)
        self.allowed_output_roots = (
            self.harness_root,
            *(root.resolve() for root in allowed_output_roots),
        )
        self.fail_after_event_append = fail_after_event_append
        self._root_identity: tuple[int, int] | None = None
        self._prepared_transaction = PreparedOutputTransaction(
            self.output_root,
            allowed_roots=self.allowed_output_roots,
            forbidden_roots=self.forbidden_roots,
        )

    def _validate_components(self, target: Path) -> None:
        if _has_ads_syntax(target):
            raise PathSafetyError(f"alternate data stream syntax rejected: {target}")
        lexical = target.absolute()
        resolved = lexical.resolve(strict=False)
        allowed_root = next(
            (root for root in self.allowed_output_roots if _within(resolved, root)),
            None,
        )
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
        self._prepared_transaction.prepare()
        self._validate_components(self.output_root)
        base = next(
            root
            for root in self.allowed_output_roots
            if _within(self.output_root.resolve(strict=False), root)
        )
        try:
            ensure_directory_path(self.output_root)
        except (MutationConflict, MutationUnsupported) as exc:
            raise PathSafetyError(str(exc)) from exc
        current = base
        self._validate_components(current)
        relative = self.output_root.relative_to(base)
        for part in relative.parts:
            current = current / part
            self._validate_components(current)
        for path in (self.snapshot_path, self.events_path):
            self._prepared_transaction.admit(path)
        info = self.output_root.stat()
        self._root_identity = (info.st_dev, info.st_ino)

    def _revalidate(self, target: Path) -> None:
        self._prepared_transaction.revalidate(target)
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

    def atomic_json(self, path: Path, value: Any) -> None:
        self._revalidate(path)
        self._prepared_transaction.atomic_json(path, value)

    def append_events(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        self._revalidate(self.events_path)
        self._prepared_transaction.append_jsonl(self.events_path, events)
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
