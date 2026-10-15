"""Portable record storage: advisory locks, atomic replace, and JSON records.

The v2 harness relies only on portable guarantees: same-volume atomic rename
and advisory process-scoped file locks.  Every record that can be replaced is
written as a unique temporary sibling in the destination's own parent
directory, validated, then atomically renamed under that record's short lock.
"""

from __future__ import annotations

import errno
import json
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from .core import canonical_json, require_schema


_WINDOWS_REPLACE_RETRY_ERRORS = frozenset({5, 32})
_WINDOWS_REPLACE_MAX_ATTEMPTS = 5
_WINDOWS_REPLACE_RETRY_DELAY_SECONDS = 0.05


def _replace_with_retry(source: Path, target: Path) -> None:
    """Replace one same-parent path, retrying transient Windows races.

    Windows can briefly report ``ERROR_ACCESS_DENIED`` or
    ``ERROR_SHARING_VIOLATION`` while another process closes a handle to the
    destination.  Retry only those native errors, and only for a bounded
    number of attempts; every other error propagates immediately.
    """
    for attempt in range(_WINDOWS_REPLACE_MAX_ATTEMPTS):
        try:
            os.replace(source, target)
            return
        except OSError as exc:
            error = getattr(exc, "winerror", None)
            if error is None:
                error = getattr(exc, "errno", None)
            if os.name != "nt" or error not in _WINDOWS_REPLACE_RETRY_ERRORS:
                raise
            if attempt + 1 >= _WINDOWS_REPLACE_MAX_ATTEMPTS:
                raise
            time.sleep(_WINDOWS_REPLACE_RETRY_DELAY_SECONDS * (attempt + 1))


class RecordError(OSError):
    """A record could not be read, written, or locked."""


def _read_plain_json(path: Path) -> dict[str, Any]:
    from .mutation import _lexical, read_bytes
    target = _lexical(path)
    value = json.loads(read_bytes(target.parent, target.name).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {path}")
    return value


def read_json(path: Any) -> dict[str, Any]:
    """Preserve the record module's reader export with no-follow authority I/O."""
    return _read_plain_json(path)


class RecordLock:
    """One short advisory lock for one record path.

    The lock is a persistent sibling file whose OS lock is released when the
    owning process closes its handle (or dies).  A per-path in-process lock is
    held around the kernel lock so threads in one process serialize too.
    """

    _registry_guard = threading.Lock()
    _registry: dict[str, threading.Lock] = {}
    _registry_users: dict[str, int] = {}

    def __init__(self, path: Path, *, root: Path | None = None) -> None:
        self.path = Path(path).absolute()
        self.root = Path(root).absolute() if root is not None else None
        self.key = os.path.normcase(str(self.path))
        self.lock_path = self.path.parent / f".{self.path.name}.lock"
        self._handle: Any = None
        self._anchor: Any = None
        self._local: threading.Lock | None = None

    def __enter__(self) -> "RecordLock":
        if self._local is not None:
            raise RecordError("a RecordLock instance cannot be entered twice")
        with self._registry_guard:
            local = self._registry.setdefault(self.key, threading.Lock())
            self._registry_users[self.key] = self._registry_users.get(self.key, 0) + 1
        try:
            local.acquire()
        except BaseException:
            self._drop_registry_user(local)
            raise
        self._local = local
        handle = None
        try:
            from .mutation import ensure_directory_path, open_append_file
            if self.root is None:
                ensure_directory_path(self.lock_path.parent)
            root = self.root if self.root is not None else self.lock_path.parent
            self._anchor = open_append_file(root, self.lock_path.relative_to(root))
            handle = self._anchor.handle
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                while True:
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                        break
                    except OSError as exc:
                        # LK_LOCK gives up after ten native attempts. Admission
                        # can legitimately cover a longer worktree preparation.
                        # Repeat acquisition only, never the protected operation.
                        if exc.errno != errno.EDEADLK:
                            raise
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            self._handle = handle
            return self
        except BaseException:
            anchor, self._anchor = self._anchor, None
            try:
                if anchor is not None:
                    anchor.close()
                elif handle is not None:
                    handle.close()
            finally:
                self._release_local()
            raise

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        handle = self._handle
        self._handle = None
        anchor, self._anchor = self._anchor, None
        try:
            if handle is not None:
                try:
                    if os.name == "nt":
                        import msvcrt

                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    if anchor is not None:
                        anchor.close()
                    else:
                        handle.close()
        finally:
            self._release_local()

    def _release_local(self) -> None:
        local = self._local
        self._local = None
        if local is not None:
            local.release()
            self._drop_registry_user(local)

    def _drop_registry_user(self, local: threading.Lock) -> None:
        with self._registry_guard:
            remaining = self._registry_users[self.key] - 1
            if remaining:
                self._registry_users[self.key] = remaining
            else:
                self._registry_users.pop(self.key)
                if self._registry.get(self.key) is local:
                    self._registry.pop(self.key)


_admission_local = threading.local()


@contextmanager
def runtime_admission_lock(rt: Path):
    """Serialize ownership effects with OPEN/SHUTTING_DOWN/CLOSED transitions.

    Lock order is runtime admission, lane operation, then individual records.
    Nested helpers in the same thread reuse the held kernel lock. Exit waits
    belong outside this transaction so a controller can finish its own startup.
    """
    path = Path(rt).absolute() / "RUNTIME_STATE.json"
    key = os.path.normcase(str(path))
    held = getattr(_admission_local, "held", None)
    if held is None:
        held = _admission_local.held = set()
    if key in held:
        yield
        return
    with RecordLock(path):
        held.add(key)
        try:
            yield
        finally:
            held.remove(key)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Publish complete bytes under held no-follow ancestors."""
    from .mutation import _lexical, ensure_directory_path, publish_bytes
    target = _lexical(path)
    ensure_directory_path(target.parent)
    publish_bytes(target.parent, target.name, data)


def atomic_write_json(path: Path, value: Any) -> None:
    """Write one canonical JSON record atomically."""
    atomic_write_bytes(path, canonical_json(value) + b"\n")


def read_record(path: Path, schema: str) -> dict[str, Any]:
    """Read and validate one JSON record's schema string."""
    record = read_json(path)
    require_schema(record, schema, path)
    return record


def write_record(path: Path, schema: str, fields: dict[str, Any]) -> dict[str, Any]:
    """Atomically write one record under its short lock and return it."""
    record = dict(fields)
    record["schema"] = schema
    with RecordLock(path):
        atomic_write_json(path, record)
    return record


def remove_record(path: Path) -> None:
    """Remove one record file under its short lock (missing is a no-op)."""
    from .mutation import _lexical, delete
    target = _lexical(path)
    with RecordLock(target):
        delete(target.parent, target.name)


def update_record(
    path: Path,
    schema: str,
    mutate: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Read-modify-replace one record under its short lock.

    ``mutate`` receives the parsed record (or ``{}`` when absent) and returns
    the replacement fields; the schema is applied on write.
    """
    with RecordLock(path):
        try:
            current = read_json(path)
        except FileNotFoundError:
            current = {}
        else:
            require_schema(current, schema, path)
        record = dict(mutate(current))
        record["schema"] = schema
        atomic_write_json(path, record)
    return record


def append_jsonl(path: Path, record: dict[str, Any], *, header: dict[str, Any] | None = None) -> None:
    """Append one event object to a JSONL audit log under its short lock.

    When ``header`` is given and the file is absent or empty, the header object
    is written as the first line first.
    """
    from .mutation import _lexical, open_append_file, MutationConflict
    target = _lexical(path)
    data = canonical_json(record) + b"\n"
    header_data = canonical_json(header) + b"\n" if header is not None else b""
    with RecordLock(target):
        with open_append_file(target.parent, target.name) as anchored:
            handle = anchored.handle
            before = os.fstat(handle.fileno())
            if before.st_nlink != 1:
                raise MutationConflict("append target gained hard links")
            payload = (header_data if before.st_size == 0 else b"") + data
            anchored.anchor.assert_stable()
            handle.seek(0, os.SEEK_END)
            if handle.write(payload) != len(payload):
                raise RecordError("record append did not write all bytes")
            handle.flush()
            os.fsync(handle.fileno())
            after = os.fstat(handle.fileno())
            named = anchored.anchor.stat(target.name)
            if (after.st_nlink != 1 or after.st_size != before.st_size + len(payload)
                    or (after.st_dev, after.st_ino) != (named.st_dev, named.st_ino)):
                raise MutationConflict("record append target changed during writing")
            anchored.anchor.assert_stable()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a JSONL audit log, skipping a malformed trailing line."""
    from .mutation import _lexical, read_bytes
    target = _lexical(path)
    try:
        raw = read_bytes(target.parent, target.name)
    except FileNotFoundError:
        return []
    records: list[dict[str, Any]] = []
    for line in raw.decode("utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records
