"""Portable record storage: advisory locks, atomic replace, and JSON records.

The v2 harness relies only on portable guarantees: same-volume atomic rename
and advisory process-scoped file locks.  Every record that can be replaced is
written as a unique temporary sibling in the destination's own parent
directory, validated, then atomically renamed under that record's short lock.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .core import canonical_json, read_json, require_schema


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


class RecordLock:
    """One short advisory lock for one record path.

    The lock is a persistent sibling file whose OS lock is released when the
    owning process closes its handle (or dies).  A per-path in-process lock is
    held around the kernel lock so threads in one process serialize too.
    """

    _registry_guard = threading.Lock()
    _registry: dict[str, threading.Lock] = {}

    def __init__(self, path: Path) -> None:
        self.path = Path(path).absolute()
        self.key = os.path.normcase(str(self.path))
        self.lock_path = self.path.parent / f".{self.path.name}.lock"
        self._handle: Any = None
        self._local: threading.Lock | None = None

    def __enter__(self) -> "RecordLock":
        with self._registry_guard:
            local = self._registry.setdefault(self.key, threading.Lock())
        local.acquire()
        self._local = local
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.lock_path.open("a+b")
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            self._handle = handle
            return self
        except Exception:
            self._release_local()
            raise

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        handle = self._handle
        self._handle = None
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
                handle.close()
        self._release_local()

    def _release_local(self) -> None:
        local = self._local
        self._local = None
        if local is not None:
            local.release()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` via a temp sibling and same-volume rename."""
    target = Path(path).absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(temp_path, target)
    except BaseException:
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise


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
    target = Path(path).absolute()
    with RecordLock(target):
        try:
            target.unlink()
        except FileNotFoundError:
            pass


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
        except (OSError, json.JSONDecodeError, ValueError):
            current = {}
        record = dict(mutate(current))
        record["schema"] = schema
        atomic_write_json(path, record)
    return record


def append_jsonl(path: Path, record: dict[str, Any], *, header: dict[str, Any] | None = None) -> None:
    """Append one event object to a JSONL audit log under its short lock.

    When ``header`` is given and the file is absent or empty, the header object
    is written as the first line first.
    """
    target = Path(path).absolute()
    with RecordLock(target):
        target.parent.mkdir(parents=True, exist_ok=True)
        if header is not None and (not target.exists() or target.stat().st_size == 0):
            with target.open("a", encoding="utf-8") as handle:
                handle.write(canonical_json(header).decode("utf-8") + "\n")
        with target.open("a", encoding="utf-8") as handle:
            handle.write(canonical_json(record).decode("utf-8") + "\n")
            handle.flush()
            os.fsync(handle.fileno())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a JSONL audit log, skipping a malformed trailing line."""
    target = Path(path).absolute()
    if not target.exists():
        return []
    records: list[dict[str, Any]] = []
    with target.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records
