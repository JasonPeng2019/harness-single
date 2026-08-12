from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .models import iso_utc
from .processes import targeted_process_query

_RECEIPT_SCHEMA = "orchestrator-operator-launch/v1"
_DETACHED_OWNERS_LOCK = threading.Lock()
_DETACHED_OWNERS: dict[tuple[int, str], "_DetachedChildOwner"] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_path(value: str, *, directory: bool) -> Path:
    path = Path(value).resolve(strict=False)
    if directory:
        if not path.is_dir():
            raise ValueError("cwd must name an existing directory")
    elif path.exists():
        raise ValueError("receipt already exists")
    if not path.parent.is_dir():
        raise ValueError("receipt parent must exist")
    return path


def _reserve_receipt(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise ValueError("receipt already exists") from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"schema": _RECEIPT_SCHEMA, "status": "reserving"}, handle)
        handle.flush()
        os.fsync(handle.fileno())


def _finalize(path: Path, receipt: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _creation_identity(pid: int, *, attempts: int = 12, delay_seconds: float = 0.05) -> str | None:
    for _ in range(attempts):
        query = targeted_process_query(pid)
        item = query.process if query.complete else None
        if item is not None and item.created_utc is not None:
            return iso_utc(item.created_utc)
        time.sleep(delay_seconds)
    return None


def _cleanup_exact_child(child: subprocess.Popen[str]) -> tuple[bool, str | None]:
    """Terminate and reap only the Popen child captured by this launch attempt."""
    try:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=1.0)
        # poll()/wait() above also reaps the exact child handle.
        return child.poll() is not None, None
    except Exception as exc:
        return False, str(exc)


class _DetachedChildOwner:
    """Own one detached child until a supported public ``wait`` completes."""

    def __init__(self, child: subprocess.Popen[str], pid: int, created_utc: str) -> None:
        self.child = child
        self.pid = pid
        self.created_utc = created_utc
        self._condition = threading.Condition()
        self._handed_off = False
        self._cancelled = False
        self._state = "waiting-for-handoff"
        self.exit_code: int | None = None
        self.error: str | None = None
        self.finished = threading.Event()
        self.thread = threading.Thread(
            target=self._reap,
            name=f"orchestrator-detached-reaper-{pid}",
            daemon=True,
        )

    def start(self) -> None:
        key = (self.pid, self.created_utc)
        with _DETACHED_OWNERS_LOCK:
            if key in _DETACHED_OWNERS:
                raise RuntimeError("detached child identity is already owned")
            _DETACHED_OWNERS[key] = self
        try:
            self.thread.start()
        except BaseException:
            with _DETACHED_OWNERS_LOCK:
                _DETACHED_OWNERS.pop(key, None)
            raise

    def handoff(self) -> None:
        with self._condition:
            if self._cancelled:
                raise RuntimeError("detached child owner was already cancelled")
            self._handed_off = True
            self._condition.notify_all()

    def cancel(self) -> None:
        with self._condition:
            self._cancelled = True
            self._condition.notify_all()

    def join(self, timeout: float | None = None) -> bool:
        if self.thread.ident is None:
            return True
        self.thread.join(timeout)
        return not self.thread.is_alive()

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            return {
                "pid": self.pid,
                "created_utc": self.created_utc,
                "state": self._state,
                "exit_code": self.exit_code,
                "error": self.error,
                "finished": self.finished.is_set(),
            }

    def _reap(self) -> None:
        key = (self.pid, self.created_utc)
        try:
            with self._condition:
                while not self._handed_off and not self._cancelled:
                    self._condition.wait()
                if self._cancelled:
                    self._state = "cancelled"
                    return
                self._state = "reaping"
            self.exit_code = self.child.wait()
        except BaseException as exc:
            self.error = str(exc)
        finally:
            with self._condition:
                if self._state != "cancelled":
                    self._state = "finished" if self.error is None else "failed"
            with _DETACHED_OWNERS_LOCK:
                if _DETACHED_OWNERS.get(key) is self:
                    _DETACHED_OWNERS.pop(key, None)
            self.finished.set()


def detached_owner_snapshot() -> list[dict[str, Any]]:
    """Return observable ownership state without exposing subprocess internals."""

    with _DETACHED_OWNERS_LOCK:
        owners = tuple(_DETACHED_OWNERS.values())
    return [owner.snapshot() for owner in owners]


def launch_process(
    *, receipt: str | Path, label: str, role: str, cwd: str | Path,
    argv: Sequence[str], expected_state_path: str | Path | None = None,
) -> dict[str, Any]:
    """Launch one detached manager-owned process and atomically prove its identity."""
    if not isinstance(label, str) or not label.strip() or not isinstance(role, str) or not role.strip():
        raise ValueError("label and role are required")
    if not argv or any(not isinstance(item, str) or not item for item in argv):
        raise ValueError("argv must contain nonempty strings")
    receipt_path = _validate_path(str(receipt), directory=False)
    cwd_path = _validate_path(str(cwd), directory=True)
    expected = Path(expected_state_path).resolve(strict=False) if expected_state_path else None
    _reserve_receipt(receipt_path)
    launched_at = _utc_now()
    flags = 0
    kwargs: dict[str, Any] = {"cwd": str(cwd_path), "shell": False}
    platform = "windows" if os.name == "nt" else "posix"
    if os.name == "nt":
        flags = subprocess.CREATE_BREAKAWAY_FROM_JOB | subprocess.CREATE_NO_WINDOW
        kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True
    child: subprocess.Popen[str] | None = None
    owner: _DetachedChildOwner | None = None
    try:
        child = subprocess.Popen(list(argv), **kwargs)
        created_utc = _creation_identity(child.pid)
        if created_utc is None or child.poll() is not None:
            raise RuntimeError("child identity could not be proved live")
        owner = _DetachedChildOwner(child, child.pid, created_utc)
        owner.start()
        result = {
            "schema": _RECEIPT_SCHEMA, "status": "launched", "label": label,
            "role": role, "argv": list(argv), "cwd": str(cwd_path), "pid": child.pid,
            "created_utc": created_utc, "launched_utc": iso_utc(launched_at),
            "platform": platform, "creationflags": flags,
            "expected_state_path": str(expected) if expected else None,
        }
        _finalize(receipt_path, result)
        owner.handoff()
        return result
    except Exception as exc:
        cleanup_confirmed: bool | None = None
        cleanup_error: str | None = None
        if owner is not None:
            owner.cancel()
            owner.join(timeout=1.0)
        if child is not None:
            cleanup_confirmed, cleanup_error = _cleanup_exact_child(child)
        if owner is not None:
            owner.join(timeout=1.0)
        failure = {
            "schema": _RECEIPT_SCHEMA, "status": "failed", "label": label,
            "role": role, "argv": list(argv), "cwd": str(cwd_path),
            "launched_utc": iso_utc(launched_at), "platform": platform,
            "creationflags": flags, "error": str(exc),
            "child_pid": child.pid if child is not None else None,
            "cleanup_confirmed": cleanup_confirmed,
            "cleanup_error": cleanup_error,
        }
        _finalize(receipt_path, failure)
        if cleanup_confirmed is False:
            raise RuntimeError(
                f"{exc}; exact child cleanup could not be confirmed: {cleanup_error or 'child remained live'}"
            ) from exc
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch one detached manager-owned process")
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--expected-state-path")
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = list(args.argv)
    if command[:1] == ["--"]:
        command = command[1:]
    try:
        result = launch_process(
            receipt=args.receipt, label=args.label, role=args.role, cwd=args.cwd,
            argv=command, expected_state_path=args.expected_state_path,
        )
    except Exception as exc:
        print(json.dumps({"launched": False, "error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
