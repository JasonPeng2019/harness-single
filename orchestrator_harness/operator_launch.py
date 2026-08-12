from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .models import iso_utc
from .processes import process_snapshot

_RECEIPT_SCHEMA = "orchestrator-operator-launch/v1"


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
        snapshot = process_snapshot()
        item = snapshot.by_pid.get(pid) if snapshot.complete else None
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


def _release_detached_child_handle(child: subprocess.Popen[str]) -> None:
    """Release this launcher's ownership after the receipt is durable.

    The operator boundary intentionally does not wait for a successful child.
    Keeping the local ``Popen`` object alive after that hand-off makes Python's
    destructor report an ignored ``ResourceWarning`` (and leaks the Windows
    process handle).  No pipe is used by this launcher, so closing the parent
    bookkeeping handle cannot affect the detached child.
    """
    if os.name == "nt":
        handle = getattr(child, "_handle", None)
        close_handle = getattr(handle, "Close", None)
        if callable(close_handle):
            close_handle()
        if hasattr(child, "_handle"):
            child._handle = None
    if hasattr(child, "_child_created"):
        child._child_created = False


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
    try:
        child = subprocess.Popen(list(argv), **kwargs)
        created_utc = _creation_identity(child.pid)
        if created_utc is None or child.poll() is not None:
            raise RuntimeError("child identity could not be proved live")
        result = {
            "schema": _RECEIPT_SCHEMA, "status": "launched", "label": label,
            "role": role, "argv": list(argv), "cwd": str(cwd_path), "pid": child.pid,
            "created_utc": created_utc, "launched_utc": iso_utc(launched_at),
            "platform": platform, "creationflags": flags,
            "expected_state_path": str(expected) if expected else None,
        }
        _finalize(receipt_path, result)
        _release_detached_child_handle(child)
        return result
    except Exception as exc:
        cleanup_confirmed: bool | None = None
        cleanup_error: str | None = None
        if child is not None:
            cleanup_confirmed, cleanup_error = _cleanup_exact_child(child)
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
