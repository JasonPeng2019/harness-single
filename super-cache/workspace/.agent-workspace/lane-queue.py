#!/usr/bin/env python3
"""Provider-neutral worker lane-queue helper (lane-inbox/v1).

Advances one ROOT assignment in the worker inbox
(``.agent-workspace/QUEUE.json``) through the closed state machine
PENDING -> ACKNOWLEDGED -> COMPLETE | BLOCKED.  The worker never writes the
manager queue; this helper only touches the worker inbox.  It prefers the
current runtime record primitives when ``orchestrator_harness`` is
importable and otherwise falls back to portable stdlib equivalents.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LANE_INBOX_SCHEMA = "lane-inbox/v1"
BINDING_SCHEMA = "harness-hook-binding/v1"
ASSIGNMENT_STATES = frozenset({"PENDING", "ACKNOWLEDGED", "COMPLETE", "BLOCKED"})

try:
    from orchestrator_harness.core import iso_utc as _runtime_iso_utc
    from orchestrator_harness.records import (
        RecordLock as _RuntimeRecordLock,
        atomic_write_json as _runtime_atomic_write_json,
    )
except Exception:  # pragma: no cover - standalone fallback
    _runtime_iso_utc = None
    _RuntimeRecordLock = None
    _runtime_atomic_write_json = None


def iso_utc() -> str:
    if _runtime_iso_utc is not None:
        return _runtime_iso_utc()
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {path}")
    return value


def _atomic_write_json(path: Path, value: Any) -> None:
    if _runtime_atomic_write_json is not None:
        _runtime_atomic_write_json(path, value)
        return
    target = Path(path).absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_path, target)
    except BaseException:
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise


class _RecordLock:
    """Portable short advisory lock mirroring ``orchestrator_harness.records.RecordLock``."""

    _guard = threading.Lock()
    _registry: dict[str, threading.Lock] = {}

    def __init__(self, path: Path) -> None:
        self.path = Path(path).absolute()
        self.key = os.path.normcase(str(self.path))
        self.lock_path = self.path.parent / f".{self.path.name}.lock"
        self._handle: Any = None
        self._local: threading.Lock | None = None

    def __enter__(self) -> "_RecordLock":
        with self._guard:
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


def _lock(path: Path) -> Any:
    if _RuntimeRecordLock is not None:
        return _RuntimeRecordLock(path)
    return _RecordLock(path)


def _binding(agent_workspace: Path) -> dict[str, Any]:
    path = agent_workspace / "harness-hook-binding.json"
    if not path.is_file():
        raise ValueError(f"worker binding missing: {path}")
    record = _read_json(path)
    if record.get("schema") != BINDING_SCHEMA:
        raise ValueError(f"binding schema mismatch at {path}")
    return record


def _inbox_path(agent_workspace: Path) -> Path:
    binding = _binding(agent_workspace)
    raw = binding.get("inbox_path")
    return Path(raw) if raw else agent_workspace / "QUEUE.json"


def _read_inbox(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"lane inbox missing: {path}")
    record = _read_json(path)
    if record.get("schema") != LANE_INBOX_SCHEMA:
        raise ValueError(f"lane inbox schema mismatch at {path}")
    return record


def _find_assignment(inbox: dict[str, Any], event_id: str) -> dict[str, Any] | None:
    for assignment in inbox.get("assignments", []):
        if assignment.get("event_id") == event_id:
            return assignment
    return None


_ALLOWED_TRANSITIONS = {
    "PENDING": frozenset({"ACKNOWLEDGED"}),
    "ACKNOWLEDGED": frozenset({"COMPLETE", "BLOCKED"}),
    "COMPLETE": frozenset(),
    "BLOCKED": frozenset(),
}


def _advance(
    inbox: dict[str, Any],
    event_id: str,
    new_state: str,
    summary: str | None = None,
) -> dict[str, Any]:
    assignment = _find_assignment(inbox, event_id)
    if assignment is None:
        raise ValueError(f"assignment not found: {event_id}")
    current = assignment.get("state")
    if current not in ASSIGNMENT_STATES:
        raise ValueError(f"assignment {event_id} has invalid state {current!r}")
    if new_state not in _ALLOWED_TRANSITIONS.get(current, frozenset()):
        raise ValueError(f"assignment {event_id} cannot move {current} -> {new_state}")
    assignment["state"] = new_state
    history = assignment.setdefault("history", [])
    history.append({"state": new_state, "at": iso_utc()})
    if summary is not None:
        assignment["summary"] = summary
    return assignment


def _run(
    agent_workspace: Path,
    command: str,
    event_id: str,
    summary: str | None,
) -> dict[str, Any]:
    path = _inbox_path(agent_workspace)
    new_state = {
        "acknowledge": "ACKNOWLEDGED",
        "complete": "COMPLETE",
        "block": "BLOCKED",
    }[command]
    with _lock(path):
        inbox = _read_inbox(path)
        assignment = _advance(inbox, event_id, new_state, summary=summary)
        _atomic_write_json(path, inbox)
    return {
        "schema": "lane-queue-result/v1",
        "event_id": event_id,
        "state": assignment["state"],
        "lane_id": inbox.get("lane_id"),
        "run_id": inbox.get("run_id"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lane-queue",
        description="Advance one ROOT assignment in the worker lane inbox.",
    )
    parser.add_argument(
        "--agent-workspace",
        default=None,
        help="path to the worktree .agent-workspace (default: this script's directory)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    acknowledge = subparsers.add_parser(
        "acknowledge", help="move one assignment PENDING -> ACKNOWLEDGED"
    )
    acknowledge.add_argument("--event-id", required=True)
    complete = subparsers.add_parser(
        "complete", help="move one assignment ACKNOWLEDGED -> COMPLETE"
    )
    complete.add_argument("--event-id", required=True)
    complete.add_argument("--summary")
    block = subparsers.add_parser(
        "block", help="move one assignment ACKNOWLEDGED -> BLOCKED"
    )
    block.add_argument("--event-id", required=True)
    block.add_argument("--summary", required=True)
    args = parser.parse_args(argv)
    agent_workspace = (
        Path(args.agent_workspace).resolve()
        if args.agent_workspace
        else Path(__file__).resolve().parent
    )
    try:
        result = _run(agent_workspace, args.command, args.event_id, args.summary)
    except (OSError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
