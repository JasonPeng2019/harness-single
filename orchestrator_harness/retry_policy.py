"""Protected, crash-safe provider budget: initial invocation plus one retry.

The worktree attempt log is diagnostic evidence. Only this controller-owned
journal under operator-only may authorize another provider process.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any

from .core import iso_utc, new_id
from .records import RecordLock

MAX_LANE_PROVIDER_INVOCATIONS = 2
WORKER_RETRY_LIMIT = "WORKER_RETRY_LIMIT"
SCHEMA = "worker-provider-history/v1"


def _identity(lane: dict[str, Any]) -> dict[str, Any]:
    return {"lane_id": lane["lane_id"],
            "worktree": os.path.normcase(str(Path(lane["worktree_path"]).resolve())),
            "provider": lane["provider"]}


def history_path(rt: Path, lane: dict[str, Any]) -> Path:
    identity = _identity(lane)
    key = hashlib.sha256((identity["lane_id"] + "\0" + identity["worktree"]).encode()).hexdigest()
    return Path(rt).absolute() / "operator-only" / "provider-invocations" / (key + ".jsonl")


def _path(lane: dict[str, Any], rt: Path | None = None) -> Path:
    saved = lane.get("provider_history_path")
    if not isinstance(saved, str) or not saved:
        raise ValueError("protected invocation history is missing; use a fresh lane/session")
    path = Path(saved).absolute()
    expected = history_path(rt if rt is not None else path.parent.parent.parent, lane)
    if path != expected or path.resolve() != path:
        raise ValueError("protected invocation history identity/path changed")
    return path


def _append(path: Path, row: dict[str, Any]) -> None:
    # Caller owns the record lock. Reservation is durable before process creation.
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _same_fact(first: dict[str, Any], second: dict[str, Any]) -> bool:
    return {k: v for k, v in first.items() if k != "recorded_at"} == {k: v for k, v in second.items() if k != "recorded_at"}


def _read(path: Path, lane: dict[str, Any]) -> dict[str, dict[str, Any]]:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ValueError("protected invocation history is missing; preserve evidence and use a fresh lane/session") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("protected invocation history is not a regular file")
    raw = path.read_text(encoding="utf-8")
    if not raw.endswith("\n"):
        raise ValueError("protected invocation history has an incomplete durable write")
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if not rows or rows[0] != {"schema": SCHEMA, **_identity(lane)}:
        raise ValueError("protected invocation history owner/schema changed")
    invocations: dict[str, dict[str, Any]] = {}
    for row in rows[1:]:
        if not isinstance(row, dict):
            raise ValueError("invalid protected invocation fact")
        token, run, attempt = row.get("invocation_id"), row.get("run_id"), row.get("attempt")
        if (not isinstance(token, str) or len(token) != 32 or not isinstance(run, str)
                or not run or type(attempt) is not int or attempt < 1):
            raise ValueError("ambiguous protected invocation identity")
        prior = invocations.get(token)
        kind = row.get("kind")
        if prior is not None and any(prior.get(k) != row.get(k) for k in ("run_id", "attempt")):
            raise ValueError("protected invocation identity was reused")
        if prior is not None and _same_fact(prior, row):
            continue
        if kind == "RESERVED":
            if prior is not None:
                raise ValueError("protected invocation reservation was rewritten")
        elif (prior is None or prior["kind"] in ("COMPLETED", "NO_PROVIDER")
              or kind not in ("STARTED", "COMPLETED", "NO_PROVIDER")):
            raise ValueError("invalid protected invocation transition")
        elif kind == "STARTED" and prior["kind"] != "RESERVED":
            raise ValueError("protected invocation started twice")
        elif kind == "NO_PROVIDER" and (prior["kind"] != "RESERVED" or row.get("cleanup_proven") is not True):
            raise ValueError("a possibly started provider cannot refund its reservation")
        elif kind == "COMPLETED" and (row.get("provider_started") is not True or type(row.get("cleanup_proven")) is not bool):
            raise ValueError("invalid protected provider outcome")
        invocations[token] = row
    return invocations


def initialize_history(rt: Path, lane: dict[str, Any]) -> Path:
    """For a new bootstrap, or native-proven never-launched migration only."""
    path = history_path(rt, lane)
    if path.resolve() != path:
        raise ValueError("protected invocation history cannot use redirected paths")
    with RecordLock(path):
        if path.exists():
            _read(path, lane)
        elif lane.get("provider_history_path") or (lane.get("session") or {}).get("session_id"):
            raise ValueError("existing session lost protected history; use a fresh lane/session")
        else:
            _append(path, {"schema": SCHEMA, **_identity(lane)})
    lane["provider_history_path"] = str(path)
    return path


def _count(invocations: dict[str, dict[str, Any]], lane: dict[str, Any]) -> int:
    rows = list(invocations.values())
    if any(row["kind"] in ("RESERVED", "STARTED") or
           (row["kind"] == "COMPLETED" and row.get("cleanup_proven") is not True) for row in rows):
        raise ValueError("prior provider creation or cleanup remains unproven")
    completed = [row for row in rows if row["kind"] == "COMPLETED"]
    session = (lane.get("session") or {}).get("session_id")
    if session and (not completed or any(row.get("session_id") != session for row in completed)):
        raise ValueError("saved native session does not match protected invocation history")
    return len(completed)


def completed_provider_invocations(lane: dict[str, Any], rt: Path | None = None) -> int:
    path = _path(lane, rt)
    with RecordLock(path):
        return _count(_read(path, lane), lane)


def current_invocation_facts(lane: dict[str, Any]) -> list[dict[str, Any]]:
    """Read protected creation/cleanup facts for this exact current run."""
    path = _path(lane)
    with RecordLock(path):
        return [row for row in _read(path, lane).values() if row["run_id"] == lane["run_id"]]


def reserve_invocation(rt: Path, lane: dict[str, Any], *, attempt_number: int = 1) -> str:
    """Reserve under native lane synchronization, immediately before spawn."""
    path = _path(lane, rt)
    with RecordLock(path):
        invocations = _read(path, lane)
        spent = _count(invocations, lane)
        if spent >= MAX_LANE_PROVIDER_INVOCATIONS:
            raise ValueError("initial provider invocation plus one explicit retry exhausted")
        if any(row["kind"] == "COMPLETED" and row["run_id"] == lane["run_id"] for row in invocations.values()):
            raise ValueError("this explicit launch already invoked its provider")
        if spent and not (lane.get("session") or {}).get("session_id"):
            raise ValueError("retry requires the saved native session; no fresh replacement launch")
        token = new_id()
        _append(path, {"invocation_id": token, "run_id": lane["run_id"],
                       "attempt": attempt_number, "kind": "RESERVED", "recorded_at": iso_utc()})
        return token


def _finish(rt: Path, lane: dict[str, Any], token: str, fields: dict[str, Any]) -> None:
    path = _path(lane, rt)
    with RecordLock(path):
        prior = _read(path, lane).get(token)
        if prior is None or prior["run_id"] != lane["run_id"]:
            raise ValueError("protected invocation outcome has no matching reservation")
        row = {**prior, **fields, "recorded_at": iso_utc()}
        if _same_fact(prior, row):
            return
        kind = fields["kind"]
        if prior["kind"] in ("COMPLETED", "NO_PROVIDER") or (
            kind in ("STARTED", "NO_PROVIDER") and prior["kind"] != "RESERVED"
        ):
            raise ValueError("protected invocation outcome cannot rewrite established facts")
        _append(path, row)


def record_started(rt: Path, lane: dict[str, Any], token: str, *, process_boundary: dict[str, Any] | None = None) -> None:
    fields: dict[str, Any] = {"kind": "STARTED"}
    if process_boundary is not None:
        fields["process_boundary"] = process_boundary
    _finish(rt, lane, token, fields)


def record_outcome(rt: Path, lane: dict[str, Any], token: str, *,
                   provider_started: bool, cleanup_proven: bool, session_id: str | None) -> None:
    if not provider_started and not cleanup_proven:
        raise ValueError("an unproven start cannot refund a provider reservation")
    fields: dict[str, Any] = {"kind": "COMPLETED" if provider_started else "NO_PROVIDER",
                            "provider_started": provider_started, "cleanup_proven": cleanup_proven,
                            "session_id": session_id}
    _finish(rt, lane, token, fields)
