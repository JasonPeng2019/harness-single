#!/usr/bin/env python3
"""Provider-neutral worker hook dispatcher (harness-hook-binding/v1).

Decides the PostToolUse and Stop hook boundaries for a managed worker lane.
The dispatcher reads only the worker binding and its inbox; it never reads
or writes the manager queue and never advances assignment state.  The Stop
boundary delegates result validation to the binding's ``result_stop_check``
helper through its existing ``check_result(agent_workspace)``.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

BINDING_SCHEMA = "harness-hook-binding/v1"
INBOX_SCHEMA = "lane-inbox/v1"
UNRESOLVED_STATES = frozenset({"PENDING", "ACKNOWLEDGED"})
BOUNDARIES = frozenset({"post-tool-use", "stop"})


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {path}")
    return value


def _reject(reason: str) -> dict[str, Any]:
    return {"decision": "REJECT", "reason": reason}


def _load_binding(agent_workspace: Path) -> tuple[dict[str, Any] | None, str | None]:
    path = agent_workspace / "harness-hook-binding.json"
    if not path.is_file():
        return None, f"binding missing: {path}"
    try:
        record = _read_json(path)
    except (OSError, ValueError) as exc:
        return None, f"binding unreadable: {exc}"
    if record.get("schema") != BINDING_SCHEMA:
        return None, f"binding schema mismatch: {record.get('schema')!r}"
    if record.get("role") != "worker":
        return None, f"binding role is not worker: {record.get('role')!r}"
    if not record.get("inbox_path"):
        return None, "binding inbox_path is missing"
    if not record.get("result_stop_check"):
        return None, "binding result_stop_check is missing"
    return record, None


def _load_inbox(binding: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    inbox_path = Path(binding["inbox_path"])
    if not inbox_path.is_file():
        return None, f"inbox missing: {inbox_path}"
    try:
        record = _read_json(inbox_path)
    except (OSError, ValueError) as exc:
        return None, f"inbox unreadable: {exc}"
    if record.get("schema") != INBOX_SCHEMA:
        return None, f"inbox schema mismatch: {record.get('schema')!r}"
    if binding.get("lane_id") and record.get("lane_id") != binding.get("lane_id"):
        return None, f"inbox lane_id mismatch: {record.get('lane_id')!r}"
    if binding.get("run_id") and record.get("run_id") != binding.get("run_id"):
        return None, f"inbox run_id mismatch: {record.get('run_id')!r}"
    return record, None


def _unresolved_event_ids(assignments: list[Any]) -> list[str]:
    return [
        str(assignment["event_id"])
        for assignment in assignments
        if isinstance(assignment, dict)
        and assignment.get("state") in UNRESOLVED_STATES
        and assignment.get("event_id")
    ]


def _stop_check_result(
    agent_workspace: Path, binding: dict[str, Any]
) -> tuple[bool, str]:
    stop_check_path = Path(binding["result_stop_check"])
    if not stop_check_path.is_file():
        return False, f"result_stop_check missing: {stop_check_path}"
    try:
        spec = importlib.util.spec_from_file_location(
            "result_stop_check", stop_check_path
        )
        if spec is None or spec.loader is None:
            return False, f"result_stop_check not importable: {stop_check_path}"
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        check_result = getattr(module, "check_result", None)
        if not callable(check_result):
            return False, (
                f"result_stop_check has no callable check_result: {stop_check_path}"
            )
        valid, reason, _record = check_result(agent_workspace)
    except Exception as exc:
        return False, f"result_stop_check failed: {exc}"
    if not valid:
        return False, reason
    return True, ""


def dispatch(agent_workspace: Path, boundary: str) -> dict[str, Any]:
    if boundary not in BOUNDARIES:
        return _reject(f"unknown boundary: {boundary!r}")
    binding, reason = _load_binding(agent_workspace)
    if binding is None:
        return _reject(reason)
    inbox, reason = _load_inbox(binding)
    if inbox is None:
        return _reject(reason)
    assignments = inbox.get("assignments")
    if not isinstance(assignments, list):
        return _reject("inbox assignments is not a list")
    unresolved = _unresolved_event_ids(assignments)
    if boundary == "post-tool-use":
        if unresolved:
            return {"decision": "NOTICE", "notice": unresolved}
        return {"decision": "ALLOW"}
    if unresolved:
        return _reject(f"unresolved assignments: {unresolved}")
    valid, reason = _stop_check_result(agent_workspace, binding)
    if not valid:
        return _reject(reason)
    return {"decision": "ALLOW"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hook-dispatch",
        description="Decide the worker PostToolUse/Stop hook boundary.",
    )
    parser.add_argument(
        "--boundary",
        required=True,
        help="hook boundary: post-tool-use or stop",
    )
    parser.add_argument(
        "--agent-workspace",
        default=None,
        help="path to the worktree .agent-workspace (default: this script's directory)",
    )
    args = parser.parse_args(argv)
    agent_workspace = (
        Path(args.agent_workspace).resolve()
        if args.agent_workspace
        else Path(__file__).resolve().parent
    )
    decision = dispatch(agent_workspace, args.boundary)
    print(json.dumps(decision, sort_keys=True))
    return 0 if decision["decision"] in ("ALLOW", "NOTICE") else 1


if __name__ == "__main__":
    sys.exit(main())
