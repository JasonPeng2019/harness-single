#!/usr/bin/env python3
"""Provider-neutral result Stop check for managed lanes (result/v1).

Validates the worktree ``RESULT.json`` exactly like the controller's
``_validate_result``: schema, lane/run identity, the closed outcome
vocabulary, a non-empty summary, an evidence list, and the content hash.
Exits 0 when the result is valid and 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

RESULT_SCHEMA = "result/v1"
RESULT_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED"})
BINDING_SCHEMA = "harness-hook-binding/v1"
REPORT_SCHEMA = "result-stop-check/v1"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def content_hash(record: dict[str, Any]) -> str:
    """Mirror ``orchestrator_harness.core.content_hash``."""
    payload = {key: item for key, item in record.items() if key != "content_hash"}
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {path}")
    return value


def _binding(agent_workspace: Path) -> dict[str, Any]:
    path = agent_workspace / "harness-hook-binding.json"
    if not path.is_file():
        raise ValueError(f"worker binding missing: {path}")
    record = _read_json(path)
    if record.get("schema") != BINDING_SCHEMA:
        raise ValueError(f"binding schema mismatch at {path}")
    return record


def check_result(
    agent_workspace: Path,
) -> tuple[bool, str, dict[str, Any] | None]:
    """Return (valid, reason, record) for the lane's RESULT.json."""
    try:
        binding = _binding(agent_workspace)
    except (OSError, ValueError) as exc:
        return False, str(exc), None
    result_path = Path(
        binding.get("result_path") or agent_workspace.parent / "RESULT.json"
    )
    if not result_path.is_file():
        return False, f"result missing: {result_path}", None
    try:
        record = _read_json(result_path)
        if record.get("schema") != RESULT_SCHEMA:
            return False, f"schema mismatch: {record.get('schema')!r}", record
        if binding.get("lane_id") and record.get("lane_id") != binding.get("lane_id"):
            return False, "lane_id mismatch", record
        if binding.get("run_id") and record.get("run_id") != binding.get("run_id"):
            return False, "run_id mismatch", record
        if record.get("outcome") not in RESULT_OUTCOMES:
            return False, f"outcome not in {sorted(RESULT_OUTCOMES)}", record
        if not isinstance(record.get("summary"), str) or not record["summary"]:
            return False, "summary is empty", record
        if not isinstance(record.get("evidence"), list):
            return False, "evidence is not a list", record
        if record.get("content_hash") != content_hash(record):
            return False, "content_hash mismatch", record
        return True, "result is valid", record
    except (OSError, ValueError) as exc:
        return False, str(exc), None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="result-stop-check",
        description="Validate the worktree RESULT.json against result/v1.",
    )
    parser.add_argument(
        "--agent-workspace",
        default=None,
        help="path to the worktree .agent-workspace (default: this script's directory)",
    )
    parser.add_argument("--json", action="store_true", help="emit a JSON report")
    args = parser.parse_args(argv)
    agent_workspace = (
        Path(args.agent_workspace).resolve()
        if args.agent_workspace
        else Path(__file__).resolve().parent
    )
    valid, reason, _record = check_result(agent_workspace)
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "valid": valid,
        "reason": reason,
    }
    try:
        binding = _binding(agent_workspace)
        report["result_path"] = str(
            Path(binding.get("result_path") or agent_workspace.parent / "RESULT.json")
        )
    except (OSError, ValueError):
        pass
    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        print(("PASS" if valid else "FAIL") + ": " + reason)
    return 0 if valid else 1


if __name__ == "__main__":
    sys.exit(main())
