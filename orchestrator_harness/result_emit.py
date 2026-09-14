"""Publish a coding result from ROOT-owned identity and worker-authored facts.

See QUICK_START.md. This utility does not run checks, commit, accept work, or launch agents.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

# Also support an absolute script path when a worker's cwd is its own worktree.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator_harness.git_safety import (
    GitSafetyError,
    declaration_from_invocation,
    inspect_repository,
    validate_coding_result,
)


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def required_text(value: dict[str, Any], key: str) -> str:
    text = value.get(key)
    if not isinstance(text, str) or not text.strip():
        raise ValueError(f"context.{key} must be a non-empty string")
    return text


def emit(context_path: Path, facts_path: Path, *, check_only: bool = False) -> dict[str, Any]:
    """Validate and atomically publish RESULT.json; return publication evidence.

    Context is the current coding invocation, or a ROOT-authored external result context
    correlated with an operator-launch receipt. Facts contain exactly outcome, summary, checks.
    Validation errors leave any previous result untouched before publication; a failed readback
    reports failure and never claims success. Only one writer may own the lane's result path.
    """
    context = load_object(context_path)
    schema = context.get("schema")
    if schema not in {"orchestrator-coding-invocation/v1", "orchestrator-lane-result-context/v1"}:
        raise ValueError("context requires a coding invocation or lane-result-context/v1 schema")
    run_root = Path(required_text(context, "run_root"))
    if not run_root.is_absolute():
        raise ValueError("context.run_root must be absolute")
    run_root = run_root.resolve(strict=True)
    lane = required_text(context, "lane_id")
    worker = required_text(context, "worker_invocation_id")
    if schema == "orchestrator-lane-result-context/v1":
        receipt = load_object(Path(required_text(context, "receipt_path")))
        if (receipt.get("schema") != "orchestrator-operator-launch/v1"
                or receipt.get("status") != "launched" or receipt.get("label") != lane
                or Path(required_text(receipt, "cwd")).resolve(strict=True) != run_root):
            raise ValueError("external context does not match its frozen operator-launch receipt")
    declaration = declaration_from_invocation(context, run_root)
    identity = inspect_repository(declaration)
    facts = load_object(facts_path)
    expected = {"outcome", "summary", "checks"}
    if set(facts) != expected:
        raise ValueError(f"facts require exactly outcome, summary, checks; "
                         f"missing={sorted(expected - set(facts))}, extra={sorted(set(facts) - expected)}. "
                         "Keep identities in ROOT's context and detailed findings in the worker handoff.")
    result = {"schema": "orchestrator-lane-result/v1", "lane_id": lane,
              "worker_invocation_id": worker, "branch": identity.branch,
              "commit": identity.head_commit, **facts}

    def validate(value: dict[str, Any]) -> None:
        validate_coding_result(value, lane_id=lane, worker_invocation_id=worker,
                              declaration=declaration)

    validate(result)
    data = (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if len(data) > 1024 * 1024:
        raise ValueError("coding result exceeds the 1 MiB controller limit")
    workspace = run_root / ".agent-workspace"
    if workspace.resolve() != workspace:
        raise ValueError("result workspace must not redirect outside its declared worktree path")
    path = workspace / "RESULT.json"
    if path.is_symlink():
        raise ValueError("RESULT.json must be a regular lane-owned file")
    if not check_only:
        workspace.mkdir(exist_ok=True)
        # Same-directory replacement prevents consumers reading a partial JSON document.
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=workspace, prefix=".result-", suffix=".tmp",
                                             delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            validate(load_object(temporary))
            os.replace(temporary, path)
            temporary = None
            if path.read_bytes() != data:
                raise ValueError("published RESULT.json differs from validated bytes")
            validate(load_object(path))
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return {"state": "VALID", "published": not check_only, "path": str(path),
            "lane_id": lane, "worker_invocation_id": worker,
            "commit": identity.head_commit, "outcome": facts["outcome"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", type=Path, required=True,
                        help="ROOT-owned current invocation or external result context JSON")
    parser.add_argument("--facts", type=Path, required=True,
                        help="worker-authored JSON containing outcome, summary, checks")
    parser.add_argument("--check-only", action="store_true", help="validate without publication")
    args = parser.parse_args(argv)
    try:
        evidence = emit(args.context, args.facts, check_only=args.check_only)
    except (OSError, ValueError, GitSafetyError) as exc:
        print(json.dumps({"state": "INVALID", "error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(evidence))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
