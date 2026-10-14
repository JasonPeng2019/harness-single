"""Provider-neutral result Stop check for managed lanes (result/v1).

Uses the canonical staged validator for current assignment, task, branch/tip,
checks, diagnostics, hash and clean PASS source. Its local assignment mirror
is worker Stop evidence; ROOT independently uses protected native authority.
Exits 0 when the result is valid and 1 otherwise.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from typing import Any

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
    try:
        assignment = _read_json(agent_workspace / "result-assignment.json")
        if assignment.get("schema") != "worker-result-assignment/v1" or assignment.get("content_hash") != content_hash(assignment):
            raise ValueError("worker result assignment is missing or corrupt")
        if binding.get("role") != "worker" or any(not binding.get(key) or binding[key] != assignment.get(key) for key in ("lane_id", "run_id")):
            raise ValueError("worker result assignment differs from its current hook binding")
        worktree = agent_workspace.parent
        if Path(assignment["worktree_path"]) != worktree:
            raise ValueError("worker result assignment names a different worktree")
        if Path(binding.get("result_path", "")) != worktree / "RESULT.json":
            raise ValueError("worker RESULT path differs from its assigned location")
        validation = agent_workspace / "validation-runtime"
        if not (validation / "orchestrator_harness/result_validation.py").is_file():
            raise ValueError("staged canonical result validator is missing; prepare a fresh lane")
        # Providers may call this API in an interpreter that already imported
        # the installed harness. Load the staged package in its own namespace
        # rather than reusing or replacing that interpreter's operator modules.
        namespace = "worker_result_validation_" + uuid.uuid4().hex
        package = validation / "orchestrator_harness"
        spec = importlib.util.spec_from_file_location(namespace, package / "__init__.py",
                                                     submodule_search_locations=[str(package)])
        if spec is None or spec.loader is None:
            raise ValueError("staged result validator package is not importable")
        try:
            module = importlib.util.module_from_spec(spec)
            sys.modules[namespace] = module
            spec.loader.exec_module(module)
            result_validation = importlib.import_module(namespace + ".result_validation")
            if not Path(result_validation.__file__).is_relative_to(validation):
                raise ValueError("result validator was imported from a different runtime")
            return True, "result is valid", result_validation.read_result(assignment)
        finally:
            for name in list(sys.modules):
                if name == namespace or name.startswith(namespace + "."):
                    sys.modules.pop(name, None)
    except (OSError, ValueError, KeyError, TypeError, ImportError) as exc:
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
