#!/usr/bin/env python3
"""Codex ROOT Stop hook bootstrap."""
from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path
PROVIDER_ID = "codex"
BOUNDARY = "stop"
WORKER_BINDING_SCHEMA = "harness-hook-binding/v1"
WORKER_DECISIONS = frozenset({"ALLOW", "NOTICE", "REJECT"})

def _failure(reason: str) -> dict[str, object]:
    return {"decision": "block", "reason": reason}

def _worker_output() -> dict[str, object] | None:
    agent_workspace = Path(__file__).resolve().parents[2] / ".agent-workspace"
    binding_path = agent_workspace / "harness-hook-binding.json"
    try:
        binding = json.loads(binding_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(binding, dict) or binding.get("role") != "worker":
        return None
    if binding.get("schema") != WORKER_BINDING_SCHEMA:
        return _failure("worker hook binding schema mismatch")
    helper = agent_workspace / "hook-dispatch.py"
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(helper),
                "--boundary",
                BOUNDARY,
                "--agent-workspace",
                str(agent_workspace),
            ],
            capture_output=True,
            text=True,
        )
        decision = json.loads(completed.stdout)
    except Exception as exc:
        return _failure(f"worker hook dispatch failed: {exc}")
    if not isinstance(decision, dict) or decision.get("decision") not in WORKER_DECISIONS:
        return _failure("worker hook dispatch returned an invalid decision")
    if decision["decision"] == "ALLOW":
        return {}
    return _failure(str(decision.get("reason") or "rejected by harness"))

def main() -> int:
    worker_output = _worker_output()
    if worker_output is not None:
        print(json.dumps(worker_output, sort_keys=True))
        return 0
    binding_path = Path(__file__).resolve().parent.parent / "orchestrator-harness-binding.json"
    try:
        binding = json.loads(binding_path.read_text(encoding="utf-8-sig"))
        harness_root = binding.get("harness_root") if isinstance(binding, dict) else None
        if not isinstance(harness_root, str) or not harness_root or not Path(harness_root).is_absolute():
            raise ValueError(f"installed {PROVIDER_ID} hook has no bound harness_root")
        sys.path.insert(0, harness_root)
        from orchestrator_harness.root_hook_wrapper import run
        output = run(PROVIDER_ID, BOUNDARY, binding_path)
    except Exception as exc:
        output = _failure(str(exc))
    print(json.dumps(output, sort_keys=True))
    return 0

if __name__ == "__main__":
    sys.exit(main())
