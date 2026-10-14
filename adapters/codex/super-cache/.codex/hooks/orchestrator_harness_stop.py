#!/usr/bin/env python3
"""Codex worker Stop hook wrapper.

The super-cache worker payload is copied unchanged into an arbitrary worker
worktree, so this wrapper resolves the worker agent workspace relative to its
installed location, asks the shared hook-dispatch.py for a boundary decision,
and translates that decision into the Codex Stop hook output contract.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

BOUNDARY = "stop"
DECISIONS = frozenset({"ALLOW", "NOTICE", "REJECT"})


def agent_workspace() -> Path:
    return Path(__file__).resolve().parents[2] / ".agent-workspace"


def dispatch_decision() -> dict[str, Any]:
    helper = agent_workspace() / "hook-dispatch.py"
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(helper),
                "--boundary",
                BOUNDARY,
                "--agent-workspace",
                str(agent_workspace()),
            ],
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        return {"decision": "REJECT", "reason": f"hook-dispatch execution failed: {exc}"}
    if not completed.stdout or not completed.stdout.strip():
        return {"decision": "REJECT", "reason": f"hook-dispatch produced no output: {helper}"}
    try:
        decision = json.loads(completed.stdout)
    except (ValueError, TypeError) as exc:
        return {"decision": "REJECT", "reason": f"hook-dispatch output invalid: {exc}"}
    if not isinstance(decision, dict) or decision.get("decision") not in DECISIONS:
        return {"decision": "REJECT", "reason": "hook-dispatch output is not a valid decision"}
    return decision


def translate(decision: dict[str, Any]) -> dict[str, Any]:
    if decision.get("decision") == "ALLOW":
        return {}
    reason = decision.get("reason") or "rejected by harness"
    return {"decision": "block", "reason": str(reason)}


def main() -> int:
    print(json.dumps(translate(dispatch_decision()), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
