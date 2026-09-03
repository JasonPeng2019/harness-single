#!/usr/bin/env python3
"""Qwen Code worker PostToolUse hook wrapper.

The super-cache worker payload is copied unchanged into an arbitrary worker
worktree, so this wrapper resolves the worker agent workspace relative to its
installed location, asks the shared hook-dispatch.py for a boundary decision,
and translates that decision into the Qwen Code PostToolUse hook output
contract.  Qwen Code passes the hook input as JSON on stdin, so main() reads
stdin before emitting exactly one JSON object on stdout.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

BOUNDARY = "post-tool-use"
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
    if completed.returncode != 0:
        try:
            parsed = json.loads(completed.stdout)
        except (ValueError, TypeError):
            parsed = None
        if (
            isinstance(parsed, dict)
            and parsed.get("decision") == "REJECT"
            and parsed.get("reason")
        ):
            return parsed
        return {
            "decision": "REJECT",
            "reason": f"hook-dispatch exited nonzero: {completed.returncode}",
        }
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
    kind = decision.get("decision")
    if kind == "ALLOW":
        return {}
    if kind == "NOTICE":
        notice = decision.get("notice")
        ids = notice if isinstance(notice, list) else []
        listed = ", ".join(str(item) for item in ids)
        return {
            "hookSpecificOutput": {
                "hookEventName": "PostToolUse",
                "additionalContext": (
                    "Worker assignments pending resolution: " + listed + ". "
                    "Inspect and resolve the listed worker assignments before continuing."
                ),
            }
        }
    reason = decision.get("reason") or "rejected by harness"
    return {"continue": False, "stopReason": str(reason)}


def read_hook_input() -> dict[str, Any]:
    raw = sys.stdin.read()
    if not raw or not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def main() -> int:
    read_hook_input()
    print(json.dumps(translate(dispatch_decision()), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
