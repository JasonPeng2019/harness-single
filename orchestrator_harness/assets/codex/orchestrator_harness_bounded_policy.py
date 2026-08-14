#!/usr/bin/env python3
"""Installed Codex PreToolUse hook enforcing the provider-owned bounded policy.

The hook applies the same editable launcher/exclusion policy to every command
launched inside this worktree.  It never decides, schedules, or supervises:
it is a mechanical mistake guard that fails closed on invalid policy and lets
the bounded supervisor own runtime deadlines.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from orchestrator_harness.codex_bounded_policy import guard_pre_tool_use


def main() -> int:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    project_root = Path(__file__).resolve().parents[2]
    result = guard_pre_tool_use(payload, policy_root=project_root)
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
