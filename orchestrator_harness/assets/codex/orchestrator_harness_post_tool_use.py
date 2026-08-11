#!/usr/bin/env python3
"""Synchronous Codex PostToolUse boundary hook for a trusted project layer."""

from __future__ import annotations

import json
import sys

from orchestrator_harness.codex_adapter import run_codex_hook


def main() -> int:
    raw = sys.stdin.read()
    payload = json.loads(raw) if raw.strip() else None
    result = run_codex_hook("post_tool_use", payload)
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
