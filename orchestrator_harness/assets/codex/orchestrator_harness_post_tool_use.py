#!/usr/bin/env python3
"""Installed synchronous Codex PostToolUse hook for the harness binding."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from orchestrator_harness.codex_adapter import run_installed_codex_hook


def main() -> int:
    raw = sys.stdin.read()
    payload = json.loads(raw) if raw.strip() else None
    result = run_installed_codex_hook(Path.cwd(), boundary="post_tool_use", payload=payload)
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
