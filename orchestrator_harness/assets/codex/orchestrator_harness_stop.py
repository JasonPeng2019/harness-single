#!/usr/bin/env python3
"""Synchronous Codex Stop finalization backstop hook."""

from __future__ import annotations

import json
import sys

from orchestrator_harness.codex_adapter import run_codex_hook


def main() -> int:
    raw = sys.stdin.read()
    payload = json.loads(raw) if raw.strip() else None
    result = run_codex_hook("stop", payload)
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
