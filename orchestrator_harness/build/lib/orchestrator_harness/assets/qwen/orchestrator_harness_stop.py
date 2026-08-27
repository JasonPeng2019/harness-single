#!/usr/bin/env python3
"""Installed Qwen Code Stop hook for the project binding."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from orchestrator_harness.qwen_installer import run_installed_qwen_hook


def main() -> int:
    raw = sys.stdin.read()
    payload = json.loads(raw) if raw.strip() else None
    result = run_installed_qwen_hook(Path.cwd(), boundary="stop", payload=payload)
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
