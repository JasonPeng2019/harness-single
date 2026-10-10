"""Inspect sandbox readiness without running any launcher, helper, or repair."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestrator_harness.windows_sandbox_preflight import require_ready


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument("--workspace", type=Path)
    args = parser.parse_args()
    try:
        result = require_ready(args.harness_dir.resolve(), workspace=args.workspace)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps({"status": "PASS" if result is not None else "NOT_APPLICABLE", "sandbox_health": result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
