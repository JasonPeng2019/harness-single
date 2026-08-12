"""Start one coding controller through the released public operator boundary."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SOURCE_ROOT))
prior_pythonpath = os.environ.get("PYTHONPATH")
os.environ["PYTHONPATH"] = str(SOURCE_ROOT) + (
    os.pathsep + prior_pythonpath if prior_pythonpath else ""
)

from orchestrator_harness.public_launch import launch_lane_controller


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch the native coding controller route")
    parser.add_argument("--invocation", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--cwd", required=True, type=Path)
    parser.add_argument("--status", required=True, type=Path)
    args = parser.parse_args()
    receipt = launch_lane_controller(
        args.invocation,
        receipt=args.receipt,
        cwd=args.cwd,
        label="real-agent-coding-controller",
        role="coding-lane-controller",
        expected_state_path=args.status,
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
