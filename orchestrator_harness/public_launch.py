from __future__ import annotations

"""Thin public composition for launching one lane controller.

The operator launcher owns detached-process identity and receipt publication;
the lane controller owns invocation validation, provider launch, lifecycle,
claims, results, and cleanup.  This module only binds the two existing public
boundaries for examples and deterministic host-only journeys.
"""

import sys
from pathlib import Path
from typing import Any

from .operator_launch import launch_process


def launch_lane_controller(
    invocation: str | Path,
    *,
    receipt: str | Path,
    cwd: str | Path,
    python_executable: str | None = None,
    label: str = "lane-controller",
    role: str = "coding-lane-controller",
    expected_state_path: str | Path | None = None,
) -> dict[str, Any]:
    """Launch the supported lane-controller CLI through the operator boundary."""

    invocation_path = Path(invocation).expanduser().resolve(strict=True)
    if not invocation_path.is_file():
        raise ValueError(f"invocation must be a regular file: {invocation_path}")
    executable = python_executable or sys.executable
    return launch_process(
        receipt=receipt,
        label=label,
        role=role,
        cwd=cwd,
        argv=(
            executable,
            "-m",
            "orchestrator_harness.lane_controller",
            str(invocation_path),
        ),
        expected_state_path=expected_state_path,
    )


__all__ = ["launch_lane_controller"]
