from __future__ import annotations

"""Thin public composition for launching one lane controller.

The operator launcher owns detached-process identity and receipt publication;
the lane controller owns invocation validation, provider launch, lifecycle,
claims, results, and cleanup.  This module only binds the two existing public
boundaries for examples and deterministic host-only journeys.
"""

import os
import sys
from pathlib import Path
from typing import Any

from .operator_launch import launch_process


def _default_controller_python() -> tuple[str, dict[str, str] | None]:
    """Return a non-redirecting Python command with the caller's import path.

    Windows virtual-environment ``python.exe`` launchers can redirect to the
    base interpreter in a second process. A detached receipt for the launcher
    would then identify the wrong process. Launch the detected base interpreter
    directly and carry forward the active import path so the receipt and
    controller status describe one exact process.
    """

    if os.name != "nt" or sys.prefix == sys.base_prefix:
        return sys.executable, None
    candidate = Path(sys.base_prefix) / Path(sys.executable).name
    try:
        executable = candidate.resolve(strict=True)
        current = Path(sys.executable).resolve(strict=True)
    except OSError:
        return sys.executable, None
    if executable == current or not executable.is_file():
        return sys.executable, None
    environment = dict(os.environ)
    import_paths = [value for value in sys.path if isinstance(value, str) and value]
    inherited = environment.get("PYTHONPATH")
    if inherited:
        import_paths.extend(value for value in inherited.split(os.pathsep) if value)
    environment["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(import_paths))
    return str(executable), environment


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
    if python_executable is None:
        executable, environment = _default_controller_python()
    else:
        executable, environment = python_executable, None
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
        environment=environment,
    )


__all__ = ["launch_lane_controller"]
