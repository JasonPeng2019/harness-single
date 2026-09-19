"""Test-local child for the STEP-006 real-CLI correction observation.

The parent must first use the public setup and bootstrap commands.  This child
then runs the real controller for that prepared lane while replacing only the
result validator: the first validation is reported as invalid, and every later
validation delegates to the product implementation.  Provider launch, adapter
argument construction, session parsing/resume, cleanup, leases, status, events,
and acceptance waiting all remain the real product paths.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Callable
from unittest.mock import patch

from orchestrator_harness import controller, processes
from orchestrator_harness.records import atomic_write_json


class InvalidOnceThenDelegate:
    """Return one injected invalid result, then call the real validator."""

    def __init__(
        self,
        delegate: Callable[[dict[str, Any]], tuple[str, dict[str, Any] | None]],
    ) -> None:
        self._delegate = delegate
        self.validation_calls = 0
        self.injected_invalid_count = 0
        self.delegated_count = 0

    def __call__(
        self, lane: dict[str, Any]
    ) -> tuple[str, dict[str, Any] | None]:
        self.validation_calls += 1
        if self.injected_invalid_count == 0:
            self.injected_invalid_count = 1
            return "invalid", None
        self.delegated_count += 1
        return self._delegate(lane)


def _write_child_result(
    path: Path,
    *,
    lane_id: str,
    controller_exit_code: int | None,
    validator: InvalidOnceThenDelegate,
    error: str | None,
) -> None:
    atomic_write_json(
        path,
        {
            "schema": "step006-real-cli-correction-child/v1",
            "lane_id": lane_id,
            "child_identity": processes.process_identity(os.getpid()),
            "controller_exit_code": controller_exit_code,
            "validation_calls": validator.validation_calls,
            "injected_invalid_count": validator.injected_invalid_count,
            "delegated_count": validator.delegated_count,
            "error": error,
        },
    )


def run(lane_id: str, evidence_path: Path) -> int:
    validator = InvalidOnceThenDelegate(controller._validate_result)
    controller_exit_code: int | None = None
    try:
        with patch.object(controller, "_validate_result", side_effect=validator):
            controller_exit_code = controller.run_controller(lane_id)
    except BaseException as exc:
        _write_child_result(
            evidence_path,
            lane_id=lane_id,
            controller_exit_code=controller_exit_code,
            validator=validator,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise
    _write_child_result(
        evidence_path,
        lane_id=lane_id,
        controller_exit_code=controller_exit_code,
        validator=validator,
        error=None,
    )
    return controller_exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one STEP-006 controller child with one invalid validation."
    )
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--evidence-path", required=True, type=Path)
    args = parser.parse_args(argv)
    return run(args.lane_id, args.evidence_path)


if __name__ == "__main__":
    raise SystemExit(main())
