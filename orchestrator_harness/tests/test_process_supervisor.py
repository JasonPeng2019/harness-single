from __future__ import annotations

import subprocess
import unittest
from datetime import datetime, timezone

from orchestrator_harness.models import ProcessInfo, ProcessQuery
from orchestrator_harness.process_supervisor import ProcessSupervisor


class _Child:
    pid = 77

    def __init__(self, *, reap_after_kill: bool = True) -> None:
        self.reap_after_kill = reap_after_kill
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return None

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    def wait(self, timeout: float | None = None) -> int:
        if self.killed and self.reap_after_kill:
            return 137
        raise subprocess.TimeoutExpired("child", timeout or 0)


def identity() -> ProcessInfo:
    return ProcessInfo(
        77, 10, "child", "child", datetime(2026, 8, 10, tzinfo=timezone.utc)
    )


class ProcessSupervisorTests(unittest.TestCase):
    def test_graceful_stop_and_reap_are_typed(self) -> None:
        child = _Child()

        def observe(pid: int, **_: object) -> ProcessQuery:
            return ProcessQuery(True, identity())

        # A child that exits during the graceful wait is represented by a
        # small stateful wrapper rather than a broad process-name operation.
        original_wait = child.wait

        def graceful_wait(timeout: float | None = None) -> int:
            child.killed = True
            return original_wait(timeout)

        child.wait = graceful_wait  # type: ignore[method-assign]
        result = ProcessSupervisor(
            child, identity(), graceful_timeout_seconds=0, observer=observe
        ).cleanup()
        self.assertTrue(result.proved_reap)
        self.assertTrue(result.terminate_attempted)
        self.assertFalse(result.kill_attempted)
        self.assertEqual("graceful_stop", result.reaped_after)

    def test_force_stop_requires_final_reap(self) -> None:
        child = _Child()
        result = ProcessSupervisor(
            child, identity(), graceful_timeout_seconds=0, observer=None
        ).cleanup()
        self.assertTrue(result.proved_reap)
        self.assertTrue(result.terminate_attempted)
        self.assertTrue(result.kill_attempted)
        self.assertEqual("force_stop", result.reaped_after)

    def test_identity_uncertainty_does_not_signal_or_prove_release(self) -> None:
        child = _Child()

        def uncertain(pid: int, **_: object) -> ProcessQuery:
            return ProcessQuery(False, None, ("synthetic incomplete inventory",))

        result = ProcessSupervisor(child, identity(), observer=uncertain).cleanup()
        self.assertEqual("IDENTITY_UNCERTAIN", result.status)
        self.assertFalse(result.proved_reap)
        self.assertFalse(child.terminated)
        self.assertFalse(child.killed)


if __name__ == "__main__":
    unittest.main()
