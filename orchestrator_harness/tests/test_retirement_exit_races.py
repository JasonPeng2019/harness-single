"""Regression for retirement racing the accepted controller's final exit."""
from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import launch


class AcceptedRetirementExitRaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()

    def _spawn_short_controller(self, sleep_seconds: float = 0.35):
        child = launch.processes.spawn_detached(
            [sys.executable, "-c", f"import time; time.sleep({sleep_seconds})"],
            cwd=str(self.root),
        )
        identity = launch.processes.process_identity(child.pid)
        self.assertIsNotNone(identity)

        def stop_exact() -> None:
            assert identity is not None
            if launch.processes.identity_matches(
                identity["pid"], identity["creation_time"]
            ):
                launch.processes.terminate_process(
                    identity["pid"], identity["creation_time"], force=True
                )
            try:
                child.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5.0)

        self.addCleanup(stop_exact)
        return child, identity

    def test_retire_converges_accepted_controller_transition_before_proof(self) -> None:
        child, identity = self._spawn_short_controller()
        assert identity is not None
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "accepted",
            "worktree_path": str(self.root / "worktree"),
            "process": identity,
        }
        running = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "running",
            "recorded_status": "review_pending",
            "provider_state": {"state": "exited"},
            "process_boundary": {"root": {"pid": 42, "creation_time": "provider"}},
            "cleanup_proven": True,
        }
        exited = {**running, "controller_state": "exited"}
        reads = 0

        def read_status(_lane):
            nonlocal reads
            reads += 1
            return running if reads == 1 else exited

        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(
                    runtime_root=self.runtime, root_workspace=self.root
                ),
            ),
            patch.object(
                launch,
                "_validate_acceptance_ref",
                return_value={"lane_id": "lane-1", "approval": "ACCEPTED"},
            ),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(launch, "_read_controller_status", side_effect=read_status),
            patch.object(
                launch.processes, "process_boundary_is_gone", return_value=True
            ),
            patch.object(launch, "release_leases"),
            patch.object(launch, "update_lane"),
            patch.object(launch, "read_active_lanes", return_value=[]),
            patch.object(launch, "write_active_lanes"),
            patch.object(launch, "_prune_worktrees"),
            patch.object(launch, "_maybe_close_epoch"),
        ):
            result = launch.run_retire("acceptance.json")

        self.assertTrue(result["ok"], result)
        self.assertEqual("RETIRE_OK", result["code"])
        self.assertGreaterEqual(reads, 2)
        self.assertFalse(
            launch.processes.identity_matches(
                identity["pid"], identity["creation_time"]
            )
        )
        child.wait(timeout=5.0)

    def _run_retire(
        self,
        status_reader,
        *,
        boundary_gone=True,
        wait_seconds=None,
        poll_seconds=None,
        controller_sleep=0.35,
    ):
        child, identity = self._spawn_short_controller(controller_sleep)
        assert identity is not None
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "accepted",
            "worktree_path": str(self.root / "worktree"),
            "process": identity,
        }
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(launch, "find_harness_root", return_value=self.root)
            )
            stack.enter_context(
                patch.object(
                    launch,
                    "load_config",
                    return_value=SimpleNamespace(
                        runtime_root=self.runtime, root_workspace=self.root
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    launch,
                    "_validate_acceptance_ref",
                    return_value={"lane_id": "lane-1", "approval": "ACCEPTED"},
                )
            )
            stack.enter_context(
                patch.object(
                    launch, "find_active_lane", return_value=("epoch-1", lane)
                )
            )
            stack.enter_context(
                patch.object(
                    launch, "_read_controller_status", side_effect=status_reader
                )
            )
            stack.enter_context(
                patch.object(
                    launch.processes,
                    "process_boundary_is_gone",
                    return_value=boundary_gone,
                )
            )
            release_leases = stack.enter_context(
                patch.object(launch, "release_leases")
            )
            update_lane = stack.enter_context(patch.object(launch, "update_lane"))
            stack.enter_context(
                patch.object(launch, "read_active_lanes", return_value=[])
            )
            stack.enter_context(patch.object(launch, "write_active_lanes"))
            stack.enter_context(patch.object(launch, "_prune_worktrees"))
            stack.enter_context(patch.object(launch, "_maybe_close_epoch"))
            if wait_seconds is not None:
                stack.enter_context(
                    patch.object(
                        launch, "RETIRE_CONTROLLER_EXIT_WAIT_SECONDS", wait_seconds
                    )
                )
            if poll_seconds is not None:
                stack.enter_context(
                    patch.object(
                        launch, "RETIRE_CONTROLLER_EXIT_POLL_SECONDS", poll_seconds
                    )
                )
            result = launch.run_retire("acceptance.json")
        return result, identity, release_leases, update_lane

    def test_retire_fails_closed_when_controller_stays_live_through_bounded_reads(
        self,
    ) -> None:
        running = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "running",
            "recorded_status": "review_pending",
            "provider_state": {"state": "exited"},
            "process_boundary": {"root": {"pid": 42, "creation_time": "provider"}},
            "cleanup_proven": True,
        }
        reads = 0

        def read_status(_lane):
            nonlocal reads
            reads += 1
            return running

        result, identity, release_leases, update_lane = self._run_retire(
            read_status, wait_seconds=0.3, poll_seconds=0.05, controller_sleep=30
        )
        self.assertFalse(result["ok"], result)
        self.assertEqual("RETIRE_CLEANUP_UNPROVEN", result["code"])
        self.assertGreaterEqual(
            reads, 2, "status must be re-read while the live controller runs"
        )
        self.assertLessEqual(
            reads, 8, "the bounded re-read must stop at the deadline"
        )
        self.assertTrue(
            launch.processes.identity_matches(
                identity["pid"], identity["creation_time"]
            ),
            "a live exact controller must never be treated as gone",
        )
        release_leases.assert_not_called()
        update_lane.assert_not_called()

    def test_retire_fails_closed_when_exited_controller_never_disappears(
        self,
    ) -> None:
        exited = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "exited",
            "recorded_status": "review_pending",
            "provider_state": {"state": "exited"},
            "process_boundary": {"root": {"pid": 42, "creation_time": "provider"}},
            "cleanup_proven": True,
        }
        reads = 0

        def read_status(_lane):
            nonlocal reads
            reads += 1
            return exited

        result, identity, release_leases, update_lane = self._run_retire(
            read_status, wait_seconds=0.3, poll_seconds=0.05, controller_sleep=30
        )
        self.assertFalse(result["ok"], result)
        self.assertEqual("RETIRE_CLEANUP_UNPROVEN", result["code"])
        self.assertEqual(
            1, reads, "a terminal exited status must not trigger endless re-reads"
        )
        self.assertTrue(
            launch.processes.identity_matches(
                identity["pid"], identity["creation_time"]
            ),
            "an exact controller that outlives the bounded wait must fail closed",
        )
        release_leases.assert_not_called()
        update_lane.assert_not_called()

    def test_retire_fails_closed_when_final_status_lacks_cleanup_or_boundary_proof(
        self,
    ) -> None:
        running = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "running",
            "recorded_status": "review_pending",
            "provider_state": {"state": "exited"},
            "process_boundary": {"root": {"pid": 42, "creation_time": "provider"}},
            "cleanup_proven": True,
        }
        for label, final, boundary_gone in (
            (
                "cleanup-unproven",
                {**running, "controller_state": "exited", "cleanup_proven": False},
                True,
            ),
            (
                "boundary-live",
                {**running, "controller_state": "exited"},
                False,
            ),
        ):
            with self.subTest(label=label):
                reads = 0

                def read_status(_lane):
                    nonlocal reads
                    reads += 1
                    return running if reads == 1 else final

                result, identity, release_leases, update_lane = self._run_retire(
                    read_status, boundary_gone=boundary_gone
                )
                self.assertFalse(result["ok"], result)
                self.assertEqual("RETIRE_CLEANUP_UNPROVEN", result["code"])
                self.assertGreaterEqual(
                    reads, 2, "status must be re-read through the transition"
                )
                if final.get("cleanup_proven") is True:
                    self.assertFalse(
                        launch.processes.identity_matches(
                            identity["pid"], identity["creation_time"]
                        ),
                        "the exact controller must be allowed to exit naturally",
                    )
                release_leases.assert_not_called()
                update_lane.assert_not_called()


if __name__ == "__main__":
    unittest.main()
