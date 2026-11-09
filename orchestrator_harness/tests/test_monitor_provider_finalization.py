"""A provider exit does not finish its live controller's result validation."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import controller, monitor, processes
from orchestrator_harness.records import read_record


class ProviderFinalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.runtime = Path(temporary.name)
        identity = processes.process_identity(os.getpid())
        assert identity is not None
        self.identity = identity
        self.lane = {
            "lane_id": "finalizing",
            "run_id": "run-1",
            "lifecycle": "running",
            "process": identity,
            "controller_status_path": str(self.runtime / "controller.status.json"),
        }

    def publish(self, **fields: object) -> dict:
        # Exercise the same persisted status snapshot consumed by the monitor.
        controller._write_status(self.lane, fields)
        return read_record(
            Path(self.lane["controller_status_path"]),
            controller.CONTROLLER_STATUS_SCHEMA,
        )

    def derive(
        self,
        status: dict,
        orphaned_leases: list[dict[str, object]] | None = None,
    ) -> str | None:
        return monitor.derive_lane_status(
            self.runtime,
            "epoch-1",
            self.lane,
            status,
            lease_records=[],
            orphaned_leases=orphaned_leases,
        )

    def test_provisional_provider_exit_waits_for_live_controller_decision(self) -> None:
        for result_state in ("absent", "invalid"):
            for exit_code in (0, 1):
                with self.subTest(result_state=result_state, exit_code=exit_code):
                    status = self.publish(
                        provider_state={"state": "exited", "exit_code": exit_code},
                        result_state=result_state,
                        cleanup_proven=False,
                        recorded_status=None,
                    )
                    self.assertEqual(result_state, status["result_state"])
                    self.assertTrue(
                        processes.identity_matches(
                            self.identity["pid"], self.identity["creation_time"]
                        )
                    )
                    self.assertIsNone(self.derive(status))

    def test_explicit_invalid_result_remains_actionable(self) -> None:
        status = self.publish(
            provider_state={"state": "exited", "exit_code": 1},
            result_state="invalid",
            cleanup_proven=True,
            recorded_status="result_invalid",
        )
        self.assertEqual("result_invalid", self.derive(status))

    def test_explicit_provider_failure_remains_actionable(self) -> None:
        status = self.publish(
            provider_state={"state": "exited", "exit_code": 1},
            result_state="invalid",
            cleanup_proven=True,
            controller_state="exited",
            recorded_status="provider_exited_no_result",
        )
        self.assertEqual("provider_exited_no_result", self.derive(status))

    def test_dead_or_mismatched_controller_remains_actionable(self) -> None:
        status = self.publish(provider_state={"state": "exited", "exit_code": 0})
        with patch.object(monitor.processes, "identity_matches", return_value=False):
            self.assertEqual("controller_exited", self.derive(status))
        self.lane["process"] = {
            "pid": self.identity["pid"],
            "creation_time": str(self.identity["creation_time"]) + "-different",
        }
        self.assertTrue(
            processes.identity_matches(
                self.identity["pid"], self.identity["creation_time"]
            )
        )
        self.assertEqual("controller_exited", self.derive(status))

    def test_orphaned_lease_remains_actionable_during_finalization(self) -> None:
        status = self.publish(provider_state={"state": "exited", "exit_code": 0})
        self.assertEqual(
            "orphaned_lease",
            self.derive(status, orphaned_leases=[{"lane_id": "finalizing"}]),
        )

    def test_transcript_contradiction_remains_actionable(self) -> None:
        status = self.publish(
            provider_state={"state": "exited", "exit_code": 0},
            result_state="invalid",
            cleanup_proven=True,
            recorded_status="result_invalid",
        )
        self.assertEqual("status_transcript_contradiction", self.derive(status))

    def test_review_without_cleanup_proof_remains_actionable(self) -> None:
        status = self.publish(
            provider_state={"state": "exited", "exit_code": 0},
            result_state="valid",
            cleanup_proven=False,
            recorded_status="review_pending",
        )
        self.assertEqual("cleanup_unproven", self.derive(status))

    def test_finalized_review_remains_actionable(self) -> None:
        status = self.publish(
            provider_state={"state": "exited", "exit_code": 0},
            result_state="valid",
            cleanup_proven=True,
            recorded_status="review_pending",
        )
        self.assertEqual("review_pending", self.derive(status))


if __name__ == "__main__":
    unittest.main()
