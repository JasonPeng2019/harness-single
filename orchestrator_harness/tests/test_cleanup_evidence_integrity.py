"""Unknown process observations cannot authorize lifecycle effects."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import lane_cleanup, launch, processes, resume, shutdown
from orchestrator_harness.models import ProcessQuery
from orchestrator_harness.records import atomic_write_json


class CleanupEvidenceIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.status_path = Path(self.temporary.name) / "status.json"
        self.identity = {"pid": 123456, "creation_time": "2026-10-11T06:00:00Z"}
        self.lane = {"lane_id": "worker", "run_id": "run", "lifecycle": "running",
                     "process": self.identity, "controller_status_path": str(self.status_path)}
        self.status = {"schema": "controller-status/v1", "lane_id": "worker", "run_id": "run",
                       "controller_identity": self.identity, "controller_state": "exited",
                       "provider_state": {"state": "not_started"}, "cleanup_proven": True}
        atomic_write_json(self.status_path, self.status)

    def unknown_process(self):
        return patch.object(processes, "targeted_process_query", return_value=ProcessQuery(False, None, ("access denied",)))

    def test_unknown_process_cannot_authorize_cleanup_or_resume_or_wait(self):
        with (patch.object(processes, "process_alive", return_value=False),
              patch.object(processes, "process_identity", return_value=None), self.unknown_process()):
            self.assertFalse(lane_cleanup.cleanup_is_proven(self.lane))
            self.assertFalse(shutdown._wait_for_controller_exit(self.lane, 0))
            self.assertFalse(launch._wait_for_controller_exit(self.lane, 0))
            with self.assertRaisesRegex(ValueError, "unobservable"):
                resume._live_controller(self.lane)
            self.assertFalse(launch._cleanup_lane_processes(self.lane)[0])

    def test_invalid_creation_identity_cannot_authorize_cleanup(self):
        self.identity["creation_time"] = "garbage"
        atomic_write_json(self.status_path, self.status)
        with (patch.object(processes, "process_alive", return_value=False),
              patch.object(processes, "process_identity", return_value=None)):
            self.assertFalse(lane_cleanup.cleanup_is_proven(self.lane))
            self.assertFalse(launch._cleanup_lane_processes(self.lane)[0])

    def test_started_provider_without_boundary_cannot_authorize_force_stop(self):
        self.status.update(cleanup_proven=False, provider_state={
            "state": "exited", "pid": 234567, "creation_time": self.identity["creation_time"]})
        atomic_write_json(self.status_path, self.status)
        with patch.object(processes, "terminate_process", return_value=True):
            self.assertFalse(launch._cleanup_lane_processes(self.lane)[0])

    def test_proven_controller_absence_preserves_no_provider_cleanup(self):
        with (patch.object(processes, "process_identity", return_value=None),
              patch.object(processes, "targeted_process_query", return_value=ProcessQuery(True, None))):
            self.assertTrue(lane_cleanup.cleanup_is_proven(self.lane))
            self.assertFalse(resume._live_controller(self.lane))
            self.assertTrue(shutdown._wait_for_controller_exit(self.lane, 0))

    def test_termination_cannot_report_unknown_pid_as_exited(self):
        with (patch.object(processes, "process_alive", return_value=False),
              patch.object(processes, "process_identity", return_value=None), self.unknown_process()):
            self.assertFalse(processes.terminate_process(**{
                "pid": self.identity["pid"], "creation_time": self.identity["creation_time"]}))

    def test_serialized_job_cannot_infer_absence_from_unknown_pid(self):
        record = {"kind": "windows-job", "root": self.identity, "processes": [self.identity]}
        with (patch.object(processes, "process_alive", return_value=False),
              patch.object(processes, "process_identity", return_value=None), self.unknown_process(),
              patch.object(processes.ProcessBoundary, "observe", return_value=False)):
            self.assertFalse(processes.process_boundary_is_gone(record))

    def test_force_stop_uses_exact_gone_job_proof_before_inventory_cleanup(self):
        boundary = {"kind": "windows-job", "root": self.identity, "processes": [self.identity]}
        self.status.update(process_boundary=boundary, cleanup_proven=False)
        atomic_write_json(self.status_path, self.status)
        with (patch.object(processes, "terminate_process", return_value=True),
              patch.object(processes, "identity_exit_proven", return_value=True),
              patch.object(processes, "process_boundary_is_gone", return_value=True),
              patch.object(processes, "cleanup_recorded_process_boundary", return_value=False) as cleanup):
            self.assertTrue(launch._cleanup_lane_processes(self.lane)[0])
        cleanup.assert_not_called()

    def test_job_exit_proof_waits_for_asynchronous_members_without_inventory(self):
        record = {"kind": "windows-job", "root": self.identity, "processes": [self.identity]}
        with (patch.object(processes.os, "name", "nt"),
              patch.object(processes, "identity_exit_proven", side_effect=[False, True]),
              patch.object(processes, "identity_matches", return_value=True),
              patch.object(processes, "process_snapshot", side_effect=AssertionError("exact exit needs no inventory"))):
            self.assertTrue(processes.process_boundary_is_gone(record, timeout_seconds=1))


if __name__ == "__main__":
    unittest.main()
