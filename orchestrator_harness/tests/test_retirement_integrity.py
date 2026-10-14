"""Retirement requires current acceptance, exact exit and unchanged ownership."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import launch, processes
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import lane_record_dir, open_epoch, write_active_lanes
from orchestrator_harness.lanes import read_lane, write_lane
from orchestrator_harness.models import ProcessQuery
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.result_fixture import LinkedResultFixture


class RetirementIntegrityTests(LinkedResultFixture, unittest.TestCase):
    def setUp(self):
        LinkedResultFixture.setUp(self)
        self.identity = {"pid": 123456, "creation_time": "2026-10-11T06:00:00Z"}
        self.lane = write_lane(self.rt, self.epoch, "worker", {
            **self.lane, "lifecycle": "accepted", "process": self.identity})
        write_active_lanes(self.rt, self.epoch, [{"lane_id": "worker", "run_id": self.lane["run_id"]}])
        atomic_write_json(Path(self.lane["controller_status_path"]), {
            "schema": "controller-status/v1", "lane_id": "worker", "run_id": self.lane["run_id"],
            "controller_state": "exited", "controller_identity": self.identity,
            "provider_state": {"state": "not_started"}, "cleanup_proven": True})
        self.folder = lane_record_dir(self.rt, self.epoch, "worker")
        self.acceptance_path = self.folder / "ORCHESTRATOR_ACCEPTANCE.json"
        self.write_chain(self.lane["run_id"])

    def write_chain(self, run):
        linked = {"lane_id": "worker", "run_id": run, "task_card_id": "card",
                  "task_card_hash": self.lane["task_card_hash"], "result_id": self.lane["run_id"],
                  "result_hash": self.record_hash(), "commit": self.record_tip(),
                  "source_assignment_hash": self.lane["result_assignment_hash"]}
        review = {"schema": "completion-review/v1", **linked, "review_outcome": "PASS"}
        review["content_hash"] = content_hash(review)
        acceptance = {"schema": "orchestrator-acceptance/v1", **linked, "approval": "ACCEPTED",
                      "accepted_by": "ROOT", "review_ref": review["content_hash"]}
        acceptance["content_hash"] = content_hash(acceptance)
        atomic_write_json(self.folder / "COMPLETION_REVIEW.json", review)
        atomic_write_json(self.acceptance_path, acceptance)

    def record_hash(self):
        import json
        return json.loads((self.worker / "RESULT.json").read_text())["content_hash"]

    def record_tip(self):
        import json
        return json.loads((self.worker / "RESULT.json").read_text())["commit"]

    def retire(self, **query):
        with (patch.object(launch, "find_harness_root", return_value=self.root),
              patch.object(launch, "load_config", return_value=self.config),
              patch.object(launch, "_prune_worktrees"),
              patch.object(processes, "process_identity", return_value=None),
              patch.object(processes, "process_alive", return_value=False),
              patch.object(processes, "targeted_process_query", return_value=ProcessQuery(**{
                  "complete": True, "process": None, **query}))):
            return launch.run_retire(str(self.acceptance_path))

    def test_current_chain_and_proven_no_provider_exit_can_retire(self):
        result = self.retire()
        self.assertTrue(result["ok"], result)
        self.assertEqual("retired", read_lane(self.rt, self.epoch, "worker")["lifecycle"])

    def test_historical_acceptance_cannot_retire_current_run(self):
        self.write_chain("older")
        with patch.object(launch, "release_leases") as release:
            result = self.retire()
        self.assertEqual(launch.RETIRE_ACCEPTANCE_INVALID, result["code"], result)
        self.assertEqual("accepted", read_lane(self.rt, self.epoch, "worker")["lifecycle"])
        release.assert_not_called()

    def test_missing_or_corrupt_review_cannot_authorize_retirement(self):
        (self.folder / "COMPLETION_REVIEW.json").write_text("{invalid", encoding="utf-8")
        self.assertEqual(launch.RETIRE_ACCEPTANCE_INVALID, self.retire()["code"])

    def test_unknown_process_evidence_preserves_lane_and_lease(self):
        with patch.object(launch, "release_leases") as release:
            result = self.retire(complete=False)
        self.assertEqual(launch.RETIRE_CLEANUP_UNPROVEN, result["code"], result)
        self.assertEqual("accepted", read_lane(self.rt, self.epoch, "worker")["lifecycle"])
        release.assert_not_called()

    def test_wait_interleaving_cannot_retire_newer_run(self):
        def replace_owner(*_):
            write_lane(self.rt, self.epoch, "worker", {**self.lane, "run_id": "new-owner"})
            return True
        with (patch.object(launch, "_wait_for_controller_exit", side_effect=replace_owner) as wait,
              patch.object(launch, "release_leases") as release,
              patch.object(processes, "identity_exit_proven", return_value=False),
              patch.object(processes, "identity_matches", return_value=True)):
            result = self.retire()
        self.assertFalse(result["ok"], result)
        wait.assert_called_once()
        release.assert_not_called()
        self.assertEqual("new-owner", read_lane(self.rt, self.epoch, "worker")["run_id"])


if __name__ == "__main__":
    unittest.main()
