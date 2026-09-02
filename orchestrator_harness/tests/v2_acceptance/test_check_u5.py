from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import claim


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "examples" / "v2_disposable_fixture.py"


class CheckU5Tests(unittest.TestCase):
    """CHECK-U5 — REQ-015/016 portability and honest-evidence oracle."""

    def test_readiness_only_rehearses_local_control_boundaries_and_cleans_up(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(FIXTURE), "--readiness-only"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        result = json.loads(completed.stdout)
        self.assertEqual("synthetic/readiness-only", result["evidence_class"])
        self.assertEqual("RESERVED_FOR_M09", result["live_claims"])
        self.assertEqual("CONFIRMED_LOCAL_FAKE_ONLY", result["no_real_side_effect"]["status"])
        for value in result["no_real_side_effect"].values():
            self.assertNotEqual(True, value)
        self.assertEqual(["--readiness-only"], result["input_arguments"])
        self.assertEqual("readiness-fixture", result["identity_correlation"]["lane_id"])
        binding = result["fake_target_binding"]
        self.assertTrue(binding["authorization"]["retained"])
        self.assertIsNone(binding["authorization"]["credential"])
        self.assertEqual("unit-2", result["checkpoint"]["resume_from_earliest_pending_unit"])
        self.assertTrue(result["checkpoint"]["failed_attempt_atomic_write"])
        self.assertTrue(result["checkpoint"]["failed_attempt_read_back_matches"])
        self.assertTrue(result["checkpoint"]["final_read_back_matches"])

        aborted = result["child_actions"]["abort"]
        recovered = result["child_actions"]["recovery"]
        requested = result["abort"]["requested_process_identity"]
        observed = result["abort"]["observed_process_identity"]
        self.assertTrue(result["abort"]["still_running_when_observed"])
        self.assertEqual(requested["pid"], observed["pid"])
        self.assertEqual(requested["identity_token"], observed["identity_token"])
        self.assertTrue(observed["creation_time_utc"])
        self.assertFalse(result["abort"]["completion_action_sent"])
        self.assertEqual([], result["abort"]["action_complete_records"])
        self.assertNotEqual(0, result["abort"]["forced_non_success_exit"])
        self.assertNotEqual(0, aborted["exit_code"])
        self.assertLessEqual(aborted["observer_ready_at"], aborted["termination_requested_at"])
        self.assertLessEqual(aborted["termination_requested_at"], aborted["ended_at"])
        self.assertEqual([], aborted["stdout_tail"])
        self.assertTrue(aborted["terminal_process_closed"])

        failed_attempt = result["checkpoint"]["failed_attempt"]
        self.assertEqual(aborted, failed_attempt)
        consumed = result["recovery_retry"]["consumed_checkpoint"]
        self.assertEqual("FAILED_ABORTED", consumed["status"])
        self.assertEqual(["unit-2", "unit-3"], consumed["pending_units"])
        self.assertEqual([failed_attempt], consumed["attempts"])
        self.assertTrue(result["checkpoint"]["retry_consumed_failed_checkpoint"])
        self.assertEqual(1, aborted["attempt"])
        self.assertEqual(2, recovered["attempt"])
        self.assertEqual("unit-2", recovered["unit"])
        self.assertEqual("unit-2", result["recovery_retry"]["resumed_unit"])
        self.assertNotEqual(
            aborted["observed_process_identity"]["identity_token"],
            recovered["observed_process_identity"]["identity_token"],
        )
        self.assertNotEqual(
            aborted["observed_process_identity"]["pid"],
            recovered["observed_process_identity"]["pid"],
        )
        self.assertEqual(aborted["observed_process_identity"], result["identity_correlation"]["abort_process_identity"])
        self.assertEqual(recovered["observed_process_identity"], result["identity_correlation"]["recovery_process_identity"])
        for attempt in (aborted, recovered):
            self.assertEqual(attempt["requested_process_identity"]["pid"], attempt["observed_process_identity"]["pid"])
            self.assertEqual(attempt["requested_process_identity"]["identity_token"], attempt["observed_process_identity"]["identity_token"])
            self.assertLessEqual(attempt["started_at"], attempt["observer_ready_at"])
            self.assertLessEqual(attempt["observer_ready_at"], attempt["ended_at"])
            self.assertTrue(attempt["command"])
        self.assertEqual(0, recovered["exit_code"])
        self.assertEqual(0, result["recovery_retry"]["exit_code"])
        self.assertLessEqual(recovered["observer_ready_at"], recovered["action_dispatched_at"])
        self.assertLessEqual(recovered["action_dispatched_at"], recovered["ended_at"])
        self.assertIn("local-fake-child:recover", recovered["stderr"])
        self.assertEqual(1, len(recovered["stdout_tail"]))
        action_record = recovered["stdout_tail"][0]
        self.assertEqual("action-complete", action_record["event"])
        self.assertEqual("recover", action_record["action"])
        self.assertEqual("unit-2", action_record["unit"])
        self.assertEqual(recovered["observed_process_identity"]["identity_token"], action_record["identity_token"])
        self.assertEqual(binding["target_root"], result["recovery_retry"]["observed_binding"]["target_root"])
        self.assertEqual(binding["authorization"], result["recovery_retry"]["observed_binding"]["authorization"])
        self.assertEqual(binding["target_root"], result["recovery_retry"]["observed_binding"]["cwd"])
        self.assertEqual(result["recovery_retry"]["observed_binding"], action_record["binding"])
        self.assertEqual(result["recovery_retry"]["observed_binding"], recovered["observer_record"]["binding"])
        self.assertIn(binding["target_root"], recovered["command"])
        self.assertIn(json.dumps(binding["authorization"], sort_keys=True), recovered["command"])
        self.assertTrue(result["terminal_closure"]["abort_process_closed"])
        self.assertTrue(result["terminal_closure"]["recovery_process_closed"])
        self.assertTrue(result["terminal_closure"]["fake_resource_closed"])
        self.assertEqual(2, result["idempotent_cleanup"]["passes"])
        self.assertTrue(result["cleanup"]["artifacts_proven_gone"])

    def test_live_claims_are_not_satisfied_by_the_disposable_fake(self) -> None:
        self.assertEqual("synthetic/static", claim("CHECK-U5").evidence_class)
        for number in range(1, 5):
            self.assertEqual("live-only", claim(f"CHECK-LIVE-{number}").evidence_class)
