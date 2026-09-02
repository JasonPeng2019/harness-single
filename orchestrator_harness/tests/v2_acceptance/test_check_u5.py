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
        self.assertFalse(result["no_real_side_effect"]["used_network_service"])
        self.assertEqual(["--readiness-only"], result["input_arguments"])
        self.assertEqual("readiness-fixture", result["identity_correlation"]["lane_id"])
        self.assertTrue(result["fake_target_binding"]["authorization"]["retained"])
        self.assertEqual("unit-2", result["checkpoint"]["resume_from_earliest_pending_unit"])
        self.assertTrue(result["checkpoint"]["atomic_write"])
        self.assertTrue(result["checkpoint"]["read_back_matches"])
        self.assertTrue(result["observer_ready_before_child_action"])
        self.assertTrue(result["abort"]["exact_identity_match"])
        self.assertTrue(result["abort"]["cooperative_exit"])
        self.assertEqual(0, result["child_actions"]["abort"]["exit_code"])
        self.assertIn("local-fake-child:abort", result["child_actions"]["abort"]["stderr"])
        self.assertEqual("unit-2", result["recovery_retry"]["resumed_unit"])
        self.assertEqual(0, result["recovery_retry"]["exit_code"])
        self.assertTrue(result["terminal_closure"]["abort_process_closed"])
        self.assertTrue(result["terminal_closure"]["recovery_process_closed"])
        self.assertTrue(result["terminal_closure"]["fake_resource_closed"])
        self.assertEqual(2, result["idempotent_cleanup"]["passes"])
        self.assertTrue(result["cleanup"]["artifacts_proven_gone"])

    def test_live_claims_are_not_satisfied_by_the_disposable_fake(self) -> None:
        self.assertEqual("synthetic/static", claim("CHECK-U5").evidence_class)
        for number in range(1, 5):
            self.assertEqual("live-only", claim(f"CHECK-LIVE-{number}").evidence_class)
