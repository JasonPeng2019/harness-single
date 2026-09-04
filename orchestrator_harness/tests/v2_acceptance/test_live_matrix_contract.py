from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import PLATFORM_CLAIMS


ROOT = Path(__file__).resolve().parents[3]
MATRIX = ROOT / "examples" / "v2_live_matrix.py"


class LiveMatrixContractTests(unittest.TestCase):
    """Controls prove that M08 rehearsal cannot impersonate M09 native proof."""

    def test_platform_matrix_has_three_explicit_native_only_claims(self) -> None:
        self.assertEqual(
            {"CHECK-PLATFORM-WINDOWS", "CHECK-PLATFORM-MACOS", "CHECK-PLATFORM-LINUX"},
            {item.name for item in PLATFORM_CLAIMS},
        )
        for item in PLATFORM_CLAIMS:
            self.assertTrue(all((item.scenario, item.trigger, item.expected, item.oracle, item.cleanup)))

    def test_live_checks_remain_reserved_without_m09_authorization(self) -> None:
        completed = subprocess.run([sys.executable, str(MATRIX)], cwd=ROOT, text=True, capture_output=True, check=True)
        result = json.loads(completed.stdout)
        self.assertEqual("RESERVED_FOR_M09", result["outcome"])
        self.assertEqual(["CHECK-LIVE-1", "CHECK-LIVE-2", "CHECK-LIVE-3", "CHECK-LIVE-4"], [item["name"] for item in result["checks"]])
