from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import PLATFORM_CLAIMS
from examples.v2_live_matrix import CHECKS, NATIVE_GAPS, expected_cells


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
        self.assertEqual([f"CHECK-LIVE-{number}" for number in range(1, 17)], [item["name"] for item in result["checks"]])
        self.assertEqual(set(CHECKS), {item["name"] for item in result["checks"]})

    def test_exhaustive_matrix_keeps_native_gaps_and_profile_reasons(self) -> None:
        cells = expected_cells()
        self.assertGreater(len(cells), 100, "matrix must not retain the obsolete four-attempt ceiling")
        self.assertTrue(all(cell["applicability_reason"] for cell in cells))
        completed = subprocess.run([sys.executable, str(MATRIX)], cwd=ROOT, text=True, capture_output=True, check=True)
        rows = json.loads(completed.stdout)["cells"]
        for platform, gap in NATIVE_GAPS.items():
            with self.subTest(platform=platform):
                native_rows = [row for row in rows if row["platform"] == platform]
                self.assertTrue(native_rows)
                self.assertTrue(all(row["outcome"] in {gap, "NOT_APPLICABLE"} for row in native_rows))
