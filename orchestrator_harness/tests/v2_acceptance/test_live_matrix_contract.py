from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import PLATFORM_CLAIMS
from examples.v2_live_matrix import AUTHORIZATION_ENV, CHECKS, MANAGED_ONLY, NATIVE_GAPS, execute, expected_cells


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
                for row in native_rows:
                    expected = "NOT_APPLICABLE" if row["applicable"] == "false" else gap
                    self.assertEqual(expected, row["outcome"])

        for platform in ("Windows", "macOS", "Linux"):
            for command in MANAGED_ONLY:
                with self.subTest(platform=platform, command=command):
                    managed_only_plain = [
                        row for row in rows
                        if row["platform"] == platform and row["command"] == command and row["profile"] == "plain"
                    ]
                    self.assertTrue(managed_only_plain)
                    self.assertTrue(all(row["applicable"] == "false" for row in managed_only_plain))
                    self.assertTrue(all(row["outcome"] == "NOT_APPLICABLE" for row in managed_only_plain))

    def test_same_input_failed_attempt_is_terminal_without_duplicate_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            counter = root / "counter.txt"
            evidence = {kind: str(root / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
            increment = (
                "from pathlib import Path; "
                f"counter = Path({str(counter)!r}); "
                "counter.write_text(str(int(counter.read_text()) + 1) if counter.exists() else '1')"
            )
            manifest = {
                "native_runner_identity": {"platform": "Windows"},
                "attempts": [{
                    "name": "CHECK-LIVE-1",
                    "target": "counter regression",
                    "provider": "codex",
                    "profile": "managed",
                    "platform": "Windows",
                    "command": [sys.executable, "-c", increment],
                    "cleanup_command": [sys.executable, "-c", "pass"],
                    "evidence": evidence,
                    "evidence_oracles": {kind: ["required token"] for kind in evidence},
                    "agent_expectations": [{"classification": "Observed"}],
                }],
            }
            checkpoint = root / "checkpoint.json"
            with patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}):
                first = execute(manifest, checkpoint)
                resumed = execute(manifest, checkpoint)

            self.assertEqual("1", counter.read_text(encoding="utf-8"))
            self.assertEqual("FAIL", first["outcome"])
            self.assertEqual("FAIL", resumed["outcome"])
            self.assertEqual(["CHECK-LIVE-1"], [row["name"] for row in resumed["checks"]])
            self.assertEqual("FAIL", resumed["checks"][0]["outcome"])
