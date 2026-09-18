from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import PLATFORM_CLAIMS
from examples.v2_live_matrix import AUTHORIZATION_ENV, CHECKS, MANAGED_ONLY, NATIVE_GAPS, execute, expected_cells


ROOT = Path(__file__).resolve().parents[3]
MATRIX = ROOT / "examples" / "v2_live_matrix.py"
ENTRYPOINT = ROOT / ".agent-workspace" / "execute-matrix.py"


class LiveMatrixContractTests(unittest.TestCase):

    @staticmethod
    def _coordinate(name, provider, *, depends_on=(), input_hashes=None):
        """A deliberately disposable coordinate; the outer runner never receives a live argv here."""
        root = Path(tempfile.gettempdir()) / "a3-live-matrix-contract"
        evidence = {kind: str(root / name / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
        return {
            "name": name, "coordinate_id": name, "target": name, "provider": provider,
            "profile": "managed", "platform": "Windows", "command": [sys.executable, "-c", "pass"],
            "cleanup_command": [sys.executable, "-c", "pass"], "evidence": evidence,
            "evidence_oracles": {kind: ["required token"] for kind in evidence},
            "agent_expectations": [{"classification": "Observed"}], "depends_on": list(depends_on),
            "input_hashes": input_hashes or {"fixture": name}, "budget_seconds": 1,
        }

    def test_coordinate_scheduler_overlaps_only_different_provider_homes(self) -> None:
        """Two providers may overlap; a shared provider home remains exclusive."""
        manifest = {"native_runner_identity": {"platform": "Windows"}, "attempts": [
            self._coordinate("CHECK-LIVE-1", "codex"),
            self._coordinate("CHECK-LIVE-2", "qwen-code"),
            self._coordinate("CHECK-LIVE-3", "codex"),
        ]}
        calls = []

        def fake_run(argv, *, timeout_seconds=None):
            calls.append((argv[-1], time.monotonic()))
            time.sleep(.12)
            return {"argv": argv, "returncode": 1, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}), patch.object(sys.modules[execute.__module__], "_run", side_effect=fake_run):
            started = time.monotonic()
            result = execute(manifest, Path(temporary) / "checkpoint.json")
            elapsed = time.monotonic() - started
        self.assertEqual(6, len(calls))
        self.assertLess(elapsed, .58, "different providers should overlap under the two-coordinate cap")
        self.assertGreaterEqual(calls[4][1] - calls[0][1], .20, "same-provider coordinates must not overlap")
        self.assertEqual(["CHECK-LIVE-1", "CHECK-LIVE-2", "CHECK-LIVE-3"], [row["name"] for row in result["checks"]])

    def test_dependency_failure_blocks_only_its_dependent_and_keeps_collecting(self) -> None:
        manifest = {"native_runner_identity": {"platform": "Windows"}, "attempts": [
            self._coordinate("CHECK-LIVE-1", "codex"),
            self._coordinate("CHECK-LIVE-2", "qwen-code", depends_on=("CHECK-LIVE-1",)),
            self._coordinate("CHECK-LIVE-3", "claude-code"),
        ]}
        calls = []

        def fake_run(argv, *, timeout_seconds=None):
            calls.append(argv)
            return {"argv": argv, "returncode": 1, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}), patch.object(sys.modules[execute.__module__], "_run", side_effect=fake_run):
            result = execute(manifest, Path(temporary) / "checkpoint.json")
        rows = {row["name"]: row for row in result["checks"]}
        self.assertEqual("BLOCKED_DEPENDENCY", rows["CHECK-LIVE-2"]["outcome"])
        self.assertEqual(4, len(calls), "the failed prerequisite and independent coordinate each receive command plus cleanup")

    def test_resume_reuses_matching_coordinate_and_invalidates_changed_consumed_input(self) -> None:
        first = {"native_runner_identity": {"platform": "Windows"}, "attempts": [
            self._coordinate("CHECK-LIVE-1", "codex", input_hashes={"fixture": "a"}),
            self._coordinate("CHECK-LIVE-2", "qwen-code", input_hashes={"fixture": "b"}),
        ]}
        changed = {**first, "attempts": [first["attempts"][0], {**first["attempts"][1], "input_hashes": {"fixture": "changed"}}]}
        calls = []

        def fake_run(argv, *, timeout_seconds=None):
            calls.append(argv)
            return {"argv": argv, "returncode": 1, "stdout": "", "stderr": ""}

        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {AUTHORIZATION_ENV: "M09"}), patch.object(sys.modules[execute.__module__], "_run", side_effect=fake_run):
            checkpoint = Path(temporary) / "checkpoint.json"
            execute(first, checkpoint)
            calls.clear()
            resumed = execute(changed, checkpoint)
        self.assertEqual(2, len(calls), "only the changed coordinate receives command plus cleanup")
        self.assertEqual(["CHECK-LIVE-1", "CHECK-LIVE-2"], [row["name"] for row in resumed["checks"]])

    def test_parameterized_entrypoint_collects_same_check_across_two_provider_manifests(self) -> None:
        """Use the real outer entrypoint with controlled commands, never a historical wrapper."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifests, checkpoints = [], []
            for provider in ("codex", "qwen-code"):
                evidence = {kind: str(root / provider / f"{kind}.txt") for kind in ("transcript", "hook", "state", "cleanup")}
                manifest = {
                    "schema": "harness-v2-live-matrix/v2", "native_runner_identity": {"platform": "Windows", "provider": provider},
                    "coverage_cells": expected_cells(), "attempts": [{
                        "name": "CHECK-LIVE-1", "coordinate_key": f"Windows/{provider}/managed/CHECK-LIVE-1",
                        "target": "controlled-fake", "provider": provider, "profile": "managed", "platform": "Windows",
                        "command": [sys.executable, "-c", "pass"], "cleanup_command": [sys.executable, "-c", "pass"],
                        "evidence": evidence, "evidence_oracles": {kind: ["required token"] for kind in evidence},
                        "agent_expectations": [{"classification": "Observed"}], "depends_on": [], "exclusive_resources": [], "budget_seconds": 1,
                    }],
                }
                manifest_path, checkpoint_path = root / f"{provider}.json", root / f"{provider}.checkpoint.json"
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                checkpoint_path.write_text(json.dumps({}), encoding="utf-8")
                manifests.append(manifest_path)
                checkpoints.append(checkpoint_path)
            command = [sys.executable, str(ENTRYPOINT), *sum((["--manifest", str(path)] for path in manifests), []), *sum((["--checkpoint", str(path)] for path in checkpoints), [])]
            completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, env={**os.environ, AUTHORIZATION_ENV: "M09"})
        self.assertEqual(1, completed.returncode, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual({"Windows/codex/managed/CHECK-LIVE-1", "Windows/qwen-code/managed/CHECK-LIVE-1"}, {row["coordinate_key"] for row in result["checks"]})
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
