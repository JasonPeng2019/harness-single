from __future__ import annotations

"""Release-surface regression tests for the fresh S3 operator journey."""

import json
import unittest
from pathlib import Path

from orchestrator_harness.discovery import discover_suite
from orchestrator_harness.reconcile import reconcile
from orchestrator_harness.tests.support import NOW, SuiteFixture, write_json

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class S3OperatorJourneyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def _dual_route_statuses(self) -> tuple[Path, Path]:
        coding = self.fixture.status(
            run="coding",
            label="coding",
            doer="Coding",
            task="C01",
            declared_lane_id="coding:operator-example",
        )
        coding_value = json.loads(coding.read_text(encoding="utf-8"))
        coding_value.update(
            {
                "invocation_schema": "orchestrator-coding-invocation/v1",
                "worker_invocation_id": "coding-operator-001",
                "resources": ["service:coding-example"],
                "exclusive_resources": ["service:coding-example"],
            }
        )
        write_json(coding, coding_value)
        firmware = self.fixture.status(
            run="firmware",
            label="firmware",
            doer="Firmware",
            task="F01",
            controller_pid=201,
            codex_pid=202,
            board_tokens=["STM-A"],
            mcp_servers=["byo-firmware-stm-a"],
            declared_lane_id="firmware:legacy-example",
        )
        return coding, firmware

    def _dual_process_snapshot(self):
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        return type(base)(
            True,
            tuple(base.processes)
            + (
                info(201, 1, "python", "firmware-controller", NOW),
                info(202, 201, "codex", "firmware-codex", NOW),
            ),
            (),
            "fake",
        )

    def test_S3_DUAL_001_legacy_example_is_schema_less_and_coding_v1_is_separate(
        self,
    ) -> None:
        legacy = json.loads(
            (
                REPOSITORY_ROOT / "examples" / "legacy-firmware.invocation.example.json"
            ).read_text(encoding="utf-8")
        )
        coding = json.loads(
            (REPOSITORY_ROOT / "examples" / "coding.invocation.example.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertNotIn("schema", legacy)
        self.assertIn("policy_sha256", legacy)
        self.assertIn("server_snapshot", legacy)
        self.assertNotIn("repository", legacy)
        self.assertEqual("orchestrator-coding-invocation/v1", coding["schema"])
        self.assertIn("repository", coding)
        self.assertIn("worker_invocation_id", coding)
        self.assertNotIn("policy_sha256", coding)

    def test_S3_DUAL_002_disposable_dual_routes_keep_events_and_resources_isolated(
        self,
    ) -> None:
        coding, firmware = self._dual_route_statuses()
        observed = reconcile(
            discover_suite(self.fixture.config),
            self._dual_process_snapshot(),
            self.fixture.config,
            now=NOW,
        )
        lanes = {lane["lane_id"]: lane for lane in observed["lanes"]}
        self.assertEqual(
            ["service:coding-example"], lanes["coding:operator-example"]["resources"]
        )
        self.assertEqual(
            ["board:stm-a", "mcp-name:byo-firmware-stm-a"],
            lanes["firmware:legacy-example"]["resources"],
        )
        self.assertEqual([], observed["resource_conflicts"])
        self.assertNotEqual(coding.parent, firmware.parent)

        for signal_id, lane_id, workspace in (
            ("coding-signal", "coding:operator-example", coding.parent),
            ("firmware-signal", "firmware:legacy-example", firmware.parent),
        ):
            write_json(
                workspace / "manager-signals" / f"{signal_id}.json",
                {
                    "schema": "manager-signal/v1",
                    "signal_id": signal_id,
                    "kind": "HELP",
                    "created_utc": "2026-07-30T12:00:00Z",
                    "lane_id": lane_id,
                    "task": "operator journey",
                    "phase": "synthetic",
                    "summary": signal_id,
                    "evidence_paths": [],
                },
            )
        signals = {
            signal.value["signal_id"]: signal.value
            for run in discover_suite(self.fixture.config)
            for signal in run.manager_signals
        }
        self.assertEqual("coding:operator-example", signals["coding-signal"]["lane_id"])
        self.assertEqual(
            "firmware:legacy-example", signals["firmware-signal"]["lane_id"]
        )

    def test_S3_DUAL_003_docs_roles_cleanup_registry_templates_and_safeguard_are_bound(
        self,
    ) -> None:
        quick_start = (REPOSITORY_ROOT / "QUICK_START.md").read_text(encoding="utf-8")
        root_readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
        recipe = (
            REPOSITORY_ROOT / "examples" / "dual-path-manager.example.md"
        ).read_text(encoding="utf-8")
        safeguard = (
            REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        ).read_text(encoding="utf-8")

        for text in (quick_start, recipe):
            self.assertIn("scan --no-write", text)
            self.assertIn("watch --until-actionable", text)
            self.assertIn("top-level-event-id", text)
        for required in (
            "resume_thread_id",
            "PID-plus-creation",
            "FirmwareCampaignPack",
            "FirmwareHardwareAdapter",
        ):
            self.assertIn(required, quick_start)
        self.assertIn("FirmwareCampaignPack", root_readme)
        self.assertIn("FirmwareHardwareAdapter", root_readme)
        # Generic public facts: no external passed-tests registry, no C3 roles,
        # and no defaulted candidate branch in the safeguard.
        for text in (quick_start, root_readme):
            self.assertNotIn("passed-tests.json", text)
            self.assertNotIn("C3-HARNESS", text)
            self.assertNotIn("F.C3.O", text)
        self.assertIn("orchestrator_harness.release_checks", safeguard)
        self.assertIn("--expected-tip", safeguard)
        self.assertNotIn("C:/Users/", safeguard)
        self.assertIn("ExpectedBranch", safeguard)
        self.assertIn("Mandatory", safeguard)
        self.assertNotIn("firmware/v2-candidate", safeguard)
        self.assertIn("if (-not $Run)", safeguard)
        for template in (
            "FINAL_REVIEW.md",
            "ACCEPTANCE_WATCHER.md",
            "TOPOLOGY_AUDIT.md",
            "CRITERIA_AUDIT.md",
            "PROTECTED_STATE.md",
            "PROMOTION.md",
            "COMPLETION.md",
        ):
            self.assertTrue(
                (REPOSITORY_ROOT / "release_evidence_templates" / template).is_file(),
                template,
            )


if __name__ == "__main__":
    unittest.main()
