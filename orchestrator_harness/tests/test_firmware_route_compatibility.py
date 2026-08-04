from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.discovery import discover_run
from orchestrator_harness.tests.support import SuiteFixture, TemporaryGitRepository, write_json


class FirmwareRouteCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run_root = self.root / "firmware-run"
        self.workspace = self.run_root / ".agent-workspace"
        self.workspace.mkdir(parents=True)
        self.policy_root = self.root / ".agent-workspace"
        self.policy_root.mkdir()
        self.policy = self.policy_root / "AUTONOMOUS_EXECUTION_POLICY.md"
        self.policy.write_text("Retained firmware policy.\n", encoding="utf-8")
        self.policy_sha256 = hashlib.sha256(self.policy.read_bytes()).hexdigest()
        (self.policy_root / "AUTONOMOUS_EXECUTION_POLICY.sha256").write_text(
            f"{self.policy_sha256}  AUTONOMOUS_EXECUTION_POLICY.md\n", encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _firmware_invocation(self) -> tuple[Path, dict[str, object]]:
        prompt = self.run_root / "bound-prompt.md"
        prompt.write_text(
            "\n".join(
                (
                    "## AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
                    f"Policy SHA-256: `{self.policy_sha256}`",
                    "## END AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
                    self.policy.read_text(encoding="utf-8"),
                    "Firmware task body.",
                    "## FINAL PRECEDENCE REMINDER",
                    f"Policy `{self.policy_sha256}` and the latest signed run amendment control.",
                )
            ),
            encoding="utf-8",
        )
        label = "stm_a_bringup"
        value: dict[str, object] = {
            "action": "resume",
            "run_root": str(self.run_root),
            "prompt_path": str(prompt),
            "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
            "output_paths": {
                "status": str(self.workspace / f"{label}_controller.status.json"),
                "jsonl": str(self.workspace / f"{label}_codex.jsonl"),
                "stderr": str(self.workspace / f"{label}_codex.stderr.log"),
                "last_message": str(self.workspace / f"{label}_last_message.txt"),
            },
            "label": label,
            "doer": "Firmware",
            "task": "S1 retained route characterization",
            "phase": "implementation",
            "declared_lane_id": "firmware:stm-a:bringup",
            "leases": ["board:stm-a", "serial:stm-a"],
            "board_tokens": ["STM-A"],
            "mcp_servers": ["byo-firmware-stm-a"],
            "server_snapshot": {"commit": "f003f84", "profile": "stm32l476rg"},
            "policy_sha256": self.policy_sha256,
            "model_settings": {
                "model": "gpt-5.6-terra",
                "reasoning_effort": "medium",
                "service_tier": "priority",
            },
            "codex_command": ["codex"],
            "config_overrides": ["model_reasoning_effort=\"medium\""],
            "resume_thread_id": "retained-firmware-thread",
            "lane_event_log": str(
                self.root / "multi-agent-logs" / "orchestrator-harness" / "s1" / "LANE_EVENTS.jsonl"
            ),
        }
        path = self.workspace / "retained-firmware.invocation.json"
        write_json(path, value)
        return path, value

    def test_retained_schema_less_policy_bound_shape_preserves_firmware_contract(self) -> None:
        path, raw = self._firmware_invocation()
        with patch.object(controller, "__file__", str(self.root / "package" / "lane_controller.py")):
            parsed = controller.load_invocation(path)

        self.assertIsNone(parsed.invocation_schema)
        self.assertEqual("firmware:stm-a:bringup", parsed.lane_id)
        self.assertEqual(["board:stm-a", "serial:stm-a"], parsed.leases)
        self.assertEqual(["STM-A"], parsed.board_tokens)
        self.assertEqual(["byo-firmware-stm-a"], parsed.mcp_servers)
        self.assertEqual({"commit": "f003f84", "profile": "stm32l476rg"}, parsed.server_snapshot)
        self.assertEqual(self.policy, parsed.policy_path)
        self.assertEqual(self.policy_sha256, parsed.policy_sha256)
        self.assertEqual("stm_a_bringup", parsed.label)
        self.assertEqual("gpt-5.6-terra", parsed.model)
        self.assertEqual("medium", parsed.reasoning_effort)
        self.assertEqual("priority", parsed.service_tier)
        self.assertEqual(["codex"], parsed.codex_command)
        self.assertEqual(["model_reasoning_effort=\"medium\""], parsed.config_overrides)
        self.assertEqual("retained-firmware-thread", parsed.requested_thread_id)
        self.assertEqual(self.workspace / "stm_a_bringup_controller.status.json", parsed.status_path)
        self.assertEqual(self.workspace / "stm_a_bringup_codex.jsonl", parsed.jsonl_path)
        self.assertEqual(
            self.root / "multi-agent-logs" / "orchestrator-harness" / "s1" / "LANE_EVENTS.jsonl",
            parsed.event_log,
        )
        self.assertEqual("danger-full-access", parsed.sandbox)
        self.assertEqual("never", parsed.approval_policy)
        self.assertNotIn("schema", raw)

    def test_valid_coding_v1_loader_contract_remains_separate(self) -> None:
        coding_root = self.root / "coding-run"
        repository = TemporaryGitRepository.create(coding_root)
        workspace = coding_root / ".agent-workspace"
        workspace.mkdir()
        runtime_root = self.root / "runtime"
        runtime_root.mkdir()
        prompt = coding_root / "prompt.md"
        prompt.write_text("Coding task.\n", encoding="utf-8")
        value: dict[str, object] = {
            "schema": controller.CODING_INVOCATION_SCHEMA,
            "action": "start",
            "run_root": str(coding_root),
            "runtime_root": str(runtime_root),
            "event_log_path": str(runtime_root / "events" / "LANE_EVENTS.jsonl"),
            "worker_invocation_id": "coding-001",
            "lane_id": "coding:parser",
            "task": "Preserved V1 coding route",
            "phase": "implementation",
            "prompt_path": str(prompt),
            "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
            "output_paths": {
                "status": str(workspace / "controller.status.json"),
                "jsonl": str(workspace / "codex.jsonl"),
                "stderr": str(workspace / "codex.stderr.log"),
                "last_message": str(workspace / "last-message.txt"),
            },
            "exclusive_resources": ["service:parser"],
            "repository": repository.declaration(),
            "codex_settings": {
                "model": "gpt-5.6-terra",
                "reasoning_effort": "medium",
                "service_tier": "priority",
                "command": ["codex"],
                "config_overrides": [],
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        path = workspace / "coding.invocation.json"
        write_json(path, value)

        parsed = controller.load_invocation(path)

        self.assertEqual(controller.CODING_INVOCATION_SCHEMA, parsed.invocation_schema)
        self.assertEqual("coding-001", parsed.worker_invocation_id)
        self.assertEqual("coding:parser", parsed.lane_id)
        self.assertEqual(["service:parser"], parsed.resources)
        self.assertIsNotNone(parsed.repository)
        self.assertEqual(coding_root, parsed.repository.worktree_root if parsed.repository else None)
        self.assertEqual(repository.branch, parsed.repository.branch if parsed.repository else None)
        self.assertEqual("workspace-write", parsed.sandbox)
        self.assertIsNone(parsed.policy_path)
        self.assertEqual([], parsed.board_tokens)
        self.assertEqual([], parsed.mcp_servers)

    def test_opposite_route_results_are_rejected_by_discovery(self) -> None:
        fixture = SuiteFixture.create()
        try:
            coding_workspace = fixture.workspace("coding")
            write_json(
                coding_workspace / "coding_controller.status.json",
                {
                    "schema": "orchestrator-lane-controller/v1",
                    "invocation_schema": controller.CODING_INVOCATION_SCHEMA,
                    "declared_lane_id": "coding:one",
                    "worker_invocation_id": "coding-001",
                },
            )
            write_json(coding_workspace / "RESULT.json", {"status": "PASS"})
            coding = discover_run(coding_workspace.parent, coding_workspace, fixture.config)
            self.assertIsNone(coding.result)
            self.assertEqual("CODING_RESULT_INVALID", coding.errors[0].code)

            firmware_workspace = fixture.workspace("firmware")
            write_json(
                firmware_workspace / "firmware_controller.status.json",
                {"state": "exited", "controller_pid": 1, "codex_pid": 2},
            )
            write_json(
                firmware_workspace / "RESULT.json",
                {
                    "schema": "orchestrator-lane-result/v1",
                    "lane_id": "coding:one",
                    "worker_invocation_id": "coding-001",
                },
            )
            firmware = discover_run(firmware_workspace.parent, firmware_workspace, fixture.config)
            self.assertIsNone(firmware.result)
            self.assertEqual("CODING_RESULT_INVALID", firmware.errors[0].code)
        finally:
            fixture.close()


if __name__ == "__main__":
    unittest.main()
