from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.invocation import adapt_coding_v1
import orchestrator_harness.lane_controller as controller
from orchestrator_harness.tests.support import TemporaryGitRepository, write_json


class CodingRouteCompatibilityTests(unittest.TestCase):
    def test_coding_v1_identity_record_remains_legacy_compatible(self) -> None:
        prompt_sha256 = "a" * 64
        adapted = adapt_coding_v1(
            {
                "schema": controller.CODING_INVOCATION_SCHEMA,
                "action": "start",
                "run_root": "C:/runs/coding",
                "runtime_root": "C:/runtime/coding",
                "event_log_path": "C:/runtime/coding/events.jsonl",
                "worker_invocation_id": "coding-identity-001",
                "lane_id": "coding:identity",
                "task": "Identity regression task",
                "phase": "implementation",
                "prompt_path": "C:/runs/coding/prompt.md",
                "prompt_sha256": prompt_sha256,
                "output_paths": {
                    "status": "C:/runs/coding/status.json",
                    "jsonl": "C:/runs/coding/codex.jsonl",
                    "stderr": "C:/runs/coding/codex.stderr.log",
                    "last_message": "C:/runs/coding/last-message.txt",
                },
                "exclusive_resources": ["service:identity"],
                "codex_settings": {"model": "gpt-5.6-terra"},
            }
        )

        self.assertEqual("legacy-coding-v1:coding:identity", adapted.profile["id"])
        self.assertEqual("legacy-coding-v1", adapted.cohort_id)
        self.assertEqual("coding-v1", adapted.workflow_id)
        self.assertEqual("1", adapted.workflow_version)
        self.assertEqual("Identity regression task", adapted.task_card_id)
        self.assertEqual("legacy", adapted.task_card_revision)
        self.assertEqual("legacy-prompt", adapted.prompt_bundle["components"][0]["id"])
        self.assertEqual("coding-v1", adapted.prompt_bundle["workflow_id"])
        self.assertEqual(
            "legacy-coding-v1:coding:identity", adapted.prompt_bundle["profile_id"]
        )

    def test_valid_coding_v1_loader_contract_remains_provider_neutral(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            coding_root = root / "coding-run"
            repository = TemporaryGitRepository.create(coding_root)
            workspace = coding_root / ".agent-workspace"
            workspace.mkdir()
            runtime_root = root / "runtime"
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
        self.assertEqual(
            coding_root, parsed.repository.worktree_root if parsed.repository else None
        )
        self.assertEqual(
            repository.branch, parsed.repository.branch if parsed.repository else None
        )
        self.assertEqual("workspace-write", parsed.sandbox)

    def test_obsolete_old_shaped_input_uses_ordinary_unsupported_schema_validation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "obsolete.invocation.json"
            old_shape = {
                "action": "start",
                "policy_sha256": "0" * 64,
                "leases": ["service:old"],
                "board_tokens": ["board:old"],
                "mcp_servers": ["provider:old"],
                "server_snapshot": {"head": "old"},
            }
            write_json(path, old_shape)
            with self.assertRaisesRegex(
                controller.InvocationError, "unsupported invocation schema"
            ):
                controller.load_invocation(path)

            write_json(
                path,
                {
                    **old_shape,
                    "schema": "orchestrator-worker-invocation/v1",
                },
            )
            with self.assertRaisesRegex(
                controller.InvocationError, "policy_sha256"
            ):
                controller.load_invocation(path)


if __name__ == "__main__":
    unittest.main()
