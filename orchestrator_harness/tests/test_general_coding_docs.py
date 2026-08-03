from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness.config import load_config
from orchestrator_harness.lane_controller import load_invocation


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PRIMARY_DOCS = (
    REPOSITORY_ROOT / "README.md",
    REPOSITORY_ROOT / "QUICK_START.md",
    REPOSITORY_ROOT / "orchestrator_harness" / "README.md",
)


class GeneralCodingDocumentationTests(unittest.TestCase):
    def test_documented_harness_examples_parse(self) -> None:
        for relative_path in (
            "examples/harness.example.json",
            "orchestrator_harness/config.example.json",
        ):
            path = REPOSITORY_ROOT / relative_path
            self.assertTrue(path.is_file(), relative_path)
            config = load_config(path, harness_root=REPOSITORY_ROOT)
            self.assertEqual(("worktrees/*",), config.run_globs)
            self.assertEqual(".agent-workspace", config.workspace_relpath)

    def test_coding_examples_and_documented_commands_are_present(self) -> None:
        for relative_path in (
            "examples/coding.invocation.example.json",
            "examples/coding.result.example.json",
            "examples/coding.named-lock.example.json",
            "examples/disposable_coding_fixture.py",
            "orchestrator_harness/config.example.json",
        ):
            self.assertTrue((REPOSITORY_ROOT / relative_path).is_file(), relative_path)

        root_readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
        quick_start = (REPOSITORY_ROOT / "QUICK_START.md").read_text(encoding="utf-8")
        package_readme = (REPOSITORY_ROOT / "orchestrator_harness" / "README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("## Start here", root_readme)
        self.assertIn("QUICK_START.md", root_readme)
        self.assertIn("# Quick Start: Ordinary Coding", quick_start)
        self.assertIn("python -m orchestrator_harness.lane_controller", quick_start)
        self.assertIn("python -m orchestrator_harness --config", package_readme)

        invocation = json.loads(
            (REPOSITORY_ROOT / "examples/coding.invocation.example.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual("orchestrator-coding-invocation/v1", invocation["schema"])
        self.assertIn("exclusive_resources", invocation)
        self.assertNotIn("mcp_servers", invocation)
        self.assertNotIn("board_tokens", invocation)

    def test_legacy_firmware_compatibility_is_explicit_and_scoped(self) -> None:
        root_readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
        quick_start = (REPOSITORY_ROOT / "QUICK_START.md").read_text(encoding="utf-8")
        package_readme = (REPOSITORY_ROOT / "orchestrator_harness" / "README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("## Supported firmware compatibility path", root_readme)
        self.assertIn("separate compatibility path", root_readme)
        self.assertIn("not part of this quick start", quick_start)
        self.assertIn("## Legacy firmware suite use", package_readme)
        self.assertIn("schema-less legacy firmware", package_readme)
        for document in PRIMARY_DOCS:
            self.assertNotIn("MCP-Trial", document.read_text(encoding="utf-8"), document)

    def test_schema_less_firmware_invocation_still_parses(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_root = root / "run"
            workspace = run_root / ".agent-workspace"
            workspace.mkdir(parents=True)
            policy_root = root / ".agent-workspace"
            policy_root.mkdir()
            policy = "Firmware policy body.\n"
            policy_path = policy_root / "AUTONOMOUS_EXECUTION_POLICY.md"
            policy_path.write_text(policy, encoding="utf-8")
            policy_sha256 = hashlib.sha256(policy_path.read_bytes()).hexdigest()
            (policy_root / "AUTONOMOUS_EXECUTION_POLICY.sha256").write_text(
                f"{policy_sha256}  AUTONOMOUS_EXECUTION_POLICY.md\n", encoding="utf-8"
            )
            prompt = run_root / "prompt.md"
            prompt.write_text(
                "\n".join(
                    (
                        "## AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
                        f"Policy SHA-256: `{policy_sha256}`",
                        "## END AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
                        policy,
                        "## FINAL PRECEDENCE REMINDER",
                        f"Policy `{policy_sha256}` and the latest signed run amendment control.",
                    )
                ),
                encoding="utf-8",
            )
            raw = {
                "action": "start",
                "run_root": str(run_root),
                "prompt_path": str(prompt),
                "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
                "output_paths": {
                    "status": str(workspace / "legacy_controller.status.json"),
                    "jsonl": str(workspace / "legacy_codex.jsonl"),
                    "stderr": str(workspace / "legacy.stderr.log"),
                    "last_message": str(workspace / "legacy.last-message.txt"),
                },
                "label": "legacy",
                "doer": "firmware-worker",
                "task": "compatibility smoke",
                "phase": "test",
                "declared_lane_id": "firmware:legacy",
                "policy_sha256": policy_sha256,
                "server_snapshot": {},
                "model_settings": {
                    "model": "test-model",
                    "reasoning_effort": "low",
                    "service_tier": "priority",
                },
                "lane_event_log": str(
                    root / "multi-agent-logs" / "orchestrator-harness" / "LANE_EVENTS.jsonl"
                ),
            }
            invocation_path = workspace / "legacy.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            with patch(
                "orchestrator_harness.lane_controller.__file__",
                str(root / "package" / "lane_controller.py"),
            ):
                invocation = load_invocation(invocation_path)

        self.assertIsNone(invocation.invocation_schema)
        self.assertEqual("firmware:legacy", invocation.lane_id)
        self.assertEqual([], invocation.mcp_servers)


if __name__ == "__main__":
    unittest.main()
