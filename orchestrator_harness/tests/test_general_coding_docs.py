from __future__ import annotations

import json
import unittest
from pathlib import Path
from orchestrator_harness.config import load_config


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
        package_readme = (
            REPOSITORY_ROOT / "orchestrator_harness" / "README.md"
        ).read_text(encoding="utf-8")
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

    def test_canonical_coding_contract_is_explicit_and_scoped(self) -> None:
        root_readme = (REPOSITORY_ROOT / "README.md").read_text(encoding="utf-8")
        quick_start = (REPOSITORY_ROOT / "QUICK_START.md").read_text(encoding="utf-8")
        package_readme = (
            REPOSITORY_ROOT / "orchestrator_harness" / "README.md"
        ).read_text(encoding="utf-8")
        self.assertIn("canonical coding invocation", root_readme)
        self.assertIn("canonical coding invocation", package_readme)
        self.assertIn("resume_thread_id", quick_start)
        for document in PRIMARY_DOCS:
            self.assertNotIn(
                "MCP-Trial", document.read_text(encoding="utf-8"), document
            )


if __name__ == "__main__":
    unittest.main()
