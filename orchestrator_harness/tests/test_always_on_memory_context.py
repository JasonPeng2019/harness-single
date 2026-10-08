from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import bootstrap


class AlwaysOnMemoryContextTests(unittest.TestCase):
    def test_worker_memory_uses_assignment_and_resume_rationale(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            worktree = Path(raw) / "huffman"
            (worktree / ".agent-workspace").mkdir(parents=True)
            with (
                patch.dict("os.environ", {
                    "MEMORY_HARNESS_WORKER_RECALL": "1",
                    "MEMORY_HARNESS_ALWAYS_CONTEXT_PATH": str(Path(raw) / "absent.md"),
                }),
                patch("memory_harness.worker_recall.recall_worker_context", return_value="Prior Huffman defect") as recall,
                patch.object(bootstrap, "append_current_detail"),
            ):
                prompt = bootstrap._write_worker_prompt(
                    worktree, {"task": "Implement Huffman literals"},
                    managed=False, rationale="Fix table reuse",
                )
            recall.assert_called_once_with(
                "Implement Huffman literals\n\nResume rationale: Fix table reuse",
                lane_id="huffman",
            )
            self.assertIn("Prior Huffman defect", prompt.read_text(encoding="utf-8"))

    def test_ordinary_worker_prompt_receives_recalled_context(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            worktree = root / "lane"
            (worktree / ".agent-workspace").mkdir(parents=True)
            context = root / "recall.md"
            context.write_text("Atlas recalled the prior validated approach.", encoding="utf-8")
            with patch.dict(
                "os.environ",
                {"MEMORY_HARNESS_ALWAYS_CONTEXT_PATH": str(context), "MEMORY_HARNESS_WORKER_RECALL": "0"},
                clear=False,
            ):
                prompt_path = bootstrap._write_worker_prompt(
                    worktree,
                    {"task": "Implement the decoder"},
                    managed=False,
                )
            prompt = prompt_path.read_text(encoding="utf-8")
            self.assertIn("Implement the decoder", prompt)
            self.assertIn("Recalled run memory", prompt)
            self.assertIn("Atlas recalled the prior validated approach.", prompt)

    def test_enabled_policy_fails_closed_when_context_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            missing = Path(raw) / "missing.md"
            with patch.dict(
                "os.environ",
                {"MEMORY_HARNESS_ALWAYS_CONTEXT_PATH": str(missing), "MEMORY_HARNESS_WORKER_RECALL": "0"},
                clear=False,
            ):
                with self.assertRaisesRegex(ValueError, "unreadable"):
                    bootstrap._render_always_on_memory_context()

    def test_worker_recall_failure_does_not_write_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            worktree = Path(raw) / "lane-01"
            (worktree / ".agent-workspace").mkdir(parents=True)
            with (
                patch.dict("os.environ", {"MEMORY_HARNESS_WORKER_RECALL": "1"}),
                patch("memory_harness.worker_recall.recall_worker_context", side_effect=RuntimeError("backend unavailable")),
            ):
                with self.assertRaisesRegex(RuntimeError, "backend unavailable"):
                    bootstrap._write_worker_prompt(worktree, {"task": "Implement literals"}, managed=False)
            self.assertFalse((worktree / ".agent-workspace" / "worker-prompt.md").exists())


if __name__ == "__main__":
    unittest.main()
