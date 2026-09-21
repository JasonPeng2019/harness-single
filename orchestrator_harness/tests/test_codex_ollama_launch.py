"""Regression coverage for caller-selected Ollama transport on Codex lanes."""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from orchestrator_harness.provider_adapters.codex import launcher_binding as binding


class CodexOllamaLaunchTests(unittest.TestCase):
    def argv(self, **overrides):
        options = dict(
            model="requested-model:cloud",
            launch_config={"reasoning_effort": "max", "service_tier": "normal", "launcher": "ollama"},
            worktree=str(Path("worker with spaces").resolve()),
            prompt_path="prompt.txt",
        )
        options.update(overrides)
        return binding.build_argv(**options)

    def test_ollama_owns_model_and_preserves_codex_flags(self):
        argv = self.argv()
        self.assertEqual(["ollama", "launch", "codex", "--model", "requested-model:cloud", "--yes", "--"], argv[:7])
        forwarded = argv[7:]
        self.assertEqual("exec", forwarded[0])
        self.assertNotIn("-m", forwarded)
        self.assertNotIn("--model", forwarded)
        self.assertIn('model_reasoning_effort="max"', forwarded)
        self.assertIn('service_tier="normal"', forwarded)
        self.assertIn("features.hooks=true", forwarded)
        self.assertIn("--json", forwarded)
        self.assertEqual("-", forwarded[-1])

    def test_resume_keeps_exact_session_and_transport(self):
        argv = self.argv(resume=True, session_id="native-session")
        self.assertEqual(["exec", "resume", "native-session"], argv[7:10])
        self.assertNotIn("--cd", argv)
        with self.assertRaisesRegex(ValueError, "session ID"):
            self.argv(resume=True)

    def test_direct_codex_is_unchanged(self):
        argv = self.argv(launch_config={"reasoning_effort": "high", "service_tier": "normal"})
        worktree = str(Path("worker with spaces").resolve())
        self.assertEqual([
            "codex", "exec", "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check", "-c", 'approval_policy="never"',
            "-m", "requested-model:cloud", "-c", 'model_reasoning_effort="high"',
            "-c", 'service_tier="normal"', "--dangerously-bypass-hook-trust",
            "-c", "features.hooks=true", "-c",
            f'projects.{json.dumps(worktree)}.trust_level="trusted"',
            "--json", "--output-last-message",
            str(Path(worktree) / ".agent-workspace" / "last-message.txt"),
            "--cd", worktree, "-",
        ], argv)
        self.assertEqual(argv, self.argv(launch_config={
            "reasoning_effort": "high", "service_tier": "normal", "launcher": "codex",
        }))

    def test_invalid_transport_is_rejected(self):
        for launcher in ("", "other", None):
            with self.subTest(launcher=launcher), self.assertRaises(ValueError):
                self.argv(launch_config={"reasoning_effort": "max", "service_tier": "normal", "launcher": launcher})

    def test_catalog_and_registered_binding_are_identical(self):
        root = Path(__file__).resolve().parents[2]
        self.assertEqual((root / "adapters/codex/harness/launcher_binding.py").read_bytes(), Path(binding.__file__).read_bytes())

    def test_codex_event_and_failure_parsing_still_apply(self):
        self.assertEqual({"session_id": "native-session"}, binding.parse_line(json.dumps({"type": "thread.started", "thread_id": "native-session"})))
        self.assertTrue(binding.parse_line('{"type":"turn.failed"}')["non_retryable_failure"])
        self.assertIsNone(binding.parse_line("Launching Codex with Ollama"))


if __name__ == "__main__":
    unittest.main()
