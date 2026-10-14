"""Regression coverage for caller-selected Ollama transport on Codex lanes."""
from __future__ import annotations

import json
import os
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch

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
        self.assertEqual(["exec", "--ignore-user-config"], forwarded[:2])
        self.assertNotIn("-m", forwarded)
        self.assertNotIn("--model", forwarded)
        self.assertIn('model_reasoning_effort="max"', forwarded)
        self.assertIn('service_tier="normal"', forwarded)
        self.assertIn("features.hooks=true", forwarded)
        self.assertIn("--json", forwarded)
        self.assertEqual("-", forwarded[-1])

    def test_resume_keeps_exact_session_and_transport(self):
        argv = self.argv(resume=True, session_id="native-session")
        self.assertEqual(
            ["exec", "--ignore-user-config", "resume", "native-session"],
            argv[7:11],
        )
        self.assertNotIn("--cd", argv)
        with self.assertRaisesRegex(ValueError, "session ID"):
            self.argv(resume=True)

    def test_direct_codex_uses_workspace_sandbox(self):
        argv = self.argv(launch_config={"reasoning_effort": "high", "service_tier": "normal"})
        worktree = str(Path("worker with spaces").resolve())
        self.assertEqual([
            binding._direct_codex_executable(), "exec", "--ignore-user-config",
            "-c", 'default_permissions="worker-isolated"',
            *(["-c", 'windows.sandbox="elevated"'] if os.name == "nt" else []),
            "--skip-git-repo-check", "-c", 'approval_policy="never"',
            "-m", "requested-model:cloud", "-c", 'model_reasoning_effort="high"',
            "-c", 'service_tier="normal"', "--dangerously-bypass-hook-trust",
            "-c", "features.hooks=true", "-c",
            f'projects={{ {json.dumps(worktree, ensure_ascii=False)} = {{ trust_level="trusted" }} }}',
            "--json", "--output-last-message",
            str(Path(worktree) / ".agent-workspace" / "last-message.txt"),
            "--cd", worktree, "-",
        ], argv)
        self.assertEqual(argv, self.argv(launch_config={
            "reasoning_effort": "high", "service_tier": "normal", "launcher": "codex",
        }))
        if os.name == "nt":
            self.assertEqual(Path(argv[0]).name.lower(), "codex.exe")
            self.assertTrue(Path(argv[0]).is_file())

    def test_invalid_transport_is_rejected(self):
        for launcher in ("", "other", None):
            with self.subTest(launcher=launcher), self.assertRaises(ValueError):
                self.argv(launch_config={"reasoning_effort": "max", "service_tier": "normal", "launcher": launcher})

    def test_project_trust_preserves_literal_dotted_unicode_worktree_key(self):
        worktree = str(Path("worker.with dots/analysis \U0001f600 space").resolve())
        argv = self.argv(
            worktree=worktree,
            launch_config={
                "reasoning_effort": "max", "service_tier": "default",
                "permission_mode": "yolo",
            },
        )
        overrides = [argv[i + 1] for i, value in enumerate(argv[:-1]) if value == "-c"]
        projects = [value.partition("=")[2] for value in overrides if value.partition("=")[0] == "projects"]
        self.assertEqual(1, len(projects), "Codex needs a projects table, not a dotted path override")
        decoded = tomllib.loads("projects=" + projects[0])["projects"]
        self.assertEqual({worktree: {"trust_level": "trusted"}}, decoded)

    def test_explicit_yolo_bypasses_sandbox_and_preserves_launch_preferences(self):
        with tempfile.TemporaryDirectory() as worktree, patch.object(
            binding, "_isolation_overrides", side_effect=AssertionError("unexpected sandbox setup")
        ):
            argv = self.argv(
                worktree=worktree,
                model="gpt-6.1-sol",
                launch_config={
                    "reasoning_effort": "max", "service_tier": "default",
                    "permission_mode": "yolo",
                },
            )
        self.assertIn("--yolo", argv)
        self.assertEqual("gpt-6.1-sol", argv[argv.index("-m") + 1])
        self.assertIn('model_reasoning_effort="max"', argv)
        self.assertIn('service_tier="default"', argv)
        self.assertIn("features.hooks=true", argv)
        self.assertFalse(any("worker-isolated" in value or "windows.sandbox" in value for value in argv))

    def test_explicit_yolo_is_preserved_on_resume(self):
        argv = self.argv(
            resume=True, session_id="native-session",
            launch_config={"reasoning_effort": "high", "service_tier": "default", "permission_mode": "yolo"},
        )
        self.assertEqual(["resume", "native-session"], argv[3:5])
        self.assertIn("--yolo", argv)
        self.assertNotIn("--cd", argv)

    def test_invalid_permission_mode_is_rejected(self):
        for mode in ("", "other", None, True):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "permission_mode"):
                self.argv(launch_config={"reasoning_effort": "max", "service_tier": "default", "permission_mode": mode})

    def test_catalog_and_registered_binding_are_identical(self):
        root = Path(__file__).resolve().parents[2]
        self.assertEqual((root / "adapters/codex/harness/launcher_binding.py").read_bytes(), Path(binding.__file__).read_bytes())

    def test_codex_event_and_failure_parsing_still_apply(self):
        self.assertEqual({"session_id": "native-session"}, binding.parse_line(json.dumps({"type": "thread.started", "thread_id": "native-session"})))
        self.assertTrue(binding.parse_line('{"type":"turn.failed"}')["non_retryable_failure"])
        self.assertIsNone(binding.parse_line("Launching Codex with Ollama"))


if __name__ == "__main__":
    unittest.main()
