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
from orchestrator_harness.provider_adapters import usage_observation


class CodexOllamaLaunchTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.catalog = Path(temporary.name) / "ollama-model-catalog.json"
        self.catalog.write_text(json.dumps({"models": [{
            "slug": "requested-model:cloud",
            "supported_reasoning_levels": [{"effort": "max"}, {"effort": "high"}],
        }]}), encoding="utf-8")
        catalog_path = patch.object(binding, "_ollama_catalog_path", return_value=self.catalog)
        catalog_path.start()
        self.addCleanup(catalog_path.stop)

    def argv(self, **overrides):
        options = dict(
            model="requested-model:cloud",
            launch_config={"reasoning_effort": "max", "service_tier": "normal", "launcher": "ollama"},
            worktree=str(Path("worker with spaces").resolve()),
            prompt_path="prompt.txt",
        )
        options.update(overrides)
        return binding.build_argv(**options)

    def test_ollama_uses_native_oss_transport_and_preserves_codex_flags(self):
        argv = self.argv()
        self.assertEqual([
            binding._direct_codex_executable(), "exec", "--oss", "--local-provider", "ollama",
            "--ignore-user-config",
        ], argv[:6])
        self.assertEqual("requested-model:cloud", argv[argv.index("-m") + 1])
        self.assertIn('model_reasoning_effort="max"', argv)
        self.assertIn('service_tier="normal"', argv)
        self.assertIn('default_permissions="worker-isolated"', argv)
        self.assertIn("features.hooks=true", argv)
        self.assertIn("--json", argv)
        self.assertIn("model_catalog_json=" + json.dumps(str(self.catalog.resolve())), argv)
        self.assertEqual("-", argv[-1])

    def test_resume_keeps_exact_session_and_transport(self):
        argv = self.argv(resume=True, session_id="native-session")
        self.assertEqual(
            ["exec", "--oss", "--local-provider", "ollama", "--ignore-user-config", "resume", "native-session"],
            argv[1:8],
        )
        self.assertNotIn("--cd", argv)
        with self.assertRaisesRegex(ValueError, "session ID"):
            self.argv(resume=True)

    def test_ollama_omits_server_web_search_on_launch_and_resume(self):
        for resume in (False, True):
            with self.subTest(resume=resume):
                argv = self.argv(resume=resume, session_id="native-session" if resume else None)
                overrides = [argv[i + 1] for i, value in enumerate(argv[:-1]) if value == "-c"]
                config = tomllib.loads("\n".join(overrides))
                self.assertEqual("disabled", config["web_search"])
                self.assertEqual("max", config["model_reasoning_effort"])
                self.assertEqual("normal", config["service_tier"])
                self.assertEqual(str(self.catalog.resolve()), config["model_catalog_json"])
                self.assertEqual("worker-isolated", config["default_permissions"])
                self.assertEqual("never", config["approval_policy"])
                self.assertTrue(config["features"]["hooks"])
                if resume:
                    self.assertEqual("native-session", argv[argv.index("resume") + 1])

    def test_ollama_requires_explicit_matching_model_and_effort_catalog(self):
        self.catalog.unlink()
        with self.assertRaisesRegex(ValueError, "operator-owned Ollama model catalog"):
            self.argv()
        self.catalog.write_text('{"models": []}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "exactly one entry"):
            self.argv()
        self.catalog.write_text(json.dumps({"models": [{
            "slug": "requested-model:cloud",
            "supported_reasoning_levels": [{"effort": "low"}],
        }]}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "does not advertise reasoning effort max"):
            self.argv()

    def test_direct_codex_does_not_read_ollama_catalog(self):
        self.catalog.unlink()
        argv = self.argv(launch_config={"reasoning_effort": "high", "service_tier": "default"})
        self.assertNotIn("--oss", argv)
        self.assertFalse(any(value.startswith("model_catalog_json=") for value in argv))
        self.assertFalse(any(value.startswith("web_search=") for value in argv))

    def test_ollama_preserves_protected_isolation_config_on_launch_and_resume(self):
        config = self.catalog.parent / "protected-isolation.toml"
        codex_home = str(Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")).resolve())
        filesystem = {codex_home: "deny", str(self.catalog.parent / "worker"): "write"}
        config.write_text(
            '[permissions.worker-isolated]\nextends=":workspace"\nfilesystem='
            + binding._toml_inline(filesystem), encoding="utf-8",
        )
        for resume in (False, True):
            with self.subTest(resume=resume):
                argv = self.argv(
                    resume=resume, session_id="saved-session" if resume else None,
                    isolation_config_path=str(config),
                    launch_config={"launcher": "ollama", "reasoning_effort": "high", "service_tier": "default"},
                )
                self.assertIn("--oss", argv)
                self.assertIn('service_tier="default"', argv)
                self.assertIn("permissions.worker-isolated.filesystem=" + binding._toml_inline(filesystem), argv)
                self.assertNotIn("--yolo", argv)
                self.assertEqual(resume, "resume" in argv)

    def test_ollama_preserves_explicit_yolo_and_preferences_on_resume(self):
        argv = self.argv(resume=True, session_id="saved-session", launch_config={
            "launcher": "ollama", "permission_mode": "yolo",
            "reasoning_effort": "high", "service_tier": "default",
        })
        self.assertIn("--oss", argv)
        self.assertIn("--yolo", argv)
        self.assertIn('service_tier="default"', argv)
        self.assertFalse(any("worker-isolated" in value or "windows.sandbox" in value for value in argv))

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

    def test_native_final_text_survives_terminal_usage_event(self):
        final = "Committed the change.\n\nChecks passed. \U0001f600\n"
        usage = {"input_tokens": 181363, "cached_input_tokens": 166144, "output_tokens": 2882}
        events = [
            {"type": "thread.started", "thread_id": "native-session"},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "Working."}},
            {"type": "item.completed", "item": {"type": "command_execution", "aggregated_output": "tool text"}},
            {"type": "item.completed", "item": {"type": "agent_message", "text": final}},
            {"type": "turn.completed", "usage": usage},
        ]
        last_message, session = None, None
        for event in events:
            parsed = binding.parse_line(json.dumps(event))
            if isinstance(parsed, dict):
                last_message = parsed.get("message") or last_message
                session = parsed.get("session_id") or session
        self.assertEqual(final, last_message)
        self.assertEqual("native-session", session)
        self.assertEqual({"event_type": "turn.completed", "usage": usage}, usage_observation(events[-1], "codex"))

    def test_terminal_success_preserves_native_file_without_agent_message(self):
        parsed = binding.parse_line('{"type":"turn.completed","threadId":"saved-session","usage":{}}')
        self.assertEqual({"session_id": "saved-session"}, parsed)
        self.assertNotIn("message", parsed)
        for event in (
            {"type": "item.started", "item": {"type": "agent_message", "text": "partial"}},
            {"type": "item.completed", "item": {"type": "reasoning", "text": "private reasoning"}},
        ):
            self.assertIsNone(binding.parse_line(json.dumps(event)))
        for event_type in ("turn.failed", "turn.cancelled"):
            self.assertEqual({"message": event_type, "non_retryable_failure": True},
                             binding.parse_line(json.dumps({"type": event_type})))

    def test_dsml_in_reasoning_remains_untrusted_text(self):
        reasoning = {"type": "item.completed", "item": {
            "type": "reasoning",
            "text": '<\uff5cDSML\uff5c calls><\uff5cDSML\uff5c invoke name="read_toy_number">'
                    '</\uff5cDSML\uff5c invoke></\uff5cDSML\uff5c calls>',
        }}
        self.assertIsNone(binding.parse_line(json.dumps(reasoning)))
        self.assertEqual({}, binding.parse_line('{"type":"turn.completed","usage":{}}'))


if __name__ == "__main__":
    unittest.main()
