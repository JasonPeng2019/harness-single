"""STEP-13-2 fixture contract for the provider payload used at spawn."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import bootstrap, controller, provider_network_payload


ROOT = Path(__file__).resolve().parents[2]


def binding(provider: str):
    path = ROOT / "orchestrator_harness" / "provider_adapters" / provider / "launcher_binding.py"
    spec = importlib.util.spec_from_file_location(f"network_{provider}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIGS = {
    "codex": {"reasoning_effort": "high", "service_tier": "normal"},
    "claude-code": {"effort": "high"},
    "qwen-code": {},
}


class ProviderNetworkPayloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.worktree = Path(self.temp.name)
        bootstrap._install_codex_worker_isolation(
            self.worktree, self.worktree / "harness", self.worktree / "root-workspace"
        )

    def argv(self, provider: str, *, resume: bool = False):
        return binding(provider).build_argv(
            model="test-model", launch_config=CONFIGS[provider],
            worktree=str(self.worktree), prompt_path=str(self.worktree / "prompt.md"),
            session_id="session-1" if resume else None, resume=resume,
        )

    def captured_controller_profile(self, profile: str):
        """Fixture the already captured record at the private spawn boundary."""
        from contextlib import ExitStack

        stack = ExitStack()
        stack.enter_context(patch.object(controller.memory_handoff, "load_envelope", return_value={"decision_id": "decision-1"}))
        stack.enter_context(patch.object(
            controller.memory_handoff, "captured_network_resolution",
            return_value={
                "requested_mode": profile,
                "effective_mode": "soft_guardrail_network",
                "enforcement_sources": ["requested_soft_guardrail_policy"],
                "disclosed_limits": ["shell egress remains possible"],
            },
        ))
        return stack

    def test_soft_native_controls_are_in_spawn_payload_on_first_and_resume(self):
        for provider in CONFIGS:
            with self.subTest(provider=provider):
                if provider == "qwen-code":
                    path = self.worktree / ".qwen" / "settings.json"
                    path.parent.mkdir(exist_ok=True)
                    path.write_text(json.dumps({"userChoice": 7, "tools": {"disabled": ["other"]}}))
                    provider_network_payload.install_soft_controls(provider, self.worktree)
                for resume in (False, True):
                    argv, facts = provider_network_payload.resolve_launch(
                        provider, self.worktree, self.argv(provider, resume=resume),
                        "soft_guardrail_network",
                    )
                    self.assertEqual(facts["effective_profile"], "soft_guardrail_network")
                    self.assertTrue(facts["shell_egress_possible"])
                    self.assertFalse(facts["hardened_sandbox"])
                    if provider == "codex":
                        self.assertIn('web_search="disabled"', argv)
                    elif provider == "claude-code":
                        self.assertIn("--disallowedTools", argv)
                        self.assertIn("WebSearch", argv)
                        self.assertIn("WebFetch", argv)
                    else:
                        self.assertEqual(argv[-2:], ["--exclude-tools", "web_search,web_fetch"])
                        settings = json.loads((self.worktree / ".qwen/settings.json").read_text())
                        self.assertEqual(settings["userChoice"], 7)
                        self.assertFalse(settings["tools"]["webSearch"]["enabled"])
                        self.assertTrue({"web_fetch", "web_search"} <= set(settings["tools"]["disabled"]))
                        self.assertTrue({"web_fetch", "web_search"} <= set(settings["permissions"]["deny"]))

    def test_missing_or_tampered_settings_and_unknown_provider_downgrade(self):
        argv = self.argv("qwen-code")
        _, missing = provider_network_payload.resolve_launch(
            "qwen-code", self.worktree, argv, "soft_guardrail_network")
        self.assertEqual(missing["effective_profile"], "uncontrolled_network")
        provider_network_payload.install_soft_controls("qwen-code", self.worktree)
        path = self.worktree / ".qwen/settings.json"
        settings = json.loads(path.read_text())
        settings["tools"]["webSearch"]["enabled"] = True
        path.write_text(json.dumps(settings))
        _, tampered = provider_network_payload.resolve_launch(
            "qwen-code", self.worktree, argv, "soft_guardrail_network")
        self.assertEqual(tampered["effective_profile"], "uncontrolled_network")
        _, unknown = provider_network_payload.resolve_launch(
            "other", self.worktree, ["other"], "soft_guardrail_network")
        self.assertEqual(unknown["effective_profile"], "uncontrolled_network")
        provider_network_payload.install_soft_controls("qwen-code", self.worktree)
        with patch.object(provider_network_payload, "_qwen_cli_supports_deny", return_value=False):
            _, unsupported = provider_network_payload.resolve_launch(
                "qwen-code", self.worktree, argv, "soft_guardrail_network")
        self.assertEqual(unsupported["effective_profile"], "uncontrolled_network")

    def test_qwen_installed_cli_argument_and_version_gate(self):
        provider_network_payload.install_soft_controls("qwen-code", self.worktree)
        argv = self.argv("qwen-code")
        executable = shutil.which("qwen")
        self.assertIsNotNone(executable)
        accepted = subprocess.run(
            [executable, "--exclude-tools", "web_search,web_fetch", "--version"],
            cwd=self.worktree, capture_output=True, text=True, timeout=5, check=True,
        )
        self.assertEqual(accepted.stdout.strip(), "0.21.10")
        _, valid = provider_network_payload.resolve_launch(
            "qwen-code", self.worktree, argv, "soft_guardrail_network")
        self.assertEqual(valid["effective_profile"], "soft_guardrail_network")
        with patch.object(provider_network_payload.subprocess, "run", return_value=subprocess.CompletedProcess(
            argv, 0, stdout="0.22.0\n", stderr="",
        )):
            _, changed = provider_network_payload.resolve_launch(
                "qwen-code", self.worktree, argv, "soft_guardrail_network")
        self.assertEqual(changed["effective_profile"], "uncontrolled_network")

    def test_atlas_only_without_independent_egress_proof_downgrades(self):
        _, facts = provider_network_payload.resolve_launch(
            "codex", self.worktree, self.argv("codex"), "atlas_memory_only")
        self.assertEqual(facts["requested_profile"], "atlas_memory_only")
        self.assertEqual(facts["effective_profile"], "soft_guardrail_network")
        self.assertIn("independent", facts["reason"])
        self.assertNotIn("atlas_memory_only", facts["enforcement_sources"])

    def test_restricted_payload_does_not_enable_atlas_task_path(self):
        argv, facts = provider_network_payload.resolve_launch(
            "codex", self.worktree, self.argv("codex"), "restricted_local")
        self.assertIn('web_search="disabled"', argv)
        self.assertEqual(facts["effective_profile"], "soft_guardrail_network")
        self.assertFalse(facts["atlas_task_path_reenabled_by_network_controls"])
        self.assertIn("service", facts["reason"])

    def test_no_selected_profile_preserves_legacy_argv_and_settings(self):
        original = self.argv("codex")
        argv, facts = provider_network_payload.resolve_launch(
            "codex", self.worktree, original, None)
        self.assertEqual(argv, original)
        self.assertIsNone(facts)
        self.assertFalse((self.worktree / ".qwen/settings.json").exists())

    def test_controller_rechecks_and_sends_suppressed_argv_to_spawn(self):
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir()
        prompt = workspace / "worker-prompt.md"
        prompt.write_text("work", encoding="utf-8")
        (workspace / "task-card.json").write_text("{}", encoding="utf-8")
        lane = {
            "worktree_path": str(self.worktree), "run_id": "run-1",
            "memory_plan_state": "execution_accepted",
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "last_message_path": str(workspace / "last-message.txt"),
        }
        for provider, expected in (("codex", 'web_search="disabled"'), ("claude-code", "--disallowedTools")):
            with self.subTest(provider=provider):
                invocation = {"provider": {"model": "test-model", "launch_config": CONFIGS[provider]}}
                with self.captured_controller_profile("soft_guardrail_network"), patch.object(controller.processes, "spawn_provider", side_effect=RuntimeError("stop at spawn")) as spawn:
                    with self.assertRaises(controller.ControllerError):
                        controller._run_provider(
                            self.worktree, "epoch-1", lane, invocation, binding(provider), prompt,
                        )
                self.assertIn(expected, spawn.call_args.args[0])
                self.assertEqual(lane["_network_payload"]["effective_profile"], "soft_guardrail_network")

    def test_controller_refuses_missing_qwen_controls_before_spawn(self):
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir()
        prompt = workspace / "worker-prompt.md"
        prompt.write_text("work", encoding="utf-8")
        (workspace / "task-card.json").write_text("{}", encoding="utf-8")
        lane = {
            "worktree_path": str(self.worktree), "run_id": "run-1",
            "memory_plan_state": "execution_accepted",
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "last_message_path": str(workspace / "last-message.txt"),
        }
        invocation = {"provider": {"model": "test-model", "launch_config": {}}}
        for tampered in (False, True):
            with self.subTest(tampered=tampered):
                if tampered:
                    provider_network_payload.install_soft_controls("qwen-code", self.worktree)
                    path = self.worktree / ".qwen/settings.json"
                    settings = json.loads(path.read_text())
                    settings["tools"]["webSearch"]["enabled"] = True
                    path.write_text(json.dumps(settings))
                with self.captured_controller_profile("soft_guardrail_network"), patch.object(controller.processes, "spawn_provider") as spawn:
                    with self.assertRaises(controller.ControllerError):
                        controller._run_provider(
                            self.worktree, "epoch-1", lane, invocation, binding("qwen-code"), prompt,
                        )
                spawn.assert_not_called()
                self.assertEqual(lane["_network_payload"]["effective_profile"], "uncontrolled_network")

    def test_qwen_plain_and_managed_settings_are_checked_at_spawn(self):
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir()
        prompt = workspace / "worker-prompt.md"
        prompt.write_text("work", encoding="utf-8")
        (workspace / "task-card.json").write_text("{}", encoding="utf-8")
        path = self.worktree / ".qwen/settings.json"
        for installation in ("plain", "managed"):
            with self.subTest(installation=installation):
                if installation == "managed":
                    path.parent.mkdir(exist_ok=True)
                    path.write_bytes((ROOT / "adapters/qwen-code/super-cache/.qwen/settings.json").read_bytes())
                    original_hooks = json.loads(path.read_text())["hooks"]
                provider_network_payload.install_soft_controls("qwen-code", self.worktree)
                lane = {
                    "worktree_path": str(self.worktree), "run_id": "run-1",
                    "memory_plan_state": "execution_accepted",
                    "controller_events_path": str(workspace / "controller.events.jsonl"),
                    "last_message_path": str(workspace / "last-message.txt"),
                }
                invocation = {"provider": {"model": "test-model", "launch_config": {}}}
                with self.captured_controller_profile("soft_guardrail_network"), patch.object(controller.processes, "spawn_provider", side_effect=RuntimeError("stop at spawn")) as spawn:
                    with self.assertRaises(controller.ControllerError):
                        controller._run_provider(
                            self.worktree, "epoch-1", lane, invocation, binding("qwen-code"), prompt,
                        )
                self.assertEqual(spawn.call_args.kwargs["cwd"], str(self.worktree))
                self.assertEqual(spawn.call_args.args[0][-2:], ["--exclude-tools", "web_search,web_fetch"])
                self.assertEqual(lane["_network_payload"]["effective_profile"], "soft_guardrail_network")
                if installation == "managed":
                    self.assertEqual(json.loads(path.read_text())["hooks"], original_hooks)
                path.unlink()

    def test_qwen_untrusted_project_settings_and_web_search_env_override(self):
        qwen_home = self.worktree / "qwen-home"
        qwen_home.mkdir()
        (qwen_home / "settings.json").write_text(json.dumps({
            "security": {"folderTrust": {"enabled": True}},
            "tools": {"webSearch": {"enabled": True, "model": "user-model"}},
            "permissions": {"allow": ["web_search", "web_fetch"]},
        }))
        project = self.worktree / "project"
        project.mkdir()
        provider_network_payload.install_soft_controls("qwen-code", project)
        trust_file = qwen_home / "trustedFolders.json"
        executable = shutil.which("qwen")
        self.assertIsNotNone(executable, "installed Qwen 0.21.10 is required for this source probe")
        launcher = Path(executable).resolve()
        package = next((candidate for candidate in (
            launcher.parent.parent, launcher.parent.parent / "qwen-code",
        ) if (candidate / "package.json").is_file()
            and json.loads((candidate / "package.json").read_text())["name"] == "@qwen-code/qwen-code"), None)
        self.assertIsNotNone(package)
        self.assertEqual(json.loads((package / "package.json").read_text())["version"], "0.21.10")
        settings_module = package / "lib/chunks/chunk-ABVRJU3D.js"
        self.assertTrue(settings_module.is_file())
        script = (
            f'import {{loadSettings}} from {json.dumps(settings_module.as_uri())};'
            ' const x=loadSettings(process.cwd(),{skipLoadEnvironment:true,consumeCorruptionEnvVars:false});'
            ' console.log(JSON.stringify({trusted:x.isTrusted,tools:x.merged.tools,permissions:x.merged.permissions}));'
        )

        def merged_settings():
            result = subprocess.run(
                [shutil.which("node") or str(package.parent / "node/node.exe"),
                 "--input-type=module", "-e", script],
                cwd=project, env=os.environ.copy(), capture_output=True,
                text=True, timeout=15, check=True,
            )
            return json.loads(result.stdout)

        with patch.dict(os.environ, {"QWEN_HOME": str(qwen_home), "ENABLE_WEB_SEARCH": "true"}):
            trust_file.write_text(json.dumps({str(project): "DO_NOT_TRUST"}))
            untrusted_merged = merged_settings()
            self.assertFalse(untrusted_merged["trusted"])
            self.assertNotIn("disabled", untrusted_merged["tools"])
            self.assertNotIn("deny", untrusted_merged.get("permissions") or {})
            self.assertTrue({"web_search", "web_fetch"} <= set(untrusted_merged["permissions"]["allow"]))
            untrusted_argv, untrusted = provider_network_payload.resolve_launch(
                "qwen-code", project, self.argv("qwen-code"), "soft_guardrail_network")
            self.assertEqual(untrusted["effective_profile"], "soft_guardrail_network")
            self.assertEqual(untrusted_argv[-2:], ["--exclude-tools", "web_search,web_fetch"])
            self.assertIn("argv", untrusted["enforcement_sources"][0])
            trust_file.write_text(json.dumps({str(project): "TRUST_FOLDER"}))
            trusted_merged = merged_settings()
            self.assertTrue(trusted_merged["trusted"])
            self.assertTrue({"web_search", "web_fetch"} <= set(trusted_merged["tools"]["disabled"]))
            trusted_argv, trusted = provider_network_payload.resolve_launch(
                "qwen-code", project, self.argv("qwen-code"), "soft_guardrail_network")
            self.assertEqual(trusted["effective_profile"], "soft_guardrail_network")
            self.assertEqual(trusted_argv[-2:], ["--exclude-tools", "web_search,web_fetch"])

    def test_qwen_junction_or_symlink_cannot_escape_worktree(self):
        outside = self.worktree / "outside"
        outside.mkdir()
        settings = outside / "settings.json"
        settings.write_text('{"userChoice":"original"}')
        project = self.worktree / "project"
        project.mkdir()
        link = project / ".qwen"
        if os.name == "nt":
            subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(outside)], check=True, capture_output=True)
        else:
            link.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            provider_network_payload.install_soft_controls("qwen-code", project)
        self.assertEqual(settings.read_text(), '{"userChoice":"original"}')
        _, facts = provider_network_payload.resolve_launch(
            "qwen-code", project, self.argv("qwen-code"), "soft_guardrail_network")
        self.assertEqual(facts["effective_profile"], "uncontrolled_network")

    def test_qwen_malformed_disabled_and_deny_shapes_are_rejected(self):
        path = self.worktree / ".qwen/settings.json"
        path.parent.mkdir()
        for field in ("disabled", "deny"):
            for malformed in ({"web_search": True, "web_fetch": True}, ["web_search", "web_fetch", 7]):
                with self.subTest(field=field, malformed=malformed):
                    settings = {
                        "tools": {"webSearch": {"enabled": False}, "disabled": ["web_search", "web_fetch"]},
                        "permissions": {"deny": ["web_search", "web_fetch"]},
                    }
                    settings["tools" if field == "disabled" else "permissions"][field] = malformed
                    path.write_text(json.dumps(settings))
                    _, facts = provider_network_payload.resolve_launch(
                        "qwen-code", self.worktree, self.argv("qwen-code"), "soft_guardrail_network")
                    self.assertEqual(facts["effective_profile"], "uncontrolled_network")
                    with self.assertRaises(ValueError):
                        provider_network_payload.install_soft_controls("qwen-code", self.worktree)

    def test_explicit_profile_validation_is_total_before_spawn(self):
        original = self.argv("codex")
        for profile in (
            None, "soft_guardrail_network", "atlas_memory_only", "restricted_local",
            "invalid", False, 7, 1.5, [], {},
        ):
            with self.subTest(profile=profile):
                if profile in (None, "soft_guardrail_network", "atlas_memory_only", "restricted_local"):
                    argv, facts = provider_network_payload.resolve_launch(
                        "codex", self.worktree, original, profile,
                    )
                    if profile is None:
                        self.assertIs(argv, original)
                        self.assertIsNone(facts)
                    else:
                        self.assertIn('web_search="disabled"', argv)
                        self.assertEqual(facts["requested_profile"], profile)
                        self.assertEqual(facts["effective_profile"], "soft_guardrail_network")
                else:
                    with self.assertRaises(ValueError):
                        provider_network_payload.resolve_launch(
                            "codex", self.worktree, original, profile,
                        )

        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir()
        prompt = workspace / "worker-prompt.md"
        prompt.write_text("work")
        (workspace / "task-card.json").write_text("{}")
        invocation = {"provider": {"model": "test-model", "launch_config": CONFIGS["codex"]}}
        lane = {
            "worktree_path": str(self.worktree), "run_id": "run-1",
            "memory_plan_state": "execution_accepted",
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "last_message_path": str(workspace / "last-message.txt"),
        }
        with (
            patch.object(controller.memory_handoff, "load_envelope", return_value={"decision_id": "decision-1"}),
            patch.object(controller.memory_handoff, "captured_network_resolution",
                         side_effect=controller.memory_handoff.MemoryHandoffError("captured profile invalid")),
            patch.object(controller.processes, "spawn_provider") as spawn,
        ):
            with self.assertRaises(controller.ControllerError) as raised:
                controller._run_provider(
                    self.worktree, "epoch-1", lane, invocation, binding("codex"), prompt,
                )
        self.assertEqual(raised.exception.code, controller.LAUNCH_INVOCATION_INVALID)
        self.assertTrue(raised.exception.no_provider_started)
        spawn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
