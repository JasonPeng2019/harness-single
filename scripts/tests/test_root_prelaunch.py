from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_codex_usage as collector
import prepare_root_launch as preparation
from orchestrator_harness import processes, setup, shutdown
from orchestrator_harness.config import load_config
from orchestrator_harness.root_session import bind_native_root_session, read_root_session


class RootPrelaunchTests(unittest.TestCase):
    def setUp(self) -> None:
        readiness = patch.object(preparation, "require_ready", return_value=None)
        readiness.start()
        self.addCleanup(readiness.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name)
        self.harness, self.workspace = base / "harness", base / "workspace"
        self.harness.mkdir()
        self.workspace.mkdir()
        self.write(self.harness / "harness-config.json", {"root_workspace": str(self.workspace), "managed_coordination": "enabled"})
        self.write(self.harness / "resource-manifest.json", {"schema": "resource-manifest/v1", "resources": []})
        self.cleanup_config = (self.harness / "harness-config.json").read_bytes()
        self.cleanup_policy = None
        source = Path(__file__).resolve().parents[2]
        for name in ("adapters", "super-cache", "orchestrator_harness", "harness_common", "scripts"):
            shutil.copytree(source / name, self.harness / name, ignore=shutil.ignore_patterns("__pycache__"))
        self.environment = patch.dict(os.environ, {"PYTHONPATH": str(source)}, clear=False)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.children = []
        real_spawn = processes.spawn_detached

        def owned_spawn(*arguments, **keywords):
            child = real_spawn(*arguments, **keywords)
            self.children.append(child)
            return child

        spawn = patch.object(processes, "spawn_detached", side_effect=owned_spawn)
        spawn.start()
        self.addCleanup(spawn.stop)
        self.addCleanup(self.close_runtime)

    def write(self, path: Path, value: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def installed(self) -> None:
        self.cleanup_config = (self.harness / "harness-config.json").read_bytes()
        with patch.object(setup, "find_harness_root", return_value=self.harness):
            result = setup.run_setup()
        self.assertTrue(result["ok"], result)

    def close_runtime(self) -> None:
        # Restore deliberately damaged inputs in this owned disposable fixture
        # before asking native cleanup to validate the original service/monitor.
        (self.harness / "harness-config.json").write_bytes(self.cleanup_config)
        if self.cleanup_policy is not None:
            (self.harness / "local-config/public-check-policy.json").write_bytes(self.cleanup_policy)
        config = load_config(self.harness)
        if setup.read_runtime_state(config.runtime_root) is not None:
            with patch.object(shutdown, "find_harness_root", return_value=self.harness):
                result = shutdown.run_shutdown()
            self.assertTrue(result["ok"], result)
        for child in self.children:
            self.assertIsNotNone(child.wait(timeout=10), "owned service/monitor must exit before fixture removal")

    def enable_service(self) -> None:
        """An actual pinned deterministic service process, not isolation proof."""
        import hashlib
        from orchestrator_harness.tests.test_public_checks import ADAPTER_SOURCE
        root = load_config(self.harness).root_workspace
        if not (root / ".git").exists():
            subprocess.run(["git", "init", "-q", str(root)], check=True, capture_output=True)
            (root / "example.txt").write_text("fixture\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "example.txt"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                            "commit", "-qm", "fixture"], check=True, capture_output=True)
        adapter = self.harness / "fixture-adapter.py"
        adapter.write_text(ADAPTER_SOURCE, encoding="utf-8")
        self.write(self.harness / "local-config/public-check-policy.json", {
            "schema": "public-check-policy/v2", "adapter": {"path": str(adapter), "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest()},
            "input_paths": ["example.txt"], "optional_input_paths": [], "fixed_paths": [],
            "commands": {key: [sys.executable, "-B", "-c", "print('fixture')"] for key in ("build", "public")},
            "allow_focused": True, "timeout_seconds": 10, "max_parallel_checks": 1,
        })
        self.cleanup_policy = (self.harness / "local-config/public-check-policy.json").read_bytes()

    def prepared(self) -> None:
        self.cleanup_config = (self.harness / "harness-config.json").read_bytes()
        self.preparation = preparation.prepare(self.harness, self.workspace, "fixture")

    def split_preparation(self, provider_id: str) -> None:
        product = self.workspace.parent / "product"
        product.mkdir()
        self.write(self.harness / "harness-config.json", {
            "root_workspace": str(product), "manager_workspace": str(self.workspace),
            "managed_coordination": "enabled",
        })
        self.cleanup_config = (self.harness / "harness-config.json").read_bytes()
        if provider_id == "codex":
            self.enable_service()
        with patch.object(preparation, "require_ready", return_value=None) as health:
            prepared = preparation.prepare(self.harness, self.workspace, "fixture", provider_id=provider_id)
            self.assertEqual(prepared, preparation.validate_prepared_root(
                self.harness, self.workspace, "fixture", provider_id=provider_id))
        if provider_id == "codex":
            self.assertTrue(health.call_args_list)
            self.assertTrue(all(call.args == (self.harness,) and call.kwargs == {"workspace": product}
                                for call in health.call_args_list))
        else:
            health.assert_not_called()
        self.assertEqual(str(product / ".harness-runtime"), prepared["runtime_root"])
        self.assertFalse((self.workspace / ".harness-runtime").exists())
        self.assertTrue((self.workspace / ".agent-workspace/ROOT_SESSION.json").is_file())
        self.assertFalse((product / ".agent-workspace/ROOT_SESSION.json").exists())
        for relative in preparation.hook_files(provider_id):
            self.assertTrue((self.workspace / relative).is_file())
            self.assertFalse((product / relative).exists())

    def test_codex_split_preparation_uses_manager_hooks_and_product_service(self) -> None:
        self.split_preparation("codex")

    def test_claude_split_preparation_uses_registered_provider_hooks(self) -> None:
        self.split_preparation("claude-code")

    def test_qwen_split_preparation_includes_notification_hook(self) -> None:
        self.split_preparation("qwen-code")

    def test_legacy_receipt_requires_explicit_preparation_and_preserves_exact_bytes(self) -> None:
        self.installed()
        receipt_path = self.workspace / preparation.RECEIPT
        self.write(receipt_path, {"schema": preparation.LEGACY_SCHEMA, "status": "PASS",
            "run_id": "fixture", "harness_dir": str(self.harness), "workspace": str(self.workspace)})
        original = receipt_path.read_bytes()
        with self.assertRaisesRegex(ValueError, "legacy v1.*explicit"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")
        with patch.object(preparation.subprocess, "run") as launch:
            prepared = preparation.prepare(self.harness, self.workspace, "fixture")
        launch.assert_not_called()
        receipt = preparation.read(receipt_path)
        self.assertEqual(preparation.SCHEMA, receipt["schema"])
        self.assertEqual(original, Path(receipt["prior_receipt"]["path"]).read_bytes())
        self.assertEqual(prepared["root_session_id"], preparation.read(
            self.workspace / ".agent-workspace/ROOT_SESSION.json")["session_id"])

    def test_missing_required_harness_input_blocks_preparation_before_session_publication(self) -> None:
        real_run = preparation.subprocess.run

        def installed_then_removed(command, **kwargs):
            completed = real_run(command, **kwargs)
            if "orchestrator_harness.operator_launch" in command and command[-1] == "setup":
                (self.harness / "orchestrator_harness/root_hook_dispatch.py").unlink()
            return completed

        with patch.object(preparation.subprocess, "run", side_effect=installed_then_removed):
            with self.assertRaisesRegex(ValueError, "required ROOT configuration input is missing"):
                preparation.prepare(self.harness, self.workspace, "fixture")
        self.assertFalse((self.workspace / preparation.RECEIPT).exists())
        self.assertFalse((self.workspace / ".agent-workspace/ROOT_SESSION.json").exists())

    def test_replaced_native_monitor_requires_explicit_preparation(self) -> None:
        self.prepared()
        old_session = self.preparation["root_session_id"]
        old_receipt = (self.workspace / preparation.RECEIPT).read_bytes()
        old_owner = preparation.read(self.workspace / preparation.RECEIPT)["monitor"]
        self.close_runtime()
        self.installed()
        with self.assertRaisesRegex(ValueError, "monitor owner changed"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")
        with patch.object(preparation.subprocess, "run") as launch:
            refreshed = preparation.prepare(self.harness, self.workspace, "fixture")
        launch.assert_not_called()
        self.assertEqual(old_session, refreshed["root_session_id"])
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.assertNotEqual(old_owner["startup_id"], receipt["monitor"]["startup_id"])
        self.assertEqual(old_receipt, Path(receipt["prior_receipt"]["path"]).read_bytes())

    def collector_args(self) -> argparse.Namespace:
        args = self.resume_args()
        args.resume_task_file = None
        args.model = "deterministic-fixture"
        args.reasoning_effort = "fixture"
        args.service_tier = "fixture"
        args.sandbox = "read-only"
        args.interrupted_run = True
        args.inhibit_sleep = False
        return args

    def fixture_collector(self, args, native_threads):
        """Observe real owned process events; not native model/sandbox proof."""
        entry = self.workspace / "provider-fixture.py"
        entry.write_text(
            "import json, os\n"
            "print(json.dumps({'environment': {key: os.environ.get(key) for key in "
            "('HARNESS_ROOT_SESSION_ID', 'HARNESS_BINDING_ID', 'CODEX_THREAD_ID', 'CLAUDE_SESSION_ID', 'QWEN_SESSION_ID')}}), flush=True)\n"
            "print('malformed retained evidence', flush=True)\n"
            f"for native in {native_threads!r}:\n"
            "    print(json.dumps({'type': 'thread.started', 'thread_id': native}), flush=True)\n",
            encoding="utf-8")
        real_spawn = processes.spawn_provider
        commands = []

        def owned_provider(argv, **kwargs):
            commands.append(argv)
            return real_spawn([sys.executable, "-B", str(entry)], **kwargs)

        with (patch.object(collector.shutil, "which", return_value=sys.executable),
              patch.object(processes, "spawn_provider", side_effect=owned_provider),
              contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO())):
            result = collector.run_codex(args)
        self.assertEqual(1, len(commands), "collector must invoke exactly one owned provider")
        run_dir = (Path(args.results_dir) / args.run_id / "segments/0002"
                   if args.resume_task_file else Path(args.results_dir) / args.run_id)
        manifest = preparation.read(run_dir / "manifest.json")
        self.assertTrue(manifest["cleanup_proven"], manifest)
        root = manifest["process_boundary"]["root"]
        self.assertTrue(processes.identity_exit_proven(root["pid"], root["creation_time"]))
        lines = (run_dir / "codex-events.jsonl").read_text(encoding="utf-8").splitlines()
        return result, manifest, json.loads(lines[0])["environment"], lines

    def test_collector_binds_observed_native_thread_once_and_passes_prepared_scope(self) -> None:
        self.prepared()
        args = self.collector_args()
        with patch.dict(os.environ, {"CODEX_THREAD_ID": "inherited", "CLAUDE_SESSION_ID": "inherited",
                                     "QWEN_SESSION_ID": "inherited", "HARNESS_ROOT_SESSION_ID": "inherited"}):
            result, manifest, environment, lines = self.fixture_collector(args, ["observed-thread", "observed-thread"])
        self.assertEqual(0, result, manifest)
        self.assertEqual(self.preparation["root_session_id"], environment["HARNESS_ROOT_SESSION_ID"])
        self.assertEqual("codex", environment["HARNESS_BINDING_ID"])
        self.assertTrue(all(environment[key] is None for key in ("CODEX_THREAD_ID", "CLAUDE_SESSION_ID", "QWEN_SESSION_ID")))
        self.assertIn("malformed retained evidence", lines)
        self.assertEqual("observed-thread", read_root_session(load_config(self.harness))["native_session_id"])

    def test_collector_changed_native_thread_seals_failure_and_preserves_original_binding(self) -> None:
        self.prepared()
        result, manifest, _environment, _lines = self.fixture_collector(
            self.collector_args(), ["owned-thread", "foreign-thread"])
        self.assertEqual(2, result, manifest)
        self.assertIn("cannot replace a known thread", manifest["launch_error"])
        self.assertEqual("owned-thread", read_root_session(load_config(self.harness))["native_session_id"])

    def test_collector_success_without_native_thread_identity_seals_failure(self) -> None:
        self.prepared()
        result, manifest, _environment, _lines = self.fixture_collector(self.collector_args(), [])
        self.assertEqual(2, result, manifest)
        self.assertIn("without a thread.started identity", manifest["launch_error"])
        self.assertIsNone(read_root_session(load_config(self.harness))["native_session_id"])

    def test_receipt_replaced_during_validation_cannot_supply_unvalidated_hash(self) -> None:
        self.prepared()
        real_hashes = preparation.configuration_hashes
        receipt_path = self.workspace / preparation.RECEIPT

        def replace_receipt(*arguments, **keywords):
            hashes = real_hashes(*arguments, **keywords)
            receipt = preparation.read(receipt_path)
            self.write(receipt_path, {**receipt, "status": "FAIL"})
            return hashes

        with patch.object(preparation, "configuration_hashes", side_effect=replace_receipt):
            with self.assertRaisesRegex(ValueError, "receipt changed during validation"):
                preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_collector_known_native_thread_refuses_fresh_launch_before_output(self) -> None:
        self.prepared()
        bind_native_root_session(load_config(self.harness), run_id="fixture",
            session_id=self.preparation["root_session_id"], native_session_id="known-thread")
        with patch.object(processes, "spawn_provider") as spawn:
            with self.assertRaisesRegex(collector.LedgerError, "already has a native thread"):
                collector.run_codex(self.collector_args())
        spawn.assert_not_called()
        self.assertFalse((self.workspace / "results").exists())

    def test_collector_saved_resume_mismatch_refuses_launch_before_output(self) -> None:
        self.prepared()
        bind_native_root_session(load_config(self.harness), run_id="fixture",
            session_id=self.preparation["root_session_id"], native_session_id="known-thread")
        args = self.collector_args()
        args.resume_task_file = str(self.workspace / "task.json")
        segment = self.workspace / "results/fixture/segments/0002"
        with (patch.object(collector, "prepare_resume", return_value=(segment, 2, "foreign-thread", 1)),
              patch.object(processes, "spawn_provider") as spawn):
            with self.assertRaisesRegex(collector.LedgerError, "saved native ROOT session differs"):
                collector.run_codex(args)
        spawn.assert_not_called()
        self.assertFalse((self.workspace / "results").exists())

    def test_collector_closed_resume_uses_same_native_and_logical_scope_without_setup(self) -> None:
        self.prepared()
        args = self.collector_args()
        result, manifest, _environment, _lines = self.fixture_collector(args, ["owned-thread"])
        self.assertEqual(0, result, manifest)
        self.close_runtime()
        args.resume_task_file = str(self.workspace / "results/fixture/task.json")
        segment = self.workspace / "results/fixture/segments/0002"
        with (patch.object(collector, "prepare_resume", return_value=(segment, 2, "owned-thread", 1)),
              patch.object(preparation.subprocess, "run") as setup_command):
            result, manifest, environment, _lines = self.fixture_collector(args, ["owned-thread"])
        setup_command.assert_not_called()
        self.assertEqual(0, result, manifest)
        self.assertEqual("owned-thread", environment["CODEX_THREAD_ID"])
        self.assertEqual(self.preparation["root_session_id"], environment["HARNESS_ROOT_SESSION_ID"])

    def test_fresh_preparation_runs_setup_then_service_exactly_once(self) -> None:
        from orchestrator_harness import public_checks
        calls = []
        self.enable_service()
        health = {"health_receipt_sha256": "fixture-health", "marker_sha256": "fixture-marker"}
        real_run = preparation.subprocess.run
        real_start = public_checks.start_service

        def setup_once(command, **kwargs):
            if "orchestrator_harness.operator_launch" in command and command[-1] == "setup":
                calls.append("setup")
            return real_run(command, **kwargs)

        def start_once(*arguments):
            calls.append("service")
            return real_start(*arguments)

        with (patch.object(preparation, "require_ready", return_value=health),
              patch.object(preparation.subprocess, "run", side_effect=setup_once) as setup,
              patch.object(public_checks, "start_service", side_effect=start_once) as start):
            first = preparation.prepare(self.harness, self.workspace, "fixture")
            again = preparation.prepare(self.harness, self.workspace, "fixture")
        self.assertEqual(first, again)
        self.assertEqual(["setup", "service"], calls)
        self.assertEqual(1, sum("orchestrator_harness.operator_launch" in call.args[0]
                                and call.args[0][-1] == "setup" for call in setup.call_args_list))
        start.assert_called_once()
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.assertEqual("READY", receipt["public_check_service"]["status"])
        self.assertTrue(processes.identity_matches(receipt["public_check_service"]["pid"],
                                                  receipt["public_check_service"]["creation_time"]))
        self.assertEqual(health, receipt["windows_sandbox_health"])

    def test_prepared_sandbox_proof_cannot_change_before_root_launch(self) -> None:
        self.prepared()
        with patch.object(preparation, "require_ready", return_value={"health_receipt_sha256": "changed"}):
            with self.assertRaisesRegex(ValueError, "health changed after"):
                preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_prepared_service_identity_cannot_change_before_root_launch(self) -> None:
        from orchestrator_harness import public_checks
        self.enable_service()
        self.prepared()
        with patch.object(public_checks, "validate_service", return_value={"pid": 999}):
            with self.assertRaisesRegex(ValueError, "service changed after"):
                preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_existing_runtime_cannot_be_adopted_without_receipt(self) -> None:
        self.installed()
        with self.assertRaisesRegex(ValueError, "receipt is missing"):
            preparation.prepare(self.harness, self.workspace, "fixture")

    def test_prepared_runtime_reused_without_repeating_setup(self) -> None:
        self.prepared()
        with patch.object(preparation.subprocess, "run") as process:
            result = preparation.prepare(self.harness, self.workspace, "fixture")
        process.assert_not_called()
        self.assertEqual(result["prepared_at_utc"], self.preparation["prepared_at_utc"])

    def test_different_run_cannot_reuse_preparation(self) -> None:
        self.prepared()
        with self.assertRaisesRegex(ValueError, "different ROOT run"):
            preparation.validate_prepared_root(self.harness, self.workspace, "another")

    def test_filename_mentioned_in_inert_hook_does_not_prove_registration(self) -> None:
        self.installed()
        hooks = preparation.read(self.workspace / ".codex/hooks.json")
        hooks["hooks"]["Stop"][0]["hooks"][0]["command"] = "echo orchestrator_harness_stop.py"
        self.write(self.workspace / ".codex/hooks.json", hooks)
        with self.assertRaisesRegex(ValueError, "Stop hook"):
            preparation.check_installed(self.harness, self.workspace)

    def test_disabled_native_hooks_refuse_preparation(self) -> None:
        self.installed()
        (self.workspace / ".codex/config.toml").write_text('[features]\nhooks = false\n', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, "hooks"):
            preparation.check_installed(self.harness, self.workspace)

    def test_prelaunch_uses_native_config_default_managed_mode(self):
        self.installed()
        self.write(self.harness / "harness-config.json", {"root_workspace": str(self.workspace)})
        self.assertEqual(set(preparation.HOOK_FILES),
                         set(preparation.check_installed(self.harness, self.workspace)))

    def test_prelaunch_cannot_accept_unknown_native_config_keys(self):
        self.installed()
        self.write(self.harness / "harness-config.json", {"root_workspace": str(self.workspace),
                   "managed_coordination": "enabled", "profile": "managed"})
        with self.assertRaisesRegex(ValueError, "unknown key"):
            preparation.check_installed(self.harness, self.workspace)

    def test_codex_config_change_after_preparation_refuses_launch(self) -> None:
        self.prepared()
        (self.workspace / ".codex/config.toml").write_text('model = "changed"\n[features]\nhooks = true\n', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, "configuration changed"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_removed_service_policy_cannot_evade_original_service_receipt(self) -> None:
        self.prepared()
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.write(self.workspace / preparation.RECEIPT, {**receipt, "public_check_service": {"pid": 123}})
        with self.assertRaisesRegex(ValueError, "service"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_missing_stop_hook_blocks_launch(self) -> None:
        self.prepared()
        hooks = preparation.read(self.workspace / ".codex/hooks.json")
        del hooks["hooks"]["Stop"]
        self.write(self.workspace / ".codex/hooks.json", hooks)
        with self.assertRaisesRegex(ValueError, "Stop hook is missing"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_modified_hook_blocks_launch(self) -> None:
        self.prepared()
        (self.workspace / preparation.HOOK_FILES[-1]).write_text("# changed\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "changed after"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_closed_runtime_blocks_fresh_launch(self) -> None:
        self.prepared()
        self.close_runtime()
        with self.assertRaisesRegex(ValueError, "not OPEN"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_closed_runtime_resume_revalidates_without_repeating_setup(self) -> None:
        self.prepared()
        self.close_runtime()
        with patch.object(preparation.subprocess, "run") as process:
            result = preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)
        process.assert_not_called()
        self.assertEqual(self.preparation["prepared_at_utc"], result["prepared_at_utc"])

    def test_closed_runtime_resume_checks_same_stopped_service(self) -> None:
        from orchestrator_harness import public_checks
        self.enable_service()
        self.prepared()
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.close_runtime()
        with patch.object(public_checks, "validate_service", wraps=public_checks.validate_service) as validate:
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)
            self.assertGreaterEqual(validate.call_count, 2)
            self.assertTrue(all(call.args == (self.workspace, self.harness)
                                and call.kwargs == {"runtime_closed": True} for call in validate.call_args_list))
        self.write(self.workspace / preparation.RECEIPT, {**receipt,
                   "public_check_service": {**receipt["public_check_service"], "pid": 456}})
        with self.assertRaisesRegex(ValueError, "service changed after"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)

    def resume_args(self) -> argparse.Namespace:
        prompt = self.workspace / "prompt.txt"
        prompt.write_text("fixture", encoding="utf-8")
        return argparse.Namespace(prompt_file=str(prompt), cwd=str(self.workspace), task="fixture", run_id="fixture",
            budget_tokens=None, watchdog_hours=1, arm="harness", trust_project_hooks=True,
            harness_runtime=str(self.workspace / ".harness-runtime"), harness_dir=str(self.harness),
            results_dir=str(self.workspace / "results"), resume_task_file=str(self.workspace / "task.json"))

    def test_collector_resume_cannot_skip_prelaunch_receipt(self) -> None:
        args = self.resume_args()
        segment = self.workspace / "results/fixture/segments/0002"
        with (patch.object(collector, "prepare_resume", return_value=(segment, 2, "fixture-session", 1)),
              patch.object(collector.subprocess, "Popen") as process):
            with self.assertRaisesRegex(collector.LedgerError, "receipt is missing"):
                collector.run_codex(args)
        process.assert_not_called()
        self.assertFalse((self.workspace / "results").exists())

    def test_collector_resume_cannot_skip_sandbox_health_or_run_block(self) -> None:
        self.prepared()
        args = self.resume_args()
        segment = self.workspace / "results/fixture/segments/0002"
        for state in ("OPEN", "CLOSED"):
            with self.subTest(state=state):
                if state == "CLOSED":
                    self.close_runtime()
                with (patch.object(collector, "prepare_resume", return_value=(segment, 2, "fixture-session", 1)),
                      patch.object(preparation, "require_ready", side_effect=ValueError("WINDOWS_SANDBOX_RUN_BLOCKED")) as health,
                      patch.object(collector.subprocess, "Popen") as process):
                    with self.assertRaisesRegex(collector.LedgerError, "WINDOWS_SANDBOX_RUN_BLOCKED"):
                        collector.run_codex(args)
                health.assert_called_once_with(self.harness, workspace=self.workspace)
                process.assert_not_called()
                self.assertFalse((self.workspace / "results").exists())

    def test_collector_resume_cannot_skip_changed_hook_validation(self) -> None:
        self.prepared()
        (self.workspace / preparation.HOOK_FILES[-1]).write_text("# changed\n", encoding="utf-8")
        args = self.resume_args()
        segment = self.workspace / "results/fixture/segments/0002"
        with (patch.object(collector, "prepare_resume", return_value=(segment, 2, "fixture-session", 1)),
              patch.object(collector.subprocess, "Popen") as process):
            with self.assertRaisesRegex(collector.LedgerError, "hook files changed"):
                collector.run_codex(args)
        process.assert_not_called()
        self.assertFalse((self.workspace / "results").exists())

    def test_collector_never_starts_root_without_preparation(self) -> None:
        prompt = self.workspace / "prompt.txt"
        prompt.write_text("fixture", encoding="utf-8")
        args = argparse.Namespace(prompt_file=str(prompt), cwd=str(self.workspace), task="fixture", run_id="fixture",
            budget_tokens=None, watchdog_hours=1, arm="harness", trust_project_hooks=True,
            harness_runtime=str(self.workspace / ".harness-runtime"), harness_dir=str(self.harness), results_dir=str(self.workspace / "results"))
        with patch.object(collector.subprocess, "Popen") as process:
            with self.assertRaisesRegex(collector.LedgerError, "receipt is missing"):
                collector.run_codex(args)
        process.assert_not_called()
        self.assertFalse((self.workspace / "results").exists())

    def test_sandbox_failure_blocks_setup_without_launching_any_process(self) -> None:
        with patch.object(preparation, "require_ready", side_effect=ValueError("WINDOWS_SANDBOX_NOT_READY")):
            with patch.object(preparation.subprocess, "run") as process:
                with self.assertRaisesRegex(ValueError, "WINDOWS_SANDBOX_NOT_READY"):
                    preparation.prepare(self.harness, self.workspace, "fixture")
        process.assert_not_called()
        self.assertFalse((self.workspace / ".harness-runtime").exists())

    def worker_review(self, lifecycle="review_pending"):
        """Synthetic authority branching only; not native identity/cleanup evidence."""
        runtime = self.workspace / ".harness-runtime"
        lane = {"schema": "lane/v1", "lane_id": "worker", "run_id": "one", "lifecycle": lifecycle,
                "worktree_path": str(self.workspace / "worker"), "provider": {"id": "codex"},
                "process": {"pid": 123, "creation_time": "2026-10-11T06:00:00Z"},
                "controller_status_path": str(self.workspace / "worker-status.json")}
        self.write(runtime / "epochs/epoch/lanes/worker/lane.json", lane)
        status = {"schema": "controller-status/v1", "lane_id": "worker", "run_id": "one",
                  "controller_state": "exited", "provider_state": {"state": "exited"},
                  "recorded_status": "review_pending", "cleanup_proven": True,
                  "process_boundary": {"fixture": "exact-provider-boundary"}}
        self.write(Path(lane["controller_status_path"]), status)
        return runtime, lane, status

    def test_open_review_cannot_resume_over_live_controller_identity(self):
        runtime, lane, status = self.worker_review()
        with patch("orchestrator_harness.processes.identity_matches", return_value=True):
            self.assertFalse(collector.quiescent_open_runtime(runtime))

    def test_retired_label_does_not_hide_live_process_on_closed_resume(self):
        runtime, lane, status = self.worker_review("retired")
        with patch("orchestrator_harness.processes.identity_matches", return_value=True):
            self.assertTrue(collector.active_harness_lanes(runtime))

    def test_open_review_accepts_only_matching_proven_cleanup(self):
        runtime, lane, status = self.worker_review()
        with patch("orchestrator_harness.processes.identity_exit_proven", return_value=True), patch(
            "orchestrator_harness.processes.process_boundary_is_gone", return_value=True):
            self.assertTrue(collector.quiescent_open_runtime(runtime))
            self.write(Path(lane["controller_status_path"]), {**status, "lane_id": "foreign"})
            self.assertFalse(collector.quiescent_open_runtime(runtime))

    def test_retired_absent_status_requires_native_never_launched_proof(self):
        runtime, lane, status = self.worker_review("retired")
        Path(lane["controller_status_path"]).unlink()
        lane = {**lane, "process": {}, "session": {}}
        path = runtime / "epochs/epoch/lanes/worker/lane.json"
        self.write(path, lane)
        self.assertTrue(collector.active_harness_lanes(runtime))
        self.write(path, {**lane, "retirement_proof": {"schema": "lane-retirement-proof/v1",
            "lane_id": lane["lane_id"], "run_id": lane["run_id"], "kind": "never_launched"}})
        self.assertFalse(collector.active_harness_lanes(runtime))


if __name__ == "__main__":
    unittest.main()
