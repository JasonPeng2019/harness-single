"""Failure gates without executing Codex, requesting elevation, or repairing ACLs."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import MagicMock
from types import SimpleNamespace

from orchestrator_harness import windows_sandbox_preflight as sandbox


class WindowsSandboxPreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.harness = self.base / "harness"
        self.home = self.base / "codex-home"
        self.workspace = self.base / "workspace"
        self.cache = self.base / "cache"
        self.native = self.base / "codex.exe"
        self.native.write_bytes(b"fixture binary: never execute")
        source_root = Path(__file__).resolve().parents[2]
        for relative in (
            "orchestrator_harness/bootstrap.py", "orchestrator_harness/isolation_policy.py",
            "orchestrator_harness/provider_adapters/codex/launcher_binding.py",
            "adapters/codex/harness/launcher_binding.py",
            "orchestrator_harness/windows_sandbox_preflight.py",
        ):
            target = self.harness / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_root / relative, target)
        self.marker = self.home / ".sandbox/setup_marker.json"
        self.evidence = self.base / "probe.json"
        self.write(self.harness / sandbox.POLICY, {
            "schema": "windows-sandbox-policy/v1", "max_launcher_attempts": 1,
            "max_setup_requests_per_run": 1, "automatic_setup": False,
            "automatic_fallback": False, "require_health_receipt": True,
        })
        self.environment = patch.dict(os.environ, {"CODEX_HOME": str(self.home), "LOCALAPPDATA": str(self.cache)})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.real_popen = subprocess.Popen
        self.spawn = patch("subprocess.Popen", side_effect=AssertionError("no process may launch"))
        self.process = self.spawn.start()
        self.addCleanup(self.spawn.stop)
        self.addCleanup(self.process.assert_not_called)

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def ready(self, permission_mode="worker-isolated"):
        self.write(self.marker, {"version": 1, "offline_username": "offline", "online_username": "online"})
        profile = self.base / "probe-worker/.codex/config.toml"
        profile.parent.mkdir(parents=True, exist_ok=True)
        profile.write_text(
            f'default_permissions = "{permission_mode}"\n'
            f'[permissions.{permission_mode}]\nextends = ":workspace"\n'
            f'[permissions.{permission_mode}.filesystem]\n'
            + (f'{json.dumps(str(self.home))} = "deny"\n' if permission_mode == "worker-isolated" else "")
            + f'[permissions.{permission_mode}.filesystem.":workspace_roots"]\n"." = "write"\n',
            encoding="utf-8",
        )
        protected = {"root/.git/index": "1" * 64, "peer/.git/index": "2" * 64,
                     "unrelated/sentinel": "3" * 64}
        self.write(self.evidence, {
            "permission_mode": permission_mode, "exit_code": 0, "cleanup_proven": True,
            "protected_unchanged": True, "local_appdata": str(self.cache),
            "protected_before": protected, "protected_after": protected,
            "protected_write_denials": list(protected), "unexpectedly_writable": [], "probe_errors": [],
            "worker_base_tip": "a" * 40, "worker_tip": "b" * 40,
            "profile_path": str(profile), "profile_sha256": sandbox.digest(profile),
            "profile_contract_sha256": sandbox.profile_contract_digest(self.harness, permission_mode),
            "profile_contract_sha256_after": sandbox.profile_contract_digest(self.harness, permission_mode),
            "argv": [str(self.native), "-c", 'windows.sandbox="elevated"', "sandbox",
                     "--include-managed-config", "--permission-profile", permission_mode],
        })
        self.write(sandbox.health_path(self.harness, permission_mode), {
            "schema": "windows-sandbox-health/v2", "status": "PASS", "probe_exit_code": 0,
            "codex_home": str(self.home), "local_appdata": str(self.cache),
            "native_codex_executable": str(self.native), "native_codex_sha256": sandbox.digest(self.native),
            "setup_marker_sha256": sandbox.digest(self.marker),
            "probe_evidence_path": str(self.evidence), "probe_evidence_sha256": sandbox.digest(self.evidence),
            "permission_mode": permission_mode,
            "profile_contract_sha256": sandbox.profile_contract_digest(self.harness, permission_mode),
            "backend_id": "codex-windows-elevated", "probe_profile_sha256": sandbox.digest(profile),
        })

    def check(self, *, permission_mode="worker-isolated", **kwargs):
        with patch.object(sandbox.os, "name", "nt"):
            return sandbox.require_ready(self.harness, workspace=self.workspace, executable=str(self.native),
                                         permission_mode=permission_mode, **kwargs)

    def test_missing_local_policy_does_not_disable_windows_gate(self):
        (self.harness / sandbox.POLICY).unlink()
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "setup_marker.json"):
            self.check()
        self.assertFalse(self.home.exists())

    def test_default_policy_accepts_matching_operator_proof(self):
        self.ready()
        (self.harness / sandbox.POLICY).unlink()
        self.assertIsNotNone(self.check())

    def test_root_generic_gate_retains_historical_v1_receipt_contract(self):
        self.ready()
        worker_path = sandbox.health_path(self.harness, "worker-isolated")
        value = json.loads(worker_path.read_text())
        for key in ("permission_mode", "profile_contract_sha256", "backend_id", "probe_profile_sha256"):
            value.pop(key)
        value["schema"] = "windows-sandbox-health/v1"
        path = sandbox.health_path(self.harness, None)
        self.write(path, value)
        self.write(self.evidence, {"exit_code": 0, "isolated_worker_command": "fixture"})
        value["probe_evidence_sha256"] = sandbox.digest(self.evidence)
        self.write(path, value)
        health = self.check(permission_mode=None)
        self.assertIsNone(health["permission_mode"])

    def test_non_windows_does_not_require_windows_setup(self):
        with patch.object(sandbox.os, "name", "posix"):
            self.assertIsNone(sandbox.require_ready(self.harness, workspace=self.workspace))

    def test_missing_marker_blocks_without_launching_or_writing(self):
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "setup_marker.json"):
            self.check()
        self.assertFalse(self.home.exists())

    def test_empty_marker_blocks_without_repairing_it(self):
        self.marker.parent.mkdir(parents=True)
        self.marker.touch()
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "empty"):
            self.check()
        self.assertEqual(b"", self.marker.read_bytes())

    def test_malformed_marker_blocks(self):
        self.marker.parent.mkdir(parents=True)
        self.marker.write_text('{"partial":', encoding="utf-8")
        with self.assertRaises(sandbox.SandboxPreflightError):
            self.check()

    def test_parseable_marker_is_insufficient_without_operator_probe(self):
        self.write(self.marker, {"version": 1})
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "windows-sandbox-health"):
            self.check()

    def test_matching_operator_proof_passes_without_processes(self):
        self.ready()
        health = self.check()
        self.assertEqual(sandbox.digest(self.marker), health["marker_sha256"])

    def test_upgraded_cli_requires_a_new_probe(self):
        self.ready()
        self.native.write_bytes(b"another CLI version")
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "changed after verification"):
            self.check()

    def test_marker_change_requires_diagnosis(self):
        self.ready()
        self.write(self.marker, {"version": 2})
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "changed after verification"):
            self.check()

    def test_other_cache_cannot_reuse_proof(self):
        self.ready()
        with patch.dict(os.environ, {"LOCALAPPDATA": str(self.base / "different-cache")}):
            with self.assertRaisesRegex(sandbox.SandboxPreflightError, "this profile, cache"):
                self.check()

    def test_receipt_is_bound_to_the_selected_permission_profile(self):
        self.ready()
        path = sandbox.health_path(self.harness, "worker-isolated")
        value = json.loads(path.read_text())
        value["permission_mode"] = "worker-workspace"
        self.write(path, value)
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "different worker permission mode"):
            self.check()

    def test_reduced_worker_profile_has_workspace_write_without_auth_deny(self):
        self.ready(permission_mode="worker-workspace")
        config = self.base / "worker-config.toml"
        config.write_text(
            'default_permissions = "worker-workspace"\n'
            '[permissions.worker-workspace]\nextends = ":workspace"\n'
            '[permissions.worker-workspace.filesystem]\n'
            '":workspace_roots" = { "." = "write" }\n"C:/git/private" = "write"\n',
            encoding="utf-8",
        )
        health = self.check(permission_mode="worker-workspace", isolation_config_path=config)
        self.assertEqual("worker-workspace", health["permission_mode"])
        self.assertEqual(sandbox.digest(config), health["worker_profile_sha256"])

    def test_reduced_health_cannot_use_strict_mode_receipt(self):
        self.ready()
        strict_health = json.loads(sandbox.health_path(self.harness, "worker-isolated").read_text())
        self.write(sandbox.health_path(self.harness, "worker-workspace"), strict_health)
        config = self.base / "worker-config.toml"
        config.write_text(
            'default_permissions = "worker-workspace"\n'
            '[permissions.worker-workspace]\nextends = ":workspace"\n'
            '[permissions.worker-workspace.filesystem]\n'
            '":workspace_roots" = { "." = "write" }\n', encoding="utf-8",
        )
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "different worker permission mode"):
            self.check(permission_mode="worker-workspace", isolation_config_path=config)

    def test_root_and_reduced_worker_receipts_coexist(self):
        self.ready(permission_mode="worker-workspace")
        root_path = sandbox.health_path(self.harness, None)
        root_value = {
            "schema": "windows-sandbox-health/v1", "status": "PASS", "probe_exit_code": 0,
            "codex_home": str(self.home), "local_appdata": str(self.cache),
            "native_codex_executable": str(self.native), "native_codex_sha256": sandbox.digest(self.native),
            "setup_marker_sha256": sandbox.digest(self.marker),
            "probe_evidence_path": str(self.evidence), "probe_evidence_sha256": sandbox.digest(self.evidence),
        }
        self.write(root_path, root_value)
        self.assertEqual("worker-workspace", self.check(permission_mode="worker-workspace")["permission_mode"])
        self.assertIsNone(self.check(permission_mode=None)["permission_mode"])

    def test_failed_probe_blocks(self):
        self.ready()
        path = sandbox.health_path(self.harness, "worker-isolated")
        value = json.loads(path.read_text())
        value["probe_exit_code"] = 5
        self.write(path, value)
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "did not succeed"):
            self.check()

    def test_run_block_prevents_a_fresh_lane_even_with_valid_proof(self):
        self.ready()
        self.write(self.workspace / sandbox.BLOCK, {"reason": "first setup failure"})
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "already recorded"):
            self.check()

    def test_policy_cannot_enable_retry_or_fallback(self):
        path = self.harness / sandbox.POLICY
        value = json.loads(path.read_text())
        value["automatic_fallback"] = True
        self.write(path, value)
        with self.assertRaisesRegex(sandbox.SandboxPreflightError, "launch policy"):
            self.check()

    def test_running_provider_detects_marker_invalidation(self):
        self.ready()
        health = self.check()
        stderr = self.base / "stderr.txt"
        stderr.write_text("", encoding="utf-8")
        self.marker.write_bytes(b"")
        self.assertIn("marker changed", sandbox.startup_failure(health, stderr, 0))

    def test_only_current_invocation_stderr_is_examined(self):
        self.ready()
        health = self.check()
        stderr = self.base / "stderr.txt"
        old = b"old sandbox setup required\n"
        stderr.write_bytes(old + b"current normal output\n")
        self.assertIsNone(sandbox.startup_failure(health, stderr, len(old)))
        with stderr.open("ab") as stream:
            stream.write(b"helper_sandbox_lock_failed\n")
        self.assertIn("reported sandbox", sandbox.startup_failure(health, stderr, len(old)))

    def test_setup_error_beyond_first_64k_is_not_missed(self):
        self.ready()
        health = self.check()
        stderr = self.base / "large-stderr.txt"
        stderr.write_bytes(b"ordinary diagnostics\n" * 5000 + b"helper_sandbox_lock_failed\n")
        self.assertIn("reported sandbox", sandbox.startup_failure(health, stderr, 0) or "")

    def test_markers_split_across_chunks_and_poll_cycles_are_detected(self):
        self.ready()
        health = self.check()
        stderr = self.base / "split-stderr.txt"
        stderr.write_bytes(b"." * (65536 - 10) + b"helper_san")
        self.assertIsNone(sandbox.startup_failure(health, stderr, 0))
        with stderr.open("ab") as stream:
            stream.write(b"dbox_lock_failed")
        self.assertIn("reported sandbox", sandbox.startup_failure(health, stderr, 0) or "")

    def test_diagnostic_truncation_is_unproven_health(self):
        self.ready()
        health = self.check()
        stderr = self.base / "truncated.txt"
        stderr.write_bytes(b"normal output" * 100)
        self.assertIsNone(sandbox.startup_failure(health, stderr, 0))
        stderr.write_bytes(b"short")
        self.assertIn("truncated", sandbox.startup_failure(health, stderr, 0) or "")

    def test_successive_failures_preserve_first_durable_cause(self):
        from orchestrator_harness import controller
        runtime = self.workspace / ".harness-runtime"
        lane = {"lane_id": "worker", "run_id": "first"}
        with patch.object(controller, "_write_status"), patch.object(controller, "_append_event"):
            controller._record_sandbox_failure(runtime, lane, SimpleNamespace(pid=123),
                                              SimpleNamespace(root_creation_time="first-child"), "first cause")
            controller._record_sandbox_failure(runtime, {**lane, "run_id": "second"}, SimpleNamespace(pid=124),
                                              SimpleNamespace(root_creation_time="second-child"), "second cause")
        block = json.loads((self.workspace / sandbox.BLOCK).read_text())
        self.assertEqual(("first", "first cause", 123), (block["run_id"], block["reason"], block["provider_pid"]))

    def test_controller_stops_only_owned_boundary_and_blocks_the_run(self):
        self._assert_controller_sandbox_failure(already_exited=False)

    def test_fast_exit_still_records_run_wide_sandbox_block(self):
        self._assert_controller_sandbox_failure(already_exited=True)

    def _assert_controller_sandbox_failure(self, *, already_exited):
        from orchestrator_harness import controller, git_source
        runtime = self.workspace / ".harness-runtime"
        worktree = runtime / "worktrees/epoch/worker"
        agent = worktree / ".agent-workspace"
        agent.mkdir(parents=True)
        prompt = agent / "worker-prompt.md"
        prompt.write_text("fixture", encoding="utf-8")
        lane = {"lane_id": "worker", "run_id": "one", "worktree_path": str(worktree),
                "transcript_path": str(agent / "transcript.jsonl"), "stderr_path": str(agent / "stderr.txt"),
                "last_message_path": str(agent / "last.txt"), "controller_status_path": str(agent / "status.json")}
        from orchestrator_harness.tests.support import prepare_lane_fixture, seed_provider_history
        lane["provider"] = {"id": "codex", "model": "fixture", "launch_config": {}}
        # Git prepares the owned fixture before the controller's no-process boundary.
        with patch("subprocess.Popen", self.real_popen):
            lane = prepare_lane_fixture(runtime, "epoch", lane)
            seed_provider_history(runtime, lane, epoch_id="epoch")
        child = MagicMock(pid=123, returncode=17)
        child.poll.return_value = 17 if already_exited else None
        child.take_job_handle.return_value = object()
        boundary = MagicMock(root_pid=123, root_creation_time="owned-child", process_group_id=None, session_id=None)
        boundary.record.return_value = {"root": {"pid": 123, "creation_time": "owned-child"}}
        boundary.cleanup.return_value = True
        health = {"harness_dir": str(self.harness)}
        binding = SimpleNamespace(build_argv=lambda **_kw: [str(self.native), "exec", "-"], parse_line=lambda _line: None)
        git_read = git_source._run

        def read_fixture_git(source, *args, **kwargs):
            # Keep the controller's real assignment validation; only its owned Git reads may spawn.
            self.assertEqual(lane["git_source"], source)
            with patch("subprocess.Popen", self.real_popen):
                return git_read(source, *args, **kwargs)

        with (patch.object(controller, "require_ready", return_value=health),
              patch.object(git_source, "_run", side_effect=read_fixture_git),
              patch.object(controller, "startup_failure", return_value="first sandbox setup error"),
              patch.object(controller.processes, "spawn_provider", return_value=child) as spawn,
              patch.object(controller.processes.ProcessBoundary, "for_process", return_value=boundary),
              patch.object(controller, "_append_event"), patch.object(controller, "_write_status"),
              patch.object(controller, "append_detail")):
            execution = controller._run_provider(
                runtime, "epoch", lane, {"provider": {"id": "codex", "model": "fixture", "launch_config": {}}},
                binding, prompt, sandbox_health=health,
            )
        spawn.assert_called_once()
        if already_exited:
            boundary.cleanup.assert_not_called()  # Caller proves final cleanup.
        else:
            boundary.cleanup.assert_called_once_with(force=True, timeout_seconds=10.0)
        child.kill.assert_not_called()
        self.assertTrue(execution.non_retryable_failure)
        block = json.loads((self.workspace / sandbox.BLOCK).read_text())
        self.assertEqual((123, "owned-child", "one"),
                         (block["provider_pid"], block["provider_creation_time"], block["run_id"]))
        self.assertFalse(block["automatic_retry"])


if __name__ == "__main__":
    unittest.main()
