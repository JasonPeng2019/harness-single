from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_codex_usage as collector
import prepare_root_launch as preparation


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

    def write(self, path: Path, value: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def installed(self) -> None:
        self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {"state": "OPEN"})
        self.write(self.workspace / ".codex/orchestrator-harness-binding.json", {
            "schema": "harness-hook-binding/v1", "role": "root", "provider_id": "codex",
            "harness_root": str(self.harness), "runtime_root": str(self.workspace / ".harness-runtime"),
        })
        self.write(self.workspace / ".codex/hooks.json", {"hooks": {
            event: [{**({"matcher": ".*"} if event == "PostToolUse" else {}),
                     "hooks": [{"type": "command", "command": "python .codex/hooks/" + filename}]}]
            for event, filename in (("Stop", "orchestrator_harness_stop.py"), ("PostToolUse", "orchestrator_harness_post_tool_use.py"))
        }})
        for name in preparation.HOOK_FILES[2:]:
            path = self.workspace / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# installed fixture hook\n", encoding="utf-8")
        (self.workspace / ".codex/config.toml").write_text('[features]\nhooks = true\n', encoding='utf-8')

    def prepared(self) -> None:
        self.installed()
        self.write(self.workspace / preparation.RECEIPT, {
            "schema": preparation.SCHEMA, "status": "PASS", "run_id": "fixture",
            "harness_dir": str(self.harness), "workspace": str(self.workspace),
            "prepared_at_utc": "2026-10-09T00:00:00+00:00",
            "hook_sha256": preparation.check_installed(self.harness, self.workspace),
            "harness_config_sha256": preparation.digest(self.harness / "harness-config.json"),
            "configuration_sha256": preparation.configuration_hashes(self.harness, self.workspace),
            "workspace_identity": preparation.workspace_identity(self.workspace),
        })

    def test_fresh_preparation_runs_setup_then_service_exactly_once(self) -> None:
        from orchestrator_harness import public_checks
        calls = []
        service = {"pid": 123, "creation_time": "fixture", "policy_sha256": "policy"}
        health = {"health_receipt_sha256": "fixture-health", "marker_sha256": "fixture-marker"}

        def setup_once(command, **kwargs):
            calls.append("setup")
            self.assertEqual("setup", command[-1])
            self.installed()
            return SimpleNamespace(returncode=0, stdout='{"ok":true}')

        def start_once(*_args):
            calls.append("service")
            return service

        with (patch.object(preparation, "require_ready", return_value=health),
              patch.object(preparation.subprocess, "run", side_effect=setup_once) as setup,
              patch.object(public_checks, "start_service", side_effect=start_once) as start,
              patch.object(public_checks, "policy", return_value={"schema": "public-check-policy/v2"}),
              patch.object(public_checks, "validate_service", return_value=service)):
            first = preparation.prepare(self.harness, self.workspace, "fixture")
            again = preparation.prepare(self.harness, self.workspace, "fixture")
        self.assertEqual(first, again)
        self.assertEqual(["setup", "service"], calls)
        setup.assert_called_once()
        start.assert_called_once()
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.assertEqual(service, receipt["public_check_service"])
        self.assertEqual(health, receipt["windows_sandbox_health"])

    def test_prepared_sandbox_proof_cannot_change_before_root_launch(self) -> None:
        self.prepared()
        with patch.object(preparation, "require_ready", return_value={"health_receipt_sha256": "changed"}):
            with self.assertRaisesRegex(ValueError, "health changed after"):
                preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_prepared_service_identity_cannot_change_before_root_launch(self) -> None:
        from orchestrator_harness import public_checks
        self.prepared()
        with (patch.object(public_checks, "policy", return_value={"schema": "public-check-policy/v2"}),
              patch.object(public_checks, "validate_service", return_value={"pid": 999})):
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
        self.assertEqual(result["prepared_at_utc"], "2026-10-09T00:00:00+00:00")

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
        self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {"state": "CLOSED"})
        with self.assertRaisesRegex(ValueError, "not OPEN"):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture")

    def test_closed_runtime_resume_revalidates_without_repeating_setup(self) -> None:
        self.prepared()
        self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {
            "schema": "runtime-state/v1", "state": "CLOSED", "cleanup_owners": {"monitor": None, "service": None}})
        with patch.object(preparation.subprocess, "run") as process:
            result = preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)
        process.assert_not_called()
        self.assertEqual("2026-10-09T00:00:00+00:00", result["prepared_at_utc"])

    def test_closed_runtime_resume_checks_same_stopped_service(self) -> None:
        from orchestrator_harness import public_checks
        self.prepared()
        service = {"schema": "worker-public-check-service/v1", "pid": 123,
                   "creation_time": "windows-filetime:134361000000000000", "root": str(self.workspace),
                   "harness": str(self.harness), "status": "READY", "startup_id": "3" * 32,
                   "policy_sha256": "4" * 64, "cleanup_proven": True}
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.write(self.workspace / preparation.RECEIPT, {**receipt, "public_check_service": service})
        self.write(public_checks.home(self.workspace) / "service.json", {**service, "status": "STOPPED"})
        self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {
            "schema": "runtime-state/v1", "state": "CLOSED",
            "cleanup_owners": {"monitor": None, "service": {field: service[field] for field in
                ("pid", "creation_time", "root", "harness", "policy_sha256", "startup_id")}}})
        with (patch.object(public_checks, "policy", return_value={"schema": "public-check-policy/v2"}),
              patch.object(public_checks, "validate_service", return_value={**service, "status": "STOPPED"}) as validate):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)
            self.assertEqual(2, validate.call_count)
            self.assertTrue(all(call.args == (self.workspace, self.harness)
                                and call.kwargs == {"runtime_closed": True} for call in validate.call_args_list))
            validate.return_value = {**service, "status": "STOPPED", "pid": 456}
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
                self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {
                    "schema": "runtime-state/v1", "state": state, "cleanup_owners": {"monitor": None, "service": None}})
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
