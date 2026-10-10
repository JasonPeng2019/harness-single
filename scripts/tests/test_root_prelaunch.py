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
            event: [{"hooks": [{"command": "python .codex/hooks/" + filename}]}]
            for event, filename in (("Stop", "orchestrator_harness_stop.py"), ("PostToolUse", "orchestrator_harness_post_tool_use.py"))
        }})
        for name in preparation.HOOK_FILES[2:]:
            path = self.workspace / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# installed fixture hook\n", encoding="utf-8")

    def prepared(self) -> None:
        self.installed()
        self.write(self.workspace / preparation.RECEIPT, {
            "schema": preparation.SCHEMA, "status": "PASS", "run_id": "fixture",
            "harness_dir": str(self.harness), "workspace": str(self.workspace),
            "prepared_at_utc": "2026-10-09T00:00:00+00:00",
            "hook_sha256": preparation.check_installed(self.harness, self.workspace),
            "harness_config_sha256": preparation.digest(self.harness / "harness-config.json"),
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
        self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {"state": "CLOSED"})
        with patch.object(preparation.subprocess, "run") as process:
            result = preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)
        process.assert_not_called()
        self.assertEqual("2026-10-09T00:00:00+00:00", result["prepared_at_utc"])

    def test_closed_runtime_resume_checks_same_stopped_service(self) -> None:
        from orchestrator_harness import public_checks
        self.prepared()
        service = {"schema": "worker-public-check-service/v1", "pid": 123,
                   "creation_time": "fixture", "root": str(self.workspace),
                   "harness": str(self.harness), "status": "READY"}
        receipt = preparation.read(self.workspace / preparation.RECEIPT)
        self.write(self.workspace / preparation.RECEIPT, {**receipt, "public_check_service": service})
        self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {"state": "CLOSED"})
        with (patch.object(public_checks, "policy", return_value={"schema": "public-check-policy/v2"}),
              patch.object(public_checks, "validate_service", return_value={**service, "status": "STOPPED"}) as validate):
            preparation.validate_prepared_root(self.harness, self.workspace, "fixture", resuming=True)
            validate.assert_called_once_with(self.workspace, self.harness, runtime_closed=True)
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
                self.write(self.workspace / ".harness-runtime/RUNTIME_STATE.json", {"state": state})
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


if __name__ == "__main__":
    unittest.main()
