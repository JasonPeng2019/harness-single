"""Harness combined automatic/manual retry boundary regressions."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import controller, resume
from orchestrator_harness.retry_policy import completed_provider_invocations, WORKER_RETRY_LIMIT


class RetryPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / ".agent-workspace"
        self.workspace.mkdir()
        self.path = self.workspace / "controller.attempts.jsonl"
        self.lane = {
            "lane_id": "lane", "run_id": "run-new", "lifecycle": "review_pending",
            "worktree_path": str(self.root), "attempts_path": str(self.path),
            "controller_status_path": str(self.workspace / "controller.status.json"),
            "controller_events_path": str(self.workspace / "controller.events.jsonl"),
            "provider": {"id": "codex"}, "session": {}, "process": {},
        }

    def history(self, *rows):
        self.path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    def launch(self, run, attempt):
        return {"run_id": run, "attempt": attempt, "provider_started": True}

    def test_initial_lane_and_pre_provider_failures_cost_zero(self):
        self.assertEqual(0, completed_provider_invocations(self.lane))
        self.history({"schema": "controller-attempts/v1"}, {"run_id": "old", "attempt": 1, "provider_started": False})
        self.assertEqual(0, completed_provider_invocations(self.lane))

    def test_auto_correction_and_manual_run_id_reset_share_budget(self):
        self.history(self.launch("old", 1), self.launch("old", 2))
        self.assertEqual(2, completed_provider_invocations(self.lane))
        self.history(self.launch("old", 1), self.launch("new", 1))
        self.assertEqual(2, completed_provider_invocations(self.lane))

    def test_duplicate_rows_and_headers_do_not_add_launches(self):
        row = self.launch("old", 1)
        self.history({"schema": "controller-attempts/v1"}, row, row)
        self.assertEqual(1, completed_provider_invocations(self.lane))

    def test_unknown_or_corrupt_session_history_fails_closed(self):
        self.lane["session"] = {"session_id": "native"}
        with self.assertRaises(ValueError):
            completed_provider_invocations(self.lane)
        self.path.write_text('{"truncated":', encoding="utf-8")
        with self.assertRaises(ValueError):
            completed_provider_invocations(self.lane)
        self.history({"schema": "controller-attempts/v1"})
        with self.assertRaises(ValueError):
            completed_provider_invocations(self.lane)

    def test_resume_rejects_exhausted_budget_before_mutating_evidence(self):
        self.history(self.launch("old", 1), self.launch("old", 2))
        original = self.path.read_bytes()
        with (
            patch.object(resume, "find_harness_root", return_value=self.root),
            patch.object(resume, "load_config", return_value=SimpleNamespace(runtime_root=self.root)),
            patch.object(resume, "find_active_lane", return_value=("epoch", self.lane)),
            patch.object(resume, "_has_valid_acceptance_chain", return_value=False),
            patch.object(resume, "_live_controller", return_value=False),
            patch.object(resume, "_clear_prior_run") as clear,
            patch.object(resume, "_read_task_card") as task,
        ):
            result = resume.run_resume(lane_id="lane", resume_task_card="unused.json")
        self.assertEqual(WORKER_RETRY_LIMIT, result["code"])
        clear.assert_not_called()
        task.assert_not_called()
        self.assertEqual(original, self.path.read_bytes())

    def test_controller_cannot_launch_a_third_provider(self):
        self.history(self.launch("old", 1), self.launch("old", 2))
        with (
            patch.object(controller, "find_harness_root", return_value=self.root),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=self.root)),
            patch.object(controller, "find_active_lane", return_value=("epoch", self.lane)),
            patch.object(controller, "read_record", return_value={"lane_id": "lane", "run_id": "run-new", "provider": {"id": "codex"}}),
            patch.object(controller.processes, "process_identity", return_value={"pid": 7, "creation_time": "controller"}),
            patch.object(controller, "_write_status") as status,
            patch.object(controller, "_append_event"),
            patch.object(controller, "update_lane", return_value=self.lane),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "acquire_leases"),
            patch.object(controller, "release_leases") as release,
            patch.object(controller, "_run_provider") as provider,
        ):
            self.assertEqual(0, controller.run_controller("lane"))
        provider.assert_not_called()
        release.assert_called_once()
        self.assertTrue(status.call_args.args[1]["retry_limit_reached"])


if __name__ == "__main__":
    unittest.main()
