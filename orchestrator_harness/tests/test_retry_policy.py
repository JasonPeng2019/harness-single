"""Harness combined automatic/manual retry boundary regressions."""
import json
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import controller, resume
from orchestrator_harness import retry_policy as policy
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
        self.protected = policy.initialize_history(self.root, self.lane)

    def history(self, *rows):
        facts = [{"schema": "worker-provider-history/v1", "lane_id": "lane",
                  "worktree": os.path.normcase(str(self.root.resolve())), "provider": {"id": "codex"}}]
        seen = set()
        for row in rows:
            if "run_id" not in row:
                continue
            token = hashlib.sha256((row["run_id"] + str(row["attempt"])).encode()).hexdigest()[:32]
            identity = {"invocation_id": token, "run_id": row["run_id"], "attempt": row["attempt"]}
            if token not in seen:
                facts.append({**identity, "kind": "RESERVED"})
                seen.add(token)
            facts.append({**identity, "kind": "COMPLETED" if row["provider_started"] else "NO_PROVIDER",
                          "provider_started": row["provider_started"], "cleanup_proven": True,
                          "session_id": "native"})
        self.protected.write_text("\n".join(json.dumps(row) for row in facts) + "\n", encoding="utf-8")

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
        self.protected.unlink()
        with self.assertRaises(ValueError):
            completed_provider_invocations(self.lane)
        self.protected.write_text('{"truncated":', encoding="utf-8")
        with self.assertRaises(ValueError):
            completed_provider_invocations(self.lane)
        self.history({"schema": "controller-attempts/v1"})
        with self.assertRaises(ValueError):
            completed_provider_invocations(self.lane)

    def test_resume_rejects_exhausted_budget_before_mutating_evidence(self):
        self.history(self.launch("old", 1), self.launch("old", 2))
        original = self.protected.read_bytes()
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
        self.assertEqual(original, self.protected.read_bytes())

    def test_crash_between_reservation_and_completion_does_not_refund(self):
        policy.reserve_invocation(self.root, self.lane)
        self.lane["run_id"] = "run-after-crash"
        with self.assertRaisesRegex(ValueError, "unproven"):
            policy.reserve_invocation(self.root, self.lane)

    def test_proven_no_provider_failure_does_not_spend_budget(self):
        token = policy.reserve_invocation(self.root, self.lane)
        policy.record_outcome(self.root, self.lane, token, provider_started=False,
                              cleanup_proven=True, session_id=None)
        self.assertEqual(0, completed_provider_invocations(self.lane))
        policy.reserve_invocation(self.root, self.lane)

    def test_start_fact_cannot_be_refunded_or_rewritten(self):
        token = policy.reserve_invocation(self.root, self.lane)
        policy.record_started(self.root, self.lane, token)
        before = self.protected.read_bytes()
        with self.assertRaises(ValueError):
            policy.record_outcome(self.root, self.lane, token, provider_started=False,
                                  cleanup_proven=True, session_id=None)
        self.assertEqual(before, self.protected.read_bytes())

    def test_one_manual_same_session_retry_then_third_rejected(self):
        for run in ("initial", "manual-retry"):
            self.lane["run_id"] = run
            token = policy.reserve_invocation(self.root, self.lane)
            policy.record_started(self.root, self.lane, token)
            policy.record_outcome(self.root, self.lane, token, provider_started=True,
                                  cleanup_proven=True, session_id="native")
            self.lane["session"] = {"session_id": "native"}
        self.assertEqual(2, completed_provider_invocations(self.lane))
        self.lane["run_id"] = "third"
        with self.assertRaisesRegex(ValueError, "exhausted"):
            policy.reserve_invocation(self.root, self.lane)

    def test_duplicate_launch_same_run_and_changed_native_session_rejected(self):
        token = policy.reserve_invocation(self.root, self.lane)
        policy.record_outcome(self.root, self.lane, token, provider_started=True,
                              cleanup_proven=True, session_id="native")
        self.lane["session"] = {"session_id": "native"}
        with self.assertRaisesRegex(ValueError, "already invoked"):
            policy.reserve_invocation(self.root, self.lane)
        self.lane["run_id"] = "new-run"
        self.lane["session"] = {"session_id": "replacement"}
        with self.assertRaisesRegex(ValueError, "does not match"):
            policy.reserve_invocation(self.root, self.lane)

    def test_deleted_protected_history_cannot_be_reinitialized(self):
        self.protected.unlink()
        with self.assertRaisesRegex(ValueError, "lost protected"):
            policy.initialize_history(self.root, self.lane)

    def test_concurrent_reservations_allow_only_one_start(self):
        from concurrent.futures import ThreadPoolExecutor
        def reserve():
            try:
                return policy.reserve_invocation(self.root, self.lane)
            except ValueError:
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: reserve(), range(2)))
        self.assertEqual(1, sum(result is not None for result in results))

    def test_controller_cannot_launch_a_third_provider(self):
        from orchestrator_harness import lane_authority
        self.lane["prompt_path"] = str(self.workspace / "worker-prompt.md")
        self.history(self.launch("old", 1), self.launch("old", 2))
        with (
            patch.object(controller, "find_harness_root", return_value=self.root),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=self.root)),
            patch.object(controller, "find_active_lane", return_value=("epoch", self.lane)),
            patch.object(lane_authority, "read_invocation", return_value={"lane_id": "lane", "run_id": "run-new", "provider": {"id": "codex"}, "exclusive_resources": []}),
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
