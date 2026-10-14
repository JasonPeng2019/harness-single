"""Count actual provider starts; preserve unknown and contradictory evidence."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import token_ledger
from orchestrator_harness import controller


class WorkerUsageDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "runtime/worktrees/epoch/worker/.agent-workspace"
        self.workspace.mkdir(parents=True)
        self.provider = {"id": "codex", "model": "fixture", "launch_config": {
            "reasoning_effort": "high", "service_tier": "priority"}}
        self.lane = {"lane_id": "worker", "run_id": "refused", "provider": self.provider,
                     "worktree_path": str(self.workspace.parent),
                     "attempts_path": str(self.workspace / "controller.attempts.jsonl")}
        self.task = {"run_id": "task", "arm": "harness", "task": "fixture", "budget_tokens": 1000,
                     "root_run_dir": str(self.root / "root-run"), "harness_runtime": str(self.root / "runtime")}
        launch = {"schema": token_ledger.RUN_SCHEMA, **self.task, "model": "fixture",
                  "reasoning_effort": "high", "service_tier": "priority",
                  "started_at_utc": "2026-10-11T00:00:00+00:00",
                  "command": ["codex", "exec", "-m", "fixture", "-c", 'model_reasoning_effort="high"',
                              "-c", 'service_tier="priority"']}
        Path(self.task["root_run_dir"]).mkdir()
        (Path(self.task["root_run_dir"]) / "launch.json").write_text(json.dumps(launch), encoding="utf-8")
        self.write_invocation()

    def test_root_cleanup_failure_keeps_usage_but_cannot_grant_launch_credit(self):
        run = Path(self.task["root_run_dir"])
        launch = json.loads((run / "launch.json").read_text(encoding="utf-8"))
        launch["root_boundary_required"] = True
        (run / "launch.json").write_text(json.dumps(launch), encoding="utf-8")
        identity = {"pid": 41, "creation_time": "windows-filetime:134361777213898726"}
        boundary = {"kind": "windows-job", "root": identity, "processes": [identity]}
        final = {**launch, "ended_at_utc": "2026-10-11T00:00:01+00:00", "exit_code": 0,
                 "elapsed_seconds": 1, "cleanup_proven": False, "process_boundary": boundary}
        (run / "codex-events.jsonl").write_text(
            json.dumps({"type": "thread.started", "thread_id": "fixture-root"}) + "\n" +
            json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1, "cached_input_tokens": 0,
                                                               "output_tokens": 2}}) + "\n", encoding="utf-8")
        task_path = run / "task.json"
        task_path.write_text(json.dumps({"schema": token_ledger.TASK_SCHEMA, **self.task}), encoding="utf-8")
        for attested, gone in ((False, True), (True, False), (True, True)):
            with self.subTest(attested=attested, gone=gone):
                (run / "manifest.json").write_text(json.dumps({**final, "cleanup_proven": attested}), encoding="utf-8")
                with patch.object(token_ledger, "_worker_invocations", return_value=[]), \
                     patch("orchestrator_harness.processes.process_boundary_is_gone", return_value=gone):
                    snapshot = token_ledger.build_snapshot(task_path)
                self.assertEqual(3, snapshot["known_total_tokens"], snapshot)
                self.assertEqual(attested and gone, snapshot["gate_allows_new_work"], snapshot)
                self.assertEqual(not (attested and gone), bool(snapshot["errors"]), snapshot)

    def write_invocation(self):
        (self.workspace / "invocation.json").write_text(json.dumps({
            "schema": "controller-invocation/v1", "lane_id": "worker", "run_id": self.lane["run_id"],
            "provider": self.provider, "paths": {}}), encoding="utf-8")

    def receipt(self, *, started=False, cleanup=True, argv=None):
        controller._append_attempt(self.lane, attempt_number=1, argv=argv,
            session_id=None, prompt_path=self.workspace / "prompt.md",
            paths=controller._attempt_paths(self.lane, 1), exit_code=None,
            result_state="invalid", cleanup_proven=cleanup, validation_error="fixture",
            provider_started=started)

    def start(self, run):
        (self.workspace / "controller.events.jsonl").write_text(json.dumps({
            "event_type": "provider_started", "run_id": run, "ts": "2026-10-11T01:00:00+00:00"}) + "\n", encoding="utf-8")

    def test_proven_no_provider_receipt_does_not_count_as_invocation(self):
        self.receipt()
        self.assertEqual([], token_ledger._worker_invocations(self.task))

    def test_unknown_cleanup_receipt_still_requires_start_evidence(self):
        self.receipt(cleanup=False)
        with self.assertRaises(token_ledger.LedgerError):
            token_ledger._worker_invocations(self.task)

    def test_no_provider_flag_cannot_hide_a_matching_start(self):
        self.receipt()
        self.start("refused")
        with self.assertRaises(token_ledger.LedgerError):
            token_ledger._worker_invocations(self.task)

    def test_later_started_run_pairs_with_its_own_receipt(self):
        self.receipt()
        self.lane["run_id"] = "started"
        self.write_invocation()
        self.receipt(started=True, argv=["codex", "exec", "resume", "saved"])
        self.start("started")
        rows = token_ledger._worker_invocations(self.task)
        self.assertEqual(1, len(rows))
        self.assertEqual("started", rows[0]["lane_run_id"])
        self.assertEqual(["codex", "exec", "resume", "saved"], rows[0]["launch_argv"])

    def test_started_receipt_cannot_pair_with_another_run(self):
        self.receipt(started=True, argv=["codex", "exec"])
        self.start("different")
        with self.assertRaises(token_ledger.LedgerError):
            token_ledger._worker_invocations(self.task)


if __name__ == "__main__":
    unittest.main()
