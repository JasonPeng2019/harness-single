"""Duplicate controller dispatch must not yield plausible worker usage."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import token_ledger
from orchestrator_harness import controller


class DuplicateDispatchUsageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root_run = self.root / "root-run"
        self.root_run.mkdir()
        self.runtime = self.root / "runtime"
        self.workspace = self.runtime / "worktrees/epoch/worker/.agent-workspace"
        self.workspace.mkdir(parents=True)
        self.provider = {"id": "codex", "model": "fixture", "launch_config": {
            "reasoning_effort": "high", "service_tier": "priority"}}
        self.lane = {"lane_id": "worker", "run_id": "run-1", "provider": self.provider,
                     "worktree_path": str(self.workspace.parent),
                     "attempts_path": str(self.workspace / "controller.attempts.jsonl")}
        self.task = {"run_id": "task", "arm": "harness", "task": "fixture", "budget_tokens": 1000,
                     "root_run_dir": str(self.root_run), "harness_runtime": str(self.runtime)}
        self.write_root()
        self.write_invocation()

    def write_root(self):
        launch = {"schema": token_ledger.RUN_SCHEMA, **self.task, "model": "fixture",
                  "reasoning_effort": "high", "service_tier": "priority",
                  "started_at_utc": "2026-10-11T00:00:00+00:00",
                  "command": ["codex", "exec", "-m", "fixture", "-c", 'model_reasoning_effort="high"',
                              "-c", 'service_tier="priority"']}
        (self.root_run / "launch.json").write_text(json.dumps(launch), encoding="utf-8")
        final = {**launch, "ended_at_utc": "2026-10-11T02:00:00+00:00", "exit_code": 0,
                 "elapsed_seconds": 7200}
        (self.root_run / "manifest.json").write_text(json.dumps(final), encoding="utf-8")

    def write_invocation(self):
        (self.workspace / "invocation.json").write_text(json.dumps({
            "schema": "controller-invocation/v1", "lane_id": "worker", "run_id": self.lane["run_id"],
            "provider": self.provider, "paths": {}}), encoding="utf-8")

    def write_attempt(self, number, run):
        controller._append_attempt(self.lane, attempt_number=number, argv=["codex", "exec", "-m", "fixture"],
            session_id=None, prompt_path=self.workspace / "prompt.md",
            paths=controller._attempt_paths(self.lane, number), exit_code=1,
            result_state="invalid", cleanup_proven=True, validation_error="fixture",
            provider_started=True)
        # Controller's writer uses the current lane run, so fix the serialized identity for
        # the synthetic manual-retry fixture where a separate controller run is represented.
        attempts = self.workspace / "controller.attempts.jsonl"
        lines = [json.loads(line) for line in attempts.read_text(encoding="utf-8").splitlines()]
        lines[-1]["run_id"] = run
        attempts.write_text("".join(json.dumps(row) + "\n" for row in lines), encoding="utf-8")

    def events(self, rows):
        path = self.workspace / "controller.events.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    def test_actual_discovery_rejects_same_run_duplicate_controller_dispatch(self):
        self.write_attempt(1, "run-1")
        events = [
            {"event_type": "controller_started", "run_id": "run-1", "ts": "2026-10-11T00:10:00+00:00"},
            {"event_type": "provider_started", "run_id": "run-1", "ts": "2026-10-11T00:10:01+00:00"},
            {"event_type": "controller_started", "run_id": "run-1", "ts": "2026-10-11T00:10:02+00:00"},
        ]
        self.events(events)
        with self.assertRaisesRegex(token_ledger.LedgerError, "duplicate controller dispatch"):
            token_ledger._worker_invocations(self.task)

    def test_distinct_run_manual_retry_is_still_discovered(self):
        self.write_attempt(1, "run-1")
        self.lane["run_id"] = "run-2"
        self.write_invocation()
        self.write_attempt(2, "run-2")
        events = [
            {"event_type": "controller_started", "run_id": "run-1", "ts": "2026-10-11T00:10:00+00:00"},
            {"event_type": "provider_started", "run_id": "run-1", "ts": "2026-10-11T00:10:01+00:00"},
            {"event_type": "controller_started", "run_id": "run-2", "ts": "2026-10-11T00:11:00+00:00"},
            {"event_type": "provider_started", "run_id": "run-2", "ts": "2026-10-11T00:11:01+00:00"},
        ]
        self.events(events)
        rows = token_ledger._worker_invocations(self.task)
        self.assertEqual(["run-1", "run-2"], [row["lane_run_id"] for row in rows])

    def test_malformed_complete_event_refuses_and_partial_tail_is_ignored(self):
        path = self.events([])
        path.write_text('{broken}\n', encoding="utf-8")
        with self.assertRaises(token_ledger.LedgerError):
            token_ledger._worker_invocations(self.task)
        path.write_text('{broken}', encoding="utf-8")
        self.assertEqual([], token_ledger._worker_invocations(self.task))


if __name__ == "__main__":
    unittest.main()
