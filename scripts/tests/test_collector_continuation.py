"""Actual resume preparation with protected worker evidence and owned OS processes.

These are deterministic offline fixtures, not live provider or health proof.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_codex_usage as collector
import token_ledger as ledger
from orchestrator_harness import controller, processes


class CollectorContinuationTests(unittest.TestCase):
    def setUp(self):
        # Use shallow OS Temp; never a project/worktree-relative temporary tree.
        self.temporary = tempfile.TemporaryDirectory(prefix="cc-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.addCleanup(self.retain_evidence)
        self.harness = self.base / "h"
        self.product = self.base / "p"
        self.manager = self.base / "m"
        self.run = self.base / "r"
        self.runtime = self.product / ".harness-runtime"
        for directory in (self.harness, self.manager, self.run):
            directory.mkdir(parents=True)
        self.write(self.harness / "harness-config.json", {
            "root_workspace": str(self.product), "manager_workspace": str(self.manager)})
        self.task = {"schema": ledger.TASK_SCHEMA, "run_id": "task", "task": "unfinished ROOT review",
                     "arm": "harness", "budget_tokens": None, "interruption_policy": "planned_resume",
                     "root_run_dir": str(self.run), "harness_runtime": str(self.runtime)}
        self.task_path = self.run / "task.json"
        self.write(self.task_path, self.task)
        self.root_boundary = self.owned_exit("root")
        self.controller_boundary = self.owned_exit("controller")
        self.worker_boundary = self.owned_exit("worker")
        self.launch = {"schema": ledger.RUN_SCHEMA, "run_id": "task", "task": self.task["task"],
                       "arm": "harness", "budget_tokens": None, "harness_runtime": str(self.runtime),
                       "harness_dir": str(self.harness), "workspace": str(self.manager),
                       "model": "fixture", "reasoning_effort": "high", "service_tier": "priority",
                       "sandbox": "workspace-write", "trust_project_hooks": True, "watchdog_hours": 1,
                       "root_boundary_required": True,
                       "started_at_utc": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=120)).isoformat(),
                       "command": self.argv()}
        self.write(self.run / "launch.json", self.launch)
        self.final = {**self.launch, "ended_at_utc": ledger.utc_now(), "exit_code": 0,
                      "elapsed_seconds": 120, "cleanup_proven": True, "process_boundary": self.root_boundary}
        self.write(self.run / "manifest.json", self.final)
        self.write_usage(self.run / "codex-events.jsonl", "saved-root", 30)
        self.epoch = self.runtime / "epochs/e"
        self.native = self.epoch / "lanes/w"
        self.worktree = self.runtime / "worktrees/e/w"
        self.worktree.mkdir(parents=True)
        self.control = self.native / "runs/worker-run"
        self.control.mkdir(parents=True)
        self.transcript = self.control / "provider-transcript.jsonl"
        self.damaged_bytes = b'{"type":"thread.started","thread_id":"worker-session"}\n{"type":\xffbroken}\n'
        self.transcript.write_bytes(self.damaged_bytes)
        self.lane = {"schema": "lane/v1", "lane_id": "w", "run_id": "worker-run", "lifecycle": "retired",
                     "worktree_path": str(self.worktree), "control_dir": str(self.control),
                     "process": self.controller_boundary["root"],
                     "provider": {"id": "codex", "model": "fixture", "launch_config": {
                         "reasoning_effort": "high", "service_tier": "priority"}},
                     "controller_status_path": str(self.control / "controller.status.json"),
                     "attempts_path": str(self.control / "controller.attempts.jsonl")}
        self.write(self.native / "lane.json", self.lane)
        self.status = {"schema": "controller-status/v1", "lane_id": "w", "run_id": "worker-run",
                       "controller_identity": self.controller_boundary["root"], "controller_state": "exited",
                       "provider_state": {"state": "exited", **self.worker_boundary["root"]},
                       "process_boundary": self.worker_boundary, "cleanup_proven": True}
        self.write(Path(self.lane["controller_status_path"]), self.status)
        self.write(self.control / "invocation.json", {"schema": "controller-invocation/v1",
                   "lane_id": "w", "run_id": "worker-run", "cwd": str(self.worktree),
                   "provider": self.lane["provider"], "paths": {}})
        (self.control / "controller.events.jsonl").write_text(json.dumps({"event_type": "provider_started",
            "run_id": "worker-run", "ts": ledger.utc_now()}) + "\n", encoding="utf-8")
        # Use the actual native receipt writer; it also records the malformed capture.
        controller._append_attempt(self.lane, attempt_number=1, argv=self.argv(), session_id="worker-session",
            prompt_path=self.control / "prompt.md", paths={"transcript": self.transcript,
            "stderr": self.control / "provider-stderr.txt"}, exit_code=0,
            result_state="invalid", cleanup_proven=True, provider_started=True)
        self.closed_at = ledger.utc_now()
        self.final["ended_at_utc"] = self.closed_at
        self.write(self.run / "manifest.json", self.final)
        self.write(self.epoch / "epoch-state.json", {"schema": "epoch-state/v1", "lifecycle": "closed",
                   "closed_at": self.closed_at})
        self.write(self.runtime / "RUNTIME_STATE.json", {"state": "CLOSED"})

    def retain_evidence(self):
        destination = os.environ.get("COLLECTOR_CONTINUATION_EVIDENCE")
        if destination:
            shutil.copytree(self.base, Path(destination) / self.id().rsplit(".", 1)[-1])

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")

    def argv(self):
        return ["codex", "exec", "-m", "fixture", "-c", 'model_reasoning_effort="high"',
                "-c", 'service_tier="priority"']

    def write_usage(self, path, session, total):
        path.write_text(json.dumps({"type": "thread.started", "thread_id": session}) + "\n" +
                        json.dumps({"type": "turn.completed", "usage": {"input_tokens": total - 10,
                                    "output_tokens": 10}}) + "\n", encoding="utf-8")

    def owned_exit(self, name):
        output = self.base / (name + "-stdout.txt")
        errors = self.base / (name + "-stderr.txt")
        with output.open("w", encoding="utf-8") as out, errors.open("w", encoding="utf-8") as err, \
                open(os.devnull, "r") as source:
            child = processes.spawn_provider([sys.executable, "-B", "-c", "print('owned offline fixture')"],
                cwd=self.base, stdin=source, stdout=out, stderr=err, env=os.environ.copy())
            take_job = getattr(child, "take_job_handle", None)
            boundary = processes.ProcessBoundary.for_process(
                child.pid, windows_job_handle=take_job() if callable(take_job) else None)
            try:
                resume = getattr(child, "resume", None)
                if callable(resume):
                    resume()
                native_exit = child.wait(timeout=10)
            finally:
                cleaned = boundary.cleanup(force=True, timeout_seconds=10)
                record = boundary.record()
            self.assertEqual(0, native_exit)
            self.assertTrue(cleaned, record)
            self.assertTrue(processes.process_boundary_is_gone(record), record)
            self.write(self.base / (name + "-cleanup.json"), {
                "native_exit": native_exit, "cleanup_proven": cleaned, "process_boundary": record})
            return record

    def recover(self, **overrides):
        self.gap = {"invocation_id": "task.e.w.worker-run.attempt-1", "epoch_id": "e", "lane_id": "w",
                    "lane_run_id": "worker-run", "epoch_closed_at": self.closed_at,
                    "transcript_sha256": hashlib.sha256(self.transcript.read_bytes()).hexdigest(),
                    "reason": "Preserve malformed sealed worker transcript; token usage is unknown",
                    "operator_observed_absent_at_utc": ledger.utc_now(), **overrides}
        self.write(self.run / "usage-gaps.json", {"schema": ledger.USAGE_GAPS_SCHEMA,
                   "run_id": "task", "gaps": [self.gap]})

    def args(self):
        return argparse.Namespace(resume_task_file=str(self.task_path), continue_open_runtime=False)

    def set_budget(self, budget):
        self.task["budget_tokens"] = budget
        self.launch["budget_tokens"] = budget
        self.final["budget_tokens"] = budget
        for path, value in ((self.task_path, self.task), (self.run / "launch.json", self.launch),
                            (self.run / "manifest.json", self.final)):
            self.write(path, value)

    def test_malformed_sealed_worker_requires_explicit_recovery(self):
        with self.assertRaisesRegex(ledger.LedgerError, "malformed transcript"):
            collector.prepare_resume(self.args())
        self.assertEqual(self.damaged_bytes, self.transcript.read_bytes())
        self.assertFalse((self.run / "segments").exists())

    def test_recovery_prepares_correct_saved_session_and_preserves_unknown_usage(self):
        self.recover()
        args = self.args()
        segment, number, session, limit = collector.prepare_resume(args)
        self.assertEqual((self.run / "segments/0002", 2, "saved-root", 1), (segment, number, session, limit))
        self.assertAlmostEqual(1 - 120 / 3600, args.watchdog_hours)
        for field in ("model", "reasoning_effort", "service_tier", "sandbox", "trust_project_hooks"):
            self.assertEqual(self.launch[field], getattr(args, field))
        self.assertEqual(str(self.manager), args.cwd)
        snapshot = ledger.build_snapshot(self.task_path)
        self.write(self.base / "snapshot.json", snapshot)
        self.write(self.base / "prepared.json", {"segment": str(segment), "number": number,
                   "session": session, "original_watchdog_hours": limit, "args": vars(args)})
        self.assertEqual([], snapshot["errors"])
        self.assertEqual(0, snapshot["in_flight_invocations"])
        self.assertEqual(1, snapshot["unknown_usage_invocations"])
        self.assertEqual(30, snapshot["known_total_tokens"])
        self.assertFalse(snapshot["token_total_is_exact"])
        worker = next(row for row in snapshot["rows"] if row["role"] == "worker")
        self.assertEqual("terminated_usage_unknown", worker["status"])
        self.assertIsNone(worker["charged_tokens"])
        self.assertIn("malformed transcript", worker["accounting_error"])
        self.assertEqual(self.damaged_bytes, self.transcript.read_bytes())
        archive = self.base / "archive"
        summary = ledger.finalize(self.task_path, archive)
        self.assertFalse(summary["token_total_is_exact"])
        archived = json.loads((archive / "ledger.json").read_text(encoding="utf-8"))
        worker = next(row for row in archived["rows"] if row["role"] == "worker")
        self.assertEqual(self.damaged_bytes, (archive / worker["archived_transcript"]).read_bytes())

    def test_truncated_json_tail_and_non_object_worker_recovery(self):
        for damaged in (b'{"type":"thread.started","thread_id":"worker-session"}\n{"type":', b'[]\n'):
            with self.subTest(damaged=damaged):
                self.transcript.write_bytes(damaged)
                with self.assertRaises(ledger.LedgerError):
                    collector.prepare_resume(self.args())
                self.recover()
                self.assertEqual("saved-root", collector.prepare_resume(self.args())[2])
                self.assertEqual(damaged, self.transcript.read_bytes())
                (self.run / "usage-gaps.json").unlink()

    def test_changed_transcript_or_receipt_identity_is_refused(self):
        for override in ({"transcript_sha256": "0" * 64}, {"lane_run_id": "other"},
                         {"epoch_closed_at": "other"}, {"invocation_id": "unknown"}):
            with self.subTest(override=override):
                self.recover(**override)
                with self.assertRaises(ledger.LedgerError):
                    collector.prepare_resume(self.args())

    def test_recovery_cannot_hide_countable_usage(self):
        self.write_usage(self.transcript, "worker-session", 17)
        self.recover()
        with self.assertRaisesRegex(ledger.LedgerError, "usage-gap"):
            collector.prepare_resume(self.args())

    def test_recovery_cannot_suppress_invalid_usage_values_or_conflicting_session_ids(self):
        for content in ('{"type":"turn.completed","usage":{"input_tokens":-1}}\n',
                        '{"type":"thread.started","thread_id":"one"}\n'
                        '{"type":"thread.started","thread_id":"two"}\n'):
            with self.subTest(content=content):
                self.transcript.write_text(content, encoding="utf-8")
                self.recover()
                with self.assertRaises(ledger.LedgerError):
                    collector.prepare_resume(self.args())

    def test_strict_accounting_still_counts_worker_and_rejects_malformed_bytes(self):
        self.write_usage(self.transcript, "worker-session", 17)
        snapshot = ledger.build_snapshot(self.task_path)
        self.assertEqual(47, snapshot["known_total_tokens"])
        self.assertTrue(snapshot["token_total_is_exact"])
        self.transcript.write_bytes(self.damaged_bytes)
        with self.assertRaises(ledger.LedgerError):
            ledger.build_snapshot(self.task_path)
        with self.assertRaises(ledger.LedgerError):
            ledger.extract_usage(self.transcript)

    def test_unsealed_root_and_malformed_root_remain_refused(self):
        self.recover()
        root = self.run / "codex-events.jsonl"
        original = root.read_bytes()
        root.write_bytes(self.damaged_bytes)
        with self.assertRaisesRegex(ledger.LedgerError, "malformed transcript"):
            collector.prepare_resume(self.args())
        root.write_bytes(original)
        (self.run / "manifest.json").unlink()
        with self.assertRaisesRegex(ledger.LedgerError, "not sealed"):
            collector.prepare_resume(self.args())

    def test_receipt_cannot_override_runtime_cleanup_or_watchdog(self):
        self.recover()
        cases = [(self.runtime / "RUNTIME_STATE.json", {"state": "OPEN"}),
                 (self.native / "lane.json", {**self.lane, "lifecycle": "running"}),
                 (Path(self.lane["controller_status_path"]), {**self.status, "cleanup_proven": False}),
                 (Path(self.lane["controller_status_path"]), {**self.status, "run_id": "other"}),
                 (self.native / "lane.json", {**self.lane, "process": {"pid": 1, "creation_time": "unknown"}}),
                 (self.epoch / "epoch-state.json", {"lifecycle": "open"}),
                 (self.run / "manifest.json", {**self.final, "cleanup_proven": False}),
                 (self.run / "manifest.json", {**self.final, "watchdog_expired": True}),
                 (self.run / "manifest.json", {**self.final, "elapsed_seconds": 3600})]
        for path, value in cases:
            with self.subTest(path=path, value=value):
                original = path.read_bytes()
                try:
                    self.write(path, value)
                    with self.assertRaises(ledger.LedgerError):
                        collector.prepare_resume(self.args())
                finally:
                    path.write_bytes(original)

    def test_existing_terminated_unmetered_worker_recovery_still_prepares_resume(self):
        self.transcript.write_text('{"type":"thread.started","thread_id":"worker-session"}\n', encoding="utf-8")
        Path(self.lane["attempts_path"]).unlink()
        self.recover()
        self.assertEqual("saved-root", collector.prepare_resume(self.args())[2])
        snapshot = ledger.build_snapshot(self.task_path)
        self.assertEqual([], snapshot["errors"])
        self.assertEqual(1, snapshot["unknown_usage_invocations"])
        self.assertEqual(0, snapshot["in_flight_invocations"])

    def test_receipt_and_retired_label_cannot_hide_a_real_live_controller(self):
        self.recover()
        with (self.base / "live-stdout.txt").open("w") as out, \
                (self.base / "live-stderr.txt").open("w") as err:
            child = subprocess.Popen([sys.executable, "-B", "-c", "import time; time.sleep(30)"],
                                     stdin=subprocess.DEVNULL, stdout=out, stderr=err)
            try:
                identity = processes.process_identity(child.pid)
                self.assertIsNotNone(identity)
                self.write(self.native / "lane.json", {**self.lane, "process": identity})
                with self.assertRaisesRegex(ledger.LedgerError, "close the native harness runtime"):
                    collector.prepare_resume(self.args())
                self.assertIsNone(child.poll())
            finally:
                child.terminate()  # Held native Popen handle, never a name/PID search.
                native_exit = child.wait(timeout=10)
            self.assertTrue(processes.identity_exit_proven(**identity))
            self.write(self.base / "live-cleanup.json", {"identity": identity, "native_exit": native_exit,
                       "exit_proven": processes.identity_exit_proven(**identity)})

    def test_recovery_still_requires_prelaunch_receipt_before_any_provider_attempt(self):
        self.recover()
        prompt = self.base / "prompt.txt"
        prompt.write_text("continue unfinished ROOT review", encoding="utf-8")
        args = collector.parser().parse_args(["resume", "--task-file", str(self.task_path),
                                               "--prompt-file", str(prompt)])
        with self.assertRaisesRegex(ledger.LedgerError, "pre-launch"):
            collector.run_codex(args)
        self.assertFalse((self.run / "segments").exists())

    def test_unknown_usage_cannot_authorize_a_capped_resume_or_gate(self):
        self.recover()
        self.set_budget(1000)
        with self.assertRaisesRegex(ledger.LedgerError, "budget"):
            collector.prepare_resume(self.args())
        snapshot = ledger.build_snapshot(self.task_path)
        self.assertFalse(snapshot["gate_allows_new_work"])
        self.assertIsNone(snapshot["remaining_tokens"])
        self.assertIsNone(snapshot["overshoot_tokens"])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(3, collector.gate(argparse.Namespace(task_file=str(self.task_path))))

    def test_exhausted_known_budget_remains_a_resume_refusal(self):
        self.write_usage(self.transcript, "worker-session", 17)
        self.set_budget(47)
        with self.assertRaisesRegex(ledger.LedgerError, "budget"):
            collector.prepare_resume(self.args())


if __name__ == "__main__":
    unittest.main()
