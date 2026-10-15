"""Retained-worktree accounting with native authority/attempt/history writers.

Offline fixtures use shallow OS Temp, real linked Git worktrees and finite owned
Python processes. No model/provider is launched and no lifecycle source is patched.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_codex_usage as collector
import token_ledger as ledger
from scripts.tests import test_collector_continuation as continuation
from orchestrator_harness import bootstrap, git_source, lane_authority, retry_policy


class CrossEpochUsageTests(unittest.TestCase):
    def setUp(self):
        self.f = f = continuation.CollectorContinuationTests()
        f.id = self.id  # Evidence belongs to this case, not the helper's runTest.
        self.addCleanup(self.cleanup_fixture)
        f.setUp()
        self.addCleanup(self.retain_evidence)
        # Replace only this owned fixture's abbreviated authority with genuine
        # native staging. ROOT/accounting records still use its real exit proofs.
        for path in f.control.iterdir():
            path.unlink()
        f.control.rmdir()
        self.git(f.product, "init", "-q")
        (f.product / ".gitignore").write_text(".harness-runtime/\n", encoding="utf-8")
        self.git(f.product, "add", ".gitignore")
        self.git(f.product, "-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                 "commit", "-qm", "base")
        self.git(f.product, "worktree", "add", "-qb", "lane/w/work", str(f.worktree))
        (f.worktree / ".codex").mkdir()
        (f.worktree / ".codex/config.toml").write_text("[features]\nhooks = true\n", encoding="utf-8")
        self.card = {"schema": "project-task-card/v1", "task": "Account for retained retry",
                     "acceptance_criteria": ["All actual invocations counted"], "deliverables": ["Evidence"],
                     "reason_for_acceptance_and_deliverables": "Verify actual saved-session accounting"}
        initial = {"schema": "lane/v1", "lane_id": "w", "run_id": "worker-run", "lifecycle": "retired",
                   "worktree_path": str(f.worktree), "branch": "lane/w/work", "provider": f.lane["provider"],
                   "exclusive_resources": [], "process": f.controller_boundary["root"]}
        f.lane = self.stage("e", initial)
        f.control = Path(f.lane["control_dir"])
        f.transcript = Path(f.lane["transcript_path"])
        f.transcript.write_bytes(f.damaged_bytes)
        retry_policy.initialize_history(f.runtime, f.lane)
        self.finish("e", f.lane, f.controller_boundary, f.worker_boundary, resumed=False)
        f.write(f.native / "lane.json", f.lane)
        self.first_lane_bytes = (f.native / "lane.json").read_bytes()
        self.retry_controller = f.owned_exit("retry-controller")
        self.retry_worker = f.owned_exit("retry-worker")
        self.second = self.stage("later", {**f.lane, "run_id": "retry-run",
                            "resume_from_epoch_id": "e", "session": {"session_id": "worker-session"},
                            "process": self.retry_controller["root"]})
        self.second_transcript = Path(self.second["transcript_path"])
        f.write_usage(self.second_transcript, "worker-session", 42)
        self.finish("later", self.second, self.retry_controller, self.retry_worker, resumed=True)
        f.write(f.runtime / "epochs/later/lanes/w/lane.json", self.second)
        self.seal()
        self.assertFalse((f.runtime / "worktrees/later").exists())
        self.assertEqual(2, retry_policy.completed_provider_invocations(self.second, f.runtime))
        self.assertEqual(self.first_lane_bytes, (f.native / "lane.json").read_bytes())

    def cleanup_fixture(self):
        self.assertTrue(self.f.doCleanups(), "continuation fixture cleanup failed")

    def git(self, root, *args):
        result = subprocess.run(["git", "-C", str(root), *args], capture_output=True,
                                env=git_source.environment(), timeout=20)
        self.assertEqual(0, result.returncode, result.stderr.decode("utf-8", "replace"))

    def stage(self, epoch, lane):
        f = self.f
        bootstrap._write_worker_prompt(f.worktree, self.card, managed=False)
        bootstrap._write_invocation(f.worktree, lane_id="w", run_id=lane["run_id"], provider_id="codex",
            model="fixture", launch_config=lane["provider"]["launch_config"], exclusive_resources=[])
        lane = lane_authority.stage_authority(f.runtime, epoch, lane, self.card)
        self.assertEqual(lane["run_id"], lane_authority.read_invocation(lane)["run_id"])
        return lane

    def finish(self, epoch, lane, controller_boundary, worker_boundary, *, resumed):
        f = self.f
        token = retry_policy.reserve_invocation(f.runtime, lane)
        retry_policy.record_started(f.runtime, lane, token, process_boundary=worker_boundary)
        lane["_history_token"], lane["_history_runtime"] = token, str(f.runtime)
        command = f.argv()
        if resumed:
            command[2:2] = ["--ignore-user-config", "resume", "worker-session"]
        control = Path(lane["control_dir"])
        (control / "controller.events.jsonl").write_text(json.dumps({"event_type": "provider_started",
            "run_id": lane["run_id"], "ts": ledger.utc_now()}) + "\n", encoding="utf-8")
        # Real writer: native session, byte ranges, usage observations and history.
        from orchestrator_harness import controller
        controller._append_attempt(lane, attempt_number=1, argv=command, session_id="worker-session",
            prompt_path=Path(lane["prompt_path"]), paths={"transcript": Path(lane["transcript_path"]),
            "stderr": Path(lane["stderr_path"])}, exit_code=0, result_state="invalid", cleanup_proven=True,
            provider_started=True)
        lane.pop("_history_token")
        lane.pop("_history_runtime")
        lane["session"] = {"session_id": "worker-session"}
        f.write(Path(lane["controller_status_path"]), {"schema": "controller-status/v1", "lane_id": "w",
            "run_id": lane["run_id"], "controller_identity": controller_boundary["root"],
            "controller_state": "exited", "provider_state": {"state": "exited", **worker_boundary["root"]},
            "process_boundary": worker_boundary, "cleanup_proven": True})
        f.write(f.runtime / "epochs" / epoch / "epoch-state.json", {
            "schema": "epoch-state/v1", "lifecycle": "closed", "closed_at": f.closed_at})

    def seal(self):
        f = self.f
        f.final["ended_at_utc"] = ledger.utc_now()
        f.write(f.run / "manifest.json", f.final)

    def retain_evidence(self):
        destination = os.environ.get("CROSS_EPOCH_USAGE_EVIDENCE")
        if destination:
            shutil.copytree(self.f.base, Path(destination) / self.id().rsplit(".", 1)[-1])

    def snapshot(self):
        result = ledger.build_snapshot(self.f.task_path)
        self.f.write(self.f.base / "snapshot.json", result)
        return result

    def known_first(self):
        self.f.write_usage(self.f.transcript, "worker-session", 17)

    def command(self, name):
        completed = subprocess.run([sys.executable, "-B", str(Path(collector.__file__)), name,
            "--task-file", str(self.f.task_path)], capture_output=True, text=True, timeout=20)
        self.f.write(self.f.base / (name + "-command.json"), {"command": completed.args,
            "stdout": completed.stdout, "stderr": completed.stderr, "exit": completed.returncode})
        return completed

    def test_actual_discovery_counts_both_authority_epochs_once(self):
        self.known_first()
        rows = ledger.discover_invocations(ledger.load_task(self.f.task_path))
        workers = [r for r in rows if r["role"] == "worker"]
        self.assertEqual([("e", "worker-run"), ("later", "retry-run")],
                         [(r["epoch_id"], r["lane_run_id"]) for r in workers])
        self.assertEqual(2, len({r["invocation_id"] for r in workers}))
        self.assertEqual({str(self.f.transcript), str(self.second_transcript)},
                         {r["transcript_path"] for r in workers})

    def test_snapshot_delta_and_public_capped_gate_include_retry(self):
        self.known_first()
        self.f.set_budget(60)  # ROOT 30 + R1 17 < 60; including R2 gives 72.
        snapshot = self.snapshot()
        self.assertEqual(72, snapshot["known_total_tokens"], snapshot)
        self.assertTrue(snapshot["token_total_is_exact"], snapshot)
        self.assertFalse(snapshot["gate_allows_new_work"], snapshot)
        workers = [r for r in snapshot["rows"] if r["role"] == "worker"]
        self.assertEqual([17, 25], [r["charged_tokens"] for r in workers])
        self.assertEqual(17, workers[1]["previous_cumulative_total_tokens"])
        gate = self.command("gate")
        self.assertEqual(3, gate.returncode, gate.stderr)
        self.assertEqual(72, json.loads(gate.stdout)["known_total_tokens"])
        with self.assertRaisesRegex(ledger.LedgerError, "budget"):
            collector.prepare_resume(self.f.args())

    def test_receipted_malformed_first_run_and_retry_prepare_uncapped_honestly(self):
        self.f.recover()
        prepared = collector.prepare_resume(self.f.args())
        snapshot = self.snapshot()
        self.assertEqual(3, len(snapshot["rows"]), snapshot)
        self.assertEqual([], snapshot["errors"], snapshot)
        self.assertEqual(2, snapshot["unknown_usage_invocations"], snapshot)
        self.assertEqual(30, snapshot["known_total_tokens"], snapshot)
        self.assertFalse(snapshot["token_total_is_exact"], snapshot)
        workers = [r for r in snapshot["rows"] if r["role"] == "worker"]
        self.assertTrue(all(r["charged_tokens"] is None for r in workers), workers)
        self.assertEqual(self.f.gap, workers[1]["usage_gap"])
        self.assertEqual((2, "saved-root", 1), prepared[1:])
        self.f.write(self.f.base / "prepared.json", {"segment": str(prepared[0]), "number": prepared[1],
                                                    "session": prepared[2], "watchdog": prepared[3]})
        self.assertEqual(0, self.command("snapshot").returncode)
        self.assertEqual(0, self.command("gate").returncode)
        archive = self.f.base / "archive"
        self.assertFalse(ledger.finalize(self.f.task_path, archive)["token_total_is_exact"])
        self.assertEqual(self.f.damaged_bytes, self.f.transcript.read_bytes())

    def test_recovered_retry_cannot_authorize_capped_work(self):
        self.f.recover()
        self.f.set_budget(1000)
        snapshot = self.snapshot()
        self.assertEqual(2, snapshot["unknown_usage_invocations"])
        self.assertIsNone(snapshot["remaining_tokens"])
        self.assertFalse(snapshot["gate_allows_new_work"])
        with self.assertRaisesRegex(ledger.LedgerError, "budget"):
            collector.prepare_resume(self.f.args())
        self.assertEqual(3, self.command("gate").returncode)

    def test_without_original_receipt_malformed_first_run_still_refuses(self):
        with self.assertRaisesRegex(ledger.TranscriptError, "malformed transcript"):
            collector.prepare_resume(self.f.args())

    def test_explicit_epoch_selects_authority_epoch_and_requires_baseline(self):
        self.known_first()
        task = ledger.load_task(self.f.task_path)
        for epoch, run in (("e", "worker-run"), ("later", "retry-run")):
            with self.subTest(epoch=epoch):
                rows = ledger._worker_invocations({**task, "epoch_id": epoch})
                self.assertEqual([(epoch, run)], [(r["epoch_id"], r["lane_run_id"]) for r in rows])
        self.f.task["epoch_id"] = "later"
        self.f.write(self.f.task_path, self.f.task)
        snapshot = self.snapshot()
        self.assertIn("resumed thread has no prior baseline", "; ".join(snapshot["errors"]))
        self.assertFalse(snapshot["gate_allows_new_work"])

    def test_retry_outside_root_window_is_excluded(self):
        self.known_first()
        self.retime_retry(dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1))
        snapshot = self.snapshot()
        self.assertEqual(47, snapshot["known_total_tokens"])
        self.assertEqual(2, len(snapshot["rows"]))

    def retime_retry(self, timestamp):
        events = Path(self.second["controller_events_path"])
        record = json.loads(events.read_text(encoding="utf-8"))
        record["ts"] = timestamp.isoformat()
        events.write_text(json.dumps(record) + "\n", encoding="utf-8")

    def test_retry_in_paused_interval_is_refused(self):
        self.known_first()
        f = self.f
        end = dt.datetime.fromisoformat(f.final["ended_at_utc"])
        segment = f.run / "segments/0002"
        launch = {**f.launch, "segment_number": 2, "resume_session_id": "saved-root",
                  "started_at_utc": (end + dt.timedelta(seconds=60)).isoformat(),
                  "command": ["codex", "exec", "resume", "saved-root", *f.argv()[2:]]}
        f.write(segment / "launch.json", launch)
        f.write(segment / "manifest.json", {**f.final, **launch,
            "ended_at_utc": (end + dt.timedelta(seconds=120)).isoformat()})
        f.write_usage(segment / "codex-events.jsonl", "saved-root", 50)
        self.retime_retry(end + dt.timedelta(seconds=30))
        with self.assertRaisesRegex(ledger.LedgerError, "paused interval"):
            self.snapshot()

    def test_missing_or_misbound_later_authority_cannot_silently_disappear(self):
        self.known_first()
        invocation = Path(self.second["invocation_path"])
        original = invocation.read_bytes()
        for change in (None, {"cwd": str(self.f.base)}, {"lane_id": "other"}, {"run_id": "other"}):
            with self.subTest(change=change):
                if change is None:
                    invocation.unlink()
                else:
                    self.f.write(invocation, {**json.loads(original), **change})
                try:
                    with self.assertRaises(ledger.LedgerError):
                        self.snapshot()
                finally:
                    invocation.write_bytes(original)

    def test_retry_missing_terminal_usage_remains_error(self):
        self.known_first()
        self.second_transcript.write_text('{"type":"thread.started","thread_id":"worker-session"}\n')
        snapshot = self.snapshot()
        self.assertTrue(snapshot["errors"], snapshot)
        self.assertFalse(snapshot["token_total_is_exact"])
        self.assertFalse(snapshot["gate_allows_new_work"])

    def test_recovery_without_attempt_receipt_uses_original_native_session_history(self):
        Path(self.f.lane["attempts_path"]).unlink()
        self.f.recover()
        snapshot = self.snapshot()
        self.assertEqual([], snapshot["errors"], snapshot)
        self.assertEqual(2, snapshot["unknown_usage_invocations"])
        self.assertEqual("saved-root", collector.prepare_resume(self.f.args())[2])
        workers = [r for r in snapshot["rows"] if r["role"] == "worker"]
        self.assertEqual(["worker-session", "worker-session"], [r["session_id"] for r in workers])
        self.assertIn(self.f.lane["provider_history_path"], workers[0]["source_manifest_paths"])

    def mutate_attempt(self, lane, **changes):
        path = Path(lane["attempts_path"])
        records = ledger.read_lines(path)
        records[-1].update(changes)
        path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")

    def test_recovery_cannot_cross_native_session_or_resume_target(self):
        self.f.recover()
        attempt_path = Path(self.second["attempts_path"])
        original = attempt_path.read_bytes()
        transcript = self.second_transcript.read_bytes()
        for change in ({"session_id": "different"}, {"session_id": None},
                       {"argv": ["codex", "exec", "resume", "different", *self.f.argv()[2:]]}):
            with self.subTest(change=change):
                try:
                    self.mutate_attempt(self.second, **change)
                    if change.get("session_id") is None and "session_id" in change:
                        # Journal still supplies identity; a changed transcript
                        # must fail the actual session link.
                        self.f.write_usage(self.second_transcript, "different", 42)
                    with self.assertRaises(ledger.LedgerError):
                        collector.prepare_resume(self.f.args())
                finally:
                    attempt_path.write_bytes(original)
                    self.second_transcript.write_bytes(transcript)
        first_attempt = Path(self.f.lane["attempts_path"])
        first_bytes = first_attempt.read_bytes()
        try:
            self.mutate_attempt(self.f.lane, session_id="different")
            with self.assertRaisesRegex(ledger.LedgerError, "native session"):
                collector.prepare_resume(self.f.args())
        finally:
            first_attempt.write_bytes(first_bytes)

    def test_recovery_requires_retry_cleanup_and_countable_retry_transcript(self):
        self.f.recover()
        status = Path(self.second["controller_status_path"])
        original = status.read_bytes()
        self.f.write(status, {**json.loads(original), "cleanup_proven": False})
        try:
            with self.assertRaisesRegex(ledger.LedgerError, "close the native harness runtime"):
                collector.prepare_resume(self.f.args())
            self.assertTrue(self.snapshot()["errors"])
        finally:
            status.write_bytes(original)
        self.second_transcript.write_text('{"type":"thread.started","thread_id":"worker-session"}\n')
        with self.assertRaisesRegex(ledger.LedgerError, "not sealed and countable"):
            collector.prepare_resume(self.f.args())

    def test_protected_start_and_attempt_cannot_name_a_different_run(self):
        self.known_first()
        events = Path(self.second["controller_events_path"])
        attempt = Path(self.second["attempts_path"])
        originals = (events.read_bytes(), attempt.read_bytes())
        try:
            self.mutate_attempt(self.second, run_id="other")
            with self.assertRaisesRegex(ledger.LedgerError, "identity differs"):
                self.snapshot()
            event = json.loads(events.read_text(encoding="utf-8"))
            event["run_id"] = "other"
            events.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ledger.LedgerError, "identity differs"):
                self.snapshot()
        finally:
            events.write_bytes(originals[0])
            attempt.write_bytes(originals[1])

    def test_redirected_later_control_directory_is_refused(self):
        self.known_first()
        control = Path(self.second["control_dir"])
        saved = control.with_name(control.name + "-saved")
        control.rename(saved)
        try:
            if os.name == "nt":
                result = subprocess.run(["cmd", "/c", "mklink", "/J", str(control), str(saved)],
                                        capture_output=True, text=True, timeout=10)
                self.f.write(self.f.base / "redirect-command.json", {"command": result.args,
                    "stdout": result.stdout, "stderr": result.stderr, "exit": result.returncode})
                self.assertEqual(0, result.returncode, result.stderr)
            else:
                control.symlink_to(saved, target_is_directory=True)
            with self.assertRaisesRegex(ledger.LedgerError, "redirected"):
                self.snapshot()
        finally:
            if control.is_symlink():
                control.unlink()
            elif control.exists():
                control.rmdir()  # Remove only the owned junction entry.
            saved.rename(control)

    def test_saved_session_replay_dedup_and_decreasing_totals_stay_strict(self):
        self.known_first()
        self.second_transcript.write_bytes(self.f.transcript.read_bytes())
        snapshot = self.snapshot()
        workers = [r for r in snapshot["rows"] if r["role"] == "worker"]
        self.assertEqual(47, snapshot["known_total_tokens"])
        self.assertEqual(0, workers[1]["charged_tokens"])
        self.assertEqual(workers[0]["invocation_id"], workers[1]["duplicate_of"])
        self.f.write_usage(self.second_transcript, "worker-session", 16)
        snapshot = self.snapshot()
        self.assertIn("cumulative session total decreased", "; ".join(snapshot["errors"]))
        self.assertFalse(snapshot["gate_allows_new_work"])

    def test_valid_retry_in_next_root_window_uses_that_parent_and_session_delta(self):
        self.known_first()
        f = self.f
        start = dt.datetime.fromisoformat(f.final["ended_at_utc"]) + dt.timedelta(seconds=60)
        segment = f.run / "segments/0002"
        launch = {**f.launch, "segment_number": 2, "resume_session_id": "saved-root",
                  "started_at_utc": start.isoformat(),
                  "command": ["codex", "exec", "resume", "saved-root", *f.argv()[2:]]}
        f.write(segment / "launch.json", launch)
        f.write(segment / "manifest.json", {**f.final, **launch,
            "ended_at_utc": (start + dt.timedelta(seconds=60)).isoformat()})
        f.write_usage(segment / "codex-events.jsonl", "saved-root", 50)
        self.retime_retry(start + dt.timedelta(seconds=10))
        snapshot = self.snapshot()
        self.assertEqual([], snapshot["errors"], snapshot)
        self.assertEqual(92, snapshot["known_total_tokens"])
        retry = next(r for r in snapshot["rows"] if r["lane_run_id"] == "retry-run")
        self.assertEqual("task.root.segment-2", retry["parent_id"])
        self.assertEqual(25, retry["charged_tokens"])


if __name__ == "__main__":
    unittest.main()
