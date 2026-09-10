from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

from orchestrator_harness import controller, leases, monitor, review, scan_watch
from orchestrator_harness.controller import ProviderExecution
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import CURRENT_EPOCH_SCHEMA, MANAGER_QUEUE_SCHEMA
from orchestrator_harness.manager_queue import (
    append_watch_delivery,
    promote_event,
    read_manager_queue,
)
from orchestrator_harness.records import atomic_write_json, read_jsonl


class Addendum3ProductTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime = Path(self.temp.name) / "runtime"
        atomic_write_json(
            self.runtime / "CURRENT_EPOCH.json",
            {
                "schema": CURRENT_EPOCH_SCHEMA,
                "epoch_id": "epoch-1",
                "queue_id": "queue-1",
            },
        )
        atomic_write_json(
            self.runtime / "manager" / "QUEUE.json",
            {
                "schema": MANAGER_QUEUE_SCHEMA,
                "epoch_id": "epoch-1",
                "queue_id": "queue-1",
                "events": [],
            },
        )

    def _lane(self) -> dict[str, object]:
        worktree = self.runtime / "worktree" / "lane-1"
        outbox = worktree / ".agent-workspace" / "manager-notifications"
        outbox.mkdir(parents=True)
        (worktree / ".agent-workspace" / "processed-notifications").mkdir()
        return {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "provider": {"id": "codex", "model": "model"},
            "attempts_path": str(worktree / ".agent-workspace" / "attempts.jsonl"),
        }

    def test_outbox_promotes_before_archive_and_retries_to_one_event(self) -> None:
        lane = self._lane()
        outbox = Path(str(lane["worktree_path"])) / ".agent-workspace" / "manager-notifications"
        notice_path = outbox / "notice-1.json"
        atomic_write_json(
            notice_path,
            {
                "schema": "manager-notice/v1",
                "notice_id": "signal-1",
                "signal_id": "worker-signal-1",
                "severity": "error",
                "event_class": "WORKER_ESCALATION",
                "summary": "ROOT action needed",
            },
        )
        with patch.object(monitor, "shutil") as shutil_mock:
            shutil_mock.move.side_effect = OSError("archive unavailable")
            diagnostics = monitor._consume_outbox(self.runtime, "epoch-1", lane)
        self.assertTrue(diagnostics)
        self.assertTrue(notice_path.is_file(), "promotion failure/archive failure must retain source")
        event = read_manager_queue(self.runtime)["events"][0]
        self.assertEqual("worker-signal-1", event["data"]["signal_id"])
        self.assertEqual("WORKER_ESCALATION", event["event_class"])
        with patch.object(monitor, "shutil", wraps=__import__("shutil")):
            monitor._consume_outbox(self.runtime, "epoch-1", lane)
        self.assertFalse(notice_path.exists())
        self.assertEqual(1, len(read_manager_queue(self.runtime)["events"]))

    def test_malformed_outbox_is_retained_and_reported(self) -> None:
        lane = self._lane()
        outbox = Path(str(lane["worktree_path"])) / ".agent-workspace" / "manager-notifications"
        bad_path = outbox / "malformed.json"
        atomic_write_json(bad_path, ["not", "a", "notice"])
        diagnostics = monitor._consume_outbox(self.runtime, "epoch-1", lane)
        self.assertTrue(any(item["kind"] == "outbox_inspection" for item in diagnostics))
        self.assertTrue(bad_path.is_file())
        self.assertEqual([], read_manager_queue(self.runtime)["events"])

    def test_managed_watch_delivery_is_deduplicated_by_session_and_queue(self) -> None:
        event = promote_event(
            self.runtime,
            event_type="LANE_RESULT_INVALID",
            lane_id="lane-1",
            run_id="run-1",
            summary="invalid result",
            event_class="LANE_RESULT_INVALID",
            severity="error",
            data={"signal_id": "signal-2"},
        )
        queue = read_manager_queue(self.runtime)
        self.assertIsNotNone(
            scan_watch._watch_event(
                queue, root_session_id="root-1", binding_id="codex"
            )
        )
        self.assertTrue(
            append_watch_delivery(
                self.runtime,
                event["event_id"],
                root_session_id="root-1",
                binding_id="codex",
            )
        )
        queue = read_manager_queue(self.runtime)
        self.assertIsNone(
            scan_watch._watch_event(
                queue, root_session_id="root-1", binding_id="codex"
            )
        )
        self.assertIsNotNone(
            scan_watch._watch_event(
                queue, root_session_id="root-2", binding_id="codex"
            )
        )

    def test_force_release_audit_retains_holder_and_reports_partial_terminal_audit(self) -> None:
        path = leases.lease_path(self.runtime, "resource-1")
        holder = {
            "schema": leases.LEASE_SCHEMA,
            "resource_id": "resource-1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "pid": 41,
            "creation_time": "created-1",
        }
        atomic_write_json(path, holder)
        with (
            patch.object(leases, "identity_matches", return_value=False),
            patch.object(leases, "process_alive", return_value=False),
            patch.object(leases, "append_jsonl", side_effect=[None, OSError("audit full")]),
        ):
            with self.assertRaises(leases.LeaseError) as raised:
                leases.force_release_lease_audited(self.runtime, "resource-1")
        self.assertEqual(leases.FORCE_RELEASE_AUDIT_TERMINAL_FAILED, raised.exception.code)
        self.assertTrue(raised.exception.released)
        self.assertFalse(path.exists())

    def test_monitor_heartbeat_records_zero_and_diagnostics_atomically(self) -> None:
        monitor_path = self.runtime / "monitor" / "MONITOR.json"
        atomic_write_json(
            monitor_path,
            {
                "schema": "monitor/v1",
                "config_identity": "config-1",
                "pid": 41,
                "creation_time": "created-1",
            },
        )
        monitor._heartbeat(
            self.runtime,
            "config-1",
            0,
            [{"kind": "outbox_promotion", "error": "queue unavailable"}],
        )
        record = monitor.read_monitor_record(self.runtime)
        self.assertEqual(0, record["watched_lane_count"])
        self.assertEqual("degraded", record["health"])
        self.assertEqual("outbox_promotion", record["diagnostics"][0]["kind"])

    def test_rejected_review_signal_is_consumed_without_creating_another_event(self) -> None:
        from orchestrator_harness import resume

        event = promote_event(
            self.runtime,
            event_type="LANE_RESUME_REQUIRED",
            lane_id="lane-1",
            run_id="run-old",
            summary="review rejected",
            event_class="LANE_RESUME_REQUIRED",
            severity="warning",
        )
        resume._consume_resume_signal(self.runtime, "lane-1", "run-old")
        queue = read_manager_queue(self.runtime)
        self.assertEqual("COMPLETE", queue["events"][0]["state"])
        self.assertTrue(queue["events"][0]["history"][-1]["summary"])

    def test_monitor_recovers_malformed_pair_and_one_lost_review_event(self) -> None:
        from orchestrator_harness.lanes import LANE_SCHEMA

        lane_dir = self.runtime / "epochs" / "epoch-1" / "lanes" / "lane-1"
        worktree = self.runtime / "review-worktree"
        (worktree / ".agent-workspace").mkdir(parents=True)
        lane_dir.mkdir(parents=True)
        lane = {
            "schema": LANE_SCHEMA,
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "result_path": str(worktree / "RESULT.json"),
            "lifecycle": "review_pending",
            "last_reported_actionable_status": None,
        }
        atomic_write_json(lane_dir / "lane.json", lane)
        result = {
            "schema": "result/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "outcome": "PASS",
            "summary": "completed",
            "evidence": [],
            "completed_at": "2026-09-09T00:00:00Z",
        }
        result["content_hash"] = content_hash(result)
        atomic_write_json(worktree / "RESULT.json", result)
        atomic_write_json(lane_dir / "COMPLETION_REVIEW.json", {"broken": True})
        self.assertTrue(monitor._recover_broken_review_pair(self.runtime, "epoch-1", lane))
        self.assertFalse((lane_dir / "COMPLETION_REVIEW.json").exists())
        recovered_lane = monitor.read_lane(self.runtime, "epoch-1", "lane-1")
        monitor._recover_lost_review_event(self.runtime, "epoch-1", recovered_lane)
        monitor._recover_lost_review_event(
            self.runtime, "epoch-1", monitor.read_lane(self.runtime, "epoch-1", "lane-1")
        )
        events = read_manager_queue(self.runtime)["events"]
        self.assertEqual(1, len(events))
        self.assertEqual("COMPLETION_REVIEW_REQUIRED", events[0]["type"])

    def test_review_recovery_waits_for_pair_publication(self) -> None:
        lane_dir = self.runtime / "epochs" / "epoch-1" / "lanes" / "lane-1"
        worktree = self.runtime / "review-publication-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        lane_dir.mkdir(parents=True)
        lane = {
            "schema": "lane/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "result_path": str(worktree / "RESULT.json"),
            "lifecycle": "review_pending",
        }
        atomic_write_json(lane_dir / "lane.json", lane)
        atomic_write_json(
            workspace / "task-card.json",
            {"schema": "project-task-card/v1", "card_id": "card-1", "base_commit": "commit-1"},
        )
        result = {
            "schema": "result/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "outcome": "PASS",
            "summary": "completed",
            "evidence": [],
            "completed_at": "2026-09-09T00:00:00Z",
        }
        result["content_hash"] = content_hash(result)
        atomic_write_json(worktree / "RESULT.json", result)
        review_path = lane_dir / "COMPLETION_REVIEW.json"
        first_write = threading.Event()
        allow_second_write = threading.Event()
        recovery_lock_requested = threading.Event()
        writer_errors: list[BaseException] = []
        recovery_errors: list[BaseException] = []
        recovery_result: list[bool] = []

        from orchestrator_harness.records import RecordLock

        real_atomic_write_json = review.atomic_write_json

        def write_pair(path: Path, value: object) -> None:
            real_atomic_write_json(path, value)
            if path == review_path:
                first_write.set()
                if not allow_second_write.wait(5):
                    raise AssertionError("timed out waiting to complete review publication")

        def tracked_lock(path: Path) -> RecordLock:
            if Path(path).absolute() == review_path.absolute():
                recovery_lock_requested.set()
            return RecordLock(path)

        def publish_pair() -> None:
            try:
                review._write_pair(
                    self.runtime,
                    "epoch-1",
                    lane,
                    review_outcome="PASS",
                    review_summary="reviewed",
                    evidence=[],
                    approval="ACCEPTED",
                    force_accept_reason=None,
                )
            except BaseException as exc:
                writer_errors.append(exc)

        def recover_pair() -> None:
            try:
                recovery_result.append(
                    monitor._recover_broken_review_pair(self.runtime, "epoch-1", lane)
                )
            except BaseException as exc:
                recovery_errors.append(exc)

        with (
            patch.object(review, "atomic_write_json", side_effect=write_pair),
            patch.object(review, "_worktree_commit", return_value="commit-1"),
            patch.object(monitor, "RecordLock", side_effect=tracked_lock),
        ):
            writer = threading.Thread(target=publish_pair)
            writer.start()
            self.assertTrue(first_write.wait(5))
            recovery = threading.Thread(target=recover_pair)
            recovery.start()
            lock_observed = recovery_lock_requested.wait(2)
            allow_second_write.set()
            writer.join(5)
            recovery.join(5)

        self.assertTrue(lock_observed, "recovery must acquire the writer's review lock")
        self.assertFalse(writer.is_alive())
        self.assertFalse(recovery.is_alive())
        self.assertEqual([], writer_errors)
        self.assertEqual([], recovery_errors)
        self.assertEqual([False], recovery_result)
        self.assertTrue(monitor._review_pair_is_valid(self.runtime, "epoch-1", lane))


    def test_invalid_result_uses_native_session_for_at_most_five_corrections(self) -> None:
        worktree = self.runtime / "controller-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        (workspace / "worker-prompt.md").write_text("do the work\n", encoding="utf-8")
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "transcript_path": str(workspace / "provider-transcript.jsonl"),
            "stderr_path": str(workspace / "provider-stderr.txt"),
            "attempts_path": str(workspace / "controller.attempts.jsonl"),
            "last_message_path": str(workspace / "last-message.txt"),
            "provider": {"id": "codex", "model": "model-1"},
            "process": {},
            "session": {},
            "lifecycle": "prepared",
        }
        invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {"id": "codex", "model": "model-1"},
            "exclusive_resources": ["resource-1"],
        }

        executions = []
        for index in range(3):
            boundary = MagicMock(
                root_pid=100 + index,
                root_creation_time=f"created-{index}",
                process_group_id=100 + index,
                session_id=f"process-session-{index}",
            )
            boundary.record.return_value = {"root": {"pid": 100 + index}}
            boundary.cleanup.return_value = True
            executions.append(
                ProviderExecution(
                    0,
                    boundary,
                    "native-session-1",
                    ("codex", "exec", "resume" if index else "new"),
                )
            )
        validate_results = [("invalid", None), ("invalid", None), ("valid", {"outcome": "PASS"})]
        calls = []

        def record_call(*args, **kwargs):
            calls.append((args, kwargs))
            return executions.pop(0)

        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(controller, "load_config", return_value=type("Config", (), {"runtime_root": self.runtime})()),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(controller, "read_record", return_value=invocation),
            patch.object(controller, "_write_status"),
            patch.object(controller, "_append_event"),
            patch.object(controller.processes, "process_identity", return_value={"pid": 7, "creation_time": "controller"}),
            patch.object(controller, "update_lane", return_value=lane),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "acquire_leases"),
            patch.object(controller, "release_leases"),
            patch.object(controller, "_run_provider", side_effect=record_call),
            patch.object(controller, "_validate_result", side_effect=validate_results),
            patch.object(controller, "_read_acceptance_chain", return_value={"acceptance": {"approval": "REJECTED"}}),
        ):
            self.assertEqual(0, controller.run_controller("lane-1"))
        self.assertEqual(3, len(calls))
        self.assertFalse(calls[0][1]["resume"])
        self.assertTrue(calls[1][1]["resume"])
        self.assertTrue(calls[2][1]["resume"])
        self.assertEqual(calls[1][0][2], calls[2][0][2])
        attempts = read_jsonl(Path(lane["attempts_path"]))
        self.assertEqual(3, len(attempts) - 1)  # one schema header plus three attempts
        self.assertEqual(2, len(list((workspace / "attempts").glob("correction-prompt-*.md"))))

    def test_valid_result_wins_over_nonzero_provider_exit(self) -> None:
        worktree = self.runtime / "controller-valid-result-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        (workspace / "worker-prompt.md").write_text("do the work\n", encoding="utf-8")
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "result_path": str(worktree / "RESULT.json"),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "transcript_path": str(workspace / "provider-transcript.jsonl"),
            "stderr_path": str(workspace / "provider-stderr.txt"),
            "attempts_path": str(workspace / "controller.attempts.jsonl"),
            "last_message_path": str(workspace / "last-message.txt"),
            "provider": {"id": "codex", "model": "model-1"},
            "process": {},
            "session": {},
            "lifecycle": "prepared",
        }
        invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {"id": "codex", "model": "model-1"},
            "exclusive_resources": [],
        }
        boundary = MagicMock(
            root_pid=101,
            root_creation_time="provider-created",
            process_group_id=101,
            session_id="provider-boundary",
        )
        boundary.record.return_value = {"root": {"pid": 101}}
        boundary.cleanup.return_value = True
        execution = ProviderExecution(
            17,
            boundary,
            "native-session-1",
            ("codex", "exec", "new"),
            True,
        )
        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(controller, "load_config", return_value=type("Config", (), {"runtime_root": self.runtime})()),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(controller, "read_record", return_value=invocation),
            patch.object(controller, "_write_status") as write_status,
            patch.object(controller, "_append_event") as append_event,
            patch.object(controller.processes, "process_identity", return_value={"pid": 7, "creation_time": "controller"}),
            patch.object(controller, "update_lane", return_value=lane),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "acquire_leases"),
            patch.object(controller, "release_leases"),
            patch.object(controller, "_run_provider", return_value=execution),
            patch.object(controller, "_validate_result", return_value=("valid", {"outcome": "PASS"})),
            patch.object(controller, "_read_acceptance_chain", return_value={"acceptance": {"approval": "REJECTED"}}),
        ):
            self.assertEqual(0, controller.run_controller("lane-1"))

        review_status = next(
            call.args[1]
            for call in write_status.call_args_list
            if call.args[1].get("recorded_status") == "review_pending"
        )
        self.assertEqual("valid", review_status["result_state"])
        event_types = [call.args[1] for call in append_event.call_args_list]
        self.assertIn("provider_exited", event_types)
        self.assertIn("result_valid", event_types)
        self.assertNotIn("correction_requested", event_types)
        attempts = read_jsonl(Path(lane["attempts_path"]))
        self.assertEqual(17, attempts[-1]["exit_code"])
        self.assertEqual("valid", attempts[-1]["result_state"])

    def test_dead_controller_does_not_hide_correction_pending(self) -> None:
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "running",
            "process": {"pid": 7, "creation_time": "controller-created"},
        }
        status = {"recorded_status": "correction_pending"}
        with patch.object(monitor.processes, "identity_matches", return_value=True):
            self.assertIsNone(
                monitor.derive_lane_status(
                    self.runtime,
                    "epoch-1",
                    lane,
                    status,
                    lease_records=[],
                )
            )
        with patch.object(monitor.processes, "identity_matches", return_value=False):
            self.assertEqual(
                "controller_exited",
                monitor.derive_lane_status(
                    self.runtime,
                    "epoch-1",
                    lane,
                    status,
                    lease_records=[],
                ),
            )


if __name__ == "__main__":
    unittest.main()
