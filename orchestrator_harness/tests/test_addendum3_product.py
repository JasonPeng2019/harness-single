from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

from orchestrator_harness import controller, leases, manager_queue, monitor, review, scan_watch
from orchestrator_harness.controller import ProviderExecution
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import CURRENT_EPOCH_SCHEMA, MANAGER_QUEUE_SCHEMA
from orchestrator_harness.manager_queue import (
    append_assignment,
    append_watch_delivery,
    promote_event,
    read_lane_inbox,
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

    def test_managed_watch_wakes_on_lane_status_with_empty_queue(self) -> None:
        with (
            patch.object(scan_watch, "find_harness_root", return_value=Path("root")),
            patch.object(scan_watch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(scan_watch, "_active_epoch", return_value=("epoch-1", {"lane_mode": "managed"})),
            patch.object(scan_watch, "_discover_orphaned_leases", return_value=[]),
            patch.object(scan_watch, "read_manager_queue", return_value={"queue_id": "queue-1", "events": []}),
            patch.object(scan_watch, "_find_actionable", return_value=("lane-1", "review_pending")),
        ):
            result = scan_watch.run_watch(timeout="0s", root_session_id="root-1")
        self.assertTrue(result["ok"], result)
        self.assertEqual("WATCH_ACTIONABLE", result["code"])
        self.assertEqual("lane_status", result["wake_reason"])
        self.assertEqual("lane-1", result["summary"].split(" ")[0])

    def test_plain_watch_skips_queue_reads_and_wakes_on_lane_status(self) -> None:
        def fail_queue(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("plain watch must not read the manager queue")

        with (
            patch.object(scan_watch, "find_harness_root", return_value=Path("root")),
            patch.object(scan_watch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(scan_watch, "_active_epoch", return_value=("epoch-1", {"lane_mode": "plain"})),
            patch.object(scan_watch, "_discover_orphaned_leases", return_value=[]),
            patch.object(scan_watch, "read_manager_queue", side_effect=fail_queue),
            patch.object(scan_watch, "_find_actionable", return_value=("lane-1", "review_pending")),
        ):
            result = scan_watch.run_watch(timeout="0s", root_session_id="root-1")
        self.assertTrue(result["ok"], result)
        self.assertEqual("WATCH_ACTIONABLE", result["code"])

    def test_managed_watch_prefers_delivered_manager_event_over_lane_status(self) -> None:
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
        with (
            patch.object(scan_watch, "find_harness_root", return_value=Path("root")),
            patch.object(scan_watch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(scan_watch, "_active_epoch", return_value=("epoch-1", {"lane_mode": "managed"})),
            patch.object(scan_watch, "_discover_orphaned_leases", return_value=[]),
            patch.object(scan_watch, "_find_actionable", return_value=("lane-1", "review_pending")),
        ):
            result = scan_watch.run_watch(timeout="0s", root_session_id="root-1")
        self.assertTrue(result["ok"], result)
        self.assertEqual("WATCH_EVENT", result["code"])
        self.assertEqual(event["event_id"], result["event_id"])

    def test_pre_spawn_transcript_offset_parses_early_session_output(self) -> None:
        worktree = self.runtime / "transcript-offset-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        transcript_path = workspace / "provider-transcript.jsonl"
        transcript_path.write_text('{"message": "prior attempt line"}\n', encoding="utf-8")
        prompt_path = workspace / "worker-prompt.md"
        prompt_path.write_text("do the work\n", encoding="utf-8")
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "transcript_path": str(transcript_path),
            "stderr_path": str(workspace / "provider-stderr.txt"),
            "last_message_path": str(workspace / "last-message.txt"),
            "provider": {"id": "codex", "model": "model-1"},
            "session": {},
        }
        invocation = {"provider": {"model": "model-1"}}
        binding = MagicMock()
        binding.build_argv.return_value = ["codex", "exec"]
        binding.parse_line.return_value = {
            "message": "early session started",
            "session_id": "session-early",
        }
        child = MagicMock(pid=41)
        child.poll.return_value = 0
        child.returncode = 0

        def fake_spawn(
            argv: object, cwd: object, stdin: object, stdout: object, stderr: object
        ) -> object:
            # The provider emits early output before the controller's read handle opens.
            with transcript_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    '{"message": "early session started", "session_id": "session-early"}\n'
                )
            return child

        boundary = MagicMock(
            root_pid=41,
            root_creation_time="created-1",
            process_group_id=41,
            session_id="boundary-session",
        )
        boundary.record.return_value = {"root": {"pid": 41}}
        boundary.cleanup.return_value = True
        with (
            patch.object(controller.processes, "spawn_provider", side_effect=fake_spawn),
            patch.object(controller.processes, "ProcessBoundary") as boundary_cls,
        ):
            boundary_cls.for_process.return_value = boundary
            execution = controller._run_provider(
                self.runtime, "epoch-1", lane, invocation, binding, prompt_path
            )
        self.assertEqual("session-early", execution.session_id)
        self.assertEqual(1, binding.parse_line.call_count)
        self.assertEqual(
            "early session started",
            Path(lane["last_message_path"]).read_text(encoding="utf-8"),
        )

    def test_validated_review_pending_with_nonzero_exit_is_not_a_contradiction(self) -> None:
        worktree = self.runtime / "review-contradiction-worktree"
        (worktree / ".agent-workspace").mkdir(parents=True)
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
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "result_path": str(worktree / "RESULT.json"),
            "lifecycle": "review_pending",
            "process": {"pid": 7, "creation_time": "controller-created"},
        }
        status = {
            "recorded_status": "review_pending",
            "result_state": "valid",
            "cleanup_proven": True,
            "provider_state": {"state": "exited", "exit_code": 17},
        }
        with patch.object(monitor.processes, "identity_matches", return_value=True):
            self.assertEqual(
                "review_pending",
                monitor.derive_lane_status(
                    self.runtime, "epoch-1", lane, status, lease_records=[]
                ),
            )
        # The real contradiction path remains: review_pending without a validated result.
        (worktree / "RESULT.json").unlink()
        status["result_state"] = "invalid"
        with patch.object(monitor.processes, "identity_matches", return_value=True):
            self.assertEqual(
                "status_transcript_contradiction",
                monitor.derive_lane_status(
                    self.runtime, "epoch-1", lane, status, lease_records=[]
                ),
            )

    def test_concurrent_inbox_appends_preserve_both_assignments(self) -> None:
        worktree = self.runtime / "inbox-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        atomic_write_json(
            workspace / "QUEUE.json",
            {
                "schema": "lane-inbox/v1",
                "lane_id": "lane-1",
                "run_id": "run-1",
                "assignments": [],
            },
        )
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
        }
        errors: list[BaseException] = []
        results: list[dict[str, object]] = []

        def append(prompt: str) -> None:
            try:
                results.append(append_assignment(self.runtime, lane, prompt))
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=append, args=("prompt-a",)),
            threading.Thread(target=append, args=("prompt-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual([], errors)
        self.assertEqual(2, len(results))
        record = read_lane_inbox(worktree)
        self.assertEqual(2, len(record["assignments"]))
        self.assertEqual(
            {"prompt-a", "prompt-b"},
            {assignment["prompt"] for assignment in record["assignments"]},
        )

    def test_launch_pending_suppresses_controller_exited_until_cleared(self) -> None:
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "running",
            "process": {},
            "launch_pending": True,
        }
        with patch.object(monitor.processes, "identity_matches", return_value=False):
            self.assertIsNone(
                monitor.derive_lane_status(
                    self.runtime, "epoch-1", lane, None, lease_records=[]
                )
            )
        lane["launch_pending"] = False
        with patch.object(monitor.processes, "identity_matches", return_value=False):
            self.assertEqual(
                "controller_exited",
                monitor.derive_lane_status(
                    self.runtime, "epoch-1", lane, None, lease_records=[]
                ),
            )

    def test_resume_marks_launch_pending_for_fresh_running_lane(self) -> None:
        from orchestrator_harness import resume

        worktree = self.runtime / "resume-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-old",
            "worktree_path": str(worktree),
            "provider": {"id": "codex", "model": "model-1"},
            "session": {"session_id": "session-1"},
            "lifecycle": "review_pending",
            "process": {},
        }
        task_card = {
            "schema": "project-task-card/v1",
            "card_id": "card-1",
            "task": "do the work",
        }
        updates: list[dict[str, object]] = []

        def update(
            _rt: object, _epoch: object, _lane_id: object, mutate: object
        ) -> dict[str, object]:
            value = mutate(dict(lane))
            updates.append(value)
            return {**lane, **value}

        with (
            patch.object(resume, "find_harness_root", return_value=Path("root")),
            patch.object(
                resume,
                "load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime, profile="plain"),
            ),
            patch.object(resume, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(resume, "_read_task_card", return_value=task_card),
            patch.object(resume, "_clear_prior_run"),
            patch.object(resume, "_write_worker_prompt"),
            patch.object(resume, "_write_result_template"),
            patch.object(resume, "_rewrite_overlay_receipt"),
            patch.object(resume, "update_lane", side_effect=update),
        ):
            result = resume.run_resume(lane_id="lane-1", resume_task_card="card.json")
        self.assertTrue(result["ok"], result)
        running_update = updates[-1]
        self.assertEqual("running", running_update["lifecycle"])
        self.assertEqual({}, running_update["process"])
        self.assertTrue(running_update["launch_pending"])

    def test_launch_clears_launch_pending_when_identity_is_recorded(self) -> None:
        from orchestrator_harness import launch

        worktree = self.runtime / "launch-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        binding = (
            self.runtime
            / "orchestrator_harness"
            / "provider_adapters"
            / "codex"
            / "launcher_binding.py"
        )
        binding.parent.mkdir(parents=True)
        binding.write_text("# binding\n", encoding="utf-8")
        lane = {
            "schema": "lane/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "running",
            "process": {},
            "launch_pending": True,
            "worktree_path": str(worktree),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
        }
        invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {"id": "codex"},
        }
        child = MagicMock(pid=41)
        child.poll.return_value = 0
        updates: list[dict[str, object]] = []

        def update(
            _rt: object, _epoch: object, _lane_id: object, mutate: object
        ) -> dict[str, object]:
            value = mutate(dict(lane))
            updates.append(value)
            return {**lane, **value}

        with (
            patch.object(launch, "find_harness_root", return_value=self.runtime),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime),
            ),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(launch, "read_record", return_value=invocation),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(
                launch.processes,
                "process_identity",
                return_value={"pid": 41, "creation_time": "created-1"},
            ),
            patch.object(launch, "update_lane", side_effect=update),
            patch.object(launch, "_read_controller_status", return_value=None),
        ):
            result = launch.run_launch("lane-1")
        self.assertFalse(result["ok"])
        self.assertFalse(updates[-1]["launch_pending"])
        self.assertEqual(
            {"pid": 41, "creation_time": "created-1"}, updates[-1]["process"]
        )

if __name__ == "__main__":
    unittest.main()
