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
from orchestrator_harness.records import atomic_write_json, read_jsonl, read_record


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
            {
                "schema": "project-task-card/v1",
                "card_id": "card-1",
                "task": "review the completed lane",
                "acceptance_criteria": ["The review pair is published atomically"],
                "deliverables": ["Completion review and acceptance records"],
                "reason_for_acceptance_and_deliverables": (
                    "Recovery must observe only a complete review pair."
                ),
                "base_commit": "commit-1",
            },
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

    def _run_candidate_correction_boundary(
        self, *, valid_on_sixth: bool, scenario: str = "bounded"
    ) -> dict[str, object]:
        suffix = {
            "bounded": "valid-sixth" if valid_on_sixth else "exhausted-sixth",
            "no-session": "no-session",
            "resume-unavailable": "resume-unavailable",
            "provider-start-failed": "provider-start-failed",
        }[scenario]
        worktree = self.runtime / f"controller-{suffix}-worktree"
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
        lane_path = self.runtime / "epochs" / "epoch-1" / "lanes" / "lane-1" / "lane.json"
        atomic_write_json(lane_path, {"schema": "lane/v1", **lane})
        atomic_write_json(
            workspace / "invocation.json",
            {
                "schema": "controller-invocation/v1",
                "lane_id": "lane-1",
                "run_id": "run-1",
                "provider": {"id": "codex", "model": "model-1"},
                "exclusive_resources": ["resource-1"],
            },
        )

        calls: list[dict[str, object]] = []
        boundaries: list[MagicMock] = []
        lease_observations: list[dict[str, object]] = []
        lane_transitions: list[dict[str, object]] = []
        status_transitions: list[dict[str, object]] = []
        release_observations: list[dict[str, object]] = []

        real_update_lane = controller.update_lane
        real_write_status = controller._write_status
        real_release_leases = controller.release_leases

        def track_update_lane(*args: object, **kwargs: object) -> dict[str, object]:
            updated = real_update_lane(*args, **kwargs)
            lane_transitions.append(updated)
            return updated

        def track_write_status(
            current_lane: dict[str, object], fields: dict[str, object]
        ) -> None:
            status_transitions.append(dict(fields))
            real_write_status(current_lane, fields)

        def track_release_leases(
            runtime: Path, lane_id: str, run_id: str
        ) -> None:
            held = leases.read_lease(runtime, "resource-1")
            self.assertIsNotNone(held)
            for boundary in boundaries:
                boundary.cleanup.assert_called_once()
            release_observations.append(
                {
                    "lane_id": lane_id,
                    "run_id": run_id,
                    "lease": held,
                    "cleaned_boundaries": len(boundaries),
                }
            )
            real_release_leases(runtime, lane_id, run_id)

        def run_provider(*args: object, **kwargs: object) -> ProviderExecution:
            attempt_number = int(kwargs["attempt_number"])
            resume = bool(kwargs["resume"])
            current_lane = args[2]
            self.assertIsInstance(current_lane, dict)
            assert isinstance(current_lane, dict)
            prompt_path = Path(str(args[5]))
            if boundaries:
                boundaries[-1].cleanup.assert_called_once()
            held = leases.read_lease(self.runtime, "resource-1")
            self.assertIsNotNone(held, f"lease missing before attempt {attempt_number}")
            assert held is not None
            lease_observations.append(held)

            if attempt_number == 1:
                self.assertFalse(resume)
                self.assertEqual({}, current_lane.get("session"))
                self.assertEqual(workspace / "worker-prompt.md", prompt_path)
            else:
                self.assertTrue(resume)
                self.assertEqual(
                    {"session_id": "native-session-1"}, current_lane.get("session")
                )
                self.assertEqual(
                    workspace
                    / "attempts"
                    / f"correction-prompt-{attempt_number - 1}.md",
                    prompt_path,
                )

            if scenario == "provider-start-failed":
                calls.append(
                    {
                        "attempt_number": attempt_number,
                        "resume": resume,
                        "prompt_path": str(prompt_path),
                        "argv": (),
                        "lane_session": current_lane.get("session"),
                    }
                )
                raise controller.ControllerError(
                    controller.LAUNCH_PROVIDER_START_FAILED,
                    "provider did not start",
                    no_provider_started=True,
                )
            if scenario == "resume-unavailable" and attempt_number == 2:
                calls.append(
                    {
                        "attempt_number": attempt_number,
                        "resume": resume,
                        "prompt_path": str(prompt_path),
                        "argv": (),
                        "lane_session": current_lane.get("session"),
                    }
                )
                raise ValueError("custom adapter cannot build a native resume command")

            paths = controller._attempt_paths(lane, attempt_number)
            paths["transcript"].parent.mkdir(parents=True, exist_ok=True)
            paths["transcript"].write_text(
                f'{{"attempt": {attempt_number}, "session_id": "native-session-1"}}\n',
                encoding="utf-8",
            )
            paths["stderr"].write_text("", encoding="utf-8")

            if valid_on_sixth and attempt_number == 6:
                result = {
                    "schema": "result/v1",
                    "lane_id": "lane-1",
                    "run_id": "run-1",
                    "outcome": "PASS",
                    "summary": "valid after the fifth correction",
                    "evidence": [str(paths["transcript"])],
                    "completed_at": "2026-09-18T00:00:00Z",
                }
                result["content_hash"] = content_hash(result)
                atomic_write_json(Path(str(lane["result_path"])), result)

            boundary = MagicMock(
                root_pid=200 + attempt_number,
                root_creation_time=f"created-{attempt_number}",
                process_group_id=200 + attempt_number,
                session_id=f"process-session-{attempt_number}",
            )
            boundary.record.return_value = {
                "root": {
                    "pid": 200 + attempt_number,
                    "creation_time": f"created-{attempt_number}",
                }
            }
            boundary.cleanup.return_value = True
            boundaries.append(boundary)
            argv = (
                ("codex", "exec", "resume", "native-session-1")
                if resume
                else ("codex", "exec", "--json")
            )
            calls.append(
                {
                    "attempt_number": attempt_number,
                    "resume": resume,
                    "prompt_path": str(prompt_path),
                    "argv": argv,
                    "lane_session": current_lane.get("session"),
                }
            )
            session_id = None if scenario == "no-session" else "native-session-1"
            return ProviderExecution(0, boundary, session_id, argv)

        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(
                controller,
                "load_config",
                return_value=type("Config", (), {"runtime_root": self.runtime})(),
            ),
            patch.object(controller.processes, "process_identity", return_value={"pid": 7, "creation_time": "controller-created"}),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "update_lane", side_effect=track_update_lane),
            patch.object(controller, "_write_status", side_effect=track_write_status),
            patch.object(controller, "release_leases", side_effect=track_release_leases),
            patch.object(controller, "_run_provider", side_effect=run_provider),
            patch.object(
                controller,
                "_read_acceptance_chain",
                return_value={"acceptance": {"approval": "REJECTED"}},
            ),
        ):
            exit_code = controller.run_controller("lane-1")

        correction_prompts = sorted(
            (workspace / "attempts").glob("correction-prompt-*.md"),
            key=lambda path: int(path.stem.rsplit("-", 1)[-1]),
        )
        return {
            "exit_code": exit_code,
            "lane": read_record(lane_path, "lane/v1"),
            "status": read_record(
                Path(str(lane["controller_status_path"])),
                controller.CONTROLLER_STATUS_SCHEMA,
            ),
            "events": read_jsonl(Path(str(lane["controller_events_path"]))),
            "attempts": read_jsonl(Path(str(lane["attempts_path"]))),
            "calls": calls,
            "boundaries": boundaries,
            "lease_observations": lease_observations,
            "lane_transitions": lane_transitions,
            "status_transitions": status_transitions,
            "release_observations": release_observations,
            "lease_after": leases.read_lease(self.runtime, "resource-1"),
            "correction_prompts": correction_prompts,
            "result_path": Path(str(lane["result_path"])),
        }

    def _assert_five_correction_prompts(self, prompts: object) -> None:
        self.assertIsInstance(prompts, list)
        assert isinstance(prompts, list)
        self.assertEqual(5, len(prompts))
        required = (
            '"schema": "result/v1"',
            '"lane_id": "lane-1"',
            '"run_id": "run-1"',
            '"outcome": "PASS|FAIL|BLOCKED"',
            '"summary": "nonempty factual summary"',
            '"evidence": []',
            '"completed_at": "ISO-8601 UTC timestamp"',
            '"content_hash": "sha256 of canonical JSON excluding content_hash"',
        )
        for prompt in prompts:
            self.assertIsInstance(prompt, Path)
            text = prompt.read_text(encoding="utf-8")
            self.assertIn(
                "Continue this same lane and native provider session", text
            )
            self.assertIn("Write a valid RESULT.json at the worktree root", text)
            self.assertIn("Do not answer with prose alone", text)
            for field in required:
                self.assertIn(field, text)

    def _assert_candidate_attempt_ledger(
        self, observed: dict[str, object], expected_states: list[str]
    ) -> None:
        calls = observed["calls"]
        attempts = observed["attempts"]
        self.assertIsInstance(calls, list)
        self.assertIsInstance(attempts, list)
        assert isinstance(calls, list)
        assert isinstance(attempts, list)
        self.assertEqual({"schema": "controller-attempts/v1"}, attempts[0])
        rows = attempts[1:]
        self.assertEqual(list(range(1, 7)), [row["attempt"] for row in rows])
        self.assertEqual(expected_states, [row["result_state"] for row in rows])
        for index, (call, row) in enumerate(zip(calls, rows, strict=True), start=1):
            self.assertEqual({"id": "codex", "model": "model-1"}, row["provider"])
            self.assertEqual(list(call["argv"]), row["argv"])
            self.assertEqual("native-session-1", row["session_id"])
            self.assertEqual(call["prompt_path"], row["prompt_path"])
            self.assertEqual(0, row["exit_code"])
            self.assertTrue(row["cleanup_proven"])
            self.assertTrue(row["at"])
            self.assertTrue(Path(row["transcript_path"]).is_file())
            self.assertTrue(Path(row["stderr_path"]).is_file())
            expected_error = (
                None if expected_states[index - 1] == "valid" else "missing or invalid RESULT.json"
            )
            self.assertEqual(expected_error, row["validation_error"])

        leases_seen = observed["lease_observations"]
        self.assertEqual(6, len(leases_seen))
        self.assertTrue(all(item == leases_seen[0] for item in leases_seen))
        self.assertEqual(
            {
                "schema": "resource-lease/v1",
                "resource_id": "resource-1",
                "lane_id": "lane-1",
                "run_id": "run-1",
                "pid": 7,
                "creation_time": "controller-created",
            },
            {key: leases_seen[0][key] for key in (
                "schema",
                "resource_id",
                "lane_id",
                "run_id",
                "pid",
                "creation_time",
            )},
        )
        self.assertTrue(leases_seen[0]["acquired_at"])
        releases = observed["release_observations"]
        self.assertEqual(1, len(releases))
        self.assertEqual(6, releases[0]["cleaned_boundaries"])
        self.assertEqual(leases_seen[0], releases[0]["lease"])
        self.assertIsNone(observed["lease_after"])
        for boundary in observed["boundaries"]:
            boundary.cleanup.assert_called_once()

    def _assert_candidate_event_sequence(
        self, observed: dict[str, object], *, valid_on_sixth: bool
    ) -> None:
        events = [item for item in observed["events"] if "event_type" in item]
        actual = [item["event_type"] for item in events]
        expected = ["controller_started", "leases_acquired"]
        for _ in range(5):
            expected.extend(
                ["provider_exited", "cleanup_proven", "correction_requested"]
            )
        expected.extend(["provider_exited", "cleanup_proven"])
        if valid_on_sixth:
            expected.extend(["leases_released", "result_valid", "acceptance_rejected"])
        else:
            expected.extend(["provider_exited_no_result", "leases_released"])
        self.assertEqual(expected, actual)

    def test_run_provider_passes_saved_session_and_resume_to_binding(self) -> None:
        worktree = self.runtime / "provider-wiring-worktree"
        workspace = worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        prompt = workspace / "attempts" / "correction-prompt-1.md"
        prompt.parent.mkdir(parents=True)
        prompt.write_text("continue the same session\n", encoding="utf-8")
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(worktree),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
            "transcript_path": str(workspace / "provider-transcript.jsonl"),
            "stderr_path": str(workspace / "provider-stderr.txt"),
            "last_message_path": str(workspace / "last-message.txt"),
            "session": {"session_id": "saved-native-session"},
        }
        invocation = {
            "provider": {
                "id": "provider-1",
                "model": "model-1",
                "launch_config": {"effort": "configured-effort"},
            }
        }
        expected_argv = [
            "provider-cli",
            "--resume",
            "saved-native-session",
            "--prompt",
            str(prompt),
        ]
        binding = MagicMock()
        binding.build_argv.return_value = expected_argv
        binding.parse_line.return_value = None
        child = MagicMock(pid=456, returncode=0)
        child.poll.return_value = 0
        child.take_job_handle.return_value = 789
        boundary = MagicMock(
            root_pid=456,
            root_creation_time="provider-created",
            process_group_id=456,
            session_id="provider-boundary",
        )
        boundary.record.return_value = {
            "root": {"pid": 456, "creation_time": "provider-created"}
        }

        def spawn(
            argv: list[str],
            *,
            cwd: str,
            stdin: object,
            stdout: object,
            stderr: object,
        ) -> MagicMock:
            self.assertEqual(expected_argv, argv)
            self.assertEqual(str(worktree), cwd)
            self.assertFalse(getattr(stdin, "closed"))
            self.assertFalse(getattr(stdout, "closed"))
            self.assertFalse(getattr(stderr, "closed"))
            stdout.write('{"type":"result"}\n')
            stdout.flush()
            return child

        with (
            patch.object(controller.processes, "spawn_provider", side_effect=spawn) as spawned,
            patch.object(
                controller.processes.ProcessBoundary,
                "for_process",
                return_value=boundary,
            ) as make_boundary,
        ):
            execution = controller._run_provider(
                self.runtime,
                "epoch-1",
                lane,
                invocation,
                binding,
                prompt,
                attempt_number=2,
                resume=True,
            )

        binding.build_argv.assert_called_once_with(
            model="model-1",
            launch_config={"effort": "configured-effort"},
            worktree=str(worktree),
            prompt_path=str(prompt),
            session_id="saved-native-session",
            resume=True,
        )
        spawned.assert_called_once()
        make_boundary.assert_called_once_with(456, windows_job_handle=789)
        child.resume.assert_called_once_with()
        self.assertEqual(tuple(expected_argv), execution.argv)
        self.assertEqual("saved-native-session", execution.session_id)

    def test_fifth_correction_succeeds_on_sixth_attempt_with_full_durable_evidence(
        self,
    ) -> None:
        observed = self._run_candidate_correction_boundary(valid_on_sixth=True)
        self.assertEqual(0, observed["exit_code"])
        self._assert_five_correction_prompts(observed["correction_prompts"])

        calls = observed["calls"]
        self.assertIsInstance(calls, list)
        assert isinstance(calls, list)
        self.assertEqual(list(range(1, 7)), [call["attempt_number"] for call in calls])
        self.assertEqual([False] + [True] * 5, [call["resume"] for call in calls])
        self.assertEqual({}, calls[0]["lane_session"])
        self.assertTrue(
            all(
                call["lane_session"] == {"session_id": "native-session-1"}
                for call in calls[1:]
            )
        )
        self.assertEqual(
            ["codex", "exec", "--json"], list(calls[0]["argv"])
        )
        for index, call in enumerate(calls[1:], start=1):
            self.assertEqual(
                ["codex", "exec", "resume", "native-session-1"],
                list(call["argv"]),
            )
            self.assertTrue(str(call["prompt_path"]).endswith(
                f"correction-prompt-{index}.md"
            ))

        attempts = observed["attempts"]
        self.assertIsInstance(attempts, list)
        assert isinstance(attempts, list)
        self.assertEqual({"schema": "controller-attempts/v1"}, attempts[0])
        attempt_rows = attempts[1:]
        self.assertEqual(list(range(1, 7)), [row["attempt"] for row in attempt_rows])
        self.assertEqual(["invalid"] * 5 + ["valid"], [row["result_state"] for row in attempt_rows])
        self.assertTrue(all(row["cleanup_proven"] for row in attempt_rows))
        self.assertTrue(all(row["provider"] == {"id": "codex", "model": "model-1"} for row in attempt_rows))
        self.assertTrue(all(Path(row["transcript_path"]).is_file() for row in attempt_rows))
        self.assertTrue(all(Path(row["stderr_path"]).is_file() for row in attempt_rows))
        self._assert_candidate_attempt_ledger(
            observed, ["invalid"] * 5 + ["valid"]
        )

        status = observed["status"]
        self.assertEqual("review_pending", status["recorded_status"])
        self.assertEqual("valid", status["result_state"])
        self.assertEqual(5, status["correction_attempts"])
        self.assertTrue(status["cleanup_proven"])
        self.assertEqual("review_pending", observed["lane"]["lifecycle"])
        self.assertNotIn(
            "result_invalid",
            [item.get("lifecycle") for item in observed["lane_transitions"]],
        )
        self.assertNotIn(
            "provider_exited_no_result",
            [item.get("recorded_status") for item in observed["status_transitions"]],
        )

        result = read_record(observed["result_path"], "result/v1")
        self.assertEqual("lane-1", result["lane_id"])
        self.assertEqual("run-1", result["run_id"])
        self.assertEqual("PASS", result["outcome"])
        self.assertTrue(result["summary"])
        self.assertIsInstance(result["evidence"], list)
        self.assertTrue(result["completed_at"])
        self.assertEqual(content_hash(result), result["content_hash"])

        self._assert_candidate_event_sequence(observed, valid_on_sixth=True)

    def test_sixth_invalid_attempt_escalates_once_with_full_durable_evidence(self) -> None:
        observed = self._run_candidate_correction_boundary(valid_on_sixth=False)
        self.assertEqual(0, observed["exit_code"])
        self._assert_five_correction_prompts(observed["correction_prompts"])
        self.assertFalse(observed["result_path"].exists())

        calls = observed["calls"]
        self.assertEqual(list(range(1, 7)), [call["attempt_number"] for call in calls])
        self.assertEqual([False] + [True] * 5, [call["resume"] for call in calls])
        self.assertEqual({}, calls[0]["lane_session"])
        self.assertTrue(
            all(
                call["lane_session"] == {"session_id": "native-session-1"}
                for call in calls[1:]
            )
        )

        attempts = observed["attempts"]
        self.assertEqual({"schema": "controller-attempts/v1"}, attempts[0])
        attempt_rows = attempts[1:]
        self.assertEqual(list(range(1, 7)), [row["attempt"] for row in attempt_rows])
        self.assertEqual(["invalid"] * 6, [row["result_state"] for row in attempt_rows])
        self.assertTrue(all(row["cleanup_proven"] for row in attempt_rows))
        self.assertTrue(all(row["provider"] == {"id": "codex", "model": "model-1"} for row in attempt_rows))
        self.assertTrue(all(Path(row["transcript_path"]).is_file() for row in attempt_rows))
        self.assertTrue(all(Path(row["stderr_path"]).is_file() for row in attempt_rows))
        self._assert_candidate_attempt_ledger(observed, ["invalid"] * 6)

        status = observed["status"]
        self.assertEqual("provider_exited_no_result", status["recorded_status"])
        self.assertEqual("invalid", status["result_state"])
        self.assertEqual(5, status["correction_attempts"])
        self.assertTrue(status["cleanup_proven"])
        self.assertEqual("result_invalid", observed["lane"]["lifecycle"])
        lifecycle_updates = [
            item.get("lifecycle") for item in observed["lane_transitions"]
        ]
        self.assertEqual("result_invalid", lifecycle_updates[-1])
        self.assertNotIn("result_invalid", lifecycle_updates[:-1])
        recorded_updates = [
            item.get("recorded_status") for item in observed["status_transitions"]
        ]
        self.assertEqual("provider_exited_no_result", recorded_updates[-1])
        self.assertNotIn("provider_exited_no_result", recorded_updates[:-1])

        self._assert_candidate_event_sequence(observed, valid_on_sixth=False)

    def test_unavailable_native_resume_escalates_without_a_fresh_retry(self) -> None:
        no_session = self._run_candidate_correction_boundary(
            valid_on_sixth=False, scenario="no-session"
        )
        self.assertEqual(0, no_session["exit_code"])
        self.assertEqual(1, len(no_session["calls"]))
        self.assertEqual([], no_session["correction_prompts"])
        self.assertEqual(
            "provider_exited_no_result", no_session["status"]["recorded_status"]
        )
        self.assertEqual("result_invalid", no_session["lane"]["lifecycle"])
        self.assertEqual(
            1,
            [
                item["event_type"]
                for item in no_session["events"]
                if "event_type" in item
            ].count("provider_exited_no_result"),
        )

        unavailable = self._run_candidate_correction_boundary(
            valid_on_sixth=False, scenario="resume-unavailable"
        )
        self.assertEqual(0, unavailable["exit_code"])
        self.assertEqual(2, len(unavailable["calls"]))
        self.assertEqual([False, True], [call["resume"] for call in unavailable["calls"]])
        self.assertEqual(1, len(unavailable["correction_prompts"]))
        self.assertEqual(
            "provider_exited_no_result", unavailable["status"]["recorded_status"]
        )
        self.assertEqual("result_invalid", unavailable["lane"]["lifecycle"])
        self.assertEqual(
            1,
            [
                item["event_type"]
                for item in unavailable["events"]
                if "event_type" in item
            ].count("provider_exited_no_result"),
        )
        attempt_rows = unavailable["attempts"][1:]
        self.assertEqual([1, 2], [row["attempt"] for row in attempt_rows])
        self.assertEqual([], attempt_rows[1]["argv"])
        self.assertEqual("native-session-1", attempt_rows[1]["session_id"])
        self.assertIn("native resume unavailable", attempt_rows[1]["validation_error"])

    def test_provider_start_failure_does_not_enter_the_correction_loop(self) -> None:
        observed = self._run_candidate_correction_boundary(
            valid_on_sixth=False, scenario="provider-start-failed"
        )
        self.assertEqual(4, observed["exit_code"])
        self.assertEqual(1, len(observed["calls"]))
        self.assertEqual([], observed["correction_prompts"])
        self.assertEqual("provider_start_failed", observed["status"]["recorded_status"])
        event_types = [
            item["event_type"]
            for item in observed["events"]
            if "event_type" in item
        ]
        self.assertEqual(1, event_types.count("provider_start_failed"))
        self.assertEqual(0, event_types.count("correction_requested"))
        self.assertEqual(0, event_types.count("provider_exited_no_result"))
        self.assertEqual(1, event_types.count("leases_released"))
        self.assertIsNone(observed["lease_after"])
        attempt_rows = observed["attempts"][1:]
        self.assertEqual(1, len(attempt_rows))
        self.assertEqual([], attempt_rows[0]["argv"])
        self.assertIn("provider did not start", attempt_rows[0]["validation_error"])

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
        invocation = {
            "provider": {
                "model": "model-1",
                "launch_config": {"reasoning_effort": "high", "service_tier": "priority"},
            }
        }
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

    def test_prepared_lane_does_not_report_controller_exited_before_launch(self) -> None:
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "prepared",
            "process": {},
            "launch_pending": False,
        }
        with patch.object(monitor.processes, "identity_matches", return_value=False):
            self.assertIsNone(
                monitor.derive_lane_status(
                    self.runtime, "epoch-1", lane, None, lease_records=[]
                )
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
            "provider": {
                "id": "codex",
                "model": "model-1",
                "launch_config": {"reasoning_effort": "high", "service_tier": "priority"},
            },
            "session": {"session_id": "session-1"},
            "lifecycle": "review_pending",
            "process": {},
        }
        task_card = {
            "schema": "project-task-card/v1",
            "card_id": "card-1",
            "task": "do the work",
            "acceptance_criteria": ["The lane is ready for its next run"],
            "deliverables": ["A fresh invocation and worker prompt"],
            "reason_for_acceptance_and_deliverables": (
                "Resume must preserve an explicit definition of completion."
            ),
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
            "provider": {
                "id": "codex",
                "model": "configured-model",
                "launch_config": {"reasoning_effort": "high", "service_tier": "priority"},
            },
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
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=invocation["provider"]["launch_config"],
            ),
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
