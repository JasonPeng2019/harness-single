"""STEP-08-2: exact native evidence, before the lane-1 outcome join exists."""

from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from memory_harness import contracts, store
from orchestrator_harness import controller, launch, lanes, leases, manager_queue, memory_handoff, monitor, operator_launch, resume, review, terminal_evidence
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import current_epoch_path, lane_record_dir, manager_queue_path, read_active_lanes, write_active_lanes
from orchestrator_harness.lanes import LaneError
from orchestrator_harness.manager_queue import ManagerQueueError
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.test_memory_handoff import _finalized_lane1_fixture


class NativeReviewEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.rt = self.root / "runtime"
        self.worktree = self.root / "worktree"
        (self.worktree / ".agent-workspace").mkdir(parents=True)
        self.epoch_id = "epoch-1"
        self.lane_id = "lane-1"
        self.run_id = "run-1"
        self.folder = lane_record_dir(self.rt, self.epoch_id, self.lane_id)
        self.folder.mkdir(parents=True)
        self.plan = contracts.make_plan(
            plan_id="plan-1", objective_id="objective-1", route="ordinary",
            state="accepted", content={"steps": ["implement"]}, accepted_by="ROOT",
        )
        self.card = contracts.make_task_card(
            task="Complete the parent task", base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=self.plan,
                checkpoint="checkpoint-1",
            ),
        )
        atomic_write_json(self.worktree / ".agent-workspace" / "task-card.json", self.card)
        self.lane = {
            "schema": "lane/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "worktree_path": str(self.worktree), "lifecycle": "review_pending",
            "memory_plan_state": "execution_accepted",
            "controller_status_path": str(self.worktree / ".agent-workspace" / "controller.status.json"),
            "process": {"pid": 123, "creation_time": "incarnation-1"},
        }
        self.result_path = self.worktree / "RESULT.json"
        self._result("PASS")
        store_path, _ = memory_handoff.memory_paths(self.worktree)
        memory_store = store.MemoryStore(store_path)
        memory_store.initialize()
        try:
            memory_store.record_decision(contracts.make_decision(self.card, self.plan))
        finally:
            memory_store.close()
        finalized = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=finalized):
            self.envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card, lane_id=self.lane_id, run_id=self.run_id,
                worktree_path=self.worktree, base_commit="base-1",
            )
        assert self.envelope is not None
        context = memory_handoff.load_final_context(
            worktree_path=self.worktree, envelope=self.envelope,
        )
        self.observed = memory_handoff.native_observation(
            envelope=self.envelope, context=context,
            controller_identity=self.lane["process"],
        )
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=self.envelope,
        )
        self.operation = memory_handoff.record_observed_invocation(
            worktree_path=self.worktree, envelope=self.envelope,
            observed_invocation=self.observed,
        )

    def _result(self, outcome: str) -> None:
        value = {
            "schema": "result/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "outcome": outcome, "summary": "native work completed", "evidence": [],
            "completed_at": "2026-09-25T00:00:00Z",
        }
        value["content_hash"] = content_hash(value)
        atomic_write_json(self.result_path, value)

    def _review(self, *, outcome: str = "PASS", approval: str = "ACCEPTED",
                force_reason: str | None = None, managed: bool = False,
                close_side_effect: Exception | None = None,
                event_state: str = "ACKNOWLEDGED") -> dict:
        event = {"event_id": "event-1", "state": event_state}
        with (
            patch.object(review, "find_harness_root", return_value=self.root),
            patch.object(review, "load_config", return_value=SimpleNamespace(runtime_root=self.rt)),
            patch.object(review, "find_active_lane", return_value=(self.epoch_id, self.lane)),
            patch.object(review, "_resolve_lane_managed", return_value=(self.epoch_id, self.lane, event)),
            patch.object(review, "_worktree_commit", return_value="commit-1"),
            patch.object(review, "close_event", side_effect=close_side_effect),
        ):
            return review.run_completion_review(
                event_id="event-1" if managed else None,
                lane_id=None if managed else self.lane_id,
                review_outcome=outcome, approval=approval,
                review_summary="ROOT reviewed the native result", evidence=["review-note"],
                force_accept=force_reason is not None, force_reason=force_reason,
            )

    def _evidence(self) -> dict:
        value = terminal_evidence.read_terminal_evidence(
            self.rt, self.epoch_id, self.lane_id, run_id=self.run_id,
        )
        assert value is not None
        return value

    def _fresh_envelope(self, run_id: str) -> dict:
        finalized = _finalized_lane1_fixture(
            self.card, self.plan, self.worktree, run_id=run_id,
        )
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=finalized):
            envelope = memory_handoff.prepare_resume_envelope(
                task_card=self.card, lane_id=self.lane_id, run_id=run_id,
                worktree_path=self.worktree, base_commit="base-1",
            )
        assert envelope is not None
        return envelope

    def _native_unknown_review_queue(self) -> dict:
        self.result_path.unlink()
        self.lane["lifecycle"] = "result_invalid"
        atomic_write_json(Path(self.lane["controller_status_path"]), {
            "schema": "controller-status/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "controller_state": "exited", "provider_state": {"state": "exited", "exit_code": 1},
            "result_state": "invalid", "recorded_status": "provider_exited_no_result",
            "cleanup_proven": True, "updated_at": "2026-09-25T00:00:00Z",
        })
        atomic_write_json(current_epoch_path(self.rt), {
            "schema": "current-epoch/v1", "epoch_id": self.epoch_id, "queue_id": "queue-1",
        })
        atomic_write_json(manager_queue_path(self.rt), {
            "schema": "manager-queue/v1", "epoch_id": self.epoch_id,
            "queue_id": "queue-1", "events": [],
        })
        monitor._promote_status(self.rt, self.epoch_id, self.lane, "provider_exited_no_result")
        event = manager_queue.read_manager_queue(self.rt)["events"][0]
        return manager_queue.acknowledge_event(self.rt, event["event_id"])

    def _crash_after_unknown_preparation(self) -> None:
        original_write = review.atomic_write_json

        def crash_before_acceptance(path: Path, value: dict) -> None:
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash before acceptance publication")
            original_write(path, value)

        with patch.object(review, "atomic_write_json", side_effect=crash_before_acceptance):
            self.assertFalse(self._review(
                outcome="UNKNOWN", force_reason="ROOT accepts terminal uncertainty", managed=True,
            )["ok"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        self.assertIsNone(review.read_json(
            self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME,
        )["result"])

    def test_native_result_invalid_closed_status_recovers_once_in_real_queue(self) -> None:
        status_event = self._native_unknown_review_queue()
        self._crash_after_unknown_preparation()
        manager_queue.close_event(
            self.rt, status_event["event_id"], "COMPLETE", summary="review prepared",
        )
        with patch.object(monitor, "update_lane"):
            recovered = monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane)
            repeated = monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane)
        self.assertEqual(1, len(recovered))
        self.assertEqual([], repeated)
        queue = manager_queue.read_manager_queue(self.rt)
        self.assertEqual("COMPLETE", queue["events"][0]["state"])
        self.assertEqual("COMPLETION_REVIEW_REQUIRED", recovered[0]["type"])
        self.assertEqual("PENDING", recovered[0]["state"])
        self.assertEqual(self.run_id, recovered[0]["run_id"])
        self.assertEqual([recovered[0]["event_id"]], [
            event["event_id"] for event in queue["events"]
            if event["type"] == "COMPLETION_REVIEW_REQUIRED"
        ])

    def test_native_result_invalid_open_status_suppresses_recovery(self) -> None:
        status_event = self._native_unknown_review_queue()
        self._crash_after_unknown_preparation()
        with patch.object(monitor, "update_lane"):
            self.assertEqual([], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        queue = manager_queue.read_manager_queue(self.rt)
        self.assertEqual([status_event["event_id"]], [event["event_id"] for event in queue["events"]])
        self.assertEqual("ACKNOWLEDGED", queue["events"][0]["state"])

    def test_result_invalid_without_retained_unknown_stays_non_reviewable(self) -> None:
        status_event = self._native_unknown_review_queue()
        manager_queue.close_event(
            self.rt, status_event["event_id"], "COMPLETE", summary="no review prepared",
        )
        with patch.object(monitor, "update_lane"):
            self.assertEqual([], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        self.assertEqual(1, len(manager_queue.read_manager_queue(self.rt)["events"]))

    def test_result_invalid_rejects_wrong_run_retained_unknown(self) -> None:
        status_event = self._native_unknown_review_queue()
        self._crash_after_unknown_preparation()
        terminal_path = self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME
        terminal = review.read_json(terminal_path)
        terminal["run_id"] = "another-run"
        terminal["content_hash"] = content_hash(terminal)
        atomic_write_json(terminal_path, terminal)
        manager_queue.close_event(
            self.rt, status_event["event_id"], "COMPLETE", summary="review prepared",
        )
        with patch.object(monitor, "update_lane"):
            self.assertEqual([], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        self.assertEqual(1, len(manager_queue.read_manager_queue(self.rt)["events"]))

    def test_pass_binds_exact_parent_dispatch_result_review_and_acceptance(self) -> None:
        response = self._review()
        self.assertTrue(response["ok"], response)
        evidence = self._evidence()
        self.assertEqual("native-terminal-evidence/v1", evidence["schema"])
        self.assertEqual(self.card, evidence["task_card"])
        self.assertEqual(self.plan, evidence["accepted_plan"])
        self.assertEqual(self.envelope["decision_id"], evidence["decision_id"])
        self.assertEqual(
            {key: self.operation[key] for key in evidence["dispatch"]["operation"]},
            evidence["dispatch"]["operation"],
        )
        self.assertEqual(self.observed, evidence["dispatch"]["observed_invocation"])
        self.assertEqual(self.envelope["content_hash"], evidence["dispatch"]["envelope_digest"])
        self.assertEqual(self.envelope["configuration_digest"], evidence["configuration_digest"])
        self.assertEqual("PASS", evidence["result"]["outcome"])
        self.assertEqual("PASS", evidence["review"]["review_outcome"])
        self.assertEqual("ACCEPTED", evidence["acceptance"]["approval"])
        self.assertIsNone(evidence["terminal_proof"])
        self.assertIn(str(self.folder / "NATIVE_TERMINAL_EVIDENCE.json"), response["evidence_paths"])
        memory_store = store.MemoryStore(memory_handoff.memory_paths(self.worktree)[0])
        memory_store.initialize()
        try:
            self.assertEqual("PASS", memory_store.get_outcome(evidence["decision_id"])["status"])
        finally:
            memory_store.close()

    def test_two_marker_result_has_separate_review_and_one_fixed_outcome(self) -> None:
        self.worktree = self.root / "marker-worktree"
        (self.worktree / ".agent-workspace").mkdir(parents=True)
        self.result_path = self.worktree / "RESULT.json"
        self.lane["worktree_path"] = str(self.worktree)
        self.lane["controller_status_path"] = str(
            self.worktree / ".agent-workspace" / "controller.status.json"
        )
        atomic_write_json(self.worktree / ".agent-workspace" / "task-card.json", self.card)
        store_path, _ = memory_handoff.memory_paths(self.worktree)
        memory_store = store.MemoryStore(store_path)
        memory_store.initialize()
        try:
            memory_store.record_decision(contracts.make_decision(self.card, self.plan))
        finally:
            memory_store.close()
        sources = [
            {"id": "everos-item", "kind": "procedure", "origin": "everos_generated_skill",
             "source_id": "everos-skill-id", "revision_id": "everos-r1", "freshness": "live",
             "content": {"guidance": "EVEROS_MVP_MARKER"}},
            {"id": "atlas-item", "kind": "procedure", "origin": "atlas_trusted_procedure",
             "source_id": "atlas-procedure-id", "revision_id": "atlas-r1", "freshness": "live",
             "content": {"guidance": "ATLAS_MVP_MARKER"}},
        ]
        finalized = _finalized_lane1_fixture(
            self.card, self.plan, self.worktree, optional_items=sources,
        )
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=finalized):
            self.envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card, lane_id=self.lane_id, run_id=self.run_id,
                worktree_path=self.worktree, base_commit="base-1",
            )
        assert self.envelope is not None
        context = memory_handoff.load_final_context(
            worktree_path=self.worktree, envelope=self.envelope,
        )
        self.observed = memory_handoff.native_observation(
            envelope=self.envelope, context=context,
            controller_identity=self.lane["process"],
        )
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=self.envelope,
        )
        self.operation = memory_handoff.record_observed_invocation(
            worktree_path=self.worktree, envelope=self.envelope,
            observed_invocation=self.observed,
        )
        result = {
            "schema": "result/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "outcome": "PASS", "summary": "local marker-bearing worker result",
            "evidence": ["EVEROS_MVP_MARKER", "ATLAS_MVP_MARKER"],
            "completed_at": "2026-09-26T00:00:00Z",
        }
        result["content_hash"] = content_hash(result)
        atomic_write_json(self.result_path, result)
        self.assertTrue(self._review()["ok"])
        evidence = self._evidence()
        self.assertEqual(result, evidence["result"])
        self.assertEqual(context, evidence["final_context"])
        self.assertEqual(self.observed, evidence["dispatch"]["observed_invocation"])
        self.assertEqual("PASS", evidence["review"]["review_outcome"])
        self.assertNotEqual(result["content_hash"], evidence["review"]["content_hash"])
        memory_store = store.MemoryStore(store_path)
        memory_store.initialize()
        try:
            outcome = memory_store.get_outcome(evidence["decision_id"])
            self.assertEqual("PASS", outcome["status"])
            self.assertEqual(self.plan["plan_id"], outcome["plan_id"])
            self.assertEqual(self.run_id, outcome["linked_run_id"])
            self.assertEqual(evidence["content_hash"], outcome["evidence_digest"])
        finally:
            memory_store.close()
        self.assertTrue(self._review()["ok"])
        changed = copy.deepcopy(evidence)
        changed["result"]["evidence"] = ["fabricated"]
        changed["result"]["content_hash"] = content_hash(changed["result"])
        with self.assertRaises(terminal_evidence.TerminalEvidenceError):
            terminal_evidence.validate_terminal_evidence(changed)
        self.assertEqual(evidence, self._evidence())

    def test_exact_enhanced_retirement_keeps_other_owner_and_dirty_worktree(self) -> None:
        self.assertTrue(self._review()["ok"])
        self.lane["lifecycle"] = "accepted"
        lanes.write_lane(self.rt, self.epoch_id, self.lane_id, self.lane)
        other_worktree = self.root / "other-dirty-worktree"
        other_worktree.mkdir()
        dirty_file = other_worktree / "user-change.txt"
        dirty_file.write_text("keep this work", encoding="utf-8")
        write_active_lanes(self.rt, self.epoch_id, [
            {"lane_id": self.lane_id, "run_id": self.run_id},
            {"lane_id": "other-lane", "run_id": "other-run"},
        ])
        leases.acquire_leases(
            self.rt, ["owned-resource"], lane_id=self.lane_id, run_id=self.run_id,
            pid=123, creation_time="incarnation-1",
        )
        leases.acquire_leases(
            self.rt, ["other-resource"], lane_id="other-lane", run_id="other-run",
            pid=999, creation_time="other-incarnation",
        )
        boundary = {"root": {"pid": 42, "creation_time": "provider-incarnation"}}
        status = {
            "schema": "controller-status/v1", "lane_id": self.lane_id,
            "run_id": self.run_id, "controller_state": "exited",
            "provider_state": {"state": "exited", "pid": 42,
                               "creation_time": "provider-incarnation"},
            "process_boundary": boundary, "cleanup_proven": False,
        }
        checked = []
        def exact_identity(pid: int, creation: str) -> bool:
            checked.append((pid, creation))
            self.assertIn((pid, creation), [
                (123, "incarnation-1"), (42, "provider-incarnation"),
            ])
            return False
        acceptance = self.folder / "ORCHESTRATOR_ACCEPTANCE.json"
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(
                runtime_root=self.rt, root_workspace=self.root,
            )),
            patch.object(launch, "find_active_lane", return_value=(self.epoch_id, self.lane)),
            patch.object(launch, "_read_controller_status", return_value=status),
            patch.object(launch.processes, "identity_matches", side_effect=exact_identity),
            patch.object(launch.processes, "process_boundary_is_gone", side_effect=lambda observed: observed == boundary),
            patch.object(launch, "_prune_worktrees") as prune,
        ):
            unresolved = launch.run_retire(str(acceptance))
            self.assertFalse(unresolved["ok"], unresolved)
            self.assertEqual(launch.RETIRE_CLEANUP_UNPROVEN, unresolved["code"])
            self.assertIsNotNone(leases.read_lease(self.rt, "owned-resource"))
            status["cleanup_proven"] = True
            retired = launch.run_retire(str(acceptance))
        self.assertTrue(retired["ok"], retired)
        self.assertIn((123, "incarnation-1"), checked)
        self.assertIn((42, "provider-incarnation"), checked)
        self.assertEqual("retired", lanes.read_lane(self.rt, self.epoch_id, self.lane_id)["lifecycle"])
        self.assertIsNone(leases.read_lease(self.rt, "owned-resource"))
        self.assertEqual("other-lane", leases.read_lease(self.rt, "other-resource")["lane_id"])
        self.assertEqual(["other-lane"], [entry["lane_id"] for entry in read_active_lanes(self.rt, self.epoch_id)])
        self.assertEqual("keep this work", dirty_file.read_text(encoding="utf-8"))
        self.assertTrue(acceptance.is_file())
        prune.assert_called_once()

    def test_fail_blocked_and_forced_acceptance_remain_distinct(self) -> None:
        self._result("FAIL")
        response = self._review(outcome="FAIL", approval="REJECTED")
        self.assertTrue(response["ok"], response)
        evidence = self._evidence()
        self.assertEqual("FAIL", evidence["result"]["outcome"])
        self.assertEqual("REJECTED", evidence["acceptance"]["approval"])
        self.assertIsNone(evidence["acceptance"].get("force_accept_reason"))
        memory_store = store.MemoryStore(memory_handoff.memory_paths(self.worktree)[0])
        memory_store.initialize()
        try:
            with self.assertRaises(store.StoreError):
                memory_store.get_outcome(evidence["decision_id"])
        finally:
            memory_store.close()

    def test_blocked_forced_acceptance_fixes_blocked_quality(self) -> None:
        self._result("BLOCKED")
        response = self._review(outcome="BLOCKED", approval="ACCEPTED", force_reason="ROOT accepts blocked work")
        self.assertTrue(response["ok"], response)
        evidence = self._evidence()
        self.assertEqual("BLOCKED", evidence["result"]["outcome"])
        self.assertEqual("BLOCKED", evidence["review"]["review_outcome"])
        self.assertEqual("ACCEPTED", evidence["acceptance"]["approval"])
        self.assertEqual("ROOT accepts blocked work", evidence["acceptance"].get("force_accept_reason"))
        memory_store = store.MemoryStore(memory_handoff.memory_paths(self.worktree)[0])
        memory_store.initialize()
        try:
            self.assertEqual("BLOCKED", memory_store.get_outcome(evidence["decision_id"])["status"])
        finally:
            memory_store.close()

    def test_no_delivered_observation_or_wrong_run_is_refused_without_pair(self) -> None:
        for change in ("pending", "wrong-run", "wrong-invocation"):
            with self.subTest(change=change):
                operation = copy.deepcopy(self.operation)
                if change == "pending":
                    operation["status"] = "pending"
                    operation["observed_invocation"] = None
                elif change == "wrong-run":
                    operation["run_id"] = "other-run"
                else:
                    operation["observed_invocation"]["creation_time"] = "other-incarnation"
                operation["content_hash"] = content_hash(operation)
                with patch.object(memory_handoff, "get_dispatch_operation", return_value=operation):
                    response = self._review()
                self.assertFalse(response["ok"], response)
                self.assertFalse((self.folder / "COMPLETION_REVIEW.json").exists())

    def test_exact_replay_and_partial_publication_reuse_original_identity(self) -> None:
        real_write = review.atomic_write_json

        def fail_after_review(path: Path, value: dict) -> None:
            real_write(path, value)
            if path.name == "COMPLETION_REVIEW.json":
                raise OSError("crash after review write")

        with patch.object(review, "atomic_write_json", side_effect=fail_after_review):
            first = self._review()
        self.assertFalse(first["ok"])
        before = (self.folder / "COMPLETION_REVIEW.json").read_bytes()
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        self.assertFalse(monitor._recover_broken_review_pair(self.rt, self.epoch_id, self.lane))
        self.assertEqual(before, (self.folder / "COMPLETION_REVIEW.json").read_bytes())
        self.assertTrue(self._review()["ok"])
        evidence = self._evidence()
        self.assertTrue(self._review()["ok"])
        self.assertEqual(before, (self.folder / "COMPLETION_REVIEW.json").read_bytes())
        self.assertEqual(evidence, self._evidence())

    def test_crash_after_evidence_before_acceptance_recovers_without_advancement(self) -> None:
        real_write = review.atomic_write_json

        def fail_after_evidence(path: Path, value: dict) -> None:
            real_write(path, value)
            if path.name == "NATIVE_TERMINAL_EVIDENCE.json":
                raise OSError("crash after terminal evidence write")

        with patch.object(review, "atomic_write_json", side_effect=fail_after_evidence):
            first = self._review()
        self.assertFalse(first["ok"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        before = (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes()
        self.assertFalse(monitor._recover_broken_review_pair(self.rt, self.epoch_id, self.lane))
        self.assertTrue(self._review()["ok"])
        self.assertEqual(before, (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes())
        self.assertEqual("ACCEPTED", self._evidence()["acceptance"]["approval"])

    def test_manager_close_failure_and_ack_ambiguity_precede_acceptance(self) -> None:
        first = self._review(managed=True, close_side_effect=ManagerQueueError("QUEUE_UNAVAILABLE", "queue unavailable"))
        self.assertFalse(first["ok"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        before = (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes()
        second = self._review(managed=True, event_state="COMPLETE")
        self.assertTrue(second["ok"], second)
        self.assertEqual(before, (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes())
        completed = self._review(managed=True, event_state="COMPLETE")
        self.assertTrue(completed["ok"], completed)
        self.assertEqual(before, (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes())

    def test_managed_close_and_acceptance_crashes_replay_exact_identity(self) -> None:
        def close_after_preparation(*args, **kwargs):
            self.assertEqual((self.rt, "event-1", "COMPLETE"), args)
            self.assertIn("PASS / ACCEPTED", kwargs["summary"])
            self.assertTrue((self.folder / "COMPLETION_REVIEW.json").is_file())
            self.assertTrue((self.folder / "NATIVE_TERMINAL_EVIDENCE.json").is_file())
            self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())

        real_write = review.atomic_write_json

        def crash_after_acceptance(path: Path, value: dict) -> None:
            real_write(path, value)
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash after acceptance publication")

        with patch.object(review, "atomic_write_json", side_effect=crash_after_acceptance):
            first = self._review(managed=True, close_side_effect=close_after_preparation)
        self.assertFalse(first["ok"])
        before = self._evidence()
        self.lane["lifecycle"] = "retired"
        import shutil

        shutil.rmtree(self.worktree)
        replay = self._review(managed=True, event_state="COMPLETE")
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(before, self._evidence())

    def test_crash_after_managed_close_before_acceptance_replays_preparation(self) -> None:
        original_write = review.atomic_write_json

        def before_acceptance(path: Path, value: dict) -> None:
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash before acceptance publication")
            original_write(path, value)

        with patch.object(review, "atomic_write_json", side_effect=before_acceptance):
            first = self._review(managed=True)
        self.assertFalse(first["ok"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        before = (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes()
        closed_event = {
            "type": "COMPLETION_REVIEW_REQUIRED", "lane_id": self.lane_id,
            "run_id": self.run_id, "state": "COMPLETE",
            "dedup_key": f"review:{self.lane_id}:{self.run_id}:{self.epoch_id}",
        }
        with (
            patch.object(monitor, "read_manager_queue", return_value={"events": [closed_event]}),
            patch.object(monitor, "promote_event", return_value={"event_id": "recovery"}) as promote,
            patch.object(monitor, "update_lane"),
        ):
            self.assertEqual([{"event_id": "recovery"}], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        promote.assert_called_once()
        self.assertIsNone(promote.call_args.kwargs["dedup_key"])
        open_event = {**closed_event, "state": "ACKNOWLEDGED"}
        with (
            patch.object(monitor, "read_manager_queue", return_value={"events": [closed_event, open_event]}),
            patch.object(monitor, "promote_event") as promote,
            patch.object(monitor, "update_lane"),
        ):
            self.assertEqual([], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        promote.assert_not_called()
        with (
            patch.object(monitor, "read_manager_queue", return_value={"events": []}),
            patch.object(monitor, "promote_event", return_value={"event_id": "replacement"}) as promote,
            patch.object(monitor, "update_lane"),
        ):
            self.assertEqual([{"event_id": "replacement"}], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        promote.assert_called_once()

        def close_recovery(*_args, **_kwargs):
            self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())

        replay = self._review(managed=True, close_side_effect=close_recovery)
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(before, (self.folder / "NATIVE_TERMINAL_EVIDENCE.json").read_bytes())

    def test_closed_preparation_replays_after_retirement_without_worktree(self) -> None:
        original_write = review.atomic_write_json

        def crash_before_acceptance(path: Path, value: dict) -> None:
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash before acceptance publication")
            original_write(path, value)

        with patch.object(review, "atomic_write_json", side_effect=crash_before_acceptance):
            self.assertFalse(self._review(managed=True)["ok"])
        retained = review.read_json(self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME)
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        self.lane["lifecycle"] = "retired"
        import shutil

        shutil.rmtree(self.worktree)
        conflict = self._review(managed=True, event_state="COMPLETE", approval="REJECTED")
        self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, conflict["code"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        terminal_path = self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME
        damaged = copy.deepcopy(retained)
        damaged["decision_id"] = "other-decision"
        damaged["content_hash"] = content_hash(damaged)
        atomic_write_json(terminal_path, damaged)
        self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT,
                         self._review(managed=True, event_state="COMPLETE")["code"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        atomic_write_json(terminal_path, retained)
        replay = self._review(managed=True, event_state="COMPLETE")
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(retained["acceptance"], review.read_json(self.folder / "ORCHESTRATOR_ACCEPTANCE.json"))
        self.assertEqual(retained, self._evidence())

    def test_closed_true_unknown_preparation_without_result_recovers_one_open_event(self) -> None:
        self.result_path.unlink()
        atomic_write_json(Path(self.lane["controller_status_path"]), {
            "schema": "controller-status/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "controller_state": "exited", "provider_state": {"state": "exited", "exit_code": 1},
            "result_state": "invalid", "recorded_status": "provider_exited_no_result",
            "cleanup_proven": True, "updated_at": "2026-09-25T00:00:00Z",
        })
        original_write = review.atomic_write_json

        def crash_before_acceptance(path: Path, value: dict) -> None:
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash before acceptance publication")
            original_write(path, value)

        with patch.object(review, "atomic_write_json", side_effect=crash_before_acceptance):
            self.assertFalse(self._review(outcome="UNKNOWN", force_reason="ROOT accepts terminal uncertainty", managed=True)["ok"])
        self.assertFalse((self.folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())
        terminal = review.read_json(self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME)
        self.assertIsNone(terminal["result"])
        closed = {
            "type": "COMPLETION_REVIEW_REQUIRED", "lane_id": self.lane_id,
            "run_id": self.run_id, "state": "COMPLETE",
            "dedup_key": f"review:{self.lane_id}:{self.run_id}:{self.epoch_id}",
        }
        opened = {**closed, "event_id": "recovery", "state": "PENDING"}
        with (
            patch.object(monitor, "read_manager_queue", return_value={"events": [closed]}),
            patch.object(monitor, "promote_event", return_value=opened) as promote,
            patch.object(monitor, "update_lane"),
        ):
            self.assertEqual([opened], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        promote.assert_called_once()
        self.assertIsNone(promote.call_args.kwargs["dedup_key"])
        with (
            patch.object(monitor, "read_manager_queue", return_value={"events": [closed, opened]}),
            patch.object(monitor, "promote_event") as promote,
            patch.object(monitor, "update_lane"),
        ):
            self.assertEqual([], monitor._recover_lost_review_event(self.rt, self.epoch_id, self.lane))
        promote.assert_not_called()

    def test_scoped_retained_preparation_rejects_conflicting_root_before_acceptance(self) -> None:
        self.lane["resume_from_run_id"] = "prior-run"
        scoped = terminal_evidence.publication_dir(self.rt, self.epoch_id, self.lane)
        original_write = review.atomic_write_json

        def crash_before_acceptance(path: Path, value: dict) -> None:
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash before acceptance publication")
            original_write(path, value)

        with patch.object(review, "atomic_write_json", side_effect=crash_before_acceptance):
            self.assertFalse(self._review(managed=True)["ok"])
        prepared = review.read_json(scoped / terminal_evidence.TERMINAL_EVIDENCE_NAME)
        changed_review = copy.deepcopy(prepared["review"])
        changed_review["review_summary"] = "conflicting root review"
        changed_review["content_hash"] = content_hash(changed_review)
        changed_acceptance = copy.deepcopy(prepared["acceptance"])
        changed_acceptance["approval"] = "REJECTED"
        changed_acceptance["content_hash"] = content_hash(changed_acceptance)
        changed_terminal = copy.deepcopy(prepared)
        changed_terminal["review"] = changed_review
        changed_terminal["acceptance"]["review_ref"] = changed_review["content_hash"]
        changed_terminal["acceptance"]["content_hash"] = content_hash(changed_terminal["acceptance"])
        changed_terminal["content_hash"] = content_hash(changed_terminal)
        terminal_evidence.validate_terminal_evidence(changed_terminal, lane_id=self.lane_id, run_id=self.run_id)
        for name, conflicting in (
            ("COMPLETION_REVIEW.json", changed_review),
            ("ORCHESTRATOR_ACCEPTANCE.json", changed_acceptance),
            (terminal_evidence.TERMINAL_EVIDENCE_NAME, changed_terminal),
        ):
            with self.subTest(root_slot=name):
                root_path = self.folder / name
                atomic_write_json(root_path, conflicting)
                replay = self._review(managed=True, event_state="COMPLETE")
                self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, replay["code"])
                self.assertFalse((scoped / "ORCHESTRATOR_ACCEPTANCE.json").exists())
                self.assertIsNone(monitor._read_acceptance_chain(self.rt, self.epoch_id, self.lane_id, self.lane))
                root_path.unlink()
        for name, identical in (
            ("COMPLETION_REVIEW.json", prepared["review"]),
            ("ORCHESTRATOR_ACCEPTANCE.json", prepared["acceptance"]),
            (terminal_evidence.TERMINAL_EVIDENCE_NAME, prepared),
        ):
            atomic_write_json(self.folder / name, identical)
        self.assertTrue(self._review(managed=True, event_state="COMPLETE")["ok"])
        self.assertEqual(prepared, self._evidence())

    def test_scoped_retained_preparation_allows_different_run_root_history(self) -> None:
        self.lane["resume_from_run_id"] = "prior-run"
        scoped = terminal_evidence.publication_dir(self.rt, self.epoch_id, self.lane)
        original_write = review.atomic_write_json

        def crash_before_acceptance(path: Path, value: dict) -> None:
            if path.name == "ORCHESTRATOR_ACCEPTANCE.json":
                raise OSError("crash before acceptance publication")
            original_write(path, value)

        with patch.object(review, "atomic_write_json", side_effect=crash_before_acceptance):
            self.assertFalse(self._review(managed=True)["ok"])
        prepared = review.read_json(scoped / terminal_evidence.TERMINAL_EVIDENCE_NAME)
        for name, field in (
            ("COMPLETION_REVIEW.json", "review"),
            ("ORCHESTRATOR_ACCEPTANCE.json", "acceptance"),
            (terminal_evidence.TERMINAL_EVIDENCE_NAME, None),
        ):
            historical = copy.deepcopy(prepared if field is None else prepared[field])
            historical["run_id"] = "prior-run"
            historical["content_hash"] = content_hash(historical)
            atomic_write_json(self.folder / name, historical)
        replay = self._review(managed=True, event_state="COMPLETE")
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(prepared, self._evidence())

    def test_scoped_reader_and_consumers_reject_same_run_root_siblings(self) -> None:
        self.assertTrue(self._review()["ok"])
        original = self._evidence()
        scoped = terminal_evidence.run_publication_dir(self.rt, self.epoch_id, self.lane_id, self.run_id)
        scoped.mkdir(parents=True)
        for name in ("COMPLETION_REVIEW.json", "ORCHESTRATOR_ACCEPTANCE.json", terminal_evidence.TERMINAL_EVIDENCE_NAME):
            (scoped / name).write_bytes((self.folder / name).read_bytes())
        self.lane["resume_from_run_id"] = "prior-run"
        self.assertEqual(original, self._evidence())
        (self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME).unlink()
        altered_review = copy.deepcopy(original["review"])
        altered_review["review_summary"] = "contradictory ROOT review"
        altered_review["content_hash"] = content_hash(altered_review)
        altered_acceptance = copy.deepcopy(original["acceptance"])
        altered_acceptance["review_ref"] = altered_review["content_hash"]
        altered_acceptance["content_hash"] = content_hash(altered_acceptance)
        atomic_write_json(self.folder / "COMPLETION_REVIEW.json", altered_review)
        atomic_write_json(self.folder / "ORCHESTRATOR_ACCEPTANCE.json", altered_acceptance)
        self.assertTrue(review.validate_acceptance_chain(altered_review, altered_acceptance, lane_id=self.lane_id, run_id=self.run_id))
        with self.assertRaises(terminal_evidence.TerminalEvidenceError):
            self._evidence()
        self.assertIsNone(controller._read_acceptance_chain(self.rt, self.epoch_id, self.lane))
        self.assertIsNone(monitor._read_acceptance_chain(self.rt, self.epoch_id, self.lane_id, self.lane))
        self.assertEqual("review_pending", monitor.derive_lane_status(
            self.rt, self.epoch_id, self.lane,
            {"recorded_status": "review_pending", "cleanup_proven": True},
        ))
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.rt, root_workspace=self.root)),
            patch.object(launch, "find_active_lane", return_value=(self.epoch_id, self.lane)),
            patch.object(launch, "release_leases") as release,
        ):
            retired = launch.run_retire(str(scoped / "ORCHESTRATOR_ACCEPTANCE.json"))
        self.assertFalse(retired["ok"])
        release.assert_not_called()

        for name, changed in (
            ("COMPLETION_REVIEW.json", altered_review),
            ("ORCHESTRATOR_ACCEPTANCE.json", altered_acceptance),
        ):
            with self.subTest(root_slot=name):
                (self.folder / "COMPLETION_REVIEW.json").unlink(missing_ok=True)
                (self.folder / "ORCHESTRATOR_ACCEPTANCE.json").unlink(missing_ok=True)
                atomic_write_json(self.folder / name, changed)
                with self.assertRaises(terminal_evidence.TerminalEvidenceError):
                    self._evidence()
        (self.folder / "COMPLETION_REVIEW.json").unlink(missing_ok=True)
        (self.folder / "ORCHESTRATOR_ACCEPTANCE.json").unlink(missing_ok=True)
        changed_terminal = copy.deepcopy(original)
        changed_terminal["review"] = altered_review
        changed_terminal["acceptance"] = altered_acceptance
        changed_terminal["content_hash"] = content_hash(changed_terminal)
        terminal_evidence.validate_terminal_evidence(changed_terminal, lane_id=self.lane_id, run_id=self.run_id)
        atomic_write_json(self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME, changed_terminal)
        with self.assertRaises(terminal_evidence.TerminalEvidenceError):
            self._evidence()
        historical = copy.deepcopy(original["review"])
        historical["run_id"] = "prior-run"
        historical["content_hash"] = content_hash(historical)
        (self.folder / terminal_evidence.TERMINAL_EVIDENCE_NAME).unlink()
        atomic_write_json(self.folder / "COMPLETION_REVIEW.json", historical)
        self.assertEqual(original, self._evidence())

    def test_retired_managed_close_retry_uses_retained_exact_publication(self) -> None:
        fresh_response = self._review()
        self.assertTrue(fresh_response["ok"], fresh_response)
        before = self._evidence()
        self.lane["lifecycle"] = "retired"
        import shutil

        shutil.rmtree(self.worktree)
        failed = self._review(managed=True, close_side_effect=ManagerQueueError("QUEUE_UNAVAILABLE", "queue unavailable"))
        self.assertFalse(failed["ok"])
        replay = self._review(managed=True)
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(before, self._evidence())
        conflicting = self._review(managed=True, outcome="FAIL", force_reason="override")
        self.assertFalse(conflicting["ok"])

    def test_conflicting_acceptance_and_evidence_stay_visible(self) -> None:
        self.assertTrue(self._review()["ok"])
        acceptance_path = self.folder / "ORCHESTRATOR_ACCEPTANCE.json"
        changed = copy.deepcopy(self._evidence()["acceptance"])
        changed["approval"] = "REJECTED"
        changed["content_hash"] = content_hash(changed)
        atomic_write_json(acceptance_path, changed)
        before = acceptance_path.read_bytes()
        response = self._review()
        self.assertFalse(response["ok"])
        self.assertEqual(before, acceptance_path.read_bytes())
        self.assertTrue((self.folder / "NATIVE_TERMINAL_EVIDENCE.json").exists())

    def test_conflicting_review_and_terminal_file_are_preserved(self) -> None:
        self.assertTrue(self._review()["ok"])
        review_path = self.folder / "COMPLETION_REVIEW.json"
        changed = copy.deepcopy(self._evidence()["review"])
        changed["review_summary"] = "contradictory finding"
        changed["content_hash"] = content_hash(changed)
        atomic_write_json(review_path, changed)
        before = review_path.read_bytes()
        self.assertFalse(self._review()["ok"])
        self.assertEqual(before, review_path.read_bytes())
        terminal_path = self.folder / "NATIVE_TERMINAL_EVIDENCE.json"
        original = terminal_path.read_bytes()
        terminal_path.write_text('{"schema":"native-terminal-evidence/v1","wrong":true}', encoding="utf-8")
        corrupt = terminal_path.read_bytes()
        self.assertFalse(self._review()["ok"])
        self.assertEqual(corrupt, terminal_path.read_bytes())
        self.assertNotEqual(original, corrupt)

    def test_read_survives_worktree_retirement(self) -> None:
        self.assertTrue(self._review()["ok"])
        expected = self._evidence()
        import shutil

        shutil.rmtree(self.worktree)
        self.assertEqual(expected, self._evidence())

    def test_retained_sources_reject_rehashed_identity_mutations_after_retirement(self) -> None:
        self.assertTrue(self._review()["ok"])
        original = self._evidence()
        self.assertEqual(self.envelope, original["dispatch"]["envelope"])
        self.assertEqual(self.envelope["decision_id"], original["decision"]["decision_id"])
        self.assertEqual(self.observed["context_digest"], original["final_context"]["content_hash"])
        import shutil

        shutil.rmtree(self.worktree)
        cases = (
            ("task", lambda e: e["task_card"].update(task="unrelated task")),
            ("plan", lambda e: e["accepted_plan"].update(plan_id="unrelated plan")),
            ("objective", lambda e: e.update(objective_id="unrelated objective")),
            ("decision", lambda e: e["decision"].update(plan_id="unrelated plan")),
            ("decision-id", lambda e: e["decision"].update(decision_id="unrelated decision")),
            ("decision-configuration", lambda e: e["decision"]["configuration"].update(strategy="unrelated")),
            ("configuration", lambda e: e["configuration"].update(strategy="unrelated")),
            ("configuration-digest", lambda e: e.update(configuration_digest="unrelated digest")),
            ("envelope", lambda e: e["dispatch"]["envelope"].update(run_id="unrelated run")),
            ("envelope-task", lambda e: e["dispatch"]["envelope"].update(task_card_digest="unrelated task")),
            ("envelope-configuration", lambda e: e["dispatch"]["envelope"].update(configuration_digest="unrelated digest")),
            ("context", lambda e: e["final_context"].update(plan_id="unrelated plan")),
            ("context-id", lambda e: e["final_context"].update(context_id="unrelated context")),
            ("context-configuration", lambda e: e["final_context"]["configuration"].update(strategy="unrelated")),
            ("context-omissions", lambda e: e["final_context"].update(omitted=["unrelated item"])),
            ("operation", lambda e: e["dispatch"]["operation"].update(status="not-a-status")),
            ("operation-created", lambda e: e["dispatch"]["operation"].update(created_at=None)),
            ("operation-fields", lambda e: e["dispatch"]["operation"].update(schema="unrelated-operation/v1")),
            ("observed", lambda e: e["dispatch"]["observed_invocation"].update(context_id="unrelated context")),
            ("observed-pid", lambda e: e["dispatch"]["observed_invocation"].update(pid=999)),
            ("run", lambda e: e.update(run_id="unrelated run")),
        )
        for name, change in cases:
            with self.subTest(name=name):
                evidence = copy.deepcopy(original)
                change(evidence)
                for source in (evidence["task_card"], evidence["accepted_plan"], evidence["decision"],
                               evidence["dispatch"]["envelope"], evidence["final_context"]):
                    if "content_hash" in source:
                        source["content_hash"] = content_hash(source)
                evidence["dispatch"]["operation_digest"] = terminal_evidence.sha256_hex(evidence["dispatch"]["operation"])
                evidence["content_hash"] = content_hash(evidence)
                with self.assertRaises(terminal_evidence.TerminalEvidenceError):
                    terminal_evidence.validate_terminal_evidence(evidence)

    def test_enhanced_acceptance_requires_terminal_record_at_all_gates(self) -> None:
        self.assertTrue(self._review()["ok"])
        self.assertIsNotNone(controller._read_acceptance_chain(self.rt, self.epoch_id, self.lane))
        self.assertIsNotNone(monitor._read_acceptance_chain(self.rt, self.epoch_id, self.lane_id, self.lane))
        terminal_path = self.folder / "NATIVE_TERMINAL_EVIDENCE.json"
        original = terminal_path.read_bytes()
        for damage in (None, b'{"schema":"native-terminal-evidence/v1","wrong":true}'):
            with self.subTest(damage=damage):
                if damage is None:
                    terminal_path.unlink(missing_ok=True)
                else:
                    terminal_path.write_bytes(damage)
                self.assertIsNone(controller._read_acceptance_chain(self.rt, self.epoch_id, self.lane))
                self.assertIsNone(monitor._read_acceptance_chain(self.rt, self.epoch_id, self.lane_id, self.lane))
                self.assertEqual("review_pending", monitor.derive_lane_status(
                    self.rt, self.epoch_id, self.lane,
                    {"recorded_status": "review_pending", "cleanup_proven": True},
                ))
                with (
                    patch.object(launch, "find_harness_root", return_value=self.root),
                    patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.rt, root_workspace=self.root)),
                    patch.object(launch, "find_active_lane", return_value=(self.epoch_id, self.lane)),
                    patch.object(launch, "release_leases") as release,
                ):
                    retired = launch.run_retire(str(self.folder / "ORCHESTRATOR_ACCEPTANCE.json"))
                self.assertFalse(retired["ok"])
                release.assert_not_called()
        terminal_path.write_bytes(original)
        self.assertIsNotNone(controller._read_acceptance_chain(self.rt, self.epoch_id, self.lane))

    def test_enhanced_retire_requires_current_acceptance_slot_and_valid_evidence(self) -> None:
        self.assertTrue(self._review()["ok"])
        acceptance_path = self.folder / "ORCHESTRATOR_ACCEPTANCE.json"
        copied_path = self.root / "copied-acceptance.json"
        copied_path.write_bytes(acceptance_path.read_bytes())
        status = {
            "cleanup_proven": True, "controller_state": "exited",
            "process_boundary": {"root": {"pid": 1, "creation_time": "old"}},
            "provider_state": {"state": "exited"},
        }
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.rt, root_workspace=self.root)),
            patch.object(launch, "find_active_lane", return_value=(self.epoch_id, self.lane)),
            patch.object(launch, "_read_controller_status", return_value=status),
            patch.object(launch.processes, "identity_matches", return_value=False),
            patch.object(launch.processes, "process_boundary_is_gone", return_value=True),
            patch.object(launch, "release_leases") as release,
            patch.object(launch, "update_lane"),
            patch.object(launch, "read_active_lanes", return_value=[]),
            patch.object(launch, "write_active_lanes"),
            patch.object(launch, "_prune_worktrees"),
            patch.object(launch, "_maybe_close_epoch"),
        ):
            self.assertFalse(launch.run_retire(str(copied_path))["ok"])
            release.assert_not_called()
            self.assertTrue(launch.run_retire(str(acceptance_path))["ok"])
            release.assert_called_once()

    def test_legacy_pair_still_advances_without_native_evidence(self) -> None:
        legacy_card = contracts.make_task_card(task="legacy work", base_commit="base-1")
        atomic_write_json(self.worktree / ".agent-workspace" / "task-card.json", legacy_card)
        self.lane.pop("memory_plan_state")
        self.assertTrue(self._review()["ok"])
        self.assertIsNone(terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id))
        self.assertIsNotNone(controller._read_acceptance_chain(self.rt, self.epoch_id, self.lane))
        self.assertIsNotNone(monitor._read_acceptance_chain(self.rt, self.epoch_id, self.lane_id, self.lane))

    def test_legacy_managed_close_failure_replays_after_retirement(self) -> None:
        legacy_card = contracts.make_task_card(task="legacy work", base_commit="base-1")
        atomic_write_json(self.worktree / ".agent-workspace" / "task-card.json", legacy_card)
        self.lane.pop("memory_plan_state")
        first = self._review(managed=True, close_side_effect=ManagerQueueError("QUEUE_UNAVAILABLE", "queue unavailable"))
        self.assertFalse(first["ok"])
        before = (self.folder / "ORCHESTRATOR_ACCEPTANCE.json").read_bytes()
        self.lane["lifecycle"] = "retired"
        import shutil

        shutil.rmtree(self.worktree)
        replay = self._review(managed=True)
        self.assertTrue(replay["ok"], replay)
        self.assertEqual(before, (self.folder / "ORCHESTRATOR_ACCEPTANCE.json").read_bytes())

    def test_rejected_resume_retains_history_and_publishes_fresh_run(self) -> None:
        self.assertTrue(self._review(outcome="PASS", approval="REJECTED")["ok"])
        prior = self._evidence()
        authorization = review.read_json(self.folder / terminal_evidence.REJECTED_ATTEMPT_NAME)
        self.assertEqual(prior["content_hash"], authorization["evidence_digest"])
        rejected = memory_handoff.get_rejected_native_attempt(
            worktree_path=self.worktree,
            rejected_attempt_id=authorization["rejected_attempt_id"],
        )
        self.assertEqual(authorization, terminal_evidence.record_domain_review(
            self.rt, self.epoch_id, self.lane, prior,
        ))
        self.assertEqual("run-1", rejected["run_id"])
        self.lane["session"] = {"session_id": "saved-session"}
        self.lane["provider"] = {"id": "codex", "model": "test", "launch_config": {}}
        resume_card = self.root / "resume-card.json"
        atomic_write_json(resume_card, self.card)

        def update_lane(_rt, _epoch, _lane, mutate):
            self.lane.update(mutate(self.lane))
            return self.lane

        resumed_context = _finalized_lane1_fixture(
            self.card, self.plan, self.worktree, run_id="run-2"
        )
        with (
            patch.object(resume, "find_harness_root", return_value=self.root),
            patch.object(resume, "load_config", return_value=SimpleNamespace(runtime_root=self.rt, profile="plain")),
            patch.object(resume, "find_active_lane", return_value=(self.epoch_id, self.lane)),
            patch.object(resume, "read_lane", side_effect=lambda *_: self.lane),
            patch.object(resume, "update_lane", side_effect=update_lane),
            patch.object(resume, "_live_controller", return_value=False),
            patch.object(resume, "new_id", return_value="run-2"),
            patch.object(memory_handoff, "_prepare_memory_outcome", return_value=resumed_context),
        ):
            # This fixture stubs memory preparation but retains optional recalled content.
            resumed = resume.run_resume(
                lane_id=self.lane_id, resume_task_card=str(resume_card),
                search_stores=(SimpleNamespace(store_id="fixture-search"),),
            )
        self.assertTrue(resumed["ok"], resumed)
        self.assertEqual("run-2", self.lane["run_id"])
        self.assertEqual(rejected["rejected_attempt_id"], self.lane["native_supersession"]["rejected_attempt_id"])
        self.assertEqual(prior, terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-1"))
        self.assertIsNone(terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-2"))
        fresh_envelope = memory_handoff.load_envelope(self.worktree)
        assert fresh_envelope is not None
        self.assertEqual("checkpoint-1", fresh_envelope["checkpoint"])
        self.assertEqual("checkpoint-1", fresh_envelope["final_context"]["checkpoint"])
        binding = self.root / "orchestrator_harness" / "provider_adapters" / "codex" / "launcher_binding.py"
        binding.parent.mkdir(parents=True)
        binding.write_text("# test provider binding\n", encoding="utf-8")
        status = {
            "schema": "controller-status/v1", "lane_id": self.lane_id, "run_id": "run-2",
            "controller_state": "exited", "provider_state": {"state": "exited", "exit_code": 0},
            "cleanup_proven": True, "recorded_status": "review_pending",
        }
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.rt)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=(self.epoch_id, self.lane)),
            patch.object(launch, "read_lane", side_effect=lambda *_: self.lane),
            patch.object(launch, "update_lane", side_effect=update_lane),
            patch.object(launch, "_validate_provider_launch_config", return_value={}),
            patch.object(launch, "_wait_for_spawn_attestation", return_value={"pid": 124, "creation_time": "incarnation-2"}),
            patch.object(launch, "_read_controller_status", return_value=status),
            patch.object(launch, "_delivered_provider_outcome", return_value=("LAUNCH_OK", "provider started")),
            patch.object(launch.processes, "spawn_detached", return_value=MagicMock(pid=124)) as spawn,
            patch.object(memory_handoff, "record_dispatch_intent", wraps=memory_handoff.record_dispatch_intent) as intent,
        ):
            launched = launch.run_launch(self.lane_id)
        self.assertTrue(launched["ok"], launched)
        spawn.assert_called_once()
        self.assertEqual(
            authorization["rejected_attempt_id"],
            intent.call_args.kwargs["supersedes_rejected_attempt_id"],
        )
        self.run_id = "run-2"
        self.lane["lifecycle"] = "review_pending"
        self._result("PASS")
        fresh_response = self._review()
        self.assertTrue(fresh_response["ok"], fresh_response)
        fresh = terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-2")
        self.assertIsNotNone(fresh)
        self.assertNotEqual(prior["content_hash"], fresh["content_hash"])
        self.assertEqual(prior, terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-1"))
        self.assertFalse(self._review(outcome="FAIL", force_reason="contradiction")["ok"])
        self.assertEqual(fresh, terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-2"))
        self.lane["run_id"] = "run-1"
        collision = self._review()
        self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, collision["code"])
        self.lane["run_id"] = "run-2"
        current_path = terminal_evidence.run_publication_dir(
            self.rt, self.epoch_id, self.lane_id, "run-2",
        ) / "ORCHESTRATOR_ACCEPTANCE.json"
        current_bytes = current_path.read_bytes()
        changed_current = copy.deepcopy(fresh["acceptance"])
        changed_current["approval"] = "REJECTED"
        changed_current["content_hash"] = content_hash(changed_current)
        atomic_write_json(current_path, changed_current)
        with self.assertRaises(terminal_evidence.TerminalEvidenceError):
            terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-2")
        current_path.write_bytes(current_bytes)
        historical_review_path = self.folder / "COMPLETION_REVIEW.json"
        review_bytes = historical_review_path.read_bytes()
        changed_review = copy.deepcopy(prior["review"])
        changed_review["run_id"] = "other-run"
        changed_review["content_hash"] = content_hash(changed_review)
        atomic_write_json(historical_review_path, changed_review)
        with self.assertRaises(terminal_evidence.TerminalEvidenceError):
            terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-1")
        historical_review_path.write_bytes(review_bytes)
        historical_path = self.folder / "ORCHESTRATOR_ACCEPTANCE.json"
        historical = copy.deepcopy(prior["acceptance"])
        historical["approval"] = "ACCEPTED"
        historical["content_hash"] = content_hash(historical)
        atomic_write_json(historical_path, historical)
        with self.assertRaises(terminal_evidence.TerminalEvidenceError):
            terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-1")
        self.assertEqual(fresh, terminal_evidence.read_terminal_evidence(self.rt, self.epoch_id, self.lane_id, run_id="run-2"))

    def test_no_spawn_transfers_id_and_pending_or_wrong_id_refuses(self) -> None:
        self.assertTrue(self._review(approval="REJECTED")["ok"])
        prior = self._evidence()
        authorization = terminal_evidence.record_domain_review(self.rt, self.epoch_id, self.lane, prior)
        assert authorization is not None
        handoff = {field: authorization[field] for field in (
            "rejected_attempt_id", "run_id", "decision_id", "operation_id", "evidence_digest",
        )}
        self.lane["native_supersession"] = handoff
        second = self._fresh_envelope("run-2")
        exact_id = memory_handoff.supersession_id_for_launch(
            worktree_path=self.worktree, lane=self.lane, envelope=second,
        )
        self.assertEqual(authorization["rejected_attempt_id"], exact_id)
        for field in (
            "task_card_digest", "plan_digest", "base_commit", "lane_id",
            "worktree_path", "recipient", "checkpoint", "final_context_id",
        ):
            with self.subTest(field=field):
                changed = {**second, field: (
                    self.envelope["final_context_id"] if field == "final_context_id"
                    else "wrong-identity"
                )}
                with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, field):
                    memory_handoff.supersession_id_for_launch(
                        worktree_path=self.worktree, lane=self.lane, envelope=changed,
                    )
        self.lane["native_supersession"] = {**handoff, "evidence_digest": "wrong-digest"}
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "evidence_digest"):
            memory_handoff.supersession_id_for_launch(
                worktree_path=self.worktree, lane=self.lane, envelope=second,
            )
        self.lane["native_supersession"] = handoff
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "authorization"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=second,
                supersedes_rejected_attempt_id="wrong-id",
            )
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=second,
            supersedes_rejected_attempt_id=exact_id,
        )
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "pending"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=second,
                supersedes_rejected_attempt_id=exact_id,
            )
        third = self._fresh_envelope("run-3")
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "authorize|unresolved|conflicting"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=third,
                supersedes_rejected_attempt_id=exact_id,
            )
        memory_store = store.MemoryStore(memory_handoff.memory_paths(self.worktree)[0])
        memory_store.initialize()
        try:
            memory_store.record_operation(contracts.make_operation(
                kind="dispatch", envelope=second, status="failed_pre_spawn",
            ))
        finally:
            memory_store.close()
        transferred = memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=third,
            supersedes_rejected_attempt_id=exact_id,
        )
        self.assertEqual("pending", transferred["status"])
        self.assertEqual("run-3", transferred["run_id"])

    def test_ambiguous_corrected_intent_blocks_fresh_run(self) -> None:
        self.assertTrue(self._review(approval="REJECTED")["ok"])
        authorization = terminal_evidence.record_domain_review(
            self.rt, self.epoch_id, self.lane, self._evidence(),
        )
        assert authorization is not None
        second = self._fresh_envelope("run-2")
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=second,
            supersedes_rejected_attempt_id=authorization["rejected_attempt_id"],
        )
        memory_handoff.record_ambiguous_dispatch(worktree_path=self.worktree, envelope=second)
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "ambiguous"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=second,
                supersedes_rejected_attempt_id=authorization["rejected_attempt_id"],
            )
        third = self._fresh_envelope("run-3")
        with self.assertRaises(memory_handoff.MemoryHandoffError):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=third,
                supersedes_rejected_attempt_id=authorization["rejected_attempt_id"],
            )

    def test_second_rejection_returns_new_id_and_spends_first(self) -> None:
        self.assertTrue(self._review(approval="REJECTED")["ok"])
        first = terminal_evidence.record_domain_review(
            self.rt, self.epoch_id, self.lane, self._evidence(),
        )
        assert first is not None
        second = self._fresh_envelope("run-2")
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=second,
            supersedes_rejected_attempt_id=first["rejected_attempt_id"],
        )
        context = memory_handoff.load_final_context(worktree_path=self.worktree, envelope=second)
        self.run_id = "run-2"
        self.lane.update({
            "run_id": "run-2", "resume_from_run_id": "run-1",
            "process": {"pid": 124, "creation_time": "incarnation-2"},
        })
        memory_handoff.record_observed_invocation(
            worktree_path=self.worktree, envelope=second,
            observed_invocation=memory_handoff.native_observation(
                envelope=second, context=context, controller_identity=self.lane["process"],
            ),
        )
        self._result("PASS")
        self.assertTrue(self._review(approval="REJECTED")["ok"])
        second_attempt = terminal_evidence.record_domain_review(
            self.rt, self.epoch_id, self.lane, self._evidence(),
        )
        assert second_attempt is not None
        self.assertNotEqual(first["rejected_attempt_id"], second_attempt["rejected_attempt_id"])
        third = self._fresh_envelope("run-3")
        with self.assertRaises(memory_handoff.MemoryHandoffError):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=third,
                supersedes_rejected_attempt_id=first["rejected_attempt_id"],
            )
        self.assertEqual("pending", memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=third,
            supersedes_rejected_attempt_id=second_attempt["rejected_attempt_id"],
        )["status"])

    def test_true_unknown_requires_same_run_cleanup_and_root_exception(self) -> None:
        self.result_path.unlink()
        status = {
            "schema": "controller-status/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "controller_state": "exited", "provider_state": {"state": "exited", "exit_code": 1},
            "result_state": "invalid", "recorded_status": "provider_exited_no_result",
            "cleanup_proven": True, "updated_at": "2026-09-25T00:00:00Z",
        }
        status_path = Path(self.lane["controller_status_path"])
        for change in ("missing", "wrong-run", "no-cleanup", "mere-exit", "no-reason"):
            with self.subTest(change=change):
                candidate = copy.deepcopy(status)
                if change == "wrong-run":
                    candidate["run_id"] = "other-run"
                elif change == "no-cleanup":
                    candidate["cleanup_proven"] = False
                elif change == "mere-exit":
                    candidate["recorded_status"] = None
                if change == "missing":
                    status_path.unlink(missing_ok=True)
                else:
                    atomic_write_json(status_path, candidate)
                response = self._review(outcome="UNKNOWN", force_reason=None if change == "no-reason" else "ROOT accepts terminal uncertainty")
                self.assertFalse(response["ok"], response)
                self.assertFalse((self.folder / "COMPLETION_REVIEW.json").exists())
        atomic_write_json(status_path, status)
        response = self._review(outcome="UNKNOWN", force_reason="ROOT accepts terminal uncertainty")
        self.assertTrue(response["ok"], response)
        evidence = self._evidence()
        self.assertIsNone(evidence["result"])
        self.assertEqual("UNKNOWN", evidence["review"]["review_outcome"])
        self.assertEqual(status, evidence["terminal_proof"])
        self.assertEqual("ROOT accepts terminal uncertainty", evidence["acceptance"]["force_accept_reason"])

    def test_managed_no_result_status_event_is_reviewable_only_for_exact_status(self) -> None:
        event = {
            "event_id": "event-1", "type": "LANE_STATUS_CHANGED",
            "actionable_status": "provider_exited_no_result",
            "state": "ACKNOWLEDGED", "lane_id": self.lane_id, "run_id": self.run_id,
        }
        with (
            patch.object(review, "read_manager_queue", return_value={"events": [event]}),
            patch.object(review, "find_active_lane", return_value=(self.epoch_id, self.lane)),
        ):
            self.assertEqual(event, review._resolve_lane_managed(self.rt, "event-1")[2])
            event["actionable_status"] = "controller_exited"
            with self.assertRaises(review.ReviewError):
                review._resolve_lane_managed(self.rt, "event-1")

    def test_managed_event_resolves_retired_same_run_from_queue_epoch(self) -> None:
        self.lane["lifecycle"] = "retired"
        event = {
            "event_id": "event-1", "type": "COMPLETION_REVIEW_REQUIRED",
            "state": "ACKNOWLEDGED", "lane_id": self.lane_id, "run_id": self.run_id,
        }
        with (
            patch.object(review, "read_manager_queue", return_value={"epoch_id": self.epoch_id, "events": [event]}),
            patch.object(review, "find_active_lane", side_effect=LaneError("LANE_NOT_FOUND", "retired")),
            patch.object(review, "read_lane", return_value=self.lane) as read_lane,
        ):
            self.assertEqual((self.epoch_id, self.lane, event), review._resolve_lane_managed(self.rt, "event-1"))
        read_lane.assert_called_once_with(self.rt, self.epoch_id, self.lane_id)

    def test_cli_exposes_explicit_unknown_finding(self) -> None:
        args = operator_launch._build_parser().parse_args([
            "lane", "completion-review", "--lane-id", self.lane_id,
            "--review-outcome", "UNKNOWN", "--approval", "ACCEPTED",
            "--review-summary", "ROOT exceptional acceptance", "--force-accept",
            "--force-reason", "provider exited without a result after cleanup",
        ])
        self.assertEqual("UNKNOWN", args.review_outcome)

    def test_apc_child_legacy_and_all_off_take_ordinary_path_without_optional_store(self) -> None:
        child = contracts.make_task_card(task="Draft a plan for the parent", base_commit="base-1")
        all_off = contracts.make_task_card(
            task="Ordinary all-off work", base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=self.plan,
                configuration={"all_features": False},
            ),
        )
        for card in (child, all_off):
            with self.subTest(task=card["task"]):
                atomic_write_json(self.worktree / ".agent-workspace" / "task-card.json", card)
                self.lane.pop("memory_plan_state", None)
                with patch.object(memory_handoff, "get_dispatch_operation", side_effect=AssertionError("optional store opened")):
                    response = self._review()
                self.assertTrue(response["ok"], response)
                self.assertIsNone(terminal_evidence.read_terminal_evidence(
                    self.rt, self.epoch_id, self.lane_id,
                ))
                self.assertIsNotNone(controller._read_acceptance_chain(self.rt, self.epoch_id, self.lane))
                self.assertIsNotNone(monitor._read_acceptance_chain(self.rt, self.epoch_id, self.lane_id, self.lane))
                (self.folder / "ORCHESTRATOR_ACCEPTANCE.json").unlink()
                (self.folder / "COMPLETION_REVIEW.json").unlink()

    def test_fixture_separates_quality_replay_conflict_and_effect_progress(self) -> None:
        self.assertTrue(self._review()["ok"])
        evidence = self._evidence()
        consumer = FaithfulConsumerFixture()
        first = consumer.consume(evidence)
        consumer.effects[evidence["decision_id"]] = "failed-retryable"
        self.assertEqual(first, consumer.consume(copy.deepcopy(evidence)))
        self.assertEqual("failed-retryable", consumer.effects[evidence["decision_id"]])
        conflict = copy.deepcopy(evidence)
        conflict["review"]["review_outcome"] = "FAIL"
        conflict["review"]["content_hash"] = content_hash(conflict["review"])
        conflict["acceptance"]["review_ref"] = conflict["review"]["content_hash"]
        conflict["acceptance"]["force_accept_reason"] = "ROOT accepts failed finding"
        conflict["acceptance"]["content_hash"] = content_hash(conflict["acceptance"])
        conflict["content_hash"] = content_hash(conflict)
        terminal_evidence.validate_terminal_evidence(conflict)
        with self.assertRaises(ValueError):
            consumer.consume(conflict)
        self.assertEqual(first, consumer.fixed[evidence["decision_id"]])


class FaithfulConsumerFixture:
    """Model only one immutable quality decision and separately retryable effects."""

    def __init__(self) -> None:
        self.fixed: dict[str, dict] = {}
        self.effects: dict[str, str] = {}

    def consume(self, evidence: dict) -> dict:
        terminal_evidence.validate_terminal_evidence(evidence)
        decision_id = evidence["decision_id"]
        prior = self.fixed.get(decision_id)
        if prior is not None:
            if prior["content_hash"] != evidence["content_hash"]:
                raise ValueError("conflicting terminal evidence")
            return prior
        self.fixed[decision_id] = copy.deepcopy(evidence)
        self.effects[decision_id] = "pending"
        return self.fixed[decision_id]


if __name__ == "__main__":
    unittest.main()
