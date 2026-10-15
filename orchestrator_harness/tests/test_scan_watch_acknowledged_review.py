"""Managed queue-only watch and scoped ACK, with genuine prepared ROOTs."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from orchestrator_harness import manager_queue, scan_watch
from orchestrator_harness.epochs import manager_queue_path
from orchestrator_harness.root_session import publish_root_session, root_session_path
from orchestrator_harness.tests.support import PreparedRootFixture


class AcknowledgedReviewWatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = PreparedRootFixture(self)
        self.rt = self.fixture.rt
        self.scope = {"root_session_id": self.fixture.session["session_id"], "binding_id": "codex"}
        self.environment = patch.dict(os.environ, self.fixture.environment, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.root = patch.object(scan_watch, "find_harness_root", return_value=self.fixture.harness)
        self.root.start()
        self.addCleanup(self.root.stop)

    def event(self, *, lane_id="lane-1", run_id="run-1", event_type="COMPLETION_REVIEW_REQUIRED"):
        return manager_queue.promote_event(self.rt, event_type=event_type, event_class=event_type,
                    lane_id=lane_id, run_id=run_id, summary="unit queue condition")

    def watch(self, **kwargs):
        with patch.object(scan_watch, "_find_actionable", side_effect=AssertionError("managed watch must use only its queue")), patch.object(scan_watch, "_discover_orphaned_leases", side_effect=AssertionError("managed watch must not discover leases")):
            return scan_watch.run_watch(timeout="0s", **kwargs)

    def test_separate_public_processes_share_prepared_delivery_and_ack_scope(self):
        event = self.event()
        watched = self.fixture.command("watch", "--until-actionable", "--timeout", "0s")
        self.assertEqual(0, watched.returncode, watched.stderr + watched.stdout)
        self.assertEqual(event["event_id"], json.loads(watched.stdout)["event_id"])
        acked = self.fixture.command("manager", "acknowledge", "--event-id", event["event_id"])
        self.assertEqual(0, acked.returncode, acked.stderr + acked.stdout)
        repeated = self.fixture.command("watch", "--until-actionable", "--timeout", "0s")
        self.assertEqual(1, repeated.returncode)
        self.assertEqual("WATCH_TIMEOUT", json.loads(repeated.stdout)["code"])
        queue = manager_queue.read_manager_queue(self.rt)
        current = queue["events"][0]
        self.assertEqual("ACKNOWLEDGED", current["state"])
        receipt = current["history"][-1]
        self.assertEqual({"queue_id": queue["queue_id"], "event_id": event["event_id"],
                          "session_id": self.scope["root_session_id"], "binding_id": "codex",
                          "lane_id": "lane-1", "run_id": "run-1"},
                         {key: receipt[key] for key in ("queue_id", "event_id", "session_id", "binding_id", "lane_id", "run_id")})
        self.assertEqual(receipt["session_id"], current["delivery_history"][0]["session_id"])
        self.assertFalse((self.rt / "epochs" / self.fixture.epoch["epoch_id"] / "lanes/lane-1/review.json").exists())

    def test_scoped_ack_without_watch_does_not_repeat_review_or_close_it(self):
        event = self.event()
        manager_queue.acknowledge_event(self.rt, event["event_id"], **self.scope)
        before = manager_queue_path(self.rt).read_bytes()
        self.assertEqual("WATCH_TIMEOUT", self.watch()["code"])
        self.assertEqual(before, manager_queue_path(self.rt).read_bytes())

    def test_public_watch_stays_blocked_after_separate_ack_and_fresh_event_wakes_it(self):
        first = self.event()
        ack = self.fixture.command("manager", "acknowledge", "--event-id", first["event_id"])
        self.assertEqual(0, ack.returncode, ack.stderr + ack.stdout)
        child = subprocess.Popen(
            [sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json",
             "watch", "--until-actionable", "--timeout", "10s"],
            cwd=self.fixture.harness, env=self.fixture.environment,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                child.communicate(timeout=1)
            fresh = self.event(run_id="fresh-run", event_type="LANE_STATUS_CHANGED")
            stdout, stderr = child.communicate(timeout=10)
            self.assertEqual(0, child.returncode, stderr + stdout)
            self.assertEqual(fresh["event_id"], json.loads(stdout)["event_id"])
            queue = manager_queue.read_manager_queue(self.rt)
            self.assertEqual("ACKNOWLEDGED", queue["events"][0]["state"])
            self.assertEqual("PENDING", queue["events"][1]["state"])
        finally:
            if child.poll() is None:
                from orchestrator_harness import processes
                self.assertTrue(processes.reap_created_process(child))
                child.communicate(timeout=10)

    def test_delivery_is_deduplicated_in_the_same_scope(self):
        event = self.event()
        self.assertEqual(event["event_id"], self.watch()["event_id"])
        self.assertEqual("WATCH_TIMEOUT", self.watch()["code"])
        current = manager_queue.read_manager_queue(self.rt)["events"][0]
        self.assertEqual("PENDING", current["state"])
        self.assertEqual(1, len(current["delivery_history"]))

    def test_new_prepared_root_gets_unresolved_review_and_its_own_ack(self):
        event = self.event()
        self.watch()
        manager_queue.acknowledge_event(self.rt, event["event_id"], **self.scope)
        path = root_session_path(self.fixture.config)
        path.rename(path.with_name("PRIOR_ROOT_SESSION.json"))
        new = publish_root_session(self.fixture.config, run_id="new-unit-root", provider_id="codex")
        self.assertNotEqual(self.fixture.session["session_id"], new["session_id"])
        self.assertEqual(event["event_id"], self.watch()["event_id"])
        manager_queue.acknowledge_event(self.rt, event["event_id"], root_session_id=new["session_id"], binding_id="codex")
        self.assertEqual("WATCH_TIMEOUT", self.watch()["code"])
        current = manager_queue.read_manager_queue(self.rt)["events"][0]
        self.assertEqual("ACKNOWLEDGED", current["state"])
        self.assertEqual(2, len([r for r in current["history"] if r["state"] == "ACKNOWLEDGED"]))

    def test_legacy_ack_does_not_suppress_a_prepared_session(self):
        event = self.event()
        manager_queue.acknowledge_event(self.rt, event["event_id"])
        self.assertEqual(event["event_id"], self.watch()["event_id"])
        with self.assertRaisesRegex(manager_queue.ManagerQueueError, "already ACKNOWLEDGED"):
            manager_queue.acknowledge_event(self.rt, event["event_id"])

    def test_duplicate_scope_partial_scope_and_terminal_ack_cannot_advance_review(self):
        event = self.event(event_type="LANE_STATUS_CHANGED")
        before = manager_queue_path(self.rt).read_bytes()
        with self.assertRaisesRegex(manager_queue.ManagerQueueError, "both ROOT session and binding"):
            manager_queue.acknowledge_event(self.rt, event["event_id"], root_session_id=self.scope["root_session_id"])
        self.assertEqual(before, manager_queue_path(self.rt).read_bytes())
        manager_queue.acknowledge_event(self.rt, event["event_id"], **self.scope)
        before = manager_queue_path(self.rt).read_bytes()
        with self.assertRaisesRegex(manager_queue.ManagerQueueError, "already ACKNOWLEDGED"):
            manager_queue.acknowledge_event(self.rt, event["event_id"], **self.scope)
        self.assertEqual(before, manager_queue_path(self.rt).read_bytes())
        manager_queue.close_event(self.rt, event["event_id"], "COMPLETE", "unit condition handled")
        before = manager_queue_path(self.rt).read_bytes()
        with self.assertRaisesRegex(manager_queue.ManagerQueueError, "already COMPLETE"):
            manager_queue.acknowledge_event(self.rt, event["event_id"], root_session_id="another-unit-scope", binding_id="codex")
        self.assertEqual(before, manager_queue_path(self.rt).read_bytes())

    def test_new_run_and_failure_events_are_not_hidden_by_previous_review(self):
        first = self.event()
        manager_queue.acknowledge_event(self.rt, first["event_id"], **self.scope)
        second = self.event(run_id="run-2")
        failure = self.event(lane_id="lane-2", run_id="run-3", event_type="LANE_STATUS_CHANGED")
        self.assertEqual(second["event_id"], self.watch()["event_id"])
        self.assertEqual(failure["event_id"], self.watch()["event_id"])

    def test_selector_preserves_other_events(self):
        first = self.event()
        second = self.event(run_id="run-2")
        self.assertEqual(second["event_id"], self.watch(until_event=second["event_id"])["event_id"])
        self.assertEqual(first["event_id"], self.watch()["event_id"])

    def test_queue_corruption_fails_without_local_status_fallback(self):
        manager_queue_path(self.rt).write_bytes(b"damaged queue")
        result = self.watch()
        self.assertEqual("WATCH_EVIDENCE_INVALID", result["code"])
        self.assertFalse(result["ok"])

    def test_delivery_write_failure_is_evidence_failure_and_preserves_pending_event(self):
        event = self.event()
        before = manager_queue_path(self.rt).read_bytes()
        with patch.object(manager_queue, "_write_manager_queue", side_effect=OSError("injected delivery failure")):
            result = self.watch()
        self.assertEqual("WATCH_EVIDENCE_INVALID", result["code"])
        self.assertIn("injected delivery failure", result["summary"])
        self.assertEqual(before, manager_queue_path(self.rt).read_bytes())
        self.assertEqual(event["event_id"], self.watch()["event_id"])

    def test_missing_or_mismatched_preparation_fails_before_queue_mutation(self):
        self.event()
        before = manager_queue_path(self.rt).read_bytes()
        self.assertEqual("ROOT_SESSION_MISMATCH", self.watch(root_session_id="another-root")["code"])
        root_session_path(self.fixture.config).unlink()
        self.assertEqual("ROOT_SESSION_REQUIRED", self.watch()["code"])
        ack = self.fixture.command("manager", "acknowledge", "--event-id", "anything")
        self.assertEqual(1, ack.returncode)
        self.assertEqual("ROOT_SESSION_REQUIRED", json.loads(ack.stdout)["code"])
        self.assertEqual(before, manager_queue_path(self.rt).read_bytes())

    def test_wait_holds_no_queue_or_admission_lock_while_a_real_producer_arrives(self):
        errors = []
        created = []

        def producer():
            try:
                created.append(self.event(event_type="LANE_STATUS_CHANGED"))
            except Exception as exc:
                errors.append(exc)

        def during_wait(_seconds):
            thread = threading.Thread(target=producer)
            thread.start()
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive(), "watch must release admission and queue locks before waiting")

        with patch.object(scan_watch.time, "sleep", side_effect=during_wait) as sleeper:
            result = scan_watch.run_watch(timeout="5s")
        self.assertFalse(errors, errors)
        self.assertEqual(created[0]["event_id"], result["event_id"])
        sleeper.assert_called_once()

    def test_plain_profile_remains_queue_free_and_keeps_local_conditions(self):
        fixture = PreparedRootFixture(self, profile="plain")
        root_session_path(fixture.config).unlink()
        with patch.object(scan_watch, "find_harness_root", return_value=fixture.harness), patch.object(scan_watch, "read_manager_queue", side_effect=AssertionError("plain mode must be queue free")), patch.object(scan_watch, "_find_actionable", return_value=("resource:unit-resource", "orphaned_lease")):
            result = scan_watch.run_watch(timeout="0s")
        self.assertEqual("WATCH_ACTIONABLE", result["code"])
        self.assertEqual("unit-resource", result["resource_id"])


if __name__ == "__main__":
    unittest.main()
