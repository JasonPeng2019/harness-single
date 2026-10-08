from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from orchestrator_harness import scan_watch


class AcknowledgedReviewWatchTests(unittest.TestCase):
    def event(self, *, state="ACKNOWLEDGED", run_id="run-1", delivered=True):
        return {
            "event_id": "review-1", "event_class": "COMPLETION_REVIEW_REQUIRED",
            "lane_id": "lane-1", "run_id": run_id, "state": state,
            "delivery_history": [{"source": "watch", "queue_id": "queue-1",
                                  "event_id": "review-1", "session_id": "root-1",
                                  "binding_id": "root"}] if delivered else [],
        }

    def watch(self, events, *, statuses=None, mode="managed", queue_error=False,
              next_events=None, orphaned_leases=None, delivery_result=True,
              delivery_error=False):
        statuses = statuses or {"lane-1": "review_pending"}
        lanes = {name: {"lane_id": name, "run_id": "run-1", "derived": status}
                 for name, status in statuses.items()}
        queue = {"queue_id": "queue-1", "events": events}
        before = deepcopy(queue)
        with ExitStack() as stack:
            for name, value in (
                ("find_harness_root", Path("root")),
                ("load_config", SimpleNamespace(runtime_root=Path("runtime"))),
                ("_active_epoch", ("epoch-1", {"lane_mode": mode})),
                ("_discover_orphaned_leases", orphaned_leases or []),
                ("read_active_lanes", [{"lane_id": name} for name in lanes]),
                ("read_controller_status", {}),
            ):
                stack.enter_context(patch.object(scan_watch, name, return_value=value))
            delivery = stack.enter_context(patch.object(scan_watch, "append_watch_delivery", return_value=delivery_result))
            if delivery_error:
                delivery.side_effect = OSError("delivery record write failed")
            stack.enter_context(patch.object(scan_watch, "read_lane", side_effect=lambda _rt, _epoch, name: lanes[name]))
            stack.enter_context(patch.object(scan_watch, "derive_lane_status", side_effect=lambda _rt, _epoch, lane, _status, **_kw: lane["derived"]))
            reader = stack.enter_context(patch.object(scan_watch, "read_manager_queue", return_value=queue))
            if queue_error:
                reader.side_effect = ValueError("unreadable queue")
            elif next_events is not None:
                reader.side_effect = [queue, {"queue_id": "queue-1", "events": next_events}]
            sleeper = stack.enter_context(patch.object(scan_watch.time, "sleep"))
            result = scan_watch.run_watch(timeout="1s" if next_events is not None else "0s",
                                          root_session_id="root-1", binding_id="root")
            self.assertEqual(queue, before, "waiting must not accept or close the review")
            if mode == "plain":
                reader.assert_not_called()
            if next_events is not None:
                sleeper.assert_called_once()
            return result

    def test_acknowledged_delivered_review_allows_wait_to_timeout_without_acceptance(self):
        result = self.watch([self.event()])
        self.assertEqual("WATCH_TIMEOUT", result["code"], result)

    def test_pending_review_still_requires_action(self):
        self.assertEqual("WATCH_ACTIONABLE", self.watch([self.event(state="PENDING")])["code"])

    def test_old_invocation_acknowledgement_does_not_hide_new_review(self):
        self.assertEqual("WATCH_ACTIONABLE", self.watch([self.event(run_id="old-run")])["code"])

    def test_new_lane_review_is_not_hidden_by_acknowledged_review(self):
        result = self.watch([self.event()], statuses={"lane-1": "review_pending", "lane-2": "review_pending"})
        self.assertEqual("WATCH_ACTIONABLE", result["code"])
        self.assertTrue(result["summary"].startswith("lane-2 "), result)

    def test_changed_failure_status_is_not_hidden_by_review_acknowledgement(self):
        result = self.watch([self.event()], statuses={"lane-1": "provider_exited_no_result"})
        self.assertEqual("WATCH_ACTIONABLE", result["code"])

    def test_plain_mode_and_unreadable_queue_keep_status_fallback(self):
        for kwargs in ({"mode": "plain"}, {"queue_error": True}):
            with self.subTest(kwargs=kwargs):
                self.assertEqual("WATCH_ACTIONABLE", self.watch([self.event()], **kwargs)["code"])

    def test_review_is_delivered_to_session_before_it_can_be_deferred(self):
        result = self.watch([self.event(delivered=False)])
        self.assertEqual("WATCH_EVENT", result["code"])
        self.assertEqual("review-1", result["event_id"])

    def test_failed_delivery_write_preserves_review_status_fallback(self):
        result = self.watch([self.event(delivered=False)], delivery_result=False)
        self.assertEqual("WATCH_ACTIONABLE", result["code"], result)

    def test_delivery_write_exception_preserves_review_status_fallback(self):
        result = self.watch([self.event(delivered=False)], delivery_error=True)
        self.assertEqual("WATCH_ACTIONABLE", result["code"], result)

    def test_new_event_arriving_while_review_is_open_wakes_native_wait(self):
        event = self.event()
        new_event = {"event_id": "test-failure", "state": "PENDING", "delivery_history": []}
        result = self.watch([event], next_events=[event, new_event])
        self.assertEqual("WATCH_EVENT", result["code"])
        self.assertEqual("test-failure", result["event_id"])

    def test_orphaned_resource_still_requires_action(self):
        result = self.watch([self.event()], orphaned_leases=[{"resource_id": "probe-1"}])
        self.assertEqual("WATCH_ACTIONABLE", result["code"])
        self.assertEqual("probe-1", result["resource_id"])


if __name__ == "__main__":
    unittest.main()
