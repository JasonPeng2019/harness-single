from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.reference_runtime import ReferenceRuntime


class CheckU2Tests(unittest.TestCase):
    """CHECK-U2 — REQ-002/003/008/009 profile and queue-role oracle."""

    def test_managed_monitor_is_sole_event_producer_and_deduplicates_status(self) -> None:
        runtime = ReferenceRuntime(mode="managed")
        runtime.bootstrap("managed-lane", "run-1")
        runtime.terminal("managed-lane", "review_pending")
        runtime.terminal("managed-lane", "review_pending")
        self.assertEqual(1, len(runtime.events))
        event = runtime.events[0]
        self.assertEqual("COMPLETION_REVIEW_REQUIRED", event["type"])
        self.assertEqual("PENDING", event["state"])
        runtime.acknowledge(event["event_id"])
        self.assertEqual(["PENDING", "ACKNOWLEDGED"], event["history"])

    def test_plain_profile_never_creates_a_manager_queue_event(self) -> None:
        runtime = ReferenceRuntime(mode="plain")
        runtime.bootstrap("plain-lane", "run-1")
        runtime.terminal("plain-lane", "review_pending")
        self.assertEqual([], runtime.events)
        self.assertEqual("review_pending", runtime.lanes["plain-lane"]["lifecycle"])
