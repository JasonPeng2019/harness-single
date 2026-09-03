from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.contract import assert_review_pair
from orchestrator_harness.tests.v2_acceptance.reference_runtime import ReferenceRuntime


class CheckU3Tests(unittest.TestCase):
    """CHECK-U3 — REQ-007/010/011/012 lifecycle and lease oracle."""

    def test_lease_contention_is_fail_fast_all_or_nothing_then_exact_cleanup_reuses(self) -> None:
        runtime = ReferenceRuntime()
        runtime.bootstrap("holder", "run-holder", ("fixture-a", "fixture-b"))
        runtime.bootstrap("contender", "run-contender", ("fixture-a",))
        runtime.launch("holder", "pid:11@created:one")
        held_before = dict(runtime.leases)
        with self.assertRaisesRegex(ValueError, "LAUNCH_LEASE_BUSY"):
            runtime.launch("contender", "pid:22@created:two")
        self.assertEqual(held_before, runtime.leases, "failed launch must not partially acquire")
        runtime.cleanup("holder", "pid:11@created:one")
        runtime.launch("contender", "pid:22@created:two")
        self.assertEqual("contender", runtime.leases["fixture-a"]["lane_id"])

    def test_review_is_a_linked_pair_and_accepted_lane_cannot_resume(self) -> None:
        runtime = ReferenceRuntime()
        runtime.bootstrap("lane", "run-1")
        runtime.terminal("lane", "review_pending")
        review, acceptance = runtime.review_pair("lane", "PASS", "ACCEPTED")
        assert_review_pair(review, acceptance)
        with self.assertRaisesRegex(ValueError, "ALREADY_ACCEPTED"):
            runtime.resume("lane", "run-2")

    def test_non_pass_acceptance_requires_recorded_force_reason(self) -> None:
        runtime = ReferenceRuntime()
        runtime.bootstrap("lane", "run-1")
        with self.assertRaisesRegex(ValueError, "COMPLETION_REVIEW_FORCE_REASON_INVALID"):
            runtime.review_pair("lane", "FAIL", "ACCEPTED")

    def test_bound_009_resume_is_observable_before_new_run_artifacts_replace_old_ones(self) -> None:
        runtime = ReferenceRuntime()
        runtime.bootstrap("lane", "run-1")
        runtime.terminal("lane", "review_pending")
        self.assertEqual(
            [
                "resuming-persisted",
                "task-rationale-instructions-replaced",
                "obsolete-current-run-artifacts-cleared",
                "fresh-invocation-validated",
                "running-persisted",
            ],
            runtime.resume_ordered("lane", "run-2"),
        )
        self.assertEqual("run-2", runtime.lanes["lane"]["run_id"])

    def test_bound_010_rejected_review_is_the_single_resume_signal_owner(self) -> None:
        runtime = ReferenceRuntime(mode="managed")
        runtime.bootstrap("lane", "run-1")
        runtime.terminal("lane", "review_pending")
        runtime.review_pair("lane", "FAIL", "REJECTED")
        # The acceptance oracle permits one review-originated signal, never a second
        # producer at resume.  This reference model intentionally has no resume event API.
        self.assertEqual(1, len(runtime.events))
