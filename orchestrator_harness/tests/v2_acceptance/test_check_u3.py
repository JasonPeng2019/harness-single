from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.candidate_boundary_oracles import (
    assert_rejected_review_resume_boundary,
    assert_resume_order,
)
from orchestrator_harness.tests.v2_acceptance.contract import assert_review_pair
from orchestrator_harness.tests.v2_acceptance.reference_runtime import ReferenceRuntime


class CheckU3Tests(unittest.TestCase):
    """CHECK-U3 synthetic lifecycle contracts; this class is not live proof."""

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

    def test_bound_009_candidate_observable_resume_order_rejects_ordering_mutants(self) -> None:
        class CandidateBoundary:
            def __init__(self, mutation: str | None = None) -> None:
                self.mutation = mutation

            def resume(self) -> list[dict[str, str]]:
                observations = [
                    {"kind": "lane-record-persisted", "lifecycle": "resuming"},
                    {"kind": "current-run-artifacts-replaced"},
                    {"kind": "fresh-invocation-validated"},
                    {"kind": "lane-record-persisted", "lifecycle": "running"},
                ]
                if self.mutation == "replacement-before-resuming":
                    return [observations[1], observations[0], *observations[2:]]
                if self.mutation == "running-before-validation":
                    return [*observations[:2], observations[3], observations[2]]
                return observations

        assert_resume_order(CandidateBoundary().resume())
        for mutation in (
            "replacement-before-resuming",
            "running-before-validation",
        ):
            with self.subTest(mutant=mutation):
                with self.assertRaises(AssertionError):
                    assert_resume_order(CandidateBoundary(mutation).resume())

    def test_bound_010_candidate_observable_review_resume_boundary_rejects_mutants(self) -> None:
        class CandidateBoundary:
            def __init__(self, mutation: str | None = None) -> None:
                self.events: list[dict[str, str]] = []
                self.mutation = mutation

            def reject_review(self) -> None:
                producer = "resume" if self.mutation == "wrong-producer" else "completion-review"
                self.events.append({"type": "LANE_RESUME_REQUIRED", "producer": producer})

            def resume(self) -> None:
                if self.mutation == "duplicate-on-resume":
                    self.events.append({"type": "LANE_RESUME_REQUIRED", "producer": "resume"})

        assert_rejected_review_resume_boundary(CandidateBoundary())
        for mutation in ("duplicate-on-resume", "wrong-producer"):
            with self.subTest(mutation=mutation):
                with self.assertRaises(AssertionError):
                    assert_rejected_review_resume_boundary(CandidateBoundary(mutation))
