"""STEP-04 contract corrections: plan precedence, budget durability, bounds.

These tests pin the validated STEP-04 defects discovered after the rejected
implementation: absent versus candidate plan precedence, accepted-plan-only
finalization, truly bounded independent store calls, durable fail-closed trust
gates, the bounded APC lifecycle with cleanup-pending reconciliation, restart
budget recovery, and durable configuration reuse after a late Level 0.
"""

from __future__ import annotations

import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    apc,
    config,
    contracts,
    experience,
    harness_bridge,
    local_adapters,
    preparation,
    search,
    store,
    templates,
)


class QuietEvidenceService:
    """An inert local evidence seam; the template store is the subject."""

    def search_recent_evidence(self, scope, query):
        return []


BINDING = {
    "provider": "synthetic",
    "model": "synthetic/drafter",
    "cli": "synthetic-cli",
    "effort": "low",
    "source": "explicit",
}


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class FlakyDurableReadStore:
    """Delegate to one real store while its durable reads fail transiently.

    Only ``get_preparation`` is intercepted: one Level 0 replay can exercise
    exactly one transient durable read failure while every other store call
    still reaches the real durable record.
    """

    def __init__(self, inner, failures: int = 1) -> None:
        self._inner = inner
        self.failures_remaining = int(failures)
        self.read_attempts = 0

    def get_preparation(self, preparation_id: str) -> dict:
        self.read_attempts += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise OSError("transient durable read failure")
        return self._inner.get_preparation(preparation_id)

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


def representation(limits: config.PreparationLimits, tokens: list[str]) -> dict:
    return templates.representation_identity(limits=limits) | {
        "tokens": tokens,
        "route": "ordinary",
    }


class Step04ContractCorrectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 10.0,
                "store_seconds": 8.0,
                "minimum_optional_slice_seconds": 1.0,
                "standard_stage_seconds": 20.0,
            }
        )
        self.store_path = self.root / "memory-state.sqlite3"
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        self.card = contracts.make_task_card(
            task="Fix the regression failure in the parser test",
            base_commit="base-1",
        )
        self.candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _evidence(self, logical_id="case-1", revision="r1", **overrides):
        item = {
            "kind": "historical_evidence",
            "logical_id": logical_id,
            "revision_id": revision,
            "payload": {"summary": "prior regression evidence"},
            "scope": {
                "app": "demo",
                "project": "p",
                "namespace": "reviewed",
                "owner": "owner-1",
            },
            "origin": "everos",
            "representation": representation(self.limits, ["regression", "failure"]),
            "score": 0.5,
            "freshness": "live",
        }
        item.update(overrides)
        return item

    def _store(self, store_id, kind, items, *, scope=None, freshness="live"):
        return search.SearchStore(
            store_id=store_id,
            kind=kind,
            query=lambda query, items=items: items,
            scope=scope,
            freshness=freshness,
        )

    def _proposal_for(self, request):
        record = request["template"]
        content = {
            "fixed_steps": list(record["fixed_steps"]),
            "verification_intent": record["verification_intent"],
            "bindings": {"failure": "parser", "component": "parser"},
        }
        return contracts.make_plan(
            plan_id="apc-proposal-1",
            objective_id=request["parent_objective_id"],
            route="ordinary",
            state="proposed",
            content=content,
            source={
                "template_id": request["template_id"],
                "template_version": request["template_version"],
                "branch": "apc_proposal",
                "apc_request": request["content_hash"],
            },
        )

    # -- absent and candidate plan precedence ------------------------------

    def test_absent_current_plan_is_classified_and_planned(self) -> None:
        outcome = self.service.prepare(
            task_card=self.card, plan=None, objective_id="objective-1", route="ordinary"
        )
        self.assertEqual("absent", outcome.preparation["current_plan_state"])
        self.assertIn("fresh", outcome.plan["state"])
        self.assertIsNone(outcome.envelope)

    def test_candidate_plan_keeps_precedence_and_is_never_replaced(self) -> None:
        """A candidate plan keeps its exact identity and is never auto-accepted.

        The ordinary reuse path may still propose a bounded plan around the
        pending candidate, but nothing substitutes for ROOT acceptance: the
        recorded current-plan state stays ``candidate_review`` and no envelope
        becomes dispatchable.
        """

        scored: list[object] = []

        def recording(query):
            scored.append(query)
            return []

        outcome = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                search.SearchStore(store_id="templates", kind="template", query=recording)
            ],
        )
        self.assertEqual("candidate_review", outcome.preparation["current_plan_state"])
        self.assertEqual("candidate", outcome.preparation["plan_state"])
        self.assertEqual(
            self.candidate["content_hash"], outcome.preparation["plan_digest"]
        )
        self.assertEqual("candidate_review", outcome.disposition["branch"])
        # The pending candidate never becomes authority by itself, and review
        # never silently runs template selection over the preserved plan.
        self.assertEqual([], scored, "candidate review must not score templates")
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        self.assertEqual("disabled", attempts["templates"]["status"])
        self.assertEqual([], outcome.disposition["reuse_attempts"])
        self.assertIsNone(outcome.disposition.get("root_acceptance"))
        self.assertNotEqual("accepted", outcome.plan["state"])
        self.assertIsNone(outcome.envelope)
        self.assertFalse(outcome.dispatchable)
        durable = self.memory_store.get_preparation(
            outcome.preparation["preparation_id"]
        )
        self.assertEqual("candidate_review", durable["current_plan_state"])
        self.assertEqual(self.candidate["content_hash"], durable["plan_digest"])

    def test_absent_and_accepted_plan_states_are_distinct_from_candidate(self) -> None:
        absent = self.service.prepare(
            task_card=self.card, plan=None, objective_id="objective-1", route="ordinary"
        )
        self.assertEqual("absent", absent.preparation["current_plan_state"])
        accepted = contracts.make_plan(
            plan_id="accepted-plan-2",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            accepted_by="ROOT",
            content={"steps": ["execute"]},
        )
        preserved = self.service.prepare(
            task_card=self.card,
            plan=accepted,
            objective_id="objective-1",
            route="ordinary",
        )
        self.assertEqual(
            "execution_accepted", preserved.preparation["current_plan_state"]
        )
        self.assertEqual("preserved_accepted", preserved.disposition["branch"])
        self.assertEqual("accepted", preserved.plan["state"])

    def test_finalize_refuses_a_plan_root_has_not_accepted(self) -> None:
        for plan in (None, self.candidate):
            with self.subTest(plan=plan):
                with self.assertRaisesRegex(
                    preparation.PlanAcceptanceError, "accepted"
                ):
                    self.service.prepare(
                        task_card=self.card,
                        plan=plan,
                        objective_id="objective-1",
                        route="ordinary",
                        lane_id="lane-1",
                        run_id="run-1",
                        worktree_path=str(self.root),
                        base_commit="base-1",
                        finalize=True,
                    )

    def test_finalize_needs_the_exact_lane_run_worktree_and_base(self) -> None:
        accepted = contracts.make_plan(
            plan_id="accepted-plan",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            accepted_by="ROOT",
            content={"steps": ["execute"]},
        )
        with self.assertRaises(preparation.PreparationError):
            self.service.prepare(
                task_card=self.card,
                plan=accepted,
                objective_id="objective-1",
                route="ordinary",
                finalize=True,
            )

    # -- absent and candidate plans cannot become execution authority -------

    def test_absent_plan_gets_a_fresh_planning_disposition_and_cannot_launch(self) -> None:
        """A truly absent plan is fresh ROOT planning/review, never authority."""

        template_queries: list[object] = []

        def recording(query):
            template_queries.append(query)
            return []

        outcome = self.service.prepare(
            task_card=self.card,
            plan=None,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                search.SearchStore(
                    store_id="templates", kind="template", query=recording
                )
            ],
        )
        self.assertEqual("absent", outcome.preparation["current_plan_state"])
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual("fresh", outcome.plan["state"])
        self.assertEqual([], template_queries, "absent planning must not select templates")
        self.assertIsNone(outcome.envelope)
        self.assertFalse(outcome.dispatchable)
        with self.assertRaisesRegex(preparation.PlanAcceptanceError, "accepted"):
            self.service.prepare(
                task_card=self.card,
                plan=None,
                objective_id="objective-1",
                route="ordinary",
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=str(self.root),
                base_commit="base-1",
                finalize=True,
            )

    def test_candidate_plan_is_preserved_without_any_reuse_attempt(self) -> None:
        """Candidate/review keeps the exact plan and runs no template work."""

        template_queries: list[object] = []
        apc_calls: list[object] = []

        def recording(query):
            template_queries.append(query)
            return []

        outcome = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                search.SearchStore(
                    store_id="templates", kind="template", query=recording
                )
            ],
            bindings={"failure": "parser", "component": "parser"},
            apc_binding={
                "provider": "synthetic",
                "model": "synthetic/drafter",
                "cli": "synthetic-cli",
                "effort": "low",
                "source": "explicit",
            },
            apc_launcher=lambda request: apc_calls.append(request),
        )
        self.assertEqual("candidate_review", outcome.disposition["branch"])
        self.assertEqual(self.candidate["content_hash"], outcome.plan["content_hash"])
        self.assertEqual(self.candidate["plan_id"], outcome.plan["plan_id"])
        self.assertEqual([], template_queries, "candidate review must not select templates")
        self.assertEqual([], apc_calls, "candidate review must not launch an APC child")
        self.assertIsNone(outcome.proposal)
        self.assertIsNone(outcome.disposition.get("root_acceptance"))
        self.assertIsNone(outcome.disposition.get("fresh"))
        self.assertIsNone(outcome.envelope)
        self.assertFalse(outcome.dispatchable)
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        self.assertEqual("disabled", attempts["templates"]["status"])

    def test_candidate_plan_requires_an_explicit_root_replan_request(self) -> None:
        """Only an explicit ROOT replan request may replace or extend a candidate."""

        requests: list[object] = []

        def recording(query):
            requests.append(query)
            return []

        with self.assertRaisesRegex(preparation.MandatoryStateFailure, "ROOT replan"):
            self.service.prepare(
                task_card=self.card,
                plan=self.candidate,
                objective_id="objective-1",
                route="ordinary",
                root_replan={"requested_by": "lane-worker", "reason": "not ROOT"},
            )
        replanned = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                search.SearchStore(
                    store_id="templates", kind="template", query=recording
                )
            ],
            root_replan={
                "requested_by": "ROOT",
                "reason": "the draft omits the regression check",
            },
        )
        self.assertEqual("candidate_review", replanned.preparation["current_plan_state"])
        self.assertTrue(requests, "an explicit ROOT replan request may select templates")
        self.assertEqual("ROOT", replanned.disposition["root_replan"]["requested_by"])
        self.assertIsNone(replanned.envelope)

    # -- bounded independent store calls -----------------------------------

    def test_blocking_store_is_bounded_by_wall_clock_and_other_stores_deliver(
        self,
    ) -> None:
        limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 10.0,
                "standard_stage_seconds": 1.0,
                "store_seconds": 0.5,
                "minimum_optional_slice_seconds": 0.05,
            }
        )

        def blocking(query):
            time.sleep(5.0)
            return [self._evidence(logical_id="case-slow")]

        service = preparation.PreparationService(
            store=self.memory_store, limits=limits
        )
        started = time.monotonic()
        outcome = service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                search.SearchStore(store_id="slow", kind="historical_evidence", query=blocking),
                self._store("everos", "historical_evidence", [self._evidence()]),
            ],
        )
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.5, "a blocking store must not consume the stage")
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        self.assertEqual("timed-out", attempts["slow"]["status"])
        self.assertEqual("completed", attempts["everos"]["status"])
        selected = [
            item
            for item in outcome.trace["candidates"]
            if item["disposition"] == "selected"
        ]
        self.assertEqual(["case-1"], [item["logical_id"] for item in selected])

    def test_malformed_store_results_are_recorded_not_dropped(self) -> None:
        malformed = [
            "not-a-mapping",
            {"kind": "historical_evidence"},
            {
                "kind": "historical_evidence",
                "logical_id": "case-routes",
                "revision_id": "r1",
                "payload": {"a": 1},
                "scope": {"app": "demo"},
                "representation": representation(self.limits, ["regression"]),
                "routes": 5,
            },
            {
                "kind": "historical_evidence",
                "logical_id": "case-undeclared",
                "revision_id": "r1",
                "payload": {"a": 1},
                "scope": {"app": "demo"},
                "representation": representation(self.limits, ["regression"]),
                "freshness": "cached",
            },
        ]
        outcome = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                self._store("everos", "historical_evidence", malformed + [self._evidence()])
            ],
        )
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        rejected = attempts["everos"].get("rejected", [])
        self.assertGreaterEqual(len(rejected), 4)
        self.assertTrue(all("reason" in entry for entry in rejected))
        durable = self.memory_store.get_search_trace(
            outcome.preparation["preparation_id"]
        )
        self.assertTrue(
            durable["attempts"][0].get("rejected"),
            "malformed rejections must be durable, not silently dropped",
        )
        selected = [
            item
            for item in outcome.trace["candidates"]
            if item["disposition"] == "selected"
        ]
        self.assertEqual(["case-1"], [item["logical_id"] for item in selected])

    # -- fail-closed trust gates -------------------------------------------

    def test_untrusted_candidates_are_rejected_and_the_eligible_one_survives(
        self,
    ) -> None:
        scope = {"app": "demo", "project": "p", "namespace": "reviewed", "owner": "owner-1"}
        untrusted = [
            self._evidence(logical_id="case-revoked", revoked=True),
            self._evidence(logical_id="case-withdrawn", withdrawn=True),
            self._evidence(logical_id="case-predicates", predicates_ok=False),
            self._evidence(logical_id="case-designation", designation="superseded"),
            self._evidence(logical_id="case-approval", approval_status="revoked"),
            self._evidence(
                logical_id="case-undeclared-representation",
                representation=representation(self.limits, ["regression"])
                | {"declared": False},
            ),
            self._evidence(logical_id="case-scope", scope={"app": "other"}),
            self._evidence(logical_id="case-digest", payload_digest="not-the-payload-digest"),
        ]
        outcome = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[
                self._store(
                    "everos", "historical_evidence", untrusted + [self._evidence()], scope=scope
                )
            ],
        )
        dispositions = {
            item["logical_id"]: item
            for item in outcome.trace["candidates"]
            if item["logical_id"] != "case-1"
        }
        self.assertEqual(8, len(dispositions))
        for logical_id, item in dispositions.items():
            self.assertIn(item["disposition"], {"rejected", "invalid"})
            self.assertTrue(item["reasons"], logical_id)
        selected = [
            item
            for item in outcome.trace["candidates"]
            if item["disposition"] == "selected"
        ]
        self.assertEqual(["case-1"], [item["logical_id"] for item in selected])

    def test_procedure_trust_requires_approval_designation_and_digest(self) -> None:
        scope = {"app": "demo", "project": "p", "namespace": "shared", "owner": "owner-1"}
        base = {
            "kind": "procedure",
            "logical_id": "procedure-1",
            "revision_id": "rev-1",
            "payload": {"steps": ["approved guidance"]},
            "scope": scope,
            "origin": "atlas",
            "representation": representation(self.limits, ["regression"]),
            "score": 0.5,
            "freshness": "live",
        }
        incomplete = dict(base, logical_id="procedure-incomplete")
        trusted = dict(
            base,
            approval_status="approved",
            designation="current",
            predicates_ok=True,
            payload_digest=contracts.sha256_hex(base["payload"]),
        )
        outcome = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[self._store("atlas", "procedure", [incomplete, trusted], scope=scope)],
        )
        dispositions = {
            item["logical_id"]: item for item in outcome.trace["candidates"]
        }
        self.assertIn(dispositions["procedure-incomplete"]["disposition"], {"rejected", "invalid"})
        self.assertIn(dispositions["procedure-1"]["disposition"], {"eligible", "selected"})

    # -- bounded APC lifecycle ---------------------------------------------

    def _prepare_with_apc(self, launcher, **overrides):
        arguments = {
            "task_card": contracts.make_task_card(
                task="Fix the regression failure in the parser test", base_commit="base-1"
            ),
            "plan": self.candidate,
            "objective_id": "objective-1",
            "route": "ordinary",
            "stores": [self._template_store()],
            "apc_binding": BINDING,
            "apc_launcher": launcher,
            "root_replan": {
                "requested_by": "ROOT",
                "reason": "the test exercises the explicit ROOT replan path",
            },
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    def _template_store(self, items=None):
        """One bounded local template store over the explicit registry."""

        return local_adapters.make_local_search_stores(
            experience_service=QuietEvidenceService(),
            scope=experience.ExperienceScope(
                application="harness",
                project="product",
                namespace="contract-corrections",
                owner="root-agent",
            ),
            registry=templates.load_default_templates(),
            limits=self.limits,
        )[1]

    def test_apc_timeout_is_cleanup_pending_and_retry_is_refused(self) -> None:
        calls: list[object] = []
        clock = FakeClock()
        service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=clock
        )

        def slow_launcher(request):
            calls.append(request)
            clock.advance(5000.0)
            return {
                "invocation_id": "apc-child-slow",
                "result": apc.make_apc_result(request, self._proposal_for(request)),
            }

        first = service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=[self._template_store()],
            apc_binding=BINDING,
            apc_launcher=slow_launcher,
            root_replan={
                "requested_by": "ROOT",
                "reason": "the test exercises the explicit ROOT replan path",
            },
        )
        # A timed-out bounded child leaves the exact child cleanup_pending and
        # the decision falls back to fresh ROOT planning; the unresolved child
        # is recorded, never hidden.
        self.assertEqual("fresh", first.disposition["branch"])
        operations = self.memory_store.list_apc_child_operations(
            first.decision["decision_id"]
        )
        self.assertEqual(["cleanup_pending"], [operation["status"] for operation in operations])
        self.assertIn(
            "cleanup_pending", contracts.APC_CHILD_UNRESOLVED_STATUSES
        )
        # A later supplied deadline cannot renew the decision after its
        # original cutoff.  The unresolved child remains owned, and no second
        # optional stage or child launch is admitted.
        second = service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            deadline=clock() + 300.0,
            stores=[self._template_store()],
            apc_binding=BINDING,
            apc_launcher=slow_launcher,
            root_replan={
                "requested_by": "ROOT",
                "reason": "the test exercises the explicit ROOT replan path",
            },
        )
        self.assertEqual(1, len(calls), "an unresolved child must not relaunch")
        self.assertEqual("fresh", second.disposition["branch"])
        self.assertEqual(0.0, second.preparation["remaining_seconds"])
        self.assertEqual(0, second.trace["rounds"])
        self.assertEqual([], second.disposition["reuse_attempts"])
        self.assertEqual(
            ["cleanup_pending"],
            [operation["status"] for operation in self.memory_store.list_apc_child_operations(first.decision["decision_id"])],
        )

    def test_apc_reconciliation_clears_cleanup_pending_before_retry(self) -> None:
        def lost_ack(request):
            return None

        first = self._prepare_with_apc(lost_ack)
        operations = self.memory_store.list_apc_child_operations(
            first.decision["decision_id"]
        )
        self.assertEqual(["ambiguous"], [operation["status"] for operation in operations])
        reconciled = harness_bridge.reconcile_apc_child(
            store=self.memory_store,
            request={
                "content_hash": operations[0]["request_digest"],
                "parent_decision_id": operations[0]["parent_decision_id"],
                "parent_objective_id": operations[0]["parent_objective_id"],
                "template_id": operations[0]["template_id"],
                "template_version": operations[0]["template_version"],
                "permitted_edits": ["bindings"],
                "binding": operations[0]["binding"],
            },
            observed_invocation={"invocation_id": "controller:1:now", "pid": 1},
        )
        self.assertEqual("reconciled", reconciled["status"])
        self.assertNotIn(
            reconciled["status"], contracts.APC_CHILD_UNRESOLVED_STATUSES
        )

    # -- restart durability -------------------------------------------------

    def test_spent_budget_is_not_replenished_across_restart(self) -> None:
        first = self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1", route="ordinary"
        )
        self.assertEqual(300.0, first.preparation["remaining_seconds"])
        deadline = first.preparation["deadline_monotonic"]
        self.memory_store.close()

        self.clock.advance(250.0)
        reopened = store.MemoryStore(self.store_path)
        reopened.initialize()
        try:
            service = preparation.PreparationService(
                store=reopened, limits=self.limits, clock=self.clock
            )
            second = service.prepare(
                task_card=self.card,
                plan=self.candidate,
                objective_id="objective-1",
                route="ordinary",
            )
            self.assertEqual(
                first.decision["decision_id"], second.decision["decision_id"]
            )
            self.assertEqual(deadline, second.preparation["deadline_monotonic"])
            self.assertEqual(50.0, second.preparation["remaining_seconds"])
            self.assertEqual(250.0, second.preparation["spent_seconds"])
            self.assertEqual("trusted_deadline", second.preparation["budget_source"])
        finally:
            reopened.close()
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()

    def test_later_explicit_deadline_recovers_one_durable_cutoff(self) -> None:
        first = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 240.0,
        )
        self.clock.advance(40.0)
        restarted = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        later = restarted.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 600.0,
        )
        self.assertEqual(first.decision["decision_id"], later.decision["decision_id"])
        self.assertEqual(first.preparation["deadline_monotonic"], later.preparation["deadline_monotonic"])
        self.assertEqual(200.0, later.preparation["remaining_seconds"])
        self.assertEqual(40.0, later.preparation["spent_seconds"])
        self.assertEqual(2, later.preparation["attempt"])

    def test_omitted_deadline_recovers_original_cutoff_and_spent_cost(self) -> None:
        first = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 240.0,
        )
        self.clock.advance(35.0)
        second = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 500.0,
        )
        self.clock.advance(25.0)
        omitted = self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1"
        )
        self.assertEqual(first.decision["decision_id"], omitted.decision["decision_id"])
        self.assertEqual(first.preparation["deadline_monotonic"], omitted.preparation["deadline_monotonic"])
        self.assertEqual(180.0, omitted.preparation["remaining_seconds"])
        self.assertEqual(60.0, omitted.preparation["spent_seconds"])
        self.assertEqual(3, omitted.preparation["attempt"])
        self.assertEqual(omitted.preparation["deadline_monotonic"], second.preparation["deadline_monotonic"])

    def test_shorter_trusted_cutoff_tightens_and_stays_tight(self) -> None:
        first = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 300.0,
        )
        self.clock.advance(30.0)
        shorter = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 90.0,
        )
        self.assertEqual(1120.0, shorter.preparation["deadline_monotonic"])
        self.assertEqual(90.0, shorter.preparation["remaining_seconds"])
        self.assertEqual(30.0, shorter.preparation["spent_seconds"])
        self.clock.advance(20.0)
        later = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 600.0,
        )
        self.assertEqual(first.decision["decision_id"], later.decision["decision_id"])
        self.assertEqual(1120.0, later.preparation["deadline_monotonic"])
        self.assertEqual(70.0, later.preparation["remaining_seconds"])
        self.assertEqual(50.0, later.preparation["spent_seconds"])

    def test_followup_level_zero_uses_decision_wide_tightened_budget(self) -> None:
        first = self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1",
            deadline=self.clock() + 300.0,
        )
        self.clock.advance(30.0)
        tightened = self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1",
            deadline=self.clock() + 35.0,
        )
        self.clock.advance(20.0)
        queried: list[object] = []
        corrected = self.service.apply_level_zero(
            preparation=first.preparation,
            decision=first.decision,
            task_card=self.card,
            plan=first.plan,
            objective_id="objective-1",
            stores=[search.SearchStore(
                store_id="everos", kind="historical_evidence",
                query=lambda query: queried.append(query) or [],
            )],
        )
        self.assertEqual(first.decision["decision_id"], corrected.decision["decision_id"])
        self.assertEqual(tightened.preparation["deadline_monotonic"], corrected.preparation["deadline_monotonic"])
        self.assertEqual(15.0, corrected.preparation["remaining_seconds"])
        self.assertEqual(50.0, corrected.preparation["spent_seconds"])
        self.assertEqual(0.0, corrected.preparation["stage_allowance_seconds"])
        self.assertEqual([], queried)
        self.assertEqual(0, corrected.trace["rounds"])

    def test_followup_overlapping_callers_keep_distinct_attempts_and_minimum_cutoff(self) -> None:
        first = self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1",
            deadline=self.clock() + 300.0,
        )
        self.clock.advance(30.0)
        other_store = store.MemoryStore(self.store_path)
        other_store.initialize()
        shorter_service = preparation.PreparationService(
            store=other_store, limits=self.limits, clock=self.clock
        )
        interleaved: list[preparation.PreparationOutcome] = []

        class InterleavedReadStore:
            def __init__(self, inner):
                self.inner = inner
                self.once = False

            def list_preparations(self, decision_id):
                prior = self.inner.list_preparations(decision_id)
                if not self.once:
                    self.once = True
                    interleaved.append(shorter_service.prepare(
                        task_card=self_card, plan=self_plan, objective_id="objective-1",
                        deadline=self_clock() + 90.0,
                    ))
                return prior

            def __getattr__(self, name):
                return getattr(self.inner, name)

        self_card, self_plan, self_clock = self.card, self.candidate, self.clock
        try:
            later_service = preparation.PreparationService(
                store=InterleavedReadStore(self.memory_store),
                limits=self.limits, clock=self.clock,
            )
            later = later_service.prepare(
                task_card=self.card, plan=self.candidate, objective_id="objective-1",
                deadline=self.clock() + 600.0,
            )
            durable = self.memory_store.list_preparations(first.decision["decision_id"])
            self.assertEqual([1, 2, 3], sorted(item["attempt"] for item in durable))
            self.assertEqual(3, len({item["preparation_id"] for item in durable}))
            self.assertNotEqual(interleaved[0].preparation["preparation_id"], later.preparation["preparation_id"])
            self.assertEqual(1120.0, min(item["deadline_monotonic"] for item in durable))
            self.assertEqual(1120.0, later.preparation["deadline_monotonic"])
        finally:
            other_store.close()

    def test_followup_slow_durable_lookup_and_stage_boundary_keep_reserve(self) -> None:
        self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1",
            deadline=self.clock() + 120.0,
        )
        self.clock.advance(20.0)

        class SlowBudgetReadStore:
            def __init__(self, inner, clock):
                self.inner, self.clock = inner, clock

            def list_preparations(self, decision_id):
                prior = self.inner.list_preparations(decision_id)
                self.clock.advance(65.0)
                return prior

            def __getattr__(self, name):
                return getattr(self.inner, name)

        queried: list[object] = []
        service = preparation.PreparationService(
            store=SlowBudgetReadStore(self.memory_store, self.clock),
            limits=self.limits, clock=self.clock,
        )
        canonical = service._canonical_objective

        def delayed_canonical(*args):
            self.clock.advance(10.0)
            return canonical(*args)

        service._canonical_objective = delayed_canonical
        outcome = service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1",
            deadline=self.clock() + 600.0,
            stores=[search.SearchStore(
                store_id="everos", kind="historical_evidence",
                query=lambda query: queried.append(query) or [],
            )],
        )
        self.assertEqual(1120.0, outcome.preparation["deadline_monotonic"])
        self.assertEqual(35.0, outcome.preparation["remaining_seconds"])
        self.assertEqual(85.0, outcome.preparation["spent_seconds"])
        self.assertEqual([], queried)
        self.assertEqual(0, outcome.trace["rounds"])
        self.assertEqual("unattempted-by-budget", outcome.trace["attempts"][0]["status"])

    def test_followup_unreadable_budget_retains_mandatory_and_completed_state(self) -> None:
        accepted = contracts.make_plan(
            plan_id="accepted-plan", objective_id="objective-1", route="ordinary",
            state="accepted", accepted_by="ROOT", content={"steps": ["execute"]},
        )
        first = self.service.prepare(
            task_card=self.card, plan=accepted, objective_id="objective-1",
            stores=[self._store("everos", "historical_evidence", [self._evidence()])],
        )
        completed_trace = self.memory_store.get_search_trace(first.preparation["preparation_id"])
        self.assertEqual("optional_memory", completed_trace["outcome"])

        class UnreadableBudgetStore:
            def __init__(self, inner):
                self.inner = inner

            def list_preparations(self, decision_id):
                record = dict(self.inner.list_preparations(decision_id)[0])
                record["deadline_monotonic"] = "unreadable"
                return [record]

            def __getattr__(self, name):
                return getattr(self.inner, name)

        queried: list[object] = []
        launched: list[object] = []
        checked: list[object] = []
        service = preparation.PreparationService(
            store=UnreadableBudgetStore(self.memory_store),
            limits=self.limits, clock=self.clock,
        )
        outcome = service.prepare(
            task_card=self.card, plan=accepted, objective_id="objective-1",
            stores=[search.SearchStore(
                store_id="everos", kind="historical_evidence",
                query=lambda query: queried.append(query) or [],
            )],
            apc_launcher=lambda request: launched.append(request) or None,
            lane_id="lane-1", run_id="run-1", worktree_path=str(self.root),
            base_commit="base-1", finalize=True,
            mandatory_content=[{"id": "task", "content": self.card["task"]}],
            optional_items=[{
                "id": "plan-guidance", "kind": "historical_evidence",
                "origin": "everos", "revision_id": "r1",
                "content": {"summary": "required by the accepted plan"},
                "plan_affecting": True,
            }],
            freshness_check=lambda item: checked.append(item) or True,
        )
        self.assertEqual("no_memory_continuation", outcome.mode)
        self.assertEqual(accepted, outcome.plan)
        self.assertEqual(first.decision["decision_id"], outcome.decision["decision_id"])
        self.assertIsNone(outcome.trace)
        self.assertIsNone(outcome.context)
        self.assertIsNone(outcome.envelope)
        self.assertIn("durable", outcome.reason)
        self.assertEqual([], queried)
        self.assertEqual([], launched)
        self.assertEqual([], checked)
        self.assertEqual([first.preparation], self.memory_store.list_preparations(first.decision["decision_id"]))
        self.assertEqual(completed_trace, self.memory_store.get_search_trace(first.preparation["preparation_id"]))
        self.assertIsNone(self.memory_store.get_final_context_for_decision(first.decision["decision_id"]))

    def test_remaining_post_persistence_tightening_and_read_latency_block_search(self) -> None:
        first = self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1",
            deadline=self.clock() + 300.0,
        )
        self.clock.advance(30.0)
        other_store = store.MemoryStore(self.store_path)
        other_store.initialize()
        other_service = preparation.PreparationService(
            store=other_store, limits=self.limits, clock=self.clock,
        )
        interleaved: list[preparation.PreparationOutcome] = []

        class SlowAdmissionReadStore:
            def __init__(self, inner, clock):
                self.inner, self.clock = inner, clock
                self.reads = 0

            def list_preparations(self, decision_id):
                self.reads += 1
                prior = self.inner.list_preparations(decision_id)
                if self.reads == 2:
                    self.clock.advance(10.0)
                return prior

            def __getattr__(self, name):
                return getattr(self.inner, name)

        queried: list[object] = []
        launched: list[object] = []
        wrapped = SlowAdmissionReadStore(self.memory_store, self.clock)
        service = preparation.PreparationService(
            store=wrapped, limits=self.limits, clock=self.clock,
        )
        canonical = service._canonical_objective

        def after_persistence(*args):
            if not interleaved:
                interleaved.append(other_service.prepare(
                    task_card=self.card, plan=self.candidate, objective_id="objective-1",
                    deadline=self.clock() + 35.0,
                ))
            return canonical(*args)

        service._canonical_objective = after_persistence
        try:
            outcome = service.prepare(
                task_card=self.card, plan=self.candidate, objective_id="objective-1",
                deadline=self.clock() + 600.0,
                stores=[search.SearchStore(
                    store_id="everos", kind="historical_evidence",
                    query=lambda query: queried.append(query) or [],
                )],
                apc_launcher=lambda request: launched.append(request) or None,
            )
            durable = self.memory_store.list_preparations(first.decision["decision_id"])
            self.assertEqual(3, len(durable))
            self.assertEqual(1065.0, interleaved[0].preparation["deadline_monotonic"])
            self.assertEqual(1065.0, min(item["deadline_monotonic"] for item in durable))
            self.assertEqual(1300.0, outcome.preparation["deadline_monotonic"])
            self.assertEqual(2, wrapped.reads)
            self.assertEqual(1040.0, self.clock())
            self.assertEqual([], queried)
            self.assertEqual([], launched)
            self.assertEqual(0, outcome.trace["rounds"])
            self.assertEqual("unattempted-by-budget", outcome.trace["attempts"][0]["status"])
        finally:
            other_store.close()

    def test_remaining_unavailable_or_unreadable_admission_reread_blocks_optional_work(self) -> None:
        class UnreadableAdmissionStore:
            def __init__(self, inner, invalid):
                self.inner, self.invalid = inner, invalid
                self.reads = 0

            def list_preparations(self, decision_id):
                self.reads += 1
                if self.reads == 2:
                    if self.invalid == "failure":
                        raise OSError("admission budget unavailable")
                    record = dict(self.inner.list_preparations(decision_id)[0])
                    record["deadline_monotonic"] = "unreadable"
                    return [record]
                return self.inner.list_preparations(decision_id)

            def __getattr__(self, name):
                return getattr(self.inner, name)

        queried: list[object] = []
        for before, invalid in enumerate(("failure", "unreadable")):
            with self.subTest(invalid=invalid):
                wrapped = UnreadableAdmissionStore(self.memory_store, invalid)
                service = preparation.PreparationService(
                    store=wrapped, limits=self.limits, clock=self.clock,
                )
                outcome = service.prepare(
                    task_card=self.card, plan=self.candidate, objective_id="objective-1",
                    stores=[search.SearchStore(
                        store_id="everos", kind="historical_evidence",
                        query=lambda query: queried.append(query) or [],
                    )],
                )
                self.assertEqual(2, wrapped.reads)
                self.assertEqual("no_memory_continuation", outcome.mode)
                self.assertEqual(self.candidate, outcome.plan)
                self.assertIn("durable", outcome.reason)
                self.assertEqual([], queried)
                self.assertEqual(before + 1, len(self.memory_store.list_preparations(
                    outcome.decision["decision_id"]
                )))

    def _accepted_budget_finalization_state(self):
        accepted = contracts.make_plan(
            plan_id="accepted-plan", objective_id="objective-1", route="ordinary",
            state="accepted", accepted_by="ROOT", content={"steps": ["execute"]},
        )
        card = contracts.make_task_card(
            task=self.card["task"], base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=accepted,
                checkpoint="checkpoint-1",
            ),
        )
        first = self.service.prepare(
            task_card=card, plan=accepted, objective_id="objective-1",
        )

        class UnreadableBudgetStore:
            def __init__(self, inner):
                self.inner = inner

            def list_preparations(self, decision_id):
                raise OSError("budget state unavailable")

            def __getattr__(self, name):
                return getattr(self.inner, name)

        service = preparation.PreparationService(
            store=UnreadableBudgetStore(self.memory_store),
            limits=self.limits, clock=self.clock,
        )
        mandatory = [
            {"id": "task", "kind": "task", "content": card["task"]},
            {"id": "accepted-plan", "kind": "accepted-plan", "content": accepted["content"]},
            {"id": "base", "kind": "base", "content": "base-1"},
            {"id": "route", "kind": "route", "content": "ordinary"},
            {"id": "checkpoint", "kind": "checkpoint", "content": "checkpoint-1"},
            {"id": "security", "kind": "security", "content": contracts.FINAL_CONTEXT_SECURITY},
        ]
        return card, accepted, first, service, mandatory

    def test_remaining_failed_budget_finalizes_independent_mandatory_context(self) -> None:
        card, accepted, first, service, mandatory = self._accepted_budget_finalization_state()
        queried: list[object] = []
        launched: list[object] = []
        outcome = service.prepare(
            task_card=card, plan=accepted, objective_id="objective-1",
            stores=[search.SearchStore(
                store_id="everos", kind="historical_evidence",
                query=lambda query: queried.append(query) or [],
            )],
            apc_launcher=lambda request: launched.append(request) or None,
            lane_id="lane-1", run_id="run-1", worktree_path=str(self.root),
            base_commit="base-1", finalize=True, mandatory_content=mandatory,
            checkpoint="checkpoint-1", execution_role="worker",
            invocation_target="harness:worker", recipient="worker:lane-1",
            optional_items=[{
                "id": "independent-hint", "kind": "historical_evidence",
                "origin": "everos", "revision_id": "r1",
                "content": {"summary": "optional hint"},
                "plan_affecting": False,
            }],
        )
        self.assertTrue(outcome.dispatchable)
        self.assertEqual(accepted, outcome.plan)
        self.assertEqual(first.decision["decision_id"], outcome.decision["decision_id"])
        self.assertEqual(mandatory, outcome.envelope["mandatory_content"])
        self.assertEqual(card["content_hash"], outcome.envelope["task_card_digest"])
        self.assertEqual(accepted["plan_id"], outcome.envelope["plan_id"])
        self.assertEqual(accepted["content_hash"], outcome.envelope["plan_digest"])
        self.assertEqual("base-1", outcome.envelope["base_commit"])
        self.assertEqual("ordinary", outcome.envelope["route"])
        self.assertEqual([], outcome.envelope["optional_content"])
        self.assertIn("optional-memory-unavailable", outcome.envelope["delivery"]["omitted"])
        self.assertIn("independent-hint", outcome.envelope["delivery"]["omitted"])
        self.assertIn("budget state unavailable", outcome.reason)
        self.assertEqual(first.preparation["configuration"], outcome.envelope["configuration"])
        self.assertEqual([], queried)
        self.assertEqual([], launched)
        self.assertEqual([first.preparation], self.memory_store.list_preparations(first.decision["decision_id"]))
        self.assertEqual(outcome.context, self.memory_store.get_final_context_for_decision(first.decision["decision_id"]))

    def test_remaining_failed_budget_blocks_plan_affecting_finalization(self) -> None:
        card, accepted, first, service, mandatory = self._accepted_budget_finalization_state()
        queried: list[object] = []
        launched: list[object] = []
        outcome = service.prepare(
            task_card=card, plan=accepted, objective_id="objective-1",
            stores=[search.SearchStore(
                store_id="everos", kind="historical_evidence",
                query=lambda query: queried.append(query) or [],
            )],
            apc_launcher=lambda request: launched.append(request) or None,
            lane_id="lane-1", run_id="run-1", worktree_path=str(self.root),
            base_commit="base-1", finalize=True, mandatory_content=mandatory,
            optional_items=[{
                "id": "required-guidance", "kind": "historical_evidence",
                "origin": "everos", "revision_id": "r1",
                "content": {"summary": "plan depends on this"},
                "plan_affecting": True,
            }],
        )
        self.assertEqual("no_memory_continuation", outcome.mode)
        self.assertEqual(accepted, outcome.plan)
        self.assertIn("plan-affecting", outcome.reason)
        self.assertIsNone(outcome.context)
        self.assertIsNone(outcome.envelope)
        self.assertEqual([], queried)
        self.assertEqual([], launched)
        self.assertEqual([first.preparation], self.memory_store.list_preparations(first.decision["decision_id"]))
        self.assertIsNone(self.memory_store.get_final_context_for_decision(first.decision["decision_id"]))

    def test_failed_budget_raw_dependency_claim_does_not_authorize_replan(self) -> None:
        card, accepted, first, service, mandatory = self._accepted_budget_finalization_state()
        outcome = service.prepare(
            task_card=card, plan=accepted, objective_id="objective-1",
            lane_id="lane-1", run_id="run-1", worktree_path=str(self.root),
            base_commit="base-1", finalize=True, mandatory_content=mandatory,
            checkpoint="checkpoint-1", execution_role="worker",
            invocation_target="harness:worker", recipient="worker:lane-1",
            optional_items=[{
                "id": "unverified-claim", "kind": "historical_evidence",
                "origin": "everos", "revision_id": "r1",
                "content": {"summary": "optional hint"}, "plan_affecting": True,
            }],
        )
        self.assertTrue(outcome.dispatchable)
        self.assertIn("unverified-claim", outcome.envelope["delivery"]["omitted"])
        self.assertIsNone(outcome.context["delivery_trace"]["selected"][-1]["provenance"].get("plan_affecting"))
        self.assertEqual(outcome.context, self.memory_store.get_final_context_for_decision(first.decision["decision_id"]))

    def test_failed_or_unreadable_budget_lookup_fails_before_optional_work(self) -> None:
        first = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 240.0,
        )
        original = self.memory_store.get_preparation(first.preparation["preparation_id"])
        queried: list[object] = []
        launched: list[object] = []

        class BudgetReadStore:
            def __init__(self, inner, invalid):
                self.inner = inner
                self.invalid = invalid

            def list_preparations(self, decision_id):
                if self.invalid == "failure":
                    raise OSError("durable budget read failed")
                record = dict(original)
                record[self.invalid] = "unreadable"
                return [record]

            def __getattr__(self, name):
                return getattr(self.inner, name)

        for invalid in ("failure", "deadline_monotonic", "remaining_seconds", "spent_seconds"):
            for supplied_deadline in (None, self.clock() + 600.0):
                with self.subTest(invalid=invalid, deadline=supplied_deadline):
                    service = preparation.PreparationService(
                        store=BudgetReadStore(self.memory_store, invalid),
                        limits=self.limits,
                        clock=self.clock,
                    )
                    blocked = service.prepare(
                        task_card=self.card,
                        plan=self.candidate,
                        objective_id="objective-1",
                        deadline=supplied_deadline,
                        root_replan={"requested_by": "ROOT", "reason": "retry"},
                        stores=[search.SearchStore(
                            store_id="everos",
                            kind="historical_evidence",
                            query=lambda query: queried.append(query) or [],
                        )],
                        apc_launcher=lambda request: launched.append(request) or None,
                    )
                    self.assertEqual("no_memory_continuation", blocked.mode)
                    self.assertEqual(self.candidate, blocked.plan)
                    self.assertEqual(first.decision["decision_id"], blocked.decision["decision_id"])
                    self.assertIsNone(blocked.preparation)
                    self.assertIsNone(blocked.trace)
                    self.assertIn("durable", blocked.reason)
                    self.assertEqual([], queried)
                    self.assertEqual([], launched)
                    self.assertEqual(
                        [original],
                        self.memory_store.list_preparations(first.decision["decision_id"]),
                    )

    def test_no_store_first_call_keeps_its_supplied_deadline(self) -> None:
        service = preparation.PreparationService(limits=self.limits, clock=self.clock)
        first = service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            deadline=self.clock() + 240.0,
        )
        self.assertEqual(1240.0, first.preparation["deadline_monotonic"])
        self.assertEqual(240.0, first.preparation["remaining_seconds"])
        self.assertEqual(0.0, first.preparation["spent_seconds"])

    def test_expired_durable_deadline_makes_no_optional_call_after_restart(self) -> None:
        queried: list[object] = []

        def recording(query):
            queried.append(query)
            return []

        self.service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1", route="ordinary"
        )
        self.memory_store.close()
        self.clock.advance(400.0)
        reopened = store.MemoryStore(self.store_path)
        reopened.initialize()
        try:
            service = preparation.PreparationService(
                store=reopened, limits=self.limits, clock=self.clock
            )
            outcome = service.prepare(
                task_card=self.card,
                plan=self.candidate,
                objective_id="objective-1",
                route="ordinary",
                stores=[
                    search.SearchStore(
                        store_id="everos", kind="historical_evidence", query=recording
                    )
                ],
            )
            self.assertEqual(0.0, outcome.preparation["remaining_seconds"])
            self.assertEqual(0.0, outcome.preparation["stage_allowance_seconds"])
            self.assertEqual([], queried)
            self.assertEqual("no_optional_memory", outcome.trace["outcome"])
            self.assertEqual(0, outcome.trace["rounds"])
            attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
            self.assertEqual("unattempted-by-budget", attempts["everos"]["status"])
        finally:
            reopened.close()
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()

    # -- late Level 0 re-admission -----------------------------------------

    def test_level_zero_reuses_the_durable_recorded_configuration(self) -> None:
        queried: list[object] = []

        def recording(query):
            queried.append(query)
            return []

        recorded = {"strategy": "standard", "template_memory": False, "atlas_shared_retrieval": False}
        service = preparation.PreparationService(
            store=self.memory_store,
            limits=self.limits,
            config=config.resolve_config(recorded),
            clock=self.clock,
        )
        first = service.prepare(
            task_card=self.card, plan=self.candidate, objective_id="objective-1", route="ordinary"
        )
        self.clock.advance(30.0)
        revised = service.apply_level_zero(
            preparation=first.preparation,
            decision=first.decision,
            task_card=self.card,
            plan=first.plan,
            objective_id="objective-1",
            stores=[
                search.SearchStore(store_id="templates", kind="template", query=recording)
            ],
        )
        self.assertFalse(revised.preparation["configuration"]["template_memory"])
        self.assertFalse(revised.preparation["configuration"]["atlas_shared_retrieval"])
        attempts = {entry["store_id"]: entry for entry in revised.trace["attempts"]}
        self.assertEqual("disabled", attempts["templates"]["status"])
        self.assertEqual([], queried)
        self.assertLess(revised.preparation["remaining_seconds"], 300.0)
        self.assertEqual(
            30.0, round(300.0 - revised.preparation["remaining_seconds"], 3)
        )
        self.assertEqual(30.0, revised.preparation["spent_seconds"])

    def test_restarted_level_zero_continues_the_recorded_gates_and_budget(self) -> None:
        """A restarted service cannot loosen or re-scope one Level 0 correction.

        The original packet captured its own problem-focused strategy under
        disabled template/Atlas/experience gates.  The restarted service below
        carries conflicting all-on Deeper defaults and a spent clock: the one
        bounded ordinary re-prepare must continue the recorded configuration
        and strategy, the shared absolute deadline and recomputed spent cost,
        and the supersession lineage, and any repeated Level 0 for the
        abandoned packet must continue without a second optional search.
        """

        queried: list[object] = []

        def recording(query):
            queried.append(query)
            return []

        stores = [
            search.SearchStore(store_id="templates", kind="template", query=recording)
        ]
        first = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            request={
                "strategy": "problem_focused",
                "template_memory": False,
                "atlas_shared_retrieval": False,
                "experience_read": False,
            },
            failure_context="the parser test fails with an unexpected TypeError",
        )
        recorded_configuration = first.preparation["configuration"]
        self.assertEqual("problem_focused", recorded_configuration["strategy"])
        self.assertFalse(recorded_configuration["template_memory"])
        self.assertFalse(recorded_configuration["atlas_shared_retrieval"])
        self.assertFalse(recorded_configuration["experience_read"])

        self.memory_store.close()
        self.clock.advance(30.0)
        reopened = store.MemoryStore(self.store_path)
        reopened.initialize()
        try:
            restarted = preparation.PreparationService(
                store=reopened,
                limits=self.limits,
                config=config.resolve_config({"strategy": "deeper", "deeper": True}),
                clock=self.clock,
            )
            revised = restarted.apply_level_zero(
                preparation=first.preparation,
                decision=first.decision,
                task_card=self.card,
                plan=first.plan,
                objective_id="objective-1",
                stores=stores,
            )
            # The restarted service's current defaults are not the decision:
            # the captured effective strategy and its gates continue exactly.
            self.assertEqual(
                recorded_configuration, revised.preparation["configuration"]
            )
            self.assertEqual("problem_focused", revised.preparation["strategy"])
            self.assertEqual(
                self.limits.problem_focused_stage_seconds,
                revised.preparation["stage_allowance_seconds"],
            )
            attempts = {entry["store_id"]: entry for entry in revised.trace["attempts"]}
            self.assertEqual("disabled", attempts["templates"]["status"])
            self.assertEqual([], queried, "recorded gates must not be loosened")
            # One decision keeps one deadline and one granted budget: the
            # correction shares the recorded absolute deadline and recomputes
            # the cost already paid instead of replenishing it.
            self.assertEqual(
                first.preparation["deadline_monotonic"],
                revised.preparation["deadline_monotonic"],
            )
            self.assertEqual(270.0, revised.preparation["remaining_seconds"])
            self.assertEqual(30.0, revised.preparation["spent_seconds"])
            self.assertEqual(
                first.preparation["preparation_id"], revised.preparation["supersedes"]
            )
            abandoned = reopened.get_preparation(first.preparation["preparation_id"])
            self.assertEqual("superseded", abandoned["status"])
            self.assertEqual(
                revised.preparation["preparation_id"], abandoned["superseded_by"]
            )
            # A repeated Level 0 continues explicitly without memory: neither
            # the corrected packet nor the abandoned one may launch a second
            # optional search or hand out fresh time.
            for replay in (revised.preparation, first.preparation):
                with self.subTest(replay=replay["preparation_id"]):
                    again = restarted.apply_level_zero(
                        preparation=replay,
                        decision=revised.decision,
                        task_card=self.card,
                        plan=revised.plan,
                        objective_id="objective-1",
                        stores=stores,
                    )
                    self.assertEqual("no_memory_continuation", again.mode)
                    self.assertIsNone(again.trace)
                    self.assertEqual([], queried)
        finally:
            reopened.close()
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()

    def test_unreadable_durable_replay_fails_closed_without_a_second_optional_search(
        self,
    ) -> None:
        """An unreadable durable record is not proof of no prior correction.

        ROOT reproduced one optional query on the first Level 0 correction and
        a second query on replay after a one-call OSError, because the failed
        durable read was treated as a missing correction.  An unreadable or
        missing durable preparation must instead continue explicitly without
        memory, and the no-store path keeps its one bounded ordinary re-prepare.
        """

        queried: list[object] = []

        def recording(query):
            queried.append(query)
            return []

        stores = [
            search.SearchStore(
                store_id="everos", kind="historical_evidence", query=recording
            )
        ]
        first = self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
        )
        self.clock.advance(30.0)
        revised = self.service.apply_level_zero(
            preparation=first.preparation,
            decision=first.decision,
            task_card=self.card,
            plan=first.plan,
            objective_id="objective-1",
            stores=stores,
        )
        self.assertEqual("planning", revised.mode)
        self.assertEqual(1, len(queried), "the one correction performs one optional query")
        self.assertEqual(
            2,
            len(self.memory_store.list_preparations(first.decision["decision_id"])),
        )

        # A restarted service whose durable read fails exactly once, as ROOT
        # reproduced the second optional query on replay.
        flaky = FlakyDurableReadStore(self.memory_store)
        restarted = preparation.PreparationService(
            store=flaky, limits=self.limits, clock=self.clock
        )
        again = restarted.apply_level_zero(
            preparation=first.preparation,
            decision=first.decision,
            task_card=self.card,
            plan=first.plan,
            objective_id="objective-1",
            stores=stores,
        )
        self.assertEqual(1, flaky.read_attempts, "the replay made one durable read attempt")
        self.assertEqual(0, flaky.failures_remaining, "the transient failure was consumed")
        self.assertEqual("no_memory_continuation", again.mode)
        self.assertIsNone(again.trace)
        self.assertEqual(
            1, len(queried), "an unreadable durable replay must not search again"
        )
        self.assertEqual(
            first.preparation["preparation_id"],
            again.preparation["preparation_id"],
            "the failed replay must not mint a replacement packet",
        )
        self.assertEqual(
            2,
            len(self.memory_store.list_preparations(first.decision["decision_id"])),
            "the failed replay must not record a second replacement preparation",
        )

        # Without a store there is no durable authority to read, so the
        # accepted no-store behavior keeps its one bounded ordinary re-prepare.
        unstored = preparation.PreparationService(limits=self.limits, clock=self.clock)
        no_store = unstored.apply_level_zero(
            preparation=first.preparation,
            decision=first.decision,
            task_card=self.card,
            plan=first.plan,
            objective_id="objective-1",
            stores=stores,
        )
        self.assertEqual("planning", no_store.mode)
        self.assertEqual(2, len(queried), "the no-store path still re-prepares once")


if __name__ == "__main__":
    unittest.main()
