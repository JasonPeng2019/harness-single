"""BEHAVIOR-02: reuse produces a bounded ROOT-reviewable plan or fresh fallback."""

from __future__ import annotations

import sys
import tempfile
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
    store,
    templates,
)


BINDING = {
    "provider": "synthetic",
    "model": "synthetic/drafter",
    "cli": "synthetic-cli",
    "effort": "low",
    "source": "explicit",
}

# Only ROOT may replace or extend a current plan, so reuse and adaptation are
# exercised behind one explicit ROOT replan request.
ROOT_REPLAN = {
    "requested_by": "ROOT",
    "reason": "the tests exercise the explicit ROOT replan path",
}


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


def representation(tokens: list[str], *, limits: config.PreparationLimits) -> dict:
    return templates.representation_identity(limits=limits) | {
        "tokens": tokens,
        "route": "ordinary",
    }


class QuietEvidenceService:
    """An explicit local evidence seam with no reviewed trajectories yet.

    Template reuse consumes BEHAVIOR-01's bounded eligible shortlist, so the
    reuse tests exercise the actual local template ``SearchStore`` from the
    accepted local adapter factory; this evidence stub only keeps the sibling
    reviewed-evidence store inert without weakening it.
    """

    def search_recent_evidence(self, scope, query):
        return []


class ReusePlanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "direct_fill_threshold": 0.5,
                "near_match_threshold": 0.2,
            }
        )
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        # Reuse and adaptation consume only templates the bounded preparation
        # trace actually selected, so the fixture supplies the real local
        # template SearchStore through the accepted local adapter factory and
        # passes it to every preparation below.
        self.stores = local_adapters.make_local_search_stores(
            experience_service=QuietEvidenceService(),
            scope=experience.ExperienceScope(
                application="harness",
                project="product",
                namespace="reuse-planning",
                owner="root-agent",
            ),
            registry=templates.load_default_templates(),
            limits=self.limits,
        )
        self.plan = contracts.make_plan(
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

    def _card(self, task: str) -> dict:
        return contracts.make_task_card(task=task, base_commit="base-1")

    def _prepare(self, task: str, **overrides):
        """Prepare under an explicit ROOT replan request.

        A candidate plan continues its existing ROOT review and never runs
        template selection or adaptation by itself, so BEHAVIOR-02 reuse work
        only happens behind the explicit ROOT replan request these tests model.
        """

        arguments = {
            "task_card": self._card(task),
            "plan": self.plan,
            "objective_id": "objective-1",
            "route": "ordinary",
            "stores": list(self.stores),
            "root_replan": ROOT_REPLAN,
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    def _proposal_for(self, request, *, bindings=None, extra_surfaces=None):
        record = request["template"]
        content = {
            "fixed_steps": list(record["fixed_steps"]),
            "verification_intent": record["verification_intent"],
        }
        if "bindings" in request["permitted_edits"]:
            content["bindings"] = dict(bindings or {"failure": "parser", "component": "parser"})
        content.update(extra_surfaces or {})
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

    def _launcher(self, calls, *, extra_surfaces=None, edits=None):
        def launcher(request):
            calls.append(request)
            proposal = self._proposal_for(
                request, extra_surfaces=extra_surfaces
            )
            if edits is not None:
                proposal = edits(request, proposal)
            return {
                "invocation_id": "apc-child-1",
                "result": apc.make_apc_result(request, proposal),
                "pid": 321,
            }
        return launcher

    # -- discovery of the five seeded families -----------------------------

    def test_every_seeded_family_is_discoverable_through_the_local_path(self) -> None:
        cases = {
            "ordinary-regression-repair/v1": "regression failure test repair",
            "ordinary-public-interface-change/v1": "public api interface contract consumer",
            "ordinary-dependency-upgrade/v1": "dependency upgrade version",
            "ordinary-schema-data-migration/v1": "schema data migration",
            "ordinary-integration-failure/v1": "integration boundary failure",
        }
        for template_id, text in cases.items():
            with self.subTest(template=template_id):
                objective = templates.objective_representation(
                    text, route="ordinary", limits=self.limits
                )
                ranked = templates.rank_templates(
                    objective, templates.load_default_templates(), limits=self.limits
                )
                self.assertEqual(template_id, ranked[0].template.template_id)
                self.assertTrue(ranked[0].eligible)
                self.assertGreaterEqual(ranked[0].score, self.limits.near_match_threshold)

    def test_route_incompatible_family_stops_reuse(self) -> None:
        objective = templates.objective_representation(
            "regression failure test repair", route="deeper", limits=self.limits
        )
        ranked = templates.rank_templates(
            objective, templates.load_default_templates(), limits=self.limits
        )
        match = ranked[0]
        self.assertFalse(match.eligible)
        self.assertIn("not applicable to route", match.reason)

    # -- typed direct fill --------------------------------------------------

    def test_high_compatible_candidate_direct_fills_only_declared_typed_fields(self) -> None:
        outcome = self._prepare(
            "regression failure test repair",
            bindings={"failure": "parser test fails", "component": "parser"},
        )
        self.assertEqual("direct_fill", outcome.disposition["branch"])
        self.assertEqual("proposed", outcome.plan["state"])
        self.assertEqual("ordinary-regression-repair/v1", outcome.plan["source"]["template_id"])
        template = templates.load_default_templates()[0]
        self.assertEqual(list(template.fixed_steps), outcome.plan["content"]["fixed_steps"])
        self.assertEqual(template.verification_intent, outcome.plan["content"]["verification_intent"])
        self.assertEqual({"failure", "component"}, set(outcome.plan["content"]["bindings"]))
        self.assertIsNone(outcome.proposal)  # direct fill launches no child

    def test_undeclared_or_untyped_binding_is_rejected_and_falls_back_to_fresh(self) -> None:
        cases = (
            {"failure": "x", "component": "y", "extra": "z"},
            {"failure": {"nested": 1}, "component": "y"},
            {"failure": "  ", "component": "y"},
            {"failure": "x"},
        )
        for bindings in cases:
            with self.subTest(bindings=bindings):
                outcome = self._prepare(
                    "regression failure test repair", bindings=bindings
                )
                self.assertEqual("fresh", outcome.disposition["branch"])
                self.assertEqual("fresh", outcome.plan["state"])
                attempts = outcome.disposition["reuse_attempts"]
                self.assertEqual(1, len(attempts))
                self.assertEqual("rejected", attempts[0]["status"])
                self.assertEqual("direct_fill", attempts[0]["branch"])

    def test_direct_fill_without_trusted_bindings_is_unavailable(self) -> None:
        outcome = self._prepare("regression failure test repair")
        self.assertEqual("fresh", outcome.disposition["branch"])
        attempts = outcome.disposition["reuse_attempts"]
        self.assertEqual(1, len(attempts))
        self.assertEqual("unavailable", attempts[0]["status"])
        self.assertEqual("direct_fill", attempts[0]["branch"])

    # -- ranking and lower-ranked eligibility -------------------------------

    def test_incomparable_top_candidate_does_not_hide_a_lower_eligible_one(self) -> None:
        registry = templates.load_default_templates()
        incompatible = templates.Template(
            template_id="incompatible/v1",
            version=1,
            family="A incompatible family",
            fixed_steps=("step",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair", "parser"),
        )
        service = preparation.PreparationService(
            store=self.memory_store,
            limits=self.limits,
            clock=self.clock,
            registry=(incompatible,) + registry,
        )
        objective = templates.objective_representation(
            "regression failure test repair", route="ordinary", limits=self.limits
        )
        ranked = templates.rank_templates(objective, service.registry, limits=self.limits)
        self.assertFalse(ranked[0].comparable)
        self.assertFalse(ranked[0].eligible)
        eligible = [match for match in ranked if match.eligible]
        self.assertEqual("ordinary-regression-repair/v1", eligible[0].template.template_id)

    def test_incomparable_and_low_scores_return_to_fresh_planning(self) -> None:
        outcome = self._prepare("an unrelated request about bicycles")
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual("fresh", outcome.plan["state"])
        self.assertIsNone(outcome.disposition["proposal"])

    # -- bounded APC drafting child -----------------------------------------

    def test_near_match_launches_one_child_through_the_explicit_binding(self) -> None:
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("apc_proposal", outcome.disposition["branch"])
        self.assertEqual("proposed", outcome.proposal["state"])
        self.assertEqual(1, len(calls))
        request = calls[0]
        self.assertEqual("explicit", request["binding"]["source"])
        self.assertEqual(BINDING, request["binding"])
        self.assertEqual("objective-1", request["parent_objective_id"])
        self.assertEqual("ordinary-regression-repair/v1", request["template_id"])
        self.assertEqual(["bindings"], request["permitted_edits"])
        child = outcome.disposition["apc"]
        self.assertEqual("reconciled", child["status"])
        self.assertEqual(request["content_hash"], child["request_digest"])
        operations = self.memory_store.list_apc_child_operations(
            outcome.decision["decision_id"]
        )
        self.assertEqual(1, len(operations))
        self.assertEqual("reconciled", operations[0]["status"])
        self.assertIsNotNone(operations[0]["observed_invocation"])
        self.assertIsNotNone(operations[0]["launch_intent"])

    def test_missing_binding_makes_light_adaptation_unavailable(self) -> None:
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual([], calls)
        self.assertEqual([], outcome.disposition["reuse_attempts"])

    def test_inherited_or_credential_bearing_binding_is_rejected(self) -> None:
        inherited = dict(BINDING, source="inherited")
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=inherited,
            apc_launcher=self._launcher([]),
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        reason = outcome.disposition["reuse_attempts"][0]["reason"]
        self.assertIn("must not be inherited", reason)
        credential = dict(BINDING, api_key="not-a-real-secret")
        with self.assertRaisesRegex(apc.APCBindingError, "credentials"):
            apc.make_apc_request(
                template=templates.load_default_templates()[0].to_record(),
                parent_decision_id="decision-1",
                parent_objective_id="objective-1",
                permitted_edits=["bindings"],
                binding=credential,
            )

    def test_child_that_changes_structure_or_uses_unpermitted_edits_is_rejected(self) -> None:
        cases = {
            "structure": {"fixed_steps": ["changed"]},
            "unpermitted": {"summary": "not allowed"},
        }
        for name, extra in cases.items():
            with self.subTest(case=name):
                calls: list[object] = []
                outcome = self._prepare(
                    "Fix the regression failure in the parser test",
                    apc_binding=BINDING,
                    apc_launcher=self._launcher(calls, extra_surfaces=extra),
                )
                self.assertEqual("fresh", outcome.disposition["branch"])
                self.assertIsNone(outcome.proposal)
                reason = outcome.disposition["reuse_attempts"][0]["reason"]
                self.assertIn("ApcChildRejectedError", reason)

    def test_child_timeout_retires_the_owned_child_and_falls_back(self) -> None:
        calls: list[object] = []
        slow_clock = FakeClock()
        service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=slow_clock
        )

        def slow_launcher(request):
            calls.append(request)
            slow_clock.advance(5000.0)
            proposal = self._proposal_for(request)
            return {
                "invocation_id": "apc-child-slow",
                "result": apc.make_apc_result(request, proposal),
            }

        outcome = service.prepare(
            task_card=self._card("Fix the regression failure in the parser test"),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            stores=list(self.stores),
            apc_binding=BINDING,
            apc_launcher=slow_launcher,
            root_replan=ROOT_REPLAN,
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertIn("deadline", outcome.disposition["reuse_attempts"][0]["reason"])
        operations = self.memory_store.list_apc_child_operations(
            outcome.decision["decision_id"]
        )
        # The launched child may still be live and its cleanup is unproven, so
        # its ownership stays unresolved until exact reconciliation.
        self.assertEqual(
            ["cleanup_pending"], [operation["status"] for operation in operations]
        )
        self.assertIn(
            "cleanup_pending", contracts.APC_CHILD_UNRESOLVED_STATUSES
        )

    def test_ambiguous_acknowledgement_is_visible_and_reconciled_exactly(self) -> None:
        calls: list[object] = []

        def lost_ack(request):
            calls.append(request)
            return None

        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=BINDING,
            apc_launcher=lost_ack,
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertIn("ambiguous", outcome.disposition["reuse_attempts"][0]["reason"])
        operations = self.memory_store.list_apc_child_operations(
            outcome.decision["decision_id"]
        )
        self.assertEqual(["ambiguous"], [operation["status"] for operation in operations])
        self.assertEqual(1, len(calls))
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
            observed_invocation={
                "invocation_id": "controller:1:2026-01-01T00:00:00Z",
                "pid": 1,
                "creation_time": "2026-01-01T00:00:00Z",
            },
        )
        self.assertEqual("reconciled", reconciled["status"])
        self.assertEqual(1, len(calls))

    def test_repeated_prepare_cannot_blindly_relaunch_the_same_child(self) -> None:
        calls: list[object] = []

        def lost_ack(request):
            calls.append(request)
            return None

        first = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=BINDING,
            apc_launcher=lost_ack,
        )
        self.assertEqual(1, len(calls))
        second = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=BINDING,
            apc_launcher=lost_ack,
        )
        self.assertEqual(1, len(calls))
        self.assertIn("reconcile", second.disposition["reuse_attempts"][0]["reason"])
        self.assertEqual("fresh", second.disposition["branch"])
        self.assertIsNotNone(first.disposition)

    # -- ROOT acceptance stays distinct -------------------------------------

    def test_root_rejection_records_the_attempt_and_a_fresh_terminal_plan(self) -> None:
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        rejected = self.service.reject(
            disposition=outcome.disposition, reason="the draft omits the parser check"
        )
        self.assertEqual("fresh", rejected["branch"])
        self.assertEqual("fresh", rejected["fresh"]["state"])
        attempts = rejected["reuse_attempts"]
        self.assertTrue(any(item.get("status") == "rejected" for item in attempts))
        self.assertTrue(any(item.get("branch") == "apc_proposal" for item in attempts))

    def test_root_revision_creates_and_accepts_a_distinct_exact_plan(self) -> None:
        outcome = self._prepare(
            "regression failure test repair",
            bindings={"failure": "parser test fails", "component": "parser"},
        )
        revised, accepted = self.service.accept(
            disposition=outcome.disposition,
            proposal=outcome.plan,
            accepted_content={"steps": ["ROOT revised step"]},
            accepted_plan_id="root-revised-plan",
        )
        self.assertEqual("accepted", accepted["state"])
        self.assertEqual("root-revised-plan", accepted["plan_id"])
        self.assertTrue(revised["root_acceptance"]["revised"])
        self.assertEqual("ROOT", revised["root_acceptance"]["accepted_by"])
        self.assertNotEqual(outcome.plan["plan_id"], accepted["plan_id"])
        durable = self.memory_store.list_plan_dispositions(
            outcome.decision["decision_id"]
        )
        self.assertTrue(any(item["branch"] == "direct_fill" for item in durable))

    def test_proposal_completion_is_not_parent_acceptance(self) -> None:
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("proposed", outcome.proposal["state"])
        self.assertNotEqual("accepted", outcome.plan["state"])
        self.assertIsNone(outcome.disposition.get("root_acceptance"))


if __name__ == "__main__":
    unittest.main()
