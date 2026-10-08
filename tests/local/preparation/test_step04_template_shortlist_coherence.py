"""STEP-04: reuse planning consumes only the bounded eligible shortlist.

BEHAVIOR-01 selects one bounded eligible shortlist; BEHAVIOR-02 may only
consider template candidates that shortlist actually selected.  These tests pin
that boundary for ``preparation.PreparationService``:

- the bounded search and plan production share one canonical sanitized
  representation built from the exact task, objective, route, and real failure
  context, so the query is task-derived rather than objective-id derived;
- no template store, a denied optional stage, disabled template memory, and a
  forged or unregistered registry record all take fresh ROOT planning without a
  direct fill or an APC launch, and never fall back to a looser registry sweep;
- direct fill and near-match handoff resolve only through the exact selected
  trace candidate rejoined to the immutable registry by id, version, and
  content digest, with the trusted comparable score recomputed from the same
  canonical representation;
- a higher-ranked selected template that fails typed validation keeps every
  lower-ranked selected eligible candidate available.
"""

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
    local_adapters,
    preparation,
    privacy,
    search,
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


class QuietEvidenceService:
    """An inert local evidence seam; templates are the subject here."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, str]] = []

    def search_recent_evidence(self, scope, query):
        self.calls.append((scope, query))
        return []


class RecordingTemplateQuery:
    """Wrap one real local template store so its bounded query is observable."""

    def __init__(self, store) -> None:
        self.store = store
        self.queries: list[dict] = []

    def __call__(self, payload):
        self.queries.append(dict(payload))
        return self.store.query(payload)


def raw_template_candidate(template, *, payload=None, revision_id=None):
    """Build the raw shape one store hands the accepted bounded search."""

    record = template.to_record()
    body = dict(payload) if payload is not None else record
    return {
        "kind": "template",
        "logical_id": template.template_id,
        "revision_id": revision_id or f"v{template.version}",
        "origin": "test-template-store",
        "source_id": "test-template-store",
        "payload": body,
        "payload_digest": contracts.sha256_hex(body),
        "scope": {},
        "routes": list(template.routes),
        "representation": templates.template_representation(template),
        "score": 1.0,
        "freshness": "live",
    }


class ShortlistStore:
    """A bounded template SearchStore emitting the exact raw items it is given."""

    def __init__(self, store_id: str, items) -> None:
        self.items = [dict(item) for item in items]
        self.queries: list[dict] = []
        self.store = search.SearchStore(
            store_id=store_id,
            kind="template",
            query=self._query,
            scope=None,
            freshness="live",
            specificity="local",
        )

    @property
    def kind(self) -> str:
        return self.store.kind

    @property
    def store_id(self) -> str:
        return self.store.store_id

    @property
    def requires_network(self) -> bool:
        return self.store.requires_network

    def _query(self, payload):
        self.queries.append(dict(payload))
        return [dict(item) for item in self.items]

    def __getattr__(self, name):
        return getattr(self.store, name)


class Step04TemplateShortlistCoherenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "store_seconds": 8.0,
                "minimum_optional_slice_seconds": 1.0,
                "direct_fill_threshold": 0.5,
                "near_match_threshold": 0.2,
            }
        )
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.service_registry = templates.load_default_templates()
        self.service = self._service()
        self.evidence = QuietEvidenceService()
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="shortlist-coherence",
            owner="root-agent",
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

    def _service(self, *, registry=None, limits=None):
        return preparation.PreparationService(
            store=self.memory_store,
            limits=limits or self.limits,
            clock=self.clock,
            registry=registry if registry is not None else self.service_registry,
        )

    def _local_stores(self, *, registry=None, limits=None):
        return local_adapters.make_local_search_stores(
            experience_service=self.evidence,
            scope=self.scope,
            registry=registry,
            limits=limits or self.limits,
        )

    def _plan_for(self, route: str):
        if route == "ordinary":
            return self.plan
        return contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route=route,
            state="candidate",
            content={"steps": ["draft"]},
        )

    def _prepare(self, task: str, **overrides):
        route = str(overrides.get("route") or "ordinary")
        arguments = {
            "task_card": self._card(task),
            "plan": self._plan_for(route),
            "objective_id": "objective-1",
            "route": route,
            "root_replan": ROOT_REPLAN,
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    def _selected_templates(self, outcome):
        return [
            item
            for item in outcome.selected_candidates
            if item["kind"] == "template"
        ]

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

    def _launcher(self, calls):
        def launcher(request):
            calls.append(request)
            return {
                "invocation_id": "apc-child-1",
                "result": apc.make_apc_result(request, self._proposal_for(request)),
                "pid": 321,
            }

        return launcher

    # -- one canonical sanitized representation for both phases -------------

    def test_search_query_carries_the_task_and_failure_context_representation(self) -> None:
        """The bounded query is task-derived, not objective-id derived."""

        stores = self._local_stores()
        template_store = next(item for item in stores if item.kind == "template")
        recording = RecordingTemplateQuery(template_store)
        stores = tuple(
            search.SearchStore(
                store_id=item.store_id,
                kind=item.kind,
                query=recording if item.kind == "template" else item.query,
                scope=item.scope,
                freshness=item.freshness,
                specificity=item.specificity,
                requires_network=item.requires_network,
            )
            for item in stores
        )
        outcome = self._prepare(
            "Repair the parser regression",
            stores=list(stores),
            failure_context="indexerror empty parser",
        )
        self.assertTrue(recording.queries)
        payload = recording.queries[0]
        tokens = set(payload["tokens"])
        # The exact task and the real failure context shape the query; the
        # objective id and the fixed-strategy label never replace them.
        self.assertIn("regression", tokens)
        self.assertIn("repair", tokens)
        self.assertIn("parser", tokens)
        self.assertIn("empty", tokens)
        self.assertIn("indexerror", tokens)
        self.assertEqual("ordinary", payload["route"])
        identity = templates.representation_identity(limits=self.limits)
        self.assertEqual(
            identity,
            {
                key: payload["representation"][key]
                for key in ("model", "dimensions", "metric", "sanitizer_version")
            },
        )
        # The very same canonical representation is what the reuse phase
        # recomputes against: the selected template rejoins with that score.
        selected = self._selected_templates(outcome)
        self.assertTrue(selected)
        canonical = templates.objective_representation(
            "Repair the parser regression objective-1",
            route="ordinary",
            limits=self.limits,
            failure_context="indexerror empty parser",
        )
        template = templates.load_default_templates()[0]
        expected = templates.score_representations(
            canonical, templates.template_representation(template, limits=self.limits)
        )
        self.assertEqual(expected, selected[0]["score"])
        # The canonical representation carries exactly the task-derived
        # sanitized query: the exact task, the objective identity, the route,
        # and the real failure context, and nothing from the fixed strategy.
        self.assertEqual(set(canonical["tokens"]), tokens)
        self.assertNotIn("standard", tokens)
        self.assertNotIn("strategy", tokens)

    def test_long_task_keeps_the_shared_bounded_token_projection_eligible(self) -> None:
        """A task past the bounded projection is judged on what was queried.

        The bounded store query carries only the one shared 32-token
        projection, so the trusted reuse recomputation must score exactly that
        projection; scoring the whole canonical token set instead would reject
        the candidate the bounded search just selected at the same threshold.
        """

        limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "store_seconds": 8.0,
                "minimum_optional_slice_seconds": 1.0,
                "minimum_comparable_score": 0.05,
                "near_match_threshold": 0.1,
                "direct_fill_threshold": 0.5,
            }
        )
        # One long, sanitized task whose token set is far wider than the shared
        # projection while every declared template keyword stays inside it.
        task = (
            "regression failure test repair "
            + " ".join(f"aaa{index}" for index in range(1, 13))
            + " "
            + " ".join(f"zeta{index}" for index in range(1, 41))
        )
        service = self._service(limits=limits)
        stores = self._local_stores(limits=limits)
        calls: list[object] = []
        outcome = service.prepare(
            task_card=self._card(task),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            root_replan=ROOT_REPLAN,
            stores=list(stores),
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        canonical = templates.objective_representation(
            f"{task} objective-1", route="ordinary", limits=limits
        )
        self.assertGreater(
            len(canonical["tokens"]), templates.MAX_REPRESENTATION_TOKENS
        )
        projection = templates.bounded_token_projection(canonical["tokens"])
        self.assertEqual(templates.MAX_REPRESENTATION_TOKENS, len(projection))
        template = templates.load_default_templates()[0]
        declaration = templates.template_representation(template, limits=limits)
        for keyword in template.keywords:
            self.assertIn(keyword, projection)
        projected = templates.score_representations(
            templates.projected_objective(canonical), declaration
        )
        # The dropped long tail only exists outside the shared projection: the
        # whole-record score is strictly lower, and judging the selected
        # template on it would cross the same near-match threshold.
        self.assertLess(
            templates.score_representations(canonical, declaration),
            limits.near_match_threshold,
        )
        self.assertGreaterEqual(projected, limits.near_match_threshold)
        selected = self._selected_templates(outcome)
        self.assertEqual(1, len(selected))
        self.assertEqual(template.template_id, selected[0]["logical_id"])
        self.assertEqual(projected, selected[0]["score"])
        attempts = outcome.disposition["reuse_attempts"]
        self.assertFalse(
            [
                entry
                for entry in attempts
                if "below the configured near-match threshold"
                in str(entry.get("reason", ""))
            ],
            attempts,
        )
        # The selected long-task template stayed eligible through the rejoin:
        # its near-match band hands off to one bounded adaptation child.
        self.assertEqual("apc_proposal", outcome.disposition["branch"])
        self.assertEqual(1, len(calls))
        self.assertEqual(template.template_id, calls[0]["template_id"])
        self.assertEqual(projected, outcome.disposition["template"]["score"])

    def test_direct_fill_resolves_only_from_the_selected_trace_candidate(self) -> None:
        stores = self._local_stores()
        outcome = self._prepare(
            "regression failure test repair",
            stores=list(stores),
            bindings={"failure": "parser test fails", "component": "parser"},
        )
        selected = self._selected_templates(outcome)
        self.assertEqual(1, len(selected))
        self.assertEqual("ordinary-regression-repair/v1", selected[0]["logical_id"])
        self.assertEqual("direct_fill", outcome.disposition["branch"])
        template = templates.load_default_templates()[0]
        self.assertEqual(
            contracts.sha256_hex(template.to_record()),
            outcome.disposition["template"]["digest"],
        )
        self.assertEqual(selected[0]["payload_digest"], outcome.disposition["template"]["digest"])
        self.assertEqual("proposed", outcome.plan["state"])
        self.assertIsNone(outcome.proposal)

    def test_lower_ranked_selected_template_survives_a_typed_validation_failure(self) -> None:
        high = templates.Template(
            template_id="stricter-family/v1",
            version=1,
            family="Stricter family",
            fixed_steps=("establish the failure",),
            required_fields=("failure", "component"),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair", "objective"),
            representation=templates.representation_identity(limits=self.limits),
        )
        lower = templates.Template(
            template_id="looser-family/v1",
            version=3,
            family="Looser family",
            fixed_steps=("establish the failure",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair"),
            representation=templates.representation_identity(limits=self.limits),
        )
        # Only these two families are declared on both sides of the seam, so
        # the delivered rank order and the exact rejoin are unambiguous: the
        # strict higher-ranked family is delivered first and the looser second.
        registry = (high, lower)
        stores = self._local_stores(registry=registry)
        service = self._service(registry=registry)
        outcome = service.prepare(
            task_card=self._card("regression failure test repair"),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            root_replan=ROOT_REPLAN,
            stores=list(stores),
            bindings={"failure": "parser test fails"},
        )
        selected = self._selected_templates(outcome)
        self.assertGreaterEqual(len(selected), 2)
        self.assertEqual(["stricter-family/v1", "looser-family/v1"], [item["logical_id"] for item in selected[:2]])
        self.assertEqual("direct_fill", outcome.disposition["branch"])
        self.assertEqual("looser-family/v1", outcome.disposition["template"]["template_id"])
        attempts = outcome.disposition["reuse_attempts"]
        self.assertTrue(
            any(
                item["template_id"] == "stricter-family/v1"
                and item["status"] == "rejected"
                for item in attempts
            ),
            attempts,
        )

    def test_rejoin_orders_selected_candidates_by_recomputed_trusted_score(self) -> None:
        """Store-declared raw scores cannot choose the proposed template.

        Two exact registry-matching selected templates are delivered with their
        raw discovery scores inverted against the trusted comparable score the
        rejoin recomputes, so the delivered rank order proposes the weaker
        template.  Only the recomputed trusted score may order the selected
        shortlist, and no unselected registry entry is ever added.
        """

        strong = templates.Template(
            template_id="invert-strong/v1",
            version=1,
            family="Invert strong",
            fixed_steps=("establish the failure",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair"),
            representation=templates.representation_identity(limits=self.limits),
        )
        weak = templates.Template(
            template_id="invert-weak/v1",
            version=2,
            family="Invert weak",
            fixed_steps=("establish the failure",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test"),
            representation=templates.representation_identity(limits=self.limits),
        )
        # Only these two exact registry records are declared on both sides of
        # the seam, and the delivered raw scores are inverted against the
        # trusted recomputation: the weaker record arrives first.
        registry = (strong, weak)
        service = self._service(registry=registry)
        weak_item = raw_template_candidate(weak)
        weak_item["score"] = 0.99
        strong_item = raw_template_candidate(strong)
        strong_item["score"] = 0.01
        outcome = service.prepare(
            task_card=self._card("regression failure test repair"),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            root_replan=ROOT_REPLAN,
            stores=[ShortlistStore("inverted-templates", [weak_item, strong_item])],
            bindings={"failure": "parser test fails"},
        )
        selected = self._selected_templates(outcome)
        self.assertEqual(
            ["invert-weak/v1", "invert-strong/v1"],
            [item["logical_id"] for item in selected],
        )
        canonical = templates.objective_representation(
            "regression failure test repair objective-1",
            route="ordinary",
            limits=self.limits,
        )
        strong_score = templates.score_representations(
            canonical, templates.template_representation(strong, limits=self.limits)
        )
        weak_score = templates.score_representations(
            canonical, templates.template_representation(weak, limits=self.limits)
        )
        self.assertGreater(strong_score, weak_score)
        self.assertGreaterEqual(weak_score, self.limits.direct_fill_threshold)
        # Both selected records are direct-fill eligible, so only the trusted
        # recomputed score may decide which one is proposed.
        self.assertEqual("direct_fill", outcome.disposition["branch"])
        self.assertEqual(
            "invert-strong/v1", outcome.disposition["template"]["template_id"]
        )
        self.assertEqual(strong_score, outcome.disposition["template"]["score"])
        self.assertEqual([], outcome.disposition["reuse_attempts"])

    def test_known_secret_never_reaches_the_query_or_the_reuse_score(self) -> None:
        """A configured secret is sanitized before tokenization in both phases.

        The bounded store query and the trusted reuse score are both derived
        from the one canonical representation, and that representation carries
        only the sanitized text: a protected token can neither be queried nor
        shift the comparable score by which the shortlist is judged.
        """

        secret = "s3cr3t-canary-value"
        marker = privacy.REDACTION_MARKER
        policy = privacy.PrivacyPolicy(known_secrets=(secret,))
        service = preparation.PreparationService(
            store=self.memory_store,
            limits=self.limits,
            clock=self.clock,
            privacy_policy=policy,
        )

        def prepare(task: str, failure: str, objective_id: str = "objective-1"):
            plan = (
                self.plan
                if objective_id == "objective-1"
                else contracts.make_plan(
                    plan_id="candidate-plan",
                    objective_id=objective_id,
                    route="ordinary",
                    state="candidate",
                    content={"steps": ["draft"]},
                )
            )
            stores = self._local_stores()
            recording = RecordingTemplateQuery(
                next(item for item in stores if item.kind == "template")
            )
            wired = tuple(
                search.SearchStore(
                    store_id=item.store_id,
                    kind=item.kind,
                    query=recording if item.kind == "template" else item.query,
                    scope=item.scope,
                    freshness=item.freshness,
                    specificity=item.specificity,
                    requires_network=item.requires_network,
                )
                for item in stores
            )
            outcome = service.prepare(
                task_card=self._card(task),
                plan=plan,
                objective_id=objective_id,
                route="ordinary",
                root_replan=ROOT_REPLAN,
                failure_context=failure,
                stores=list(wired),
                bindings={"failure": "parser test fails", "component": "parser"},
            )
            return outcome, recording.queries[0]

        # The declared task and the real failure context both carry the
        # secret; the sanitized score still lands on the direct fill that the
        # same text earns when the marker is written literally.
        protected, protected_query = prepare(
            f"regression failure test repair {secret}",
            secret,
        )
        redacted, redacted_query = prepare(
            f"regression failure test repair {marker}",
            marker,
        )

        # The query never carries the secret or a token derived from it.
        serialized = repr(protected_query)
        self.assertNotIn(secret, serialized)
        tokens = set(protected_query["tokens"])
        self.assertNotIn("canary", tokens)
        self.assertNotIn("s3cr3t", tokens)
        self.assertNotIn("value", tokens)
        self.assertIn("redacted", tokens)
        # The opaque policy_context binds each query to its distinct durable
        # preparation; the sanitized search terms must still be identical.
        def search_terms(query):
            return {key: query[key] for key in ("representation", "tokens", "route")}
        self.assertEqual(search_terms(redacted_query), search_terms(protected_query))
        self.assertEqual(redacted_query["tokens"], protected_query["tokens"])

        # The trusted comparable score is computed from that same sanitized
        # record in both phases, so the secret cannot shift the band either.
        protected_selected = self._selected_templates(protected)
        redacted_selected = self._selected_templates(redacted)
        self.assertTrue(protected_selected)
        self.assertEqual("direct_fill", protected.disposition["branch"])
        self.assertEqual("direct_fill", redacted.disposition["branch"])
        canonical = templates.objective_representation(
            f"regression failure test repair {marker} objective-1",
            route="ordinary",
            limits=self.limits,
            failure_context=marker,
        )
        self.assertEqual(set(canonical["tokens"]), tokens)
        template = templates.load_default_templates()[0]
        expected = templates.score_representations(
            canonical, templates.template_representation(template, limits=self.limits)
        )
        self.assertEqual(expected, protected_selected[0]["score"])
        self.assertEqual(expected, protected.disposition["template"]["score"])
        self.assertEqual(
            redacted_selected[0]["score"], protected_selected[0]["score"]
        )

        # The caller-supplied objective identity is sanitized for the same one
        # representation, so a secret there is inert too; the durable record
        # still carries the exact objective id unchanged.
        secret_objective = f"objective-1-{secret}"
        plain_objective = f"objective-1-{marker}"
        protected_objective, protected_objective_query = prepare(
            "regression failure test repair",
            "indexerror",
            objective_id=secret_objective,
        )
        redacted_objective, redacted_objective_query = prepare(
            "regression failure test repair",
            "indexerror",
            objective_id=plain_objective,
        )
        self.assertNotIn(secret, repr(protected_objective_query))
        objective_tokens = set(protected_objective_query["tokens"])
        self.assertNotIn("canary", objective_tokens)
        self.assertNotIn("s3cr3t", objective_tokens)
        self.assertNotIn("value", objective_tokens)
        self.assertIn("redacted", objective_tokens)
        self.assertEqual(
            search_terms(redacted_objective_query),
            search_terms(protected_objective_query),
        )
        self.assertEqual(
            redacted_objective_query["tokens"], protected_objective_query["tokens"]
        )
        self.assertEqual(
            redacted_objective.disposition["template"]["score"],
            protected_objective.disposition["template"]["score"],
        )
        # The objective-id leg is judged on that same sanitized record: its
        # tokens are the canonical ones plus the declared failure context, and
        # it direct-fills from the trace candidate the bounded search selected.
        self.assertEqual(set(canonical["tokens"]) | {"indexerror"}, objective_tokens)
        self.assertEqual("direct_fill", protected_objective.disposition["branch"])
        self.assertEqual(
            templates.load_default_templates()[0].template_id,
            protected_objective.disposition["template"]["template_id"],
        )
        self.assertEqual(
            protected_objective.disposition["template"]["template_id"],
            redacted_objective.disposition["template"]["template_id"],
        )
        # Exact objective identity stays untouched in the durable record.
        self.assertEqual(secret_objective, protected_objective.preparation["objective_id"])
        self.assertEqual(secret_objective, protected_objective.decision["objective_id"])

    # -- fail-closed paths --------------------------------------------------

    def test_no_template_store_takes_fresh_planning_without_reuse_or_apc(self) -> None:
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            bindings={"failure": "parser test fails", "component": "parser"},
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual("fresh", outcome.plan["state"])
        self.assertEqual([], calls)
        self.assertEqual([], outcome.disposition["reuse_attempts"])
        self.assertEqual([], outcome.selected_candidates)
        self.assertIn("no bounded template store ran", outcome.disposition["reason"])

    def test_denied_optional_stage_takes_fresh_planning_without_reuse_or_apc(self) -> None:
        limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "standard_stage_seconds": 20.0,
                "deeper_stage_seconds": 60.0,
                "direct_fill_threshold": 0.5,
                "near_match_threshold": 0.2,
            }
        )
        queried: list[object] = []
        stores = self._local_stores()
        recording = RecordingTemplateQuery(
            next(item for item in stores if item.kind == "template")
        )
        stores = tuple(
            search.SearchStore(
                store_id=item.store_id,
                kind=item.kind,
                query=recording if item.kind == "template" else item.query,
                scope=item.scope,
                freshness=item.freshness,
                specificity=item.specificity,
                requires_network=item.requires_network,
            )
            for item in stores
        )
        calls: list[object] = []
        service = preparation.PreparationService(
            store=self.memory_store, limits=limits, clock=self.clock
        )
        outcome = service.prepare(
            task_card=self._card("Fix the regression failure in the parser test"),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            deadline=self.clock() + 79.0,
            stores=list(stores),
            bindings={"failure": "parser test fails"},
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
            root_replan=ROOT_REPLAN,
        )
        self.assertEqual(0.0, outcome.preparation["stage_allowance_seconds"])
        self.assertEqual([], recording.queries)
        self.assertEqual([], calls)
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual([], outcome.selected_candidates)
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        self.assertEqual(
            "unattempted-by-budget", attempts["local-template-registry"]["status"]
        )
        self.assertIn("no template search budget", outcome.disposition["reason"])
        self.assertEqual([], queried)

    def test_disabled_template_memory_takes_fresh_planning_without_reuse_or_apc(self) -> None:
        stores = self._local_stores()
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            stores=list(stores),
            request={"template_memory": False},
            bindings={"failure": "parser test fails"},
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual([], calls)
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual([], outcome.disposition["reuse_attempts"])
        self.assertIn("template memory is disabled", outcome.disposition["reason"])
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        self.assertEqual("disabled", attempts["local-template-registry"]["status"])

    def test_forged_registry_payload_is_rejected_without_a_looser_sweep(self) -> None:
        """A selected template whose payload does not match the registry is dead."""

        template = templates.load_default_templates()[0]
        forged = dict(template.to_record())
        forged["verification_intent"] = "forged intent"
        item = raw_template_candidate(template, payload=forged)
        calls: list[object] = []
        outcome = self._prepare(
            "regression failure test repair",
            stores=[ShortlistStore("forged-templates", [item])],
            bindings={"failure": "parser test fails", "component": "parser"},
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        # The registry record still exists and would direct fill under a loose
        # fallback; the mismatched selected record must not be salvaged.
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual([], calls)
        attempts = outcome.disposition["reuse_attempts"]
        self.assertTrue(attempts, attempts)
        self.assertTrue(
            any("content digest does not match" in item["reason"] for item in attempts),
            attempts,
        )
        self.assertNotEqual("direct_fill", outcome.disposition["branch"])

    def test_unregistered_selected_template_cannot_manufacture_a_near_match(self) -> None:
        ghost = templates.Template(
            template_id="ghost-family/v1",
            version=1,
            family="Ghost family",
            fixed_steps=("establish the failure",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair"),
            representation=templates.representation_identity(limits=self.limits),
        )
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            stores=[ShortlistStore("ghost-templates", [raw_template_candidate(ghost)])],
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual([], calls)
        attempts = outcome.disposition["reuse_attempts"]
        self.assertTrue(
            any("does not rejoin the explicit immutable registry" in item["reason"] for item in attempts),
            attempts,
        )

    def test_route_inapplicable_selected_template_is_rejected(self) -> None:
        template = templates.load_default_templates()[0]
        item = raw_template_candidate(template)
        item["routes"] = None
        calls: list[object] = []
        outcome = self._prepare(
            "Fix the regression failure in the parser test",
            route="problem_focused",
            stores=[ShortlistStore("route-unaware-templates", [item])],
            bindings={"failure": "parser test fails", "component": "parser"},
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual([], calls)
        attempts = outcome.disposition["reuse_attempts"]
        self.assertTrue(
            any("not applicable to route" in item["reason"] for item in attempts),
            attempts,
        )

    # -- near-match handoff --------------------------------------------------

    def test_near_match_handoff_uses_the_highest_selected_candidate(self) -> None:
        high = templates.Template(
            template_id="near-high/v1",
            version=1,
            family="Near high",
            fixed_steps=("establish the boundary",),
            required_fields=("boundary",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair"),
            representation=templates.representation_identity(limits=self.limits),
        )
        lower = templates.Template(
            template_id="near-low/v1",
            version=2,
            family="Near low",
            fixed_steps=("establish the boundary",),
            required_fields=("boundary",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure"),
            representation=templates.representation_identity(limits=self.limits),
        )
        # Only these two near candidates are declared on both sides of the
        # seam, so the highest selected candidate is the unambiguous handoff.
        registry = (high, lower)
        stores = self._local_stores(registry=registry)
        service = self._service(registry=registry)
        calls: list[object] = []
        outcome = service.prepare(
            task_card=self._card("Fix the regression failure in the parser test"),
            plan=self.plan,
            objective_id="objective-1",
            route="ordinary",
            root_replan=ROOT_REPLAN,
            stores=list(stores),
            apc_binding=BINDING,
            apc_launcher=self._launcher(calls),
        )
        self.assertEqual("apc_proposal", outcome.disposition["branch"])
        self.assertEqual(1, len(calls))
        request = calls[0]
        selected = self._selected_templates(outcome)
        self.assertTrue(selected)
        # The handoff uses the selected near candidate, never an unselected or
        # incompatible template, and keeps the explicit binding.
        self.assertEqual(selected[0]["logical_id"], request["template_id"])
        self.assertEqual("near-high/v1", request["template_id"])
        self.assertEqual(BINDING, request["binding"])
        self.assertEqual(["bindings"], request["permitted_edits"])
        self.assertNotEqual("near-low/v1", request["template_id"])

    def test_selected_direct_fill_without_bindings_takes_fresh_planning(self) -> None:
        stores = self._local_stores()
        outcome = self._prepare(
            "regression failure test repair",
            stores=list(stores),
        )
        self.assertEqual("fresh", outcome.disposition["branch"])
        attempts = outcome.disposition["reuse_attempts"]
        self.assertTrue(attempts)
        self.assertEqual("unavailable", attempts[0]["status"])
        self.assertEqual("direct_fill", attempts[0]["branch"])
        self.assertEqual("ordinary-regression-repair/v1", attempts[0]["template_id"])
        # A selected template did rejoin the registry with a trusted score, so
        # the fresh reason reports the failed fill, never a registry mismatch.
        self.assertIn(
            "no selected template produced a usable direct fill",
            outcome.disposition["reason"],
        )


if __name__ == "__main__":
    unittest.main()
