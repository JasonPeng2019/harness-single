"""STEP-04: local SearchStore adapters over accepted evidence and templates.

These tests pin the narrow integration slice that turns the already accepted
local reviewed-evidence service and the five bundled template families into
real ``search.SearchStore`` inputs for ``preparation.PreparationService``:

- one durable ROOT-reviewed local trajectory becomes one sanitized, exactly
  scoped historical-evidence candidate with its exact reviewed status and
  review receipt, and it never becomes procedural guidance or a plan;
- the five real template seeds participate with their declared comparable
  representation, declared routes, configured scoring, and deterministic
  ordering, and undeclared, incomparable, or route-inapplicable templates are
  never delivered;
- exact-scope mismatch, unreviewed/unreceipted evidence, an incomparable
  objective identity, and a failed optional local lookup all fail closed while
  an unrelated local store still delivers.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    config,
    contracts,
    experience,
    local_adapters,
    preparation,
    privacy,
    store,
    templates,
)

ROOT_REPLAN = {
    "requested_by": "ROOT",
    "reason": "the tests exercise the explicit ROOT replan path",
}

TEMPLATE_CASES = {
    "ordinary-regression-repair/v1": "regression failure test repair",
    "ordinary-public-interface-change/v1": "public api interface contract consumer",
    "ordinary-dependency-upgrade/v1": "dependency upgrade version",
    "ordinary-schema-data-migration/v1": "schema data migration",
    "ordinary-integration-failure/v1": "integration boundary failure",
}


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class RecordingExperienceService(experience.ReviewedExperienceService):
    """Record the exact accepted-service calls the evidence store makes."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[object, str]] = []

    def search_recent_evidence(self, scope, query):
        self.calls.append((scope, query))
        return super().search_recent_evidence(scope, query)


class StubEvidenceService:
    """Only the accepted ``search_recent_evidence`` seam exists here."""

    def __init__(self, projections) -> None:
        self.projections = [dict(item) for item in projections]
        self.calls: list[tuple[object, str]] = []

    def search_recent_evidence(self, scope, query):
        self.calls.append((scope, query))
        return [dict(item) for item in self.projections]


class ExplodingEvidenceService:
    def search_recent_evidence(self, scope, query):
        raise RuntimeError("local evidence lookup failed")


class Step04LocalSearchAdapterTests(unittest.TestCase):
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
            }
        )
        self.policy = privacy.PrivacyPolicy(known_secrets=(experience.SYNTHETIC_SECRET,))
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.experience_service = RecordingExperienceService(
            self.memory_store, privacy_policy=self.policy
        )
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="isolated-local-adapters",
            owner="root-agent",
        )
        self.other_scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="another-namespace",
            owner="root-agent",
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _reviewed_trajectory(
        self,
        scope,
        *,
        suffix: str,
        task: str,
        raw_evidence: str,
        memory_store=None,
        experience_service=None,
    ) -> dict:
        memory_store = self.memory_store if memory_store is None else memory_store
        experience_service = (
            self.experience_service
            if experience_service is None
            else experience_service
        )
        plan = contracts.make_plan(
            plan_id=f"accepted-plan-{suffix}",
            objective_id=f"objective-{suffix}",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task=task,
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id=plan["objective_id"], route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        memory_store.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"],
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status="PASS",
            evidence_digest=f"terminal-evidence-{suffix}",
            linked_run_id=f"retired-run-{suffix}",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id=f"review-{suffix}",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=(f"review://run/retired-run-{suffix}", "check://parser"),
            protected_source_refs=("evidence://protected/parser-log",),
            raw_evidence=raw_evidence,
            failed_hypotheses=("the network adapter caused the regression",),
        )
        return experience_service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=scope,
        )

    def _payload(self, text: str, *, route: str = "ordinary") -> dict:
        objective = templates.objective_representation(text, route=route, limits=self.limits)
        return privacy.safe_query_payload(
            {
                "representation": {
                    key: objective[key]
                    for key in ("model", "dimensions", "metric", "sanitizer_version")
                },
                "tokens": list(objective["tokens"])[:32],
                "route": route,
            },
            self.policy,
        )

    def _stores(self, *, service=None, registry=None, limits=None):
        return local_adapters.make_local_search_stores(
            experience_service=service or self.experience_service,
            scope=self.scope,
            registry=registry,
            limits=limits or self.limits,
        )

    @staticmethod
    def _by_kind(stores, kind):
        return next(item for item in stores if item.kind == kind)

    def _candidate_plan(self, objective_id: str = "parser-regression-repair") -> dict:
        return contracts.make_plan(
            plan_id="candidate-plan",
            objective_id=objective_id,
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    # -- the reviewed local trajectory becomes evidence --------------------

    def test_reviewed_local_trajectory_becomes_one_scoped_evidence_candidate(self) -> None:
        trajectory = self._reviewed_trajectory(
            self.scope,
            suffix="evidence",
            task="Repair the parser regression",
            raw_evidence=(
                "The parser repair passed. Keep synthetic-secret-alpha-1234567890 "
                "only in protected local evidence."
            ),
        )
        stores = self._stores()
        # Building the adapters performs no lookup at all.
        self.assertEqual([], self.experience_service.calls)
        evidence_store = self._by_kind(stores, "historical_evidence")
        self.assertEqual(self.scope.to_record(), dict(evidence_store.scope))

        payload = self._payload("parser regression repair")
        candidates = list(evidence_store.query(payload))
        self.assertEqual(1, len(candidates))
        candidate = candidates[0]
        self.assertEqual("historical_evidence", candidate["kind"])
        self.assertEqual(trajectory["trajectory_id"], candidate["logical_id"])
        self.assertEqual(trajectory["review_receipt_id"], candidate["revision_id"])
        self.assertEqual(self.scope.to_record(), dict(candidate["scope"]))

        body = candidate["payload"]
        self.assertEqual("reviewed_success", body["status"])
        self.assertEqual("reviewed_historical_evidence", body["authority"])
        self.assertEqual(trajectory["review_receipt_id"], body["review_receipt_id"])
        self.assertEqual(list(trajectory["evidence_refs"]), list(body["evidence_refs"]))
        self.assertEqual(self.scope.to_record(), dict(body["scope"]))

        # The locally derived representation is comparable with the objective.
        identity = templates.representation_identity(limits=self.limits)
        representation = dict(candidate["representation"])
        for key, value in identity.items():
            self.assertEqual(value, representation[key], key)
        self.assertTrue(representation.get("declared"))
        self.assertTrue(representation["tokens"])
        self.assertGreater(candidate["score"], 0.0)
        self.assertEqual(candidate["payload_digest"], contracts.sha256_hex(body))

        # Evidence only: no raw protected evidence and no guidance or plan shape.
        serialized = contracts.canonical_json(candidate).decode("utf-8")
        self.assertNotIn(experience.SYNTHETIC_SECRET, serialized)
        self.assertNotIn("evidence://protected/parser-log", serialized)
        self.assertNotIn("protected_source_refs", serialized)
        self.assertNotIn("raw_evidence", serialized)
        for guidance in ("fixed_steps", "steps", "bindings", "plan_id", "accepted_plan_digest"):
            self.assertNotIn(guidance, body)

        # The store queried the accepted service with the explicit scope and
        # only the sanitized objective tokens.
        self.assertEqual(1, len(self.experience_service.calls))
        requested_scope, requested_query = self.experience_service.calls[0]
        self.assertEqual(self.scope, requested_scope)
        self.assertEqual(" ".join(payload["tokens"]), requested_query)

    # -- exact scope stays exact -------------------------------------------

    def test_exact_scope_mismatch_omits_the_other_namespace_evidence(self) -> None:
        mine = self._reviewed_trajectory(
            self.scope,
            suffix="mine",
            task="Repair the parser regression",
            raw_evidence="The parser repair passed inside this namespace.",
        )
        theirs = self._reviewed_trajectory(
            self.other_scope,
            suffix="theirs",
            task="Repair the parser regression elsewhere",
            raw_evidence="The parser repair passed inside another namespace.",
        )
        evidence_store = self._by_kind(self._stores(), "historical_evidence")
        candidates = list(evidence_store.query(self._payload("parser regression repair")))
        self.assertEqual([mine["trajectory_id"]], [item["logical_id"] for item in candidates])
        for item in candidates:
            self.assertEqual(self.scope.to_record(), dict(item["scope"]))

        other_stores = local_adapters.make_local_search_stores(
            experience_service=self.experience_service,
            scope=self.other_scope,
            registry=(),
            limits=self.limits,
        )
        other_store = self._by_kind(other_stores, "historical_evidence")
        other_candidates = list(other_store.query(self._payload("parser regression repair")))
        self.assertEqual(
            [theirs["trajectory_id"]], [item["logical_id"] for item in other_candidates]
        )
        self.assertNotIn(
            mine["trajectory_id"], [item["logical_id"] for item in other_candidates]
        )

    # -- unreviewed, unreceipted, and incomparable evidence ----------------

    def test_unreviewed_or_unreceipted_projections_are_omitted(self) -> None:
        reviewed = {
            "id": "trajectory-reviewed",
            "kind": "historical_evidence",
            "authority": "reviewed_historical_evidence",
            "status": "reviewed_failure",
            "content": (
                "Task: Repair the parser regression\n"
                "Historical evidence: the first parser repair attempt failed."
            ),
            "evidence_refs": ["review://run/reviewed"],
            "review_receipt_id": "review-receipt-reviewed",
            "scope": self.scope.to_record(),
        }
        cases = {
            "unreviewed status": dict(reviewed, status="observed", id="trajectory-pending"),
            "everos kind": dict(reviewed, kind="experience", id="trajectory-everos"),
            "unreviewed authority": dict(
                reviewed, authority="generated_skill", id="trajectory-generated"
            ),
            "missing receipt": {
                key: value
                for key, value in reviewed.items()
                if key != "review_receipt_id"
            },
            "foreign scope": dict(
                reviewed, scope=self.other_scope.to_record(), id="trajectory-foreign"
            ),
        }
        stub = StubEvidenceService([reviewed] + list(cases.values()))
        stores = self._stores(service=stub, registry=())
        evidence_store = self._by_kind(stores, "historical_evidence")
        candidates = list(evidence_store.query(self._payload("parser regression repair")))

        self.assertEqual(["trajectory-reviewed"], [item["logical_id"] for item in candidates])
        self.assertEqual("reviewed_failure", candidates[0]["payload"]["status"])
        # Only the accepted local service seam was used at all.
        self.assertEqual(1, len(stub.calls))
        self.assertEqual(self.scope, stub.calls[0][0])

    def test_incomparable_objective_identity_omits_local_candidates(self) -> None:
        self._reviewed_trajectory(
            self.scope,
            suffix="incomparable",
            task="Repair the parser regression",
            raw_evidence="The parser repair passed.",
        )
        stores = self._stores()
        canonical_payload = self._payload("parser regression repair")
        foreign_payload = dict(
            canonical_payload,
            representation={
                "model": "other-model/v2",
                "dimensions": 8,
                "metric": "euclidean",
                "sanitizer_version": "v9",
            },
        )
        evidence_store = self._by_kind(stores, "historical_evidence")
        template_store = self._by_kind(stores, "template")
        # Comparable evidence is delivered; nothing is established for a
        # foreign identity.
        self.assertEqual(1, len(list(evidence_store.query(canonical_payload))))
        self.assertEqual([], list(evidence_store.query(foreign_payload)))
        self.assertEqual([], list(template_store.query(foreign_payload)))

        # The local evidence representation cannot be established under a
        # foreign local identity either.
        foreign_limits = config.PreparationLimits(
            representation_model="other-model/v2",
            representation_dimensions=8,
            representation_metric="euclidean",
            representation_sanitizer_version="v9",
        )
        foreign_stores = self._stores(limits=foreign_limits)
        self.assertEqual(
            [],
            list(self._by_kind(foreign_stores, "historical_evidence").query(canonical_payload)),
        )

    # -- the five bundled template families --------------------------------

    def test_five_seeded_template_families_participate_and_are_ordered(self) -> None:
        template_store = self._by_kind(self._stores(), "template")
        identity = templates.representation_identity(limits=self.limits)
        for template_id, text in TEMPLATE_CASES.items():
            with self.subTest(template=template_id):
                payload = self._payload(text)
                candidates = list(template_store.query(payload))
                self.assertTrue(candidates, template_id)
                self.assertEqual(template_id, candidates[0]["logical_id"])
                self.assertEqual(template_id, candidates[0]["payload"]["template_id"])
                scores = [item["score"] for item in candidates]
                self.assertEqual(sorted(scores, reverse=True), scores)
                for item in candidates:
                    representation = dict(item["representation"])
                    for key, value in identity.items():
                        self.assertEqual(value, representation[key], key)
                    self.assertTrue(representation.get("declared"))
                    self.assertIn("ordinary", item["routes"])
                    self.assertEqual("v1", item["revision_id"])
                    self.assertGreaterEqual(
                        item["score"], self.limits.minimum_comparable_score
                    )
                    self.assertEqual(
                        item["payload_digest"], contracts.sha256_hex(item["payload"])
                    )
                # The emitted order repeats deterministically.
                self.assertEqual(
                    [item["logical_id"] for item in candidates],
                    [item["logical_id"] for item in template_store.query(payload)],
                )

        # A broader objective keeps the declared registry ranking exactly.
        broad_text = " ".join(
            keyword
            for template in templates.load_default_templates()
            for keyword in template.keywords
        )
        payload = self._payload(broad_text)
        objective = templates.objective_representation(
            broad_text, route="ordinary", limits=self.limits
        )
        matches = templates.rank_templates(
            objective, templates.load_default_templates(), limits=self.limits
        )
        expected = [match.template.template_id for match in matches if match.eligible]
        self.assertTrue(expected)
        self.assertEqual(
            expected, [item["logical_id"] for item in template_store.query(payload)]
        )

    def test_undeclared_incomparable_and_route_inapplicable_templates_are_absent(self) -> None:
        undeclared = templates.Template(
            template_id="undeclared-family/v1",
            version=1,
            family="Undeclared family",
            fixed_steps=("establish the request",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair", "parser"),
        )
        incomparable = templates.Template(
            template_id="incomparable-family/v1",
            version=2,
            family="Incomparable family",
            fixed_steps=("establish the request",),
            required_fields=("failure",),
            allowed_edits=("bindings",),
            verification_intent="intent",
            keywords=("regression", "failure", "test", "repair"),
            representation=(
                ("model", "other-model/v2"),
                ("dimensions", 8),
                ("metric", "euclidean"),
                ("sanitizer_version", "v9"),
            ),
        )
        registry = (undeclared, incomparable) + templates.load_default_templates()
        template_store = self._by_kind(self._stores(registry=registry), "template")

        candidates = list(template_store.query(self._payload("regression failure test repair")))
        ids = [item["logical_id"] for item in candidates]
        self.assertEqual("ordinary-regression-repair/v1", ids[0])
        self.assertNotIn("undeclared-family/v1", ids)
        self.assertNotIn("incomparable-family/v1", ids)
        self.assertEqual(
            ["ordinary-regression-repair/v1", "ordinary-integration-failure/v1"], ids
        )

        # Every bundled family declares the ordinary route only.
        self.assertEqual(
            [], list(template_store.query(self._payload("regression failure test repair", route="deeper")))
        )

    # -- direct PreparationService consumption -----------------------------

    def test_preparation_service_consumes_the_local_stores_directly(self) -> None:
        trajectory = self._reviewed_trajectory(
            self.scope,
            suffix="consumed",
            task="Repair the parser regression",
            raw_evidence="The parser repair passed and the focused parser check is green.",
        )
        stores = self._stores()
        card = contracts.make_task_card(task="Repair the parser regression", base_commit="base-1")
        outcome = self.service.prepare(
            task_card=card,
            plan=self._candidate_plan(),
            objective_id="parser-regression-repair",
            route="ordinary",
            stores=stores,
            root_replan=ROOT_REPLAN,
        )
        self.assertEqual("optional_memory", outcome.trace["outcome"])
        selected = outcome.selected_candidates
        evidence = [item for item in selected if item["kind"] == "historical_evidence"]
        self.assertEqual(1, len(evidence))
        self.assertEqual(trajectory["trajectory_id"], evidence[0]["logical_id"])
        self.assertEqual(trajectory["review_receipt_id"], evidence[0]["revision_id"])
        self.assertEqual(self.scope.to_record(), dict(evidence[0]["scope"]))
        self.assertEqual("reviewed_historical_evidence", evidence[0]["payload"]["authority"])
        self.assertEqual("reviewed_success", evidence[0]["payload"]["status"])
        self.assertNotIn(
            experience.SYNTHETIC_SECRET, contracts.canonical_json(evidence[0]).decode("utf-8")
        )

        templates_selected = [item for item in selected if item["kind"] == "template"]
        self.assertEqual(
            ["ordinary-regression-repair/v1"],
            [item["logical_id"] for item in templates_selected],
        )
        self.assertEqual([], [item for item in selected if item["kind"] == "procedure"])

        # The reviewed evidence never became guidance or the plan.
        self.assertEqual("fresh", outcome.disposition["branch"])
        self.assertEqual("fresh", outcome.plan["state"])
        self.assertIsNone(outcome.proposal)

        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        for store in stores:
            self.assertEqual("completed", attempts[store.store_id]["status"])

    def test_failed_local_lookup_is_isolated_and_the_template_store_delivers(self) -> None:
        stores = local_adapters.make_local_search_stores(
            experience_service=ExplodingEvidenceService(),
            scope=self.scope,
            registry=templates.load_default_templates(),
            limits=self.limits,
        )
        card = contracts.make_task_card(task="Repair the parser regression", base_commit="base-1")
        outcome = self.service.prepare(
            task_card=card,
            plan=self._candidate_plan(),
            objective_id="parser-regression-repair",
            route="ordinary",
            stores=stores,
            root_replan=ROOT_REPLAN,
        )
        attempts = {entry["store_id"]: entry for entry in outcome.trace["attempts"]}
        evidence_attempt = attempts["local-reviewed-experience"]
        self.assertEqual("unavailable", evidence_attempt["status"])
        self.assertIn("RuntimeError", evidence_attempt["reason"])
        self.assertEqual(
            [], [item for item in outcome.trace["candidates"] if item["kind"] == "historical_evidence"]
        )
        self.assertEqual(
            {"template"}, {item["kind"] for item in outcome.selected_candidates}
        )
        self.assertEqual("optional_memory", outcome.trace["outcome"])

    # -- the local read seam inside the bounded worker thread --------------

    def test_local_evidence_read_survives_the_bounded_worker_thread(self) -> None:
        # The accepted bounded search runs every store query on its own bounded
        # worker thread while the shared store handle stays thread-affine, so
        # the local reviewed-evidence read must succeed off its owning thread
        # without sharing that handle, without writing, and without weakening
        # the durable receipt validation the search depends on.

        trajectory = self._reviewed_trajectory(
            self.scope,
            suffix="thread",
            task="Repair the parser regression",
            raw_evidence="The parser repair passed and the focused check is green.",
        )
        shared_connection = self.memory_store.connection
        self.assertIsNotNone(shared_connection)
        schema_before = self._schema_snapshot()

        def read_off_thread() -> None:
            try:
                result["rows"] = self.memory_store.list_recent_reviewed_trajectories(
                    self.scope.to_record()
                )
            except BaseException as exc:  # the regression must surface, not hide
                result["error"] = exc

        result: dict = {}
        reader = threading.Thread(target=read_off_thread, name="step04-store-read")
        reader.start()
        reader.join()
        self.assertNotIn(
            "error", result, f"off-thread read failed: {result.get('error')!r}"
        )
        rows = result["rows"]
        self.assertEqual(
            [trajectory["trajectory_id"]], [row["trajectory_id"] for row in rows]
        )
        self.assertEqual(self.scope.to_record(), dict(rows[0]["scope"]))

        # The seam never shared the store handle and never relaxed its thread
        # affinity: a foreign thread still cannot use it directly.
        self.assertIs(shared_connection, self.memory_store.connection)

        def use_shared_handle() -> None:
            try:
                shared_connection.execute("SELECT 1").fetchall()
            except BaseException as exc:
                result["shared_error"] = exc

        probe = threading.Thread(target=use_shared_handle, name="step04-shared-probe")
        probe.start()
        probe.join()
        self.assertIsInstance(result.get("shared_error"), sqlite3.ProgrammingError)

        # The read seam writes nothing: no schema change and every write refused.
        self.assertEqual(schema_before, self._schema_snapshot())
        read_only = self.memory_store._open_read_only_connection()
        try:
            with self.assertRaises(sqlite3.OperationalError):
                read_only.execute("CREATE TABLE step04_read_seam_must_not_write (v TEXT)")
        finally:
            read_only.close()
        self.assertEqual(schema_before, self._schema_snapshot())

        # Validation is not weakened off-thread: without its durable receipt the
        # same lookup fails closed instead of delivering flattened narrative.
        writer = sqlite3.connect(str(self.memory_store.path))
        try:
            writer.execute(
                "DELETE FROM review_receipts WHERE review_receipt_id = ?",
                (trajectory["review_receipt_id"],),
            )
            writer.commit()
        finally:
            writer.close()
        failed: dict = {}

        def read_without_receipt() -> None:
            try:
                self.memory_store.list_recent_reviewed_trajectories(
                    self.scope.to_record()
                )
            except BaseException as exc:
                failed["error"] = exc

        strict = threading.Thread(target=read_without_receipt, name="step04-store-strict")
        strict.start()
        strict.join()
        self.assertIsInstance(failed.get("error"), store.TrajectoryConflictError)

    def test_relative_store_path_read_survives_a_changed_process_cwd(self) -> None:
        # initialize() always accepted relative database paths, so the
        # read-only seam must open the database initialize() opened, not
        # re-resolve the relative path against whatever cwd the bounded
        # worker thread happens to run in.
        original_cwd = Path.cwd()
        workdir = self.root / "relative-workdir"
        workdir.mkdir()
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        relative_store = store.MemoryStore("relative-state.sqlite3")
        try:
            os.chdir(workdir)
            relative_store.initialize()
            # The public path attribute keeps the caller's relative form.
            self.assertEqual(Path("relative-state.sqlite3"), relative_store.path)
            opened_database = workdir / "relative-state.sqlite3"
            self.assertTrue(opened_database.exists())
            trajectory = self._reviewed_trajectory(
                self.scope,
                suffix="relative-path",
                task="Repair the parser regression",
                raw_evidence=(
                    "The parser repair passed inside the relative database."
                ),
                memory_store=relative_store,
                experience_service=experience.ReviewedExperienceService(
                    relative_store, privacy_policy=self.policy
                ),
            )
            os.chdir(elsewhere)
            self.assertFalse((elsewhere / "relative-state.sqlite3").exists())

            result: dict = {}

            def read_off_thread() -> None:
                try:
                    result["rows"] = relative_store.list_recent_reviewed_trajectories(
                        self.scope.to_record()
                    )
                except BaseException as exc:  # the regression must surface
                    result["error"] = exc

            reader = threading.Thread(target=read_off_thread, name="step04-relative-read")
            reader.start()
            reader.join()
            self.assertNotIn(
                "error", result, f"off-thread read failed: {result.get('error')!r}"
            )
            rows = result["rows"]
            self.assertEqual(
                [trajectory["trajectory_id"]],
                [row["trajectory_id"] for row in rows],
            )
            self.assertEqual(self.scope.to_record(), dict(rows[0]["scope"]))
        finally:
            os.chdir(original_cwd)
            relative_store.close()

    def _schema_snapshot(self) -> list[tuple]:
        connection = sqlite3.connect(str(self.memory_store.path))
        try:
            return [
                tuple(row)
                for row in connection.execute(
                    "SELECT name, sql FROM sqlite_master ORDER BY name"
                )
            ]
        finally:
            connection.close()

    # -- the factory surface stays explicit --------------------------------

    def test_factory_requires_the_explicit_local_service_and_scope(self) -> None:
        with self.assertRaises(local_adapters.LocalAdapterError):
            local_adapters.make_local_search_stores(
                experience_service=object(), scope=self.scope
            )
        with self.assertRaises(local_adapters.LocalAdapterError):
            local_adapters.make_local_search_stores(
                experience_service=self.experience_service, scope=self.scope.to_record()
            )
        stores = self._stores()
        self.assertEqual(
            ["historical_evidence", "template"], [item.kind for item in stores]
        )
        self.assertEqual(
            ["local-reviewed-experience", "local-template-registry"],
            [item.store_id for item in stores],
        )


if __name__ == "__main__":
    unittest.main()
