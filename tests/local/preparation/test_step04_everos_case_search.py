"""STEP-04: confirmed EverOS cases as a real bounded SearchStore input.

These tests pin the narrow integration slice that turns confirmed reviewed
EverOS cases into one real ``search.SearchStore`` input for
``preparation.PreparationService``, separate from the accepted local
recent/unrepresented evidence store:

- the smallest read-only scoped EverOS case query uses the accepted public
  service seam, the exact app/project/owner filters, and the exact bound root,
  and it never appends the ingestion-reconciliation receipt token;
- a remote hit becomes historical evidence only after its exact case id and
  sanitized source content rejoin a durable confirmed ingestion, case receipt,
  reviewed trajectory, and review receipt in the same scope;
- foreign, malformed, altered, unconfirmed, unreceipted, or unreviewed hits
  fail closed without discarding an unrelated eligible candidate, and no
  remote case becomes procedural guidance or the plan;
- ``experience_read`` and ``restricted_local`` suppress the actual task-path
  call while eligible local stores keep their chance, and the durable rejoin
  runs inside the bounded search worker thread on its own read-only
  connection instead of the thread-affine store handle.
"""

from __future__ import annotations

import asyncio
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
    everos_adapters,
    experience,
    local_adapters,
    preparation,
    privacy,
    store,
    templates,
)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value


class _FakeEverOS:
    """Deterministic public EverOS surface double; records every search call."""

    def __init__(self) -> None:
        self.memorize_calls: list[dict] = []
        self.search_calls: list[dict] = []
        self.case_results: list[object] = []
        self.skill_results: list[object] = []
        self.search_error: Exception | None = None
        self.response_override: object | None = None
        self.search_threads: list[int] = []

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.memorize_calls.append(dict(payload))
        return {"status": "extracted", "message_count": len(payload["messages"])}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, request: object) -> object:
        self.search_calls.append(dict(request))
        self.search_threads.append(threading.get_ident())
        if self.search_error is not None:
            raise self.search_error
        if self.response_override is not None:
            return self.response_override
        return {
            "request_id": "fake-everos-search",
            "data": {
                "agent_cases": list(self.case_results),
                "agent_skills": list(self.skill_results),
            },
        }

class Step04EverOSCaseSearchTests(unittest.TestCase):
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
            namespace="everos-case-search",
            owner="root-agent",
        )
        self.other_scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="another-namespace",
            owner="root-agent",
        )
        self.fake = _FakeEverOS()
        self.adapter = self._adapter(self.fake, scope=self.scope)
        self.card = contracts.make_task_card(
            task="Repair the parser regression", base_commit="base-1"
        )
        self.candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="parser-regression-repair",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _adapter(self, fake, *, scope=None, base_root=None) -> experience.EverOSAdapter:
        resolved_scope = self.scope if scope is None else scope
        resolved_root = (self.root / "everos") if base_root is None else base_root
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            resolved_root, resolved_scope
        )
        return experience.EverOSAdapter(
            scope=resolved_scope,
            base_root=resolved_root,
            surface=experience.EverOSPublicSurface.from_object(
                fake,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
            privacy_policy=self.policy,
        )

    def _capture(
        self,
        suffix: str,
        *,
        scope=None,
        task: str = "Repair the parser regression",
        raw_evidence: str = (
            "The parser repair passed after a discriminating local check."
        ),
    ) -> dict:
        scope = self.scope if scope is None else scope
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
        self.memory_store.record_decision(decision)
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
        self.memory_store.record_outcome(outcome)
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
        return self.experience_service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=scope,
        )


ROOT_REPLAN = {
    "requested_by": "ROOT",
    "reason": "the tests exercise the explicit ROOT replan path",
}


class RecordingExperienceService(experience.ReviewedExperienceService):
    """Record the exact durable case-join calls the remote store makes."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.evidence_calls: list[tuple[object, str]] = []
        self.evidence_threads: list[int] = []

    def confirmed_case_evidence(self, scope, source_case):
        self.evidence_calls.append((scope, source_case.get("id")))
        self.evidence_threads.append(threading.get_ident())
        return super().confirmed_case_evidence(scope, source_case)


class Step04EverOSCaseEvidenceTests(Step04EverOSCaseSearchTests):
    """The confirmed-case query, the durable rejoin, and the store gates."""

    # -- helpers -----------------------------------------------------------

    def _case(
        self,
        trajectory: dict,
        *,
        case_id: str = "case-root-1",
        session_id: object = None,
        **overrides: object,
    ) -> dict:
        """Return one deterministic remote case for a captured trajectory."""

        case: dict = {
            "id": case_id,
            "session_id": (
                self.adapter.session_id_for(trajectory["trajectory_id"])
                if session_id is None
                else session_id
            ),
            "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "task_intent": "repair the parser regression",
            "approach": "run one discriminating parser check before editing",
            "key_insight": "the local parser lock was held by the previous worker",
            # The accepted public surface returns SearchAgentCaseItem.score
            # on every hit; it is the ranking result of the query that
            # produced the hit, not part of the case's stable identity.
            "score": 0.5,
        }
        case.update(overrides)
        return case

    def _confirmed(
        self, suffix: str, *, case_id: str = "case-root-1", **capture: object
    ) -> tuple[dict, dict]:
        """Capture one trajectory and confirm one exact remote case for it."""

        trajectory = self._capture(suffix, **capture)
        case = self._case(trajectory, case_id=case_id)
        self.fake.case_results = [case]
        ingestion = asyncio.run(
            self.experience_service.extract_trajectory(
                trajectory["trajectory_id"], self.adapter
            )
        )
        self.assertEqual("confirmed", ingestion["status"])
        self.assertEqual([case_id], list(ingestion["case_ids"]))
        return trajectory, case

    def _remote_store(self, *, service=None, adapter=None, scope=None, **overrides):
        arguments = {
            "experience_service": service or self.experience_service,
            "adapter": adapter or self.adapter,
            "scope": scope or self.scope,
            "limits": self.limits,
        }
        arguments.update(overrides)
        return everos_adapters.make_everos_case_search_store(**arguments)

    def _payload(self, text: str, *, route: str = "ordinary") -> dict:
        objective = templates.objective_representation(
            text, route=route, limits=self.limits
        )
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

    def _local_stores(self, *, scope=None):
        return local_adapters.make_local_search_stores(
            experience_service=self.experience_service,
            scope=scope or self.scope,
            registry=(),
            limits=self.limits,
        )

    def _prepare(self, stores, **overrides):
        arguments = {
            "task_card": self.card,
            "plan": self.candidate,
            "objective_id": "parser-regression-repair",
            "route": "ordinary",
            "stores": list(stores),
            "root_replan": ROOT_REPLAN,
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    @staticmethod
    def _attempts(outcome):
        return {entry["store_id"]: entry for entry in outcome.trace["attempts"]}

    @staticmethod
    def _evidence_candidates(outcome):
        return [
            item
            for item in outcome.trace["candidates"]
            if item["kind"] == "historical_evidence"
        ]

    def _mutate(self, statement: str, parameters: tuple) -> None:
        writer = sqlite3.connect(str(self.memory_store.path))
        try:
            writer.execute(statement, parameters)
            writer.commit()
        finally:
            writer.close()

    # -- the smallest read-only scoped case query --------------------------

    def test_scoped_case_query_rejoins_one_confirmed_reviewed_case(self) -> None:
        trajectory, case = self._confirmed(
            "joined",
            raw_evidence=(
                "The parser repair passed after a discriminating local check. "
                f"Keep {experience.SYNTHETIC_SECRET} inside protected evidence."
            ),
        )
        queries_before = len(self.fake.search_calls)

        store = self._remote_store()
        self.assertEqual("everos-reviewed-cases", store.store_id)
        self.assertEqual("historical_evidence", store.kind)
        self.assertTrue(store.requires_network)
        self.assertEqual(self.scope.to_record(), dict(store.scope))

        payload = self._payload("parser regression repair")
        candidates = list(store.query(payload))
        self.assertEqual(1, len(candidates))
        candidate = candidates[0]
        self.assertEqual(trajectory["trajectory_id"], candidate["logical_id"])
        self.assertEqual(trajectory["review_receipt_id"], candidate["revision_id"])
        self.assertEqual(self.scope.to_record(), dict(candidate["scope"]))

        body = candidate["payload"]
        self.assertEqual("historical_evidence", body["kind"])
        self.assertEqual("reviewed_historical_evidence", body["authority"])
        self.assertEqual("reviewed_success", body["status"])
        self.assertEqual(trajectory["review_receipt_id"], body["review_receipt_id"])
        self.assertEqual(case["id"], body["case_id"])
        self.assertEqual(self.scope.to_record(), dict(body["scope"]))
        self.assertEqual(list(trajectory["evidence_refs"]), list(body["evidence_refs"]))
        self.assertEqual(candidate["payload_digest"], contracts.sha256_hex(body))

        # The remote case never becomes guidance, a plan, or raw evidence.
        serialized = contracts.canonical_json(candidate).decode("utf-8")
        self.assertNotIn(experience.SYNTHETIC_SECRET, serialized)
        self.assertNotIn("evidence://protected/parser-log", serialized)
        self.assertNotIn("protected_source_refs", serialized)
        self.assertNotIn("raw_evidence", serialized)
        for guidance in ("fixed_steps", "steps", "bindings", "plan_id"):
            self.assertNotIn(guidance, body)

        # The comparable representation is the exact central identity.
        identity = templates.representation_identity(limits=self.limits)
        representation = dict(candidate["representation"])
        for key, value in identity.items():
            self.assertEqual(value, representation[key], key)
        self.assertTrue(representation.get("declared"))
        self.assertTrue(representation["tokens"])
        self.assertGreater(candidate["score"], 0.0)

        # The real scoped query used the accepted public search surface with
        # the exact app/project/owner filters and never appended the
        # ingestion-reconciliation receipt token.
        self.assertEqual(queries_before + 1, len(self.fake.search_calls))
        request = self.fake.search_calls[-1]
        self.assertEqual(" ".join(payload["tokens"]), request["query"])
        self.assertNotIn(trajectory["trajectory_id"], str(request["query"]))
        self.assertEqual(self.adapter.everos_owner_id, request["agent_id"])
        self.assertEqual(
            self.adapter.everos_application_id, request["app_id"]
        )
        self.assertEqual(self.adapter.everos_project_id, request["project_id"])
        self.assertNotIn("session_id", request)

        # The accepted local recent/unrepresented store is a separate input:
        # this confirmed trajectory is only discoverable through the remote
        # case seam.
        local_evidence = [
            item
            for item in self._local_stores()[0].query(payload)
        ]
        self.assertEqual([], local_evidence)

    def test_case_query_appends_no_session_token_and_omits_a_cross_session_receipt(self) -> None:
        # Discovery itself carries no session token: the accepted reconcile
        # search appends the ingestion session to its own query text, while
        # this read-only case query keeps the sanitized bounded tokens only.
        # A confirmed case whose durable source-case session equals its
        # confirmed ingestion session therefore stays discoverable here, and a
        # receipt whose source session differs is omitted instead of promoted.
        trajectory = self._capture("session-token")
        ingestion = contracts.make_experience_ingestion(
            trajectory=trajectory,
            destination=self.adapter.destination,
            session_id=self.adapter.session_id_for(trajectory["trajectory_id"]),
            payload_digest=contracts.sha256_hex({"trajectory": trajectory["trajectory_id"]}),
        )
        self.memory_store.create_experience_ingestion(ingestion)
        case = self._case(trajectory, case_id="case-session-valid")
        receipt = contracts.make_case_receipt(
            trajectory=trajectory,
            ingestion=ingestion,
            source_case=case,
        )
        self.memory_store.confirm_experience_ingestion(
            ingestion["ingestion_id"], [receipt], expected_version=0
        )
        self.assertEqual(ingestion["session_id"], case["session_id"])

        self.fake.case_results = [case]
        searches_before = len(self.fake.search_calls)
        payload = self._payload("parser regression repair")
        candidates = list(self._remote_store().query(payload))
        self.assertEqual(
            ["case-session-valid"], [item["payload"]["case_id"] for item in candidates]
        )
        self.assertEqual(
            [trajectory["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

        # The one discovery request used the sanitized bounded tokens and never
        # appended the ingestion-reconciliation session token.
        self.assertEqual(searches_before + 1, len(self.fake.search_calls))
        request = self.fake.search_calls[-1]
        self.assertEqual(" ".join(payload["tokens"]), request["query"])
        self.assertNotIn("session_id", request)
        self.assertNotIn(ingestion["session_id"], str(request["query"]))

        # A direct public store confirmation can still write a receipt whose
        # durable source-case session differs from its confirmed ingestion
        # session.  The final durable join re-establishes the exact session
        # provenance the accepted reconcile path required before confirmation,
        # so that hit is omitted instead of promoted, and the unrelated
        # eligible case above is untouched.
        cross = self._capture("cross-session")
        cross_ingestion = contracts.make_experience_ingestion(
            trajectory=cross,
            destination=self.adapter.destination,
            session_id=self.adapter.session_id_for(cross["trajectory_id"]),
            payload_digest=contracts.sha256_hex({"trajectory": cross["trajectory_id"]}),
        )
        self.memory_store.create_experience_ingestion(cross_ingestion)
        cross_case = self._case(
            cross, case_id="case-cross-session", session_id="another-session"
        )
        cross_receipt = contracts.make_case_receipt(
            trajectory=cross,
            ingestion=cross_ingestion,
            source_case=cross_case,
        )
        self.memory_store.confirm_experience_ingestion(
            cross_ingestion["ingestion_id"], [cross_receipt], expected_version=0
        )
        self.assertNotEqual(cross_ingestion["session_id"], cross_case["session_id"])

        with self.assertRaises(store.StoreError):
            self.memory_store.read_confirmed_case_join(
                self.scope.to_record(), cross_case["id"]
            )

        self.fake.case_results = [cross_case, case]
        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            ["case-session-valid"], [item["payload"]["case_id"] for item in candidates]
        )
        self.assertEqual(
            [trajectory["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

    def test_query_dependent_score_change_keeps_the_unchanged_confirmed_case(self) -> None:
        # EverOS returns SearchAgentCaseItem.score on every hit, and that value
        # is the ranking result of the query that produced it.  STEP-02
        # confirmation and this preparation search use different queries, so
        # the same unchanged confirmed case comes back with another score: only
        # that query-dependent ranking metadata may be ignored, and the stable
        # sanitized case identity/content must still match exactly.
        trajectory, case = self._confirmed("score-change", case_id="case-score-change")
        rescored = dict(case, score=float(case["score"]) + 0.25)
        self.assertNotEqual(case["score"], rescored["score"])
        self.fake.case_results = [rescored]

        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            ["case-score-change"], [item["payload"]["case_id"] for item in candidates]
        )
        self.assertEqual(
            [trajectory["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

        # A changed stable field is still rejected even while the score also
        # moves: ignoring ranking metadata never widens case authority.
        self.fake.case_results = [dict(rescored, approach="a different stored approach")]
        self.assertEqual(
            [], list(self._remote_store().query(self._payload("parser regression repair")))
        )

    # -- the durable receipt join fails closed -----------------------------

    def test_missing_case_receipt_is_omitted_and_the_intact_case_delivers(self) -> None:
        intact, intact_case = self._confirmed("intact", case_id="case-intact")
        removed, removed_case = self._confirmed("removed", case_id="case-removed")
        self.fake.case_results = [removed_case, intact_case]

        self._mutate(
            "DELETE FROM experience_case_receipts WHERE case_id = ?", (removed_case["id"],)
        )
        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            [intact["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

    def test_altered_stored_source_content_is_omitted(self) -> None:
        intact, intact_case = self._confirmed("source-intact", case_id="case-source-intact")
        altered, altered_case = self._confirmed("source-altered", case_id="case-source-altered")
        self.fake.case_results = [altered_case, intact_case]

        altered_source = dict(altered_case, key_insight="a different stored insight")
        self._mutate(
            "UPDATE experience_case_receipts SET source_case = ?, source_case_digest = ? WHERE case_id = ?",
            (
                contracts.canonical_json(altered_source).decode("utf-8"),
                contracts.sha256_hex(altered_source),
                altered_case["id"],
            ),
        )
        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            [intact["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

    def test_changed_review_receipt_binding_is_omitted(self) -> None:
        intact, intact_case = self._confirmed("binding-intact", case_id="case-binding-intact")
        changed, changed_case = self._confirmed("binding-changed", case_id="case-binding-changed")
        self.fake.case_results = [changed_case, intact_case]

        self._mutate(
            "UPDATE experience_case_receipts SET review_receipt_digest = ? WHERE case_id = ?",
            ("stale-review-receipt-digest", changed_case["id"]),
        )
        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            [intact["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

    def test_unconfirmed_ingestion_is_omitted(self) -> None:
        trajectory = self._capture("unconfirmed")
        ingestion = contracts.make_experience_ingestion(
            trajectory=trajectory,
            destination=self.adapter.destination,
            session_id=self.adapter.session_id_for(trajectory["trajectory_id"]),
            payload_digest=contracts.sha256_hex({"trajectory": trajectory["trajectory_id"]}),
        )
        self.memory_store.create_experience_ingestion(ingestion)
        case = self._case(trajectory, case_id="case-unconfirmed")
        receipt = contracts.make_case_receipt(
            trajectory=trajectory, ingestion=ingestion, source_case=case
        )
        writer = sqlite3.connect(str(self.memory_store.path))
        try:
            writer.execute(
                """
                INSERT INTO experience_case_receipts (
                    case_receipt_id, case_id, trajectory_id, ingestion_id,
                    scope_digest, scope, review_receipt_id, review_receipt_digest,
                    source_case, source_case_digest, created_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt["case_receipt_id"],
                    receipt["case_id"],
                    receipt["trajectory_id"],
                    receipt["ingestion_id"],
                    receipt["scope_digest"],
                    contracts.canonical_json(receipt["scope"]).decode("utf-8"),
                    receipt["review_receipt_id"],
                    receipt["review_receipt_digest"],
                    contracts.canonical_json(receipt["source_case"]).decode("utf-8"),
                    receipt["source_case_digest"],
                    receipt["created_at"],
                    receipt["content_hash"],
                ),
            )
            writer.commit()
        finally:
            writer.close()

        self.fake.case_results = [case]
        self.assertEqual([], list(self._remote_store().query(self._payload("parser regression repair"))))
        # The pending ingestion never became a confirmed receipt.
        self.assertEqual(
            "pending",
            self.memory_store.get_experience_ingestion(ingestion["ingestion_id"])["status"],
        )

    def test_unreviewed_durable_trajectory_is_omitted(self) -> None:
        intact, intact_case = self._confirmed("reviewed-intact", case_id="case-reviewed-intact")
        unreviewed, unreviewed_case = self._confirmed("unreviewed", case_id="case-unreviewed")
        self.fake.case_results = [unreviewed_case, intact_case]

        self._mutate(
            "UPDATE reviewed_trajectories SET status = ? WHERE trajectory_id = ?",
            ("observed", unreviewed["trajectory_id"]),
        )
        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            [intact["trajectory_id"]], [item["logical_id"] for item in candidates]
        )

    def test_foreign_or_malformed_remote_cases_are_omitted(self) -> None:
        trajectory, case = self._confirmed("mixed", case_id="case-in-scope")
        self.fake.case_results = [
            "not-an-object",
            {"id": "case-no-scope"},
            dict(case, id="case-foreign-owner", agent_id=self.adapter.everos_owner_id + "-other"),
            dict(case, id="case-foreign-app", app_id="other-app"),
            dict(case, id="case-foreign-project", project_id="other-project"),
            dict(case, id="case-no-id", ),
            case,
        ]
        candidates = list(self._remote_store().query(self._payload("parser regression repair")))
        self.assertEqual(
            [trajectory["trajectory_id"]], [item["logical_id"] for item in candidates]
        )
        self.assertEqual([case["id"]], [item["payload"]["case_id"] for item in candidates])

    def test_root_rebinding_fails_closed_without_discarding_the_local_candidate(self) -> None:
        self._confirmed("root", case_id="case-root")
        local = self._capture("root-unrepresented")
        holder = {"value": self.adapter.memory_root}
        surface = experience.EverOSPublicSurface.from_object(
            self.fake,
            memory_root=self.adapter.memory_root,
            resolve_memory_root=lambda: holder["value"],
        )
        rebound = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.adapter.base_root,
            surface=surface,
            privacy_policy=self.policy,
        )
        holder["value"] = self.root / "everos-elsewhere"
        searches_before = len(self.fake.search_calls)
        store = self._remote_store(adapter=rebound)

        with self.assertRaises(experience.ScopeBoundaryError):
            list(store.query(self._payload("parser regression repair")))

        outcome = self._prepare([store] + list(self._local_stores()))
        attempts = self._attempts(outcome)
        self.assertEqual("unavailable", attempts["everos-reviewed-cases"]["status"])
        self.assertEqual(searches_before, len(self.fake.search_calls))
        self.assertEqual("completed", attempts["local-reviewed-experience"]["status"])
        self.assertEqual(
            [local["trajectory_id"]],
            [item["logical_id"] for item in self._evidence_candidates(outcome)],
        )

    # -- the feature and network gates -------------------------------------

    def test_experience_read_disabled_suppresses_the_remote_call(self) -> None:
        self._confirmed("gate-read", case_id="case-gate-read")
        searches_before = len(self.fake.search_calls)
        template_store = local_adapters.make_local_search_stores(
            experience_service=self.experience_service,
            scope=self.scope,
            registry=templates.load_default_templates(),
            limits=self.limits,
        )[1]
        outcome = self._prepare(
            [self._remote_store(), template_store],
            request={"experience_read": False, "template_memory": True},
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts["everos-reviewed-cases"]["status"])
        self.assertEqual(searches_before, len(self.fake.search_calls))
        self.assertEqual([], self._evidence_candidates(outcome))
        self.assertEqual("completed", attempts["local-template-registry"]["status"])
        self.assertTrue(
            [item for item in outcome.selected_candidates if item["kind"] == "template"]
        )

    def test_restricted_local_never_begins_the_remote_call(self) -> None:
        self._confirmed("gate-local", case_id="case-gate-local")
        searches_before = len(self.fake.search_calls)
        template_store = local_adapters.make_local_search_stores(
            experience_service=self.experience_service,
            scope=self.scope,
            registry=templates.load_default_templates(),
            limits=self.limits,
        )[1]
        outcome = self._prepare(
            [self._remote_store(), template_store],
            network_mode="restricted_local",
            request={"experience_read": True, "template_memory": True},
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts["everos-reviewed-cases"]["status"])
        self.assertIn("restricted-local", attempts["everos-reviewed-cases"]["reason"])
        self.assertEqual(searches_before, len(self.fake.search_calls))
        self.assertEqual([], self._evidence_candidates(outcome))
        self.assertEqual("completed", attempts["local-template-registry"]["status"])

    # -- the bounded worker thread and failure isolation -------------------

    def test_remote_case_read_runs_in_the_bounded_worker_thread(self) -> None:
        trajectory, case = self._confirmed("thread", case_id="case-thread")
        self.fake.case_results = [case]
        self.fake.search_threads.clear()
        self.experience_service.evidence_threads.clear()
        caller = threading.get_ident()

        outcome = self._prepare([self._remote_store()] + list(self._local_stores()))
        evidence = self._evidence_candidates(outcome)
        self.assertEqual([trajectory["trajectory_id"]], [item["logical_id"] for item in evidence])

        # The real public search and the exact durable rejoin both ran on the
        # bounded worker thread, never on the caller thread.
        self.assertTrue(self.fake.search_threads)
        self.assertNotIn(caller, self.fake.search_threads)
        self.assertTrue(self.experience_service.evidence_threads)
        self.assertNotIn(caller, self.experience_service.evidence_threads)

    def test_remote_store_failure_is_isolated_from_the_local_candidate(self) -> None:
        self._confirmed("failure", case_id="case-failure")
        local = self._capture("failure-unrepresented")
        self.fake.search_error = RuntimeError("everos search unavailable")
        outcome = self._prepare([self._remote_store()] + list(self._local_stores()))
        attempts = self._attempts(outcome)
        self.assertEqual("unavailable", attempts["everos-reviewed-cases"]["status"])
        self.assertIn("RuntimeError", attempts["everos-reviewed-cases"]["reason"])
        self.assertEqual("completed", attempts["local-reviewed-experience"]["status"])
        self.assertEqual(
            [local["trajectory_id"]],
            [item["logical_id"] for item in self._evidence_candidates(outcome)],
        )

        self.fake.search_error = None
        self.fake.response_override = {"request_id": "malformed"}
        malformed = self._prepare([self._remote_store()] + list(self._local_stores()))
        self.assertEqual(
            "unavailable", self._attempts(malformed)["everos-reviewed-cases"]["status"]
        )
        self.assertEqual(
            [local["trajectory_id"]],
            [item["logical_id"] for item in self._evidence_candidates(malformed)],
        )

    def test_incomparable_objective_identity_never_begins_the_remote_call(self) -> None:
        self._confirmed("identity", case_id="case-identity")
        searches_before = len(self.fake.search_calls)
        payload = dict(
            self._payload("parser regression repair"),
            representation={
                "model": "other-model/v2",
                "dimensions": 8,
                "metric": "euclidean",
                "sanitizer_version": "v9",
            },
        )
        self.assertEqual([], list(self._remote_store().query(payload)))
        self.assertEqual(searches_before, len(self.fake.search_calls))

    # -- the factory surface stays explicit --------------------------------

    def test_factory_requires_explicit_service_adapter_and_scope(self) -> None:
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(service=object())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(adapter=object())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(scope=self.scope.to_record())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(adapter=self._adapter(self.fake, scope=self.other_scope))
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(top_k=0)
        store = self._remote_store()
        self.assertEqual("everos-reviewed-cases", store.store_id)
        self.assertEqual("historical_evidence", store.kind)
        self.assertTrue(store.requires_network)
        self.assertEqual("live", store.freshness)


if __name__ == "__main__":
    unittest.main()