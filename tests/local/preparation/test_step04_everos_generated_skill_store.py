"""STEP-04: the EverOS generated-skill procedure SearchStore.

These tests pin the one narrow slice that connects the accepted EverOS skill
query, the durable Step-02 candidate lookup, and the accepted local generated
procedure's trusted current-candidate path into the missing
``everos_generated_skill`` SearchStore input:

- one exact in-scope remote skill hit becomes the existing local procedure
  candidate only through its intact durable Step-02 candidate (stable skill
  id, sanitized content, exact source case ids, scope, and its sanitized
  stable metadata) and a currently eligible trusted Step-03 designation for
  the intended recipient: raw remote presence and the Step-02 approval alone
  deliver nothing, and only the query-dependent EverOS ``score`` may differ;
- changed, foreign, or uncurrent hits are omitted by themselves without
  suppressing an unrelated valid hit, a changed remote score never changes
  the source identity or boosts ranking, and one revision found locally and
  remotely stays one candidate with both discovery provenances under the
  accepted local-preference dedup policy;
- the factory declares ``requires_network=True`` with
  ``source_kind="everos_generated_skill"``, so disabled or restricted-local
  preparation begins no EverOS task-path call at all.

The accepted local issuer/recipient/predicate/revocation/representation
matrix stays in ``test_step04_local_procedure_trust.py`` and is deliberately
not repeated here; this file pins only the join this slice adds.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    config,
    contracts,
    everos_adapters,
    experience,
    local_procedure_adapters,
    preparation,
    privacy,
    procedures,
    store,
    templates,
)

ROOT_REPLAN = {
    "requested_by": "ROOT",
    "reason": "the tests exercise the explicit ROOT replan path",
}

SKILL_STORE_ID = "everos-generated-skills"
GENERATED_STORE_ID = "local-generated-procedures"


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
        self.skill_results: list[object] = []
        self.case_results: list[object] = []
        self.search_error: Exception | None = None

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.memorize_calls.append(dict(payload))
        return {"status": "extracted", "message_count": len(payload["messages"])}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, request: object) -> object:
        self.search_calls.append(dict(request))
        if self.search_error is not None:
            raise self.search_error
        return {
            "request_id": "fake-everos-skill-store",
            "data": {
                "agent_cases": list(self.case_results),
                "agent_skills": list(self.skill_results),
            },
        }


class Step04EverOSGeneratedSkillStoreTests(unittest.TestCase):
    """The remote generated-skill join, its isolation, and its call gate."""

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
        self.policy = privacy.PrivacyPolicy(
            known_secrets=(experience.SYNTHETIC_SECRET,)
        )
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.experience_service = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )
        self.procedure_service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}, privacy_policy=self.policy
        )
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product-a",
            namespace="everos-generated-skill-store",
            owner="root-agent",
        )
        self.receiver = self.scope.to_record()
        self.partition = {
            "scope": "project",
            "application": self.receiver["application"],
            "project": self.receiver["project"],
            "namespace": self.receiver["namespace"],
            "recipients": [dict(self.receiver)],
        }
        self.identity = templates.representation_identity(limits=self.limits)
        self.fake = _FakeEverOS()
        self.adapter = self._adapter(self.fake)
        self.card = contracts.make_task_card(
            task="Fix the parser lock regression in the focused test",
            base_commit="base-1",
        )
        self.candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="parser-lock-repair",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _adapter(self, fake) -> experience.EverOSAdapter:
        base_root = self.root / "everos"
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            base_root, self.scope
        )
        return experience.EverOSAdapter(
            scope=self.scope,
            base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                fake,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
            privacy_policy=self.policy,
        )

    def _approve(self, procedure: dict, *, approval_id: str) -> dict:
        approval = contracts.make_procedure_approval(
            approval_id=approval_id,
            procedure=procedure,
            issuer="ROOT",
            recipients=[dict(self.receiver)],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:01Z",
        )
        return self.procedure_service.record_approved_revision(procedure, approval)

    def _representation(self, procedure: dict, *, search_text: str) -> dict:
        return contracts.make_procedure_representation(
            procedure=procedure,
            model=self.identity["model"],
            dimensions=self.identity["dimensions"],
            metric=self.identity["metric"],
            sanitizer_version=self.identity["sanitizer_version"],
            search_text=search_text,
            vector=[0.01] * int(self.identity["dimensions"]),
            created_at="2026-09-21T00:00:02Z",
        )

    def _durable_source_case(self, suffix: str) -> dict:
        """Build one genuinely reviewed, durably receipted Step-02 source case.

        Retained generated provenance must rejoin real reviewed experience: a
        durable terminal outcome, review receipt, reviewed trajectory, a
        confirmed experience ingestion, and the exact case receipt of that
        trajectory.  Invented identifiers are never durable evidence.
        """

        plan = contracts.make_plan(
            plan_id=f"generated-source-plan-{suffix}",
            objective_id=f"generated-source-objective-{suffix}",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task=f"Review the generated source case {suffix}",
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
            evidence_digest=f"generated-source-evidence-{suffix}",
            linked_run_id=f"generated-source-run-{suffix}",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id=f"generated-source-review-{suffix}",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=(f"review://generated-source/{suffix}",),
            raw_evidence="The generated source behavior passed its focused check.",
        )
        self.memory_store.record_review_receipt(review)
        trajectory = contracts.make_reviewed_trajectory(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=self.receiver,
        )
        self.memory_store.record_reviewed_trajectory(trajectory)
        ingestion = contracts.make_experience_ingestion(
            trajectory=trajectory,
            destination="everos",
            session_id=f"generated-source-session-{suffix}",
            payload_digest=f"generated-source-payload-{suffix}",
        )
        self.memory_store.create_experience_ingestion(ingestion)
        case_receipt = contracts.make_case_receipt(
            trajectory=trajectory,
            ingestion=ingestion,
            source_case={
                "id": f"generated-source-case-{suffix}",
                "session_id": ingestion["session_id"],
                "agent_id": self.receiver["owner"],
                "app_id": self.receiver["application"],
                "project_id": self.receiver["project"],
            },
        )
        self.memory_store.confirm_experience_ingestion(
            ingestion["ingestion_id"], [case_receipt], expected_version=0
        )
        return {
            "case_id": case_receipt["case_id"],
            "case_receipt_id": case_receipt["case_receipt_id"],
            "trajectory_id": case_receipt["trajectory_id"],
            "review_receipt_id": case_receipt["review_receipt_id"],
            "review_receipt_digest": case_receipt["review_receipt_digest"],
        }

    def _generated_artifacts(
        self,
        suffix: str,
        *,
        content: str = "Inspect the generated parser lock evidence before recovery.",
        metadata: dict | None = None,
        designate: bool = True,
    ) -> tuple[dict, dict, dict]:
        """Create one Step-02 candidate, its approval, and its Step-03 revision."""

        candidate = contracts.make_generated_skill_candidate(
            scope=self.receiver,
            skill_id=f"generated-parser-lock-{suffix}",
            content=content,
            source_cases=[self._durable_source_case(suffix)],
            metadata=metadata
            if metadata is not None
            else {
                "name": "generated parser lock recovery",
                "description": "Inspect the local parser lock before the network path.",
                "confidence": 0.8,
                "maturity_score": 0.6,
            },
            created_at="2026-09-21T00:00:08Z",
        )
        self.memory_store.record_generated_skill_candidate(candidate)
        source_approval = contracts.make_skill_approval(
            approval_id=f"generated-source-approval-{suffix}",
            candidate=candidate,
            issuer="ROOT",
            recipients=(self.receiver["owner"],),
            approved_at="2026-09-21T00:00:09Z",
            authority_evidence={"policy_id": "step02-test/v1", "subject": "root"},
        )
        self.memory_store.record_skill_approval(source_approval)
        procedure = self.procedure_service.procedure_from_generated_skill(
            candidate_id=candidate["candidate_id"],
            skill_approval_id=source_approval["approval_id"],
            logical_name=f"generated-parser-lock-{suffix}",
            references=[
                {"id": "guide://generated", "content": "Generated source is historical."}
            ],
            predicates={
                "applicability": {},
                "conflicts": {},
                "capabilities": {},
                "routes": {
                    "all": [{"field": "route", "operator": "equals", "value": "ordinary"}]
                },
            },
        )
        approval = self._approve(procedure, approval_id=f"generated-approval-{suffix}")
        representation = self._representation(
            procedure,
            search_text="generated parser lock evidence recovery inspection",
        )
        self.procedure_service.record_representation(representation)
        if designate:
            self.procedure_service.designate(
                procedure=procedure,
                approval=approval,
                partition=self.partition,
                issuer="ROOT",
            )
        return candidate, source_approval, procedure

    def _skill(self, candidate: dict, **overrides: object) -> dict:
        """Return one deterministic remote skill hit matching a candidate."""

        skill: dict = {
            "id": candidate["skill_id"],
            "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "name": candidate["metadata"].get("name"),
            "description": candidate["metadata"].get("description"),
            "confidence": candidate["metadata"].get("confidence"),
            "maturity_score": candidate["metadata"].get("maturity_score"),
            "content": candidate["content"],
            "source_case_ids": [
                item["case_id"] for item in candidate["source_cases"]
            ],
            "score": 0.5,
        }
        skill.update(overrides)
        return skill

    def _remote_store(self, **overrides):
        arguments = {
            "experience_service": self.experience_service,
            "procedure_service": self.procedure_service,
            "memory_store": self.memory_store,
            "adapter": self.adapter,
            "scope": self.scope,
            "receiver": self.receiver,
            "facts": {"language": "python"},
            "route": "ordinary",
            "limits": self.limits,
        }
        arguments.update(overrides)
        return everos_adapters.make_everos_generated_skill_search_store(**arguments)

    def _local_stores(self):
        return local_procedure_adapters.make_local_procedure_search_stores(
            procedure_service=self.procedure_service,
            memory_store=self.memory_store,
            receiver=self.receiver,
            facts={"language": "python"},
            route="ordinary",
            limits=self.limits,
        )

    def _local_generated_store(self):
        return next(
            item for item in self._local_stores() if item.store_id == GENERATED_STORE_ID
        )

    def _payload(self, text: str = "parser lock regression focused inspection") -> dict:
        objective = templates.objective_representation(
            text, route="ordinary", limits=self.limits
        )
        return privacy.safe_query_payload(
            {
                "representation": {
                    key: objective[key]
                    for key in ("model", "dimensions", "metric", "sanitizer_version")
                },
                "tokens": list(objective["tokens"])[:32],
                "route": "ordinary",
            },
            self.policy,
        )

    def _prepare(self, stores, **overrides):
        arguments = {
            "task_card": self.card,
            "plan": self.candidate,
            "objective_id": "parser-lock-repair",
            "route": "ordinary",
            "stores": list(stores),
            "root_replan": ROOT_REPLAN,
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    @staticmethod
    def _attempts(outcome):
        return {entry["store_id"]: entry for entry in outcome.trace["attempts"]}

    def _procedures(self, outcome):
        return [item for item in outcome.trace["candidates"] if item["kind"] == "procedure"]

    def _selected(self, outcome):
        return [
            item for item in self._procedures(outcome) if item["disposition"] == "selected"
        ]

    def _skill_row_count(self) -> int:
        reader = sqlite3.connect(str(self.memory_store.path))
        try:
            return reader.execute(
                "SELECT COUNT(*) FROM generated_skill_candidates"
            ).fetchone()[0]
        finally:
            reader.close()

    # -- the valid joined hit ---------------------------------------------

    def test_valid_joined_hit_delivers_the_current_trusted_procedure_once(self) -> None:
        candidate, _source_approval, procedure = self._generated_artifacts("joined")
        self.fake.skill_results = [self._skill(candidate)]
        rows_before = self._skill_row_count()

        store_instance = self._remote_store()
        self.assertEqual(SKILL_STORE_ID, store_instance.store_id)
        self.assertEqual("procedure", store_instance.kind)
        self.assertTrue(store_instance.requires_network)
        self.assertEqual("everos_generated_skill", store_instance.source_kind)
        self.assertEqual(self.receiver, dict(store_instance.scope))
        self.assertEqual("project", store_instance.specificity)

        payload = self._payload()
        candidates = list(store_instance.query(payload))
        self.assertEqual(1, len(candidates))
        record = candidates[0]
        self.assertEqual(procedure["logical_id"], record["logical_id"])
        self.assertEqual(procedure["revision_id"], record["revision_id"])
        self.assertEqual(SKILL_STORE_ID, record["origin"])
        self.assertEqual(SKILL_STORE_ID, record["source_id"])
        self.assertEqual(self.receiver, dict(record["scope"]))
        self.assertEqual(record["payload_digest"], contracts.sha256_hex(record["payload"]))
        source = record["payload"]["source"]
        self.assertEqual("generated_skill", source["kind"])
        self.assertEqual(candidate["candidate_id"], source["candidate_id"])
        self.assertEqual(candidate["content_hash"], source["candidate_digest"])
        self.assertEqual(
            [dict(item) for item in candidate["source_cases"]],
            record["payload"]["source_cases"],
        )
        self.assertGreater(record["score"], 0.0)

        # The one real scoped query used the accepted public skill surface
        # with the sanitized bounded tokens and never appended a session token.
        self.assertEqual(1, len(self.fake.search_calls))
        request = self.fake.search_calls[-1]
        self.assertEqual(" ".join(payload["tokens"]), request["query"])
        self.assertEqual(self.adapter.everos_owner_id, request["agent_id"])
        self.assertEqual(self.adapter.everos_application_id, request["app_id"])
        self.assertEqual(self.adapter.everos_project_id, request["project_id"])
        self.assertNotIn("session_id", request)
        # No write of any kind: no memorize, no new durable candidate.
        self.assertEqual([], self.fake.memorize_calls)
        self.assertEqual(rows_before, self._skill_row_count())

        # The remote sighting never replaces local preference: prepared with
        # the local generated store, the duplicate revision stays one
        # candidate with both discovery provenances.
        local_store = self._local_generated_store()
        local_only = self._selected(self._prepare([local_store]))
        self.assertEqual(1, len(local_only))
        outcome = self._prepare([local_store, store_instance])
        selected = self._selected(outcome)
        self.assertEqual(1, len(selected))
        merged = selected[0]
        self.assertEqual(procedure["logical_id"], merged["logical_id"])
        self.assertEqual(procedure["revision_id"], merged["revision_id"])
        self.assertEqual(
            {GENERATED_STORE_ID, SKILL_STORE_ID},
            {item["store_id"] for item in merged["provenance"]},
        )
        self.assertEqual(GENERATED_STORE_ID, merged["origin"])
        self.assertEqual(local_only[0]["score"], merged["score"])
        self.assertEqual(local_only[0]["payload_digest"], merged["payload_digest"])

    # -- isolation of changed and foreign hits -----------------------------

    def test_changed_and_foreign_hits_are_omitted_beside_the_valid_hit(self) -> None:
        valid_candidate, _source_approval, procedure = self._generated_artifacts("valid")
        changed_candidate, _changed_approval, _changed_procedure = (
            self._generated_artifacts("changed")
        )

        self.fake.skill_results = [
            "not-an-object",
            # A foreign hit never enters this scope.
            self._skill(valid_candidate, agent_id=self.adapter.everos_owner_id + "-other"),
            # A changed sanitized content matches no durable candidate.
            self._skill(changed_candidate, content="A different stored body."),
            # A changed stable metadata field matches no durable candidate.
            self._skill(changed_candidate, name="a different stored name"),
            # A changed exact source case id set matches no durable candidate.
            self._skill(changed_candidate, source_case_ids=["case-other"]),
            self._skill(valid_candidate),
        ]

        candidates = list(self._remote_store().query(self._payload()))
        self.assertEqual(
            [procedure["logical_id"]], [item["logical_id"] for item in candidates]
        )
        self.assertEqual(1, len(self.fake.search_calls))
        # The changed revision's own Step-03 designation is untouched: the
        # local generated store still delivers it independently.
        local = self._local_generated_store()
        self.assertIn(
            changed_candidate["candidate_id"],
            {
                item["payload"]["source"]["candidate_id"]
                for item in local.query(self._payload())
            },
        )

    def test_missing_current_designation_delivers_nothing(self) -> None:
        candidate, _source_approval, procedure = self._generated_artifacts(
            "uncurrent", designate=False
        )
        self.fake.skill_results = [self._skill(candidate)]

        store_instance = self._remote_store()
        self.assertEqual([], list(store_instance.query(self._payload())))
        # The durable candidate and its Step-02 approval exist; without the
        # eligible current designation nothing is delivered.
        self.assertEqual(1, len(self.memory_store.read_generated_skill_candidates_for_scope(
            candidate["skill_id"], self.receiver
        )))
        outcome = self._prepare([store_instance])
        attempts = self._attempts(outcome)
        self.assertEqual("completed", attempts[SKILL_STORE_ID]["status"])
        self.assertEqual(0, attempts[SKILL_STORE_ID]["accepted"])
        self.assertEqual([], self._selected(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])
        self.assertNotIn(
            procedure["logical_id"],
            {item["logical_id"] for item in self._procedures(outcome)},
        )

    def test_changed_remote_score_never_changes_identity_or_ranking(self) -> None:
        candidate, _source_approval, procedure = self._generated_artifacts("score")

        self.fake.skill_results = [self._skill(candidate, score=0.05)]
        low = list(self._remote_store().query(self._payload()))
        self.fake.skill_results = [self._skill(candidate, score=0.99)]
        high = list(self._remote_store().query(self._payload()))

        self.assertEqual(1, len(low))
        self.assertEqual(1, len(high))
        self.assertEqual(low[0]["logical_id"], high[0]["logical_id"])
        self.assertEqual(low[0]["revision_id"], high[0]["revision_id"])
        self.assertEqual(low[0]["payload_digest"], high[0]["payload_digest"])
        # The ranking score is the accepted local representation score, not
        # the query-dependent EverOS score of either hit.
        local_candidate = self._local_generated_store().query(self._payload())[0]
        self.assertEqual(local_candidate["score"], low[0]["score"])
        self.assertEqual(local_candidate["score"], high[0]["score"])
        self.assertNotEqual(0.99, high[0]["score"])

    # -- the accepted call gate -------------------------------------------

    def test_gate_suppresses_the_remote_call_when_disabled_or_restricted(self) -> None:
        candidate, _source_approval, procedure = self._generated_artifacts("gate")
        self.fake.skill_results = [self._skill(candidate)]
        store_instance = self._remote_store()

        calls_before = len(self.fake.search_calls)
        outcome = self._prepare(
            [store_instance], request={"generated_skill_use": False}
        )
        attempt = self._attempts(outcome)[SKILL_STORE_ID]
        self.assertEqual("disabled", attempt["status"])
        self.assertEqual(calls_before, len(self.fake.search_calls))
        self.assertEqual([], self._selected(outcome))

        outcome = self._prepare(
            [store_instance],
            network_mode="restricted_local",
            request={"generated_skill_use": True},
            task_card=contracts.make_task_card(
                task=self.card["task"], base_commit="base-restricted"
            ),
        )
        attempt = self._attempts(outcome)[SKILL_STORE_ID]
        self.assertEqual("disabled", attempt["status"])
        self.assertIn("restricted-local", attempt["reason"])
        self.assertEqual(calls_before, len(self.fake.search_calls))
        self.assertEqual([], self._selected(outcome))

        # The admitted configuration begins exactly one real query and the
        # revision is delivered through the accepted local trust path.
        outcome = self._prepare(
            [store_instance], task_card=contracts.make_task_card(
                task=self.card["task"], base_commit="base-enabled"
            ),
        )
        attempt = self._attempts(outcome)[SKILL_STORE_ID]
        self.assertEqual("completed", attempt["status"])
        self.assertEqual(calls_before + 1, len(self.fake.search_calls))
        self.assertEqual(
            [procedure["logical_id"]],
            [item["logical_id"] for item in self._selected(outcome)],
        )

    # -- the factory surface stays explicit --------------------------------

    def test_factory_requires_the_accepted_explicit_inputs(self) -> None:
        class _NoPolicy:
            pass

        candidate, _source_approval, _procedure = self._generated_artifacts("factory")
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(experience_service=_NoPolicy())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(memory_store=object())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(adapter=object())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(scope=self.scope.to_record())
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(top_k=0)
        with self.assertRaises(everos_adapters.EverOSAdapterError):
            self._remote_store(procedure_service=object())
        store_instance = self._remote_store()
        self.assertEqual(SKILL_STORE_ID, store_instance.store_id)
        self.assertTrue(store_instance.requires_network)
        self.assertEqual("everos_generated_skill", store_instance.source_kind)
        self.assertEqual("live", store_instance.freshness)


if __name__ == "__main__":
    unittest.main()
