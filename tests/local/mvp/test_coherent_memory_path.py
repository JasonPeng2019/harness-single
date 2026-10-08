"""The supported Standard path composes both trusted procedure sources."""

from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    atlas,
    atlas_adapters,
    config,
    context,
    contracts,
    everos_adapters,
    preparation,
)
from tests.local.preparation import (
    test_step04_atlas_search_adapters as atlas_fixture,
    test_step04_everos_generated_skill_store as everos_fixture,
)


EVEROS_MARKER = "EVEROS_MVP_MARKER=inspect-local-lock"
ATLAS_MARKER = "ATLAS_MVP_MARKER=verify-release-order"
RAW_APPROVAL_MATERIAL = "RAW_APPROVAL_EVIDENCE_DO_NOT_DELIVER"
RECEIVER = "worker:lane-1"


class _CoherentFixture:
    """Compose accepted provider fixtures in one durable product store."""

    def setUp(self) -> None:
        self.everos = everos_fixture.Step04EverOSGeneratedSkillStoreTests()
        self.everos.setUp()
        self.addCleanup(self.everos.tearDown)
        self.receiver = self.everos.receiver
        self.memory_store = self.everos.memory_store
        self.service = self.everos.service
        self.limits = self.everos.limits

        skill, _skill_approval, self.everos_procedure = (
            self.everos._generated_artifacts(
                "coherent-mvp",
                content=f"Inspect the generated parser lock evidence. {EVEROS_MARKER}",
            )
        )
        self.everos.fake.skill_results = [self.everos._skill(skill)]
        self.everos_store = everos_adapters.make_everos_generated_skill_search_store(
            experience_service=self.everos.experience_service,
            procedure_service=self.everos.procedure_service,
            memory_store=self.memory_store,
            adapter=self.everos.adapter,
            scope=self.everos.scope,
            receiver=self.receiver,
            facts={"language": "python"},
            route="ordinary",
            limits=self.limits,
        )

        self.atlas_procedure = contracts.make_procedure_revision(
            logical_name="atlas-coherent-mvp",
            origin="curated",
            origin_scope=self.receiver,
            body=f"Verify the release order after the repair. {ATLAS_MARKER}",
            references=[{"id": "guide://release", "content": "Check the release order."}],
            predicates={
                "applicability": {"all": [
                    {"field": "language", "operator": "equals", "value": "python"}
                ]},
                "conflicts": {}, "capabilities": {},
                "routes": {"all": [
                    {"field": "route", "operator": "equals", "value": "ordinary"}
                ]},
            },
            source={"kind": "curated_authoring", "provenance_ref": "curation://mvp"},
            created_at="2026-09-21T00:00:00Z",
        )
        self.atlas_approval = contracts.make_procedure_approval(
            approval_id="atlas-coherent-approval",
            procedure=self.atlas_procedure,
            issuer="ROOT",
            recipients=[self.receiver],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root",
                                "review_note": RAW_APPROVAL_MATERIAL},
            approved_at="2026-09-21T00:00:01Z",
        )
        owner = self.everos.procedure_service
        owner.record_approved_revision(self.atlas_procedure, self.atlas_approval)
        representation = self.everos._representation(
            self.atlas_procedure,
            search_text="parser lock regression release order verification",
        )
        owner.record_representation(representation)
        designation = owner.designate(
            procedure=self.atlas_procedure,
            approval=self.atlas_approval,
            partition=self.everos.partition,
            issuer="ROOT",
        )
        self.collection = atlas_fixture._Collection()
        self.vector_store = atlas_fixture._VectorStore()
        self.atlas_adapter = atlas.AtlasProcedureAdapter(
            collection=self.collection, vector_store=self.vector_store,
        )
        owner.publish_designation(designation, self.atlas_adapter)
        self.publication = owner.publish(
            procedure=self.atlas_procedure,
            approval=self.atlas_approval,
            representation=representation,
            designation=designation,
            adapter=self.atlas_adapter,
        )
        self.vector_store.publication_ids.append(self.publication["publication_id"])
        self.atlas_store = atlas_adapters.make_atlas_search_store(
            procedure_service=owner,
            adapter=self.atlas_adapter,
            receiver=self.receiver,
            facts={"language": "python"},
            route="ordinary",
            limits=self.limits,
        )

        self.plan = contracts.make_plan(
            plan_id="coherent-accepted-plan",
            objective_id="parser-lock-repair",
            route="ordinary",
            state="accepted",
            accepted_by="ROOT",
            content={"steps": ["inspect lock", "repair parser", "verify release"]},
            source={"kind": "selected_memory", "dependencies": [
                {"logical_id": procedure["logical_id"],
                 "revision_id": procedure["revision_id"]}
                for procedure in (self.everos_procedure, self.atlas_procedure)
            ]},
            created_at="2026-09-21T00:00:03Z",
        )
        self.card = contracts.make_task_card(
            task="Repair the parser lock regression and verify release order",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id=self.plan["objective_id"],
                route="ordinary", plan=self.plan, checkpoint="checkpoint-1",
            ),
        )

    def prepare(self, *, finalize: bool = False):
        arguments = {
            "task_card": self.card,
            "plan": self.plan,
            "objective_id": self.plan["objective_id"],
            "route": "ordinary",
            "stores": [self.everos_store, self.atlas_store],
            "request": {"strategy": "standard", "generated_skill_use": True,
                        "atlas_shared_retrieval": True},
        }
        if finalize:
            arguments.update(
                finalize=True,
                lane_id="lane-1", run_id="run-1",
                worktree_path=str(self.everos.root), base_commit="base-1",
                checkpoint="checkpoint-1", execution_role="worker",
                invocation_target="harness:worker", recipient=RECEIVER,
                mandatory_content=[
                    {"id": "task", "kind": "task", "content": self.card["task"]},
                    {"id": "accepted-plan", "kind": "accepted-plan",
                     "content": self.plan["content"]},
                    {"id": "base", "kind": "base", "content": "base-1"},
                    {"id": "route", "kind": "route", "content": "ordinary"},
                    {"id": "checkpoint", "kind": "checkpoint",
                     "content": "checkpoint-1"},
                    {"id": "security", "kind": "security",
                     "content": context.ROLE_SEPARATION},
                ],
            )
        return self.service.prepare(**arguments)


class CoherentPreparationTests(_CoherentFixture, unittest.TestCase):
    def test_standard_selects_everos_and_atlas_in_one_trace(self) -> None:
        result = self.prepare()

        self.assertEqual("standard", result.preparation["strategy"])
        self.assertEqual("preserved_accepted", result.disposition["branch"])
        self.assertEqual("optional_memory", result.trace["outcome"])
        self.assertEqual(1, result.trace["rounds"])
        self.assertEqual(
            {"everos-generated-skills", "atlas-shared-procedures"},
            {entry["store_id"] for entry in result.trace["attempts"]
             if entry["status"] == "completed"},
        )
        self.assertEqual(1, len(self.everos.fake.search_calls))
        self.assertEqual(1, len(self.vector_store.calls))
        self.assertEqual(2, len(result.selected_candidates))
        by_source = {item["source_id"]: item for item in result.selected_candidates}
        self.assertEqual({"everos-generated-skills", "atlas-shared-procedures"},
                         set(by_source))
        for source, procedure, marker in (
            ("everos-generated-skills", self.everos_procedure, EVEROS_MARKER),
            ("atlas-shared-procedures", self.atlas_procedure, ATLAS_MARKER),
        ):
            with self.subTest(source=source):
                item = by_source[source]
                self.assertEqual("procedure", item["kind"])
                self.assertEqual(procedure["logical_id"], item["logical_id"])
                self.assertEqual(procedure["revision_id"], item["revision_id"])
                self.assertEqual(self.receiver, item["scope"])
                self.assertEqual("live", item["freshness"])
                self.assertEqual(contracts.sha256_hex(item["payload"]),
                                 item["payload_digest"])
                self.assertEqual(procedure["content_hash"],
                                 item["payload"]["procedure"]["content_hash"])
                self.assertEqual(self.receiver, item["payload"]["recipient"])
                self.assertIn(marker, item["payload"]["procedure"]["behavior"]["body"])
                self.assertEqual(source, item["provenance"][0]["store_id"])
        self.assertEqual("generated_skill", by_source["everos-generated-skills"]
                         ["payload"]["source"]["kind"])
        self.assertEqual("atlas_trusted_procedure",
                         by_source["atlas-shared-procedures"]["payload"]["authority"])
        self.assertEqual(self.publication["publication_id"],
                         by_source["atlas-shared-procedures"]["payload"]["publication_id"])


class CoherentFinalContextTests(_CoherentFixture, unittest.TestCase):
    def test_both_trusted_inputs_reach_exact_safe_worker_context(self) -> None:
        result = self.prepare(finalize=True)

        self.assertTrue(result.dispatchable)
        self.assertEqual(2, len(result.selected_candidates))
        self.assertEqual(2, len(self.everos.fake.search_calls))
        self.assertEqual(2, len(self.vector_store.calls))
        restored = self.memory_store.get_final_context_for_decision(
            result.decision["decision_id"]
        )
        self.assertEqual(result.context, restored)
        context.validate_final_context(
            restored, envelope=result.envelope, task_card=self.card,
            plan=self.plan, lane_id="lane-1", run_id="run-1",
            worktree_path=str(self.everos.root), base_commit="base-1",
            checkpoint="checkpoint-1", execution_role="worker",
            invocation_target="harness:worker", recipient=RECEIVER,
        )
        self.assertEqual({"everos-generated-skills", "atlas-shared-procedures"},
                         {item["source_id"] for item in restored["optional_content"]})
        selected = {
            item["provenance"]["source_id"]: item
            for item in restored["delivery_trace"]["selected"]
        }
        self.assertEqual({"everos-generated-skills", "atlas-shared-procedures"},
                         set(selected))
        for source, procedure in (
            ("everos-generated-skills", self.everos_procedure),
            ("atlas-shared-procedures", self.atlas_procedure),
        ):
            with self.subTest(source=source):
                provenance = selected[source]["provenance"]
                self.assertEqual("procedure", provenance["kind"])
                self.assertEqual(procedure["logical_id"], provenance["logical_id"])
                self.assertEqual(procedure["revision_id"], provenance["revision_id"])
                self.assertEqual(self.receiver, provenance["scope"])
                self.assertEqual("eligible", provenance["final_recheck"]["status"])
                self.assertTrue(provenance["plan_affecting"])
                candidate = next(item for item in result.selected_candidates
                                 if item["source_id"] == source)
                self.assertEqual(candidate["payload_digest"],
                                 selected[source]["content_digest"])
        worker_bytes = contracts.canonical_json({
            "context": restored, "envelope": result.envelope,
        }).decode("utf-8")
        self.assertIn(EVEROS_MARKER, worker_bytes)
        self.assertIn(ATLAS_MARKER, worker_bytes)
        self.assertNotIn("authority_evidence", worker_bytes)
        self.assertNotIn(RAW_APPROVAL_MATERIAL, worker_bytes)
        self.assertNotIn('"approval":{', worker_bytes)
        self.assertNotIn('"authority":', worker_bytes)
        self.assertNotIn("MEMORY_HARNESS_CONTROL_TOKEN", worker_bytes)
        by_source = {item["source_id"]: item for item in restored["optional_content"]}
        for source, expected_kind, marker in (
            ("everos-generated-skills", "everos_generated_skill", EVEROS_MARKER),
            ("atlas-shared-procedures", "atlas_trusted_procedure", ATLAS_MARKER),
        ):
            with self.subTest(source=source):
                guidance = by_source[source]["content"]
                candidate = next(item for item in result.selected_candidates
                                 if item["source_id"] == source)
                self.assertEqual(expected_kind, guidance["source_kind"])
                self.assertEqual(candidate["logical_id"], guidance["logical_id"])
                self.assertEqual(candidate["revision_id"], guidance["revision_id"])
                self.assertEqual(candidate["payload"]["procedure"]["content_hash"],
                                 guidance["procedure_digest"])
                self.assertEqual(candidate["payload"]["approval"]["content_hash"],
                                 guidance["approval_digest"])
                self.assertEqual(self.receiver, guidance["recipient"])
                self.assertEqual(candidate["payload_digest"],
                                 by_source[source]["full_content_digest"])
                self.assertIn(marker, guidance["body"])
                self.assertNotIn(ATLAS_MARKER if source == "everos-generated-skills"
                                 else EVEROS_MARKER, guidance["body"])

    def test_changed_plan_dependent_source_blocks_finalization(self) -> None:
        live_query = self.atlas_store.query
        calls = 0

        def withdrawn_after_selection(payload):
            nonlocal calls
            calls += 1
            return live_query(payload) if calls == 1 else []

        self.atlas_store = replace(self.atlas_store, query=withdrawn_after_selection)
        with self.assertRaises(context.PlanAffectingFreshnessError):
            self.prepare(finalize=True)
        self.assertEqual(2, calls)


class CoherentAllOffTests(_CoherentFixture, unittest.TestCase):
    def test_all_off_finalization_request_has_zero_optional_calls(self) -> None:
        apc_calls = []
        service = preparation.PreparationService(
            store=self.memory_store, config=config.all_off(),
            limits=self.limits, clock=self.everos.clock,
        )
        result = service.prepare(
            task_card=self.card, plan=self.plan,
            objective_id=self.plan["objective_id"], route="ordinary",
            stores=[self.everos_store, self.atlas_store], finalize=True,
            apc_launcher=lambda request: apc_calls.append(request) or {},
        )
        self.assertEqual("inherited", result.mode)
        self.assertIsNone(result.context)
        self.assertIsNone(result.envelope)
        self.assertEqual(0, len(self.everos.fake.search_calls))
        self.assertEqual(0, len(self.vector_store.calls))
        self.assertEqual(0, len(apc_calls))


if __name__ == "__main__":
    unittest.main()
