"""One reviewed generated skill, from durable lineage to real EverOS search."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from tests.local.mvp.everos_fixture import CONTENT, MARKER, EverOSFixture

from memory_harness import (
    contracts,
    everos_adapters,
    experience,
    preparation,
    privacy,
    store,
    templates,
)


class _LineageFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.memory_store = store.MemoryStore(self.root / "product.sqlite3")
        self.memory_store.initialize()
        self.scope = experience.ExperienceScope(
            application="mvp-app", project="mvp-project", namespace="mvp-everos",
            owner="mvp-owner",
        )
        self.policy = privacy.PrivacyPolicy()
        self.fixture = EverOSFixture(
            root=self.root, memory_store=self.memory_store,
            scope=self.scope, policy=self.policy,
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

class EverOSStoredSkillLineageTests(_LineageFixture):
    def test_api_readback_of_reviewed_approved_current_skill(self) -> None:
        candidate, approval, procedure = self.fixture.stage_lineage()
        self.assertIsNotNone(approval)
        stored = self.memory_store.get_generated_skill_candidate(candidate["candidate_id"])
        source_approval = self.memory_store.get_skill_approval("mvp-source-approval")
        revision = self.memory_store.get_procedure_revision(procedure["revision_id"])
        delivery_approval = self.memory_store.get_procedure_approval(approval["approval_id"])
        current = self.memory_store.get_current_procedure_designation(
            procedure["logical_id"], self.fixture.partition
        )
        self.assertEqual(candidate["skill_id"], stored["skill_id"])
        self.assertEqual(CONTENT, stored["content"])
        self.assertEqual(contracts.sha256_hex({"content": CONTENT}), stored["content_digest"])
        self.assertEqual(self.fixture.receiver, stored["scope"])
        self.assertEqual(["mvp-case"], [case["case_id"] for case in stored["source_cases"]])
        receipt = self.memory_store.get_case_receipt(self.fixture.receiver, "mvp-case")
        self.assertEqual(receipt["case_receipt_id"], stored["source_cases"][0]["case_receipt_id"])
        self.assertEqual(candidate["candidate_id"], source_approval["candidate_id"])
        self.assertEqual([self.scope.owner], source_approval["recipients"])
        self.assertEqual(procedure["logical_id"], revision["logical_id"])
        self.assertEqual("generated", revision["origin"])
        self.assertEqual(candidate["candidate_id"], revision["source"]["candidate_id"])
        self.assertEqual(candidate["content_hash"], revision["source"]["candidate_digest"])
        self.assertEqual([self.fixture.receiver], delivery_approval["recipients"])
        self.assertEqual(procedure["revision_id"], current["revision_id"])
        self.assertEqual(
            contracts.normalize_procedure_partition(self.fixture.partition),
            current["record"]["partition"],
        )

        with tempfile.TemporaryDirectory() as second_directory:
            second_root = Path(second_directory)
            second_store = store.MemoryStore(second_root / "product.sqlite3")
            second_store.initialize()
            try:
                second = EverOSFixture(
                    root=second_root, memory_store=second_store,
                    scope=self.scope, policy=self.policy,
                )
                second_candidate, second_approval, second_procedure = second.stage_lineage()
                self.assertEqual(candidate["skill_id"], second_candidate["skill_id"])
                self.assertEqual(candidate["content_digest"], second_candidate["content_digest"])
                self.assertEqual(
                    ["mvp-case"],
                    [case["case_id"] for case in second_candidate["source_cases"]],
                )
                self.assertNotEqual(candidate["candidate_id"], second_candidate["candidate_id"])
                self.assertEqual(
                    second_candidate["candidate_id"],
                    second_procedure["source"]["candidate_id"],
                )
                self.assertEqual(
                    second_procedure["revision_id"],
                    second_store.get_current_procedure_designation(
                        second_procedure["logical_id"], second.partition
                    )["revision_id"],
                )
                self.assertEqual(
                    second_approval["approval_id"],
                    second_store.get_procedure_approval(second_approval["approval_id"])["approval_id"],
                )
            finally:
                second_store.close()


class EverOSStoredSkillSearchTests(_LineageFixture):
    def test_real_public_skill_search_requires_exact_current_durable_join(self) -> None:
        try:
            from everos.config import load_settings
            from everos.infra.persistence import index
        except ModuleNotFoundError as exc:
            self.skipTest(f"vendored EverOS runtime unavailable: {exc.name}")

        candidate, _, procedure = self.fixture.stage_lineage(approve=False)
        with patch.dict(os.environ, {"EVEROS_ROOT": str(self.fixture.memory_root)}):
            load_settings.cache_clear()
            try:
                skill_path = asyncio.run(self.fixture.seed_stored_skill(
                    candidate, include_foreign_scope_hits=True
                ))
                self.assertEqual("SKILL.md", skill_path.name)
                self.assertTrue(skill_path.is_file())
                asyncio.run(index.startup())
                surface = experience.load_vendored_everos_public_surface(
                    memory_root=self.fixture.memory_root
                )
                adapter = experience.EverOSAdapter(
                    scope=self.scope, base_root=self.fixture.base_root, surface=surface,
                    privacy_policy=self.policy,
                )

                hits = asyncio.run(adapter.search_skill_candidates(
                    query="parser lock evidence recovery", top_k=20
                ))
                self.assertEqual([candidate["skill_id"]], [hit["id"] for hit in hits])
                self.assertEqual(self.fixture.receiver, self.scope.to_record())
                self.assertEqual(CONTENT, hits[0]["content"])
                self.assertEqual(["mvp-case"], hits[0]["source_case_ids"])
                self.assertEqual(adapter.everos_owner_id, hits[0]["agent_id"])
                self.assertEqual(adapter.everos_application_id, hits[0]["app_id"])
                self.assertEqual(adapter.everos_project_id, hits[0]["project_id"])
                with self.assertRaises(experience.ScopeBoundaryError):
                    experience.EverOSAdapter(
                        scope=experience.ExperienceScope(
                            application=self.scope.application,
                            project=self.scope.project,
                            namespace="foreign-namespace", owner=self.scope.owner,
                        ),
                        base_root=self.fixture.base_root, surface=surface,
                    )

                search_store = everos_adapters.make_everos_generated_skill_search_store(
                    experience_service=self.fixture.experience_service,
                    procedure_service=self.fixture.procedure_service,
                    memory_store=self.memory_store,
                    adapter=adapter, scope=self.scope, receiver=self.fixture.receiver,
                    facts={"language": "python"}, route="ordinary", limits=self.fixture.limits,
                )
                objective = templates.objective_representation(
                    "parser lock evidence recovery", route="ordinary", limits=self.fixture.limits
                )
                payload = {
                    "representation": {
                        key: objective[key]
                        for key in ("model", "dimensions", "metric", "sanitizer_version")
                    },
                    "tokens": list(objective["tokens"])[:32],
                    "route": "ordinary",
                }
                self.assertEqual("everos_generated_skill", search_store.source_kind)
                self.assertEqual([], list(search_store.query(payload)))  # no procedure approval

                approval = self.fixture.approve_procedure(procedure)
                results = list(search_store.query(payload))
                self.assertEqual(1, len(results))
                record = results[0]
                self.assertEqual("everos-generated-skills", record["origin"])
                self.assertEqual(procedure["logical_id"], record["logical_id"])
                self.assertEqual(procedure["revision_id"], record["revision_id"])
                self.assertEqual(self.fixture.receiver, record["scope"])
                self.assertEqual("approved", record["approval_status"])
                self.assertEqual("current", record["designation"])
                self.assertEqual(contracts.sha256_hex(record["payload"]), record["payload_digest"])
                self.assertEqual(self.fixture.receiver, record["payload"]["recipient"])
                self.assertEqual(approval["approval_id"], record["payload"]["approval"]["approval_id"])
                self.assertEqual(candidate["candidate_id"], record["payload"]["source"]["candidate_id"])
                self.assertEqual(candidate["content_hash"], record["payload"]["source"]["candidate_digest"])
                self.assertEqual(candidate["source_cases"], record["payload"]["source_cases"])
                self.assertIn(MARKER, record["payload"]["procedure"]["behavior"]["body"])

                task_card = contracts.make_task_card(
                    task="Inspect parser lock evidence before recovery",
                    base_commit="mvp-preparation-base",
                )
                plan = contracts.make_plan(
                    plan_id="mvp-preparation-plan", objective_id="mvp-preparation-objective",
                    route="ordinary", state="candidate", content={"steps": ["draft"]},
                )
                prepared = preparation.PreparationService(
                    store=self.memory_store, limits=self.fixture.limits
                ).prepare(
                    task_card=task_card, plan=plan,
                    objective_id="mvp-preparation-objective", route="ordinary",
                    stores=[search_store],
                    root_replan={"requested_by": "ROOT", "reason": "MVP stored skill proof"},
                )
                selected = [
                    item for item in prepared.trace["candidates"]
                    if item["kind"] == "procedure" and item["disposition"] == "selected"
                ]
                self.assertEqual([procedure["revision_id"]], [item["revision_id"] for item in selected])
                self.assertEqual(record["payload_digest"], selected[0]["payload_digest"])
                self.assertIn(MARKER, selected[0]["payload"]["procedure"]["behavior"]["body"])

                current = self.memory_store.get_current_procedure_designation(
                    procedure["logical_id"], self.fixture.partition
                )
                designation = self.memory_store.get_procedure_designation(
                    current["designation_id"]
                )
                withdrawal = contracts.make_procedure_withdrawal(
                    predecessor_designation=designation, issuer="ROOT"
                )
                self.memory_store.record_procedure_withdrawal(withdrawal)
                self.assertEqual([], list(search_store.query(payload)))  # stale designation
            finally:
                asyncio.run(index.shutdown())
                load_settings.cache_clear()


if __name__ == "__main__":
    unittest.main()
