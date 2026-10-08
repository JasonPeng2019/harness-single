"""Operation-scoped feature gates at the reviewed experience boundary."""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from typing import Callable

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import config, contracts, experience, privacy, store


SECRET = "synthetic-secret-alpha-1234567890"


class _EverOSSurface:
    def __init__(self) -> None:
        self.memorize_calls: list[dict] = []
        self.search_calls: list[dict] = []
        self.case_results: list[dict] = []
        self.fail_memorize = False

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.memorize_calls.append(dict(payload))
        if self.fail_memorize:
            raise ConnectionError("response lost after submission")
        return {"status": "extracted"}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, request: dict) -> dict:
        self.search_calls.append(dict(request))
        return {"data": {"agent_cases": list(self.case_results), "agent_skills": []}}


class _RootVerifier:
    def verify_generated_skill_approval(
        self,
        *,
        issuer: str,
        candidate: dict,
        scope: dict,
        recipients: tuple[str, ...],
    ) -> experience.VerifiedApproval:
        if issuer != "ROOT" or recipients != (scope["owner"],):
            raise experience.ApprovalError("untrusted approval")
        return experience.VerifiedApproval(
            issuer=issuer,
            candidate_id=candidate["candidate_id"],
            scope=scope,
            recipients=recipients,
            authority_evidence={"policy_id": "test-root/v1", "subject": "root-operator"},
        )


class ExperienceEffectGateTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.memory_store = store.MemoryStore(self.root / "memory.sqlite3")
        self.memory_store.initialize()
        self.addCleanup(self.memory_store.close)
        self.policy = privacy.PrivacyPolicy(known_secrets=(SECRET,))
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="effect-gates",
            owner="root-agent",
        )
        self.service = experience.ReviewedExperienceService(
            self.memory_store,
            privacy_policy=self.policy,
            approval_verifier=_RootVerifier(),
        )
        self.surface = _EverOSSurface()
        base_root = self.root / "everos"
        memory_root = experience.EverOSAdapter.memory_root_for_scope(base_root, self.scope)
        self.adapter = experience.EverOSAdapter(
            scope=self.scope,
            base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                self.surface,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
            privacy_policy=self.policy,
        )

    def _capture(self) -> dict:
        plan = contracts.make_plan(
            plan_id="effect-plan",
            objective_id="effect-objective",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Repair parser failure",
            base_commit="effect-base",
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
            status="FAIL",
            evidence_digest="effect-evidence",
            linked_run_id="effect-run",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="effect-review",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://effect-run",),
            protected_source_refs=("evidence://protected/parser",),
            raw_evidence=f"The parser repair failed; trace contained {SECRET}.",
            failed_hypotheses=("the network caused the failure",),
        )
        return self.service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=self.scope,
        )

    def _case(self, session_id: str) -> dict:
        return {
            "id": "effect-case",
            "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "session_id": session_id,
            "task_intent": "repair parser failure",
            "approach": "inspect failed parser check",
            "quality_score": 0.0,
            "key_insight": "network hypothesis disproved",
            "timestamp": "2026-09-21T00:00:00Z",
            "score": 1.0,
        }

    def _skill(self, skill_id: str) -> dict:
        return {
            "id": skill_id,
            "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "name": "parser repair",
            "content": "Inspect the failed parser check before proposing a repair.",
            "source_case_ids": ["effect-case"],
        }

    def _assert_local_evidence(self, trajectory: dict) -> None:
        evidence = self.service.search_recent_evidence(self.scope, "parser")
        self.assertEqual(1, len(evidence))
        self.assertEqual(trajectory["trajectory_id"], evidence[0]["id"])
        self.assertEqual(trajectory["review_receipt_id"], evidence[0]["review_receipt_id"])
        self.assertIn("parser repair failed", evidence[0]["content"])
        self.assertNotIn(SECRET, str(evidence))

    def _assert_off_pauses(self, trajectory: dict, existing: dict) -> None:
        self.surface.memorize_calls.clear()
        self.surface.search_calls.clear()
        with (
            patch.object(
                self.memory_store,
                "create_experience_ingestion",
                wraps=self.memory_store.create_experience_ingestion,
            ) as create,
            patch.object(
                self.memory_store,
                "update_experience_ingestion",
                wraps=self.memory_store.update_experience_ingestion,
            ) as update,
            patch.object(
                self.memory_store,
                "confirm_experience_ingestion",
                wraps=self.memory_store.confirm_experience_ingestion,
            ) as confirm,
        ):
            extracted = asyncio.run(
                self.service.extract_trajectory(
                    trajectory["trajectory_id"], self.adapter, experience_write=False
                )
            )
            reconciled = asyncio.run(
                self.service.reconcile_extraction(
                    trajectory["trajectory_id"], self.adapter, experience_write=False
                )
            )
            self.assertEqual(0, create.call_count)
            self.assertEqual(0, update.call_count)
            self.assertEqual(0, confirm.call_count)
        self.assertEqual(existing, extracted)
        self.assertEqual(existing, reconciled)
        self.assertEqual(
            existing,
            self.memory_store.get_experience_ingestion_for_trajectory(trajectory["trajectory_id"]),
        )
        self.assertEqual(0, len(self.surface.memorize_calls))
        self.assertEqual(0, len(self.surface.search_calls))
        self._assert_local_evidence(trajectory)

    def _foreign_scope_adapter(self, surface: _EverOSSurface) -> experience.EverOSAdapter:
        """Bind one real adapter to a different namespace of the same product."""

        foreign_scope = experience.ExperienceScope(
            application=self.scope.application,
            project=self.scope.project,
            namespace="foreign-effect-gates",
            owner=self.scope.owner,
        )
        self.assertNotEqual(self.scope.to_record(), foreign_scope.to_record())
        base_root = self.root / "everos"
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            base_root, foreign_scope
        )
        return experience.EverOSAdapter(
            scope=foreign_scope,
            base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                surface,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
            privacy_policy=self.policy,
        )

    def _assert_foreign_scope_off_call_is_rejected(
        self, call: Callable[[], object]
    ) -> None:
        """A foreign-scope adapter fails closed before any disabled return.

        The scope assertion is a local check: rejecting the wrong scope must
        cause no EverOS call and no ingestion create/update/confirm mutation.
        """

        with (
            patch.object(
                self.memory_store,
                "create_experience_ingestion",
                wraps=self.memory_store.create_experience_ingestion,
            ) as create,
            patch.object(
                self.memory_store,
                "update_experience_ingestion",
                wraps=self.memory_store.update_experience_ingestion,
            ) as update,
            patch.object(
                self.memory_store,
                "confirm_experience_ingestion",
                wraps=self.memory_store.confirm_experience_ingestion,
            ) as confirm,
        ):
            with self.assertRaises(experience.ScopeBoundaryError):
                call()
            self.assertEqual(0, create.call_count)
            self.assertEqual(0, update.call_count)
            self.assertEqual(0, confirm.call_count)

    def _assert_ingestion_unchanged(self, trajectory: dict, expected: dict) -> None:
        self.assertEqual(
            expected,
            self.memory_store.get_experience_ingestion_for_trajectory(
                trajectory["trajectory_id"]
            ),
        )

    def test_experience_write_off_skips_new_intent_and_everos(self) -> None:
        trajectory = self._capture()
        with patch.object(
            self.memory_store,
            "create_experience_ingestion",
            wraps=self.memory_store.create_experience_ingestion,
        ) as create:
            result = asyncio.run(
                self.service.extract_trajectory(
                    trajectory["trajectory_id"], self.adapter, experience_write=False
                )
            )
            self.assertEqual(0, create.call_count)
        self.assertIsNone(result)
        self.assertIsNone(
            self.memory_store.get_experience_ingestion_for_trajectory(trajectory["trajectory_id"])
        )
        self.assertEqual(0, len(self.surface.memorize_calls))
        self.assertEqual(0, len(self.surface.search_calls))
        self._assert_local_evidence(trajectory)

    def test_experience_write_off_pauses_pending_ingestion(self) -> None:
        trajectory = self._capture()
        pending = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], self.adapter)
        )
        self.assertEqual("pending", pending["status"])
        self._assert_off_pauses(trajectory, pending)

    def test_experience_write_off_pauses_uncertain_ingestion(self) -> None:
        trajectory = self._capture()
        self.surface.fail_memorize = True
        uncertain = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], self.adapter)
        )
        self.assertEqual("uncertain", uncertain["status"])
        self._assert_off_pauses(trajectory, uncertain)

    def test_experience_write_off_extraction_rejects_foreign_scope_boundary(self) -> None:
        trajectory = self._capture()
        pending = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], self.adapter)
        )
        self.assertEqual("pending", pending["status"])
        foreign_surface = _EverOSSurface()
        foreign_adapter = self._foreign_scope_adapter(foreign_surface)
        with self.assertRaises(experience.ScopeBoundaryError):
            foreign_adapter.assert_scope(trajectory["scope"])

        self._assert_foreign_scope_off_call_is_rejected(
            lambda: asyncio.run(
                self.service.extract_trajectory(
                    trajectory["trajectory_id"], foreign_adapter, experience_write=False
                )
            )
        )
        self.assertEqual([], foreign_surface.memorize_calls)
        self.assertEqual([], foreign_surface.search_calls)
        self._assert_ingestion_unchanged(trajectory, pending)

    def test_experience_write_off_reconciliation_rejects_foreign_scope_boundary(
        self,
    ) -> None:
        trajectory = self._capture()
        self.surface.fail_memorize = True
        uncertain = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], self.adapter)
        )
        self.assertEqual("uncertain", uncertain["status"])
        foreign_surface = _EverOSSurface()
        foreign_adapter = self._foreign_scope_adapter(foreign_surface)
        with self.assertRaises(experience.ScopeBoundaryError):
            foreign_adapter.assert_scope(trajectory["scope"])

        self._assert_foreign_scope_off_call_is_rejected(
            lambda: asyncio.run(
                self.service.reconcile_extraction(
                    trajectory["trajectory_id"], foreign_adapter, experience_write=False
                )
            )
        )
        self.assertEqual([], foreign_surface.memorize_calls)
        self.assertEqual([], foreign_surface.search_calls)
        self._assert_ingestion_unchanged(trajectory, uncertain)

    def test_generation_off_skips_candidate_store_and_preserves_approval_and_use(self) -> None:
        trajectory = self._capture()
        pending = asyncio.run(
            self.service.extract_trajectory(
                trajectory["trajectory_id"], self.adapter, experience_write=True
            )
        )
        self.assertEqual(1, len(self.surface.memorize_calls))
        self.surface.case_results = [self._case(pending["session_id"])]
        confirmed = asyncio.run(
            self.service.reconcile_extraction(trajectory["trajectory_id"], self.adapter)
        )
        self.assertEqual("confirmed", confirmed["status"])
        existing = self.service.resolve_generated_skill_candidate(
            self._skill("existing-skill"), self.adapter
        )
        with patch.object(
            self.memory_store,
            "record_generated_skill_candidate",
            wraps=self.memory_store.record_generated_skill_candidate,
        ) as record:
            self.assertIsNone(
                self.service.resolve_generated_skill_candidate(
                    self._skill("new-skill"),
                    self.adapter,
                    generated_skill_creation=False,
                )
            )
            self.assertIsNone(
                self.service.resolve_generated_skill_candidate(
                    self._skill("existing-skill"),
                    self.adapter,
                    generated_skill_creation=False,
                )
            )
            self.assertEqual(0, record.call_count)
        self.assertEqual(
            existing,
            self.memory_store.get_generated_skill_candidate(existing["candidate_id"]),
        )
        self.assertEqual(
            [],
            self.memory_store.read_generated_skill_candidates_for_scope(
                "new-skill", self.scope.to_record()
            ),
        )
        approval = self.service.approve_generated_skill(
            candidate_id=existing["candidate_id"],
            scope=self.scope,
            approval_id="effect-approval",
            issuer="ROOT",
            recipients=(self.scope.owner,),
            approved_at="2026-09-21T00:00:00Z",
        )
        self.assertEqual(existing["candidate_id"], approval["candidate_id"])
        resolved = config.resolve_config(
            {
                "experience_write": True,
                "generated_skill_creation": False,
                "generated_skill_use": True,
            }
        )
        self.assertFalse(resolved.generated_skill_creation)
        self.assertTrue(resolved.generated_skill_use)


if __name__ == "__main__":
    unittest.main()
