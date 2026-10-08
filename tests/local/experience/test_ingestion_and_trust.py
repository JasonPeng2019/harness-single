from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts, experience, privacy, store


class _FakeEverOS:
    def __init__(self, *, fail_add: bool = False) -> None:
        self.fail_add = fail_add
        self.add_calls: list[dict] = []
        self.case_results: list[dict] = []

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.add_calls.append(payload)
        if self.fail_add:
            raise ConnectionError(
                "response lost after EverOS add: synthetic-secret-alpha-1234567890"
            )
        return {"status": "extracted", "message_count": len(payload["messages"])}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, _: object) -> dict:
        return {
            "request_id": "fake-search",
            "data": {"agent_cases": list(self.case_results), "agent_skills": []},
        }


class ReviewedExperienceIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.memory_store = store.MemoryStore(self.root / "memory.sqlite3")
        self.memory_store.initialize()
        self.policy = privacy.PrivacyPolicy(
            known_secrets=("synthetic-secret-alpha-1234567890",),
        )
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="isolated",
            owner="root-agent",
        )
        self.service = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def _capture(self) -> dict:
        plan = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        task_card = contracts.make_task_card(
            task="Repair parser failure",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(task_card, plan)
        self.memory_store.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"],
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status="FAIL",
            evidence_digest="evidence-1",
            linked_run_id="run-1",
            task_card_digest=task_card["content_hash"],
            objective_id="objective-1",
        )
        self.memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="review-1",
            outcome=outcome,
            decision=decision,
            task_card=task_card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://run-1",),
            protected_source_refs=("evidence://protected/trace",),
            raw_evidence=(
                "The parser repair failed after synthetic-secret-alpha-1234567890 "
                "was found in a protected trace."
            ),
            failed_hypotheses=("network timeout",),
        )
        return self.service.capture(
            task_card=task_card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=self.scope,
        )

    def test_uncertain_ingestion_never_replays_and_recent_evidence_waits_for_receipt(
        self,
    ) -> None:
        trajectory = self._capture()
        failed_surface = _FakeEverOS(fail_add=True)
        adapter = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.root / "everos",
            surface=experience.EverOSPublicSurface.from_object(
                failed_surface,
                memory_root=experience.EverOSAdapter.memory_root_for_scope(
                    self.root / "everos", self.scope
                ),
                resolve_memory_root=lambda: experience.EverOSAdapter.memory_root_for_scope(
                    self.root / "everos", self.scope
                ),
            ),
            privacy_policy=self.policy,
        )

        uncertain = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], adapter)
        )
        self.assertEqual("uncertain", uncertain["status"])
        self.assertEqual(1, len(failed_surface.add_calls))
        self.assertNotIn(
            "synthetic-secret-alpha-1234567890", uncertain["error"]
        )
        self.assertNotIn(
            "synthetic-secret-alpha-1234567890",
            str(failed_surface.add_calls[0]),
        )
        self.assertTrue(self.service.search_recent_evidence(self.scope, "parser"))

        # A restart/retry only reconciles exact source receipts; it must not
        # issue a second add while the first request's commit is unknown.
        recovery_surface = _FakeEverOS()
        recovery = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.root / "everos",
            surface=experience.EverOSPublicSurface.from_object(
                recovery_surface,
                memory_root=experience.EverOSAdapter.memory_root_for_scope(
                    self.root / "everos", self.scope
                ),
                resolve_memory_root=lambda: experience.EverOSAdapter.memory_root_for_scope(
                    self.root / "everos", self.scope
                ),
            ),
            privacy_policy=self.policy,
        )
        replay = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], recovery)
        )
        self.assertEqual("uncertain", replay["status"])
        self.assertEqual([], recovery_surface.add_calls)

        recovery_surface.case_results = [
            {
                "id": "case-1",
                "agent_id": recovery.everos_owner_id,
                "app_id": recovery.everos_application_id,
                "project_id": recovery.everos_project_id,
                "session_id": uncertain["session_id"],
                "task_intent": "repair parser failure",
                "approach": "inspect failing parser check",
                "quality_score": 0.0,
                "key_insight": (
                    "network timeout was disproved; "
                    "synthetic-secret-alpha-1234567890"
                ),
                "timestamp": "2026-09-21T00:00:00Z",
                "score": 1.0,
            }
        ]
        confirmed = asyncio.run(
            self.service.reconcile_extraction(trajectory["trajectory_id"], recovery)
        )

        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(["case-1"], confirmed["case_ids"])
        self.assertFalse(self.service.search_recent_evidence(self.scope, "parser"))
        receipt = self.memory_store.get_case_receipt(self.scope.to_record(), "case-1")
        self.assertEqual(trajectory["trajectory_id"], receipt["trajectory_id"])
        self.assertEqual(trajectory["review_receipt_id"], receipt["review_receipt_id"])
        self.assertEqual(
            trajectory["review_receipt_digest"], receipt["review_receipt_digest"]
        )
        self.assertNotIn(
            "synthetic-secret-alpha-1234567890", str(receipt["source_case"])
        )

    def test_ingestion_status_compare_and_swap_rejects_a_stale_writer(self) -> None:
        trajectory = self._capture()
        source = _FakeEverOS()
        adapter = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.root / "everos",
            surface=experience.EverOSPublicSurface.from_object(
                source,
                memory_root=experience.EverOSAdapter.memory_root_for_scope(
                    self.root / "everos", self.scope
                ),
                resolve_memory_root=lambda: experience.EverOSAdapter.memory_root_for_scope(
                    self.root / "everos", self.scope
                ),
            ),
            privacy_policy=self.policy,
        )
        pending = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], adapter)
        )
        self.assertEqual("pending", pending["status"])

        second_connection = store.MemoryStore(self.root / "memory.sqlite3")
        second_connection.initialize()
        try:
            stale = second_connection.get_experience_ingestion(pending["ingestion_id"])

            source.case_results = [
                {
                    "id": "cas-locked",
                    "agent_id": adapter.everos_owner_id,
                    "app_id": adapter.everos_application_id,
                    "project_id": adapter.everos_project_id,
                    "session_id": pending["session_id"],
                    "task_intent": "repair parser failure",
                    "approach": "inspect failing parser check",
                    "quality_score": 0.0,
                    "key_insight": "the network hypothesis was disproved",
                    "timestamp": "2026-09-21T00:00:00Z",
                    "score": 1.0,
                }
            ]
            confirmed = asyncio.run(
                self.service.reconcile_extraction(trajectory["trajectory_id"], adapter)
            )
            self.assertEqual("confirmed", confirmed["status"])

            with self.assertRaisesRegex(store.ExperienceConflictError, "stale"):
                second_connection.update_experience_ingestion(
                    pending["ingestion_id"],
                    status="uncertain",
                    expected_version=stale["version"],
                    error="a stale retry must not overwrite confirmation",
                )

            durable_receipt = self.memory_store.get_case_receipt(
                self.scope.to_record(), "cas-locked"
            )
            retried = second_connection.confirm_experience_ingestion(
                pending["ingestion_id"],
                [durable_receipt],
                expected_version=confirmed["version"],
            )
            self.assertEqual("confirmed", retried["status"])
            self.assertEqual(confirmed["version"], retried["version"])
        finally:
            second_connection.close()


if __name__ == "__main__":
    unittest.main()
