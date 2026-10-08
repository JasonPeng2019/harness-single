from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts, experience, privacy, store


class ReviewedTrajectoryPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store_path = self.root / "memory-state.sqlite3"
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()
        self.policy = privacy.PrivacyPolicy(
            known_secrets=("synthetic-secret-alpha-1234567890",),
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def _linked_records(self, *, status: str = "PASS") -> tuple[dict, dict, dict, dict]:
        plan = contracts.make_plan(
            plan_id="accepted-plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Repair the parser regression",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        self.memory_store.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"],
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status=status,
            evidence_digest="terminal-evidence-1",
            linked_run_id="retired-run-1",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        return card, plan, decision, outcome

    def test_reviewed_trajectory_survives_restart_with_provenance_and_protected_evidence(
        self,
    ) -> None:
        card, plan, decision, outcome = self._linked_records()
        review = contracts.make_review_receipt(
            review_id="review-1",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://run/retired-run-1", "check://parser"),
            protected_source_refs=("evidence://protected/parser-log",),
            raw_evidence=(
                "The parser repair passed. Keep synthetic-secret-alpha-1234567890 "
                "only in protected local evidence."
            ),
            failed_hypotheses=("the network adapter caused the regression",),
        )
        scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="isolated-test",
            owner="root-agent",
        )
        service = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )
        trajectory = service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=scope,
        )

        self.assertEqual("reviewed_success", trajectory["status"])
        self.assertEqual("retired-run-1", trajectory["run_id"])
        self.assertEqual(plan["content_hash"], trajectory["accepted_plan_digest"])
        self.assertEqual(
            ("the network adapter caused the regression",),
            tuple(trajectory["failed_hypotheses"]),
        )
        self.assertEqual("review-1", trajectory["review_id"])
        self.assertEqual(review["content_hash"], trajectory["review_receipt_digest"])
        self.assertEqual(review["reviewed_at"], trajectory["reviewed_at"])
        self.assertIn("synthetic-secret-alpha-1234567890", trajectory["raw_evidence"])

        direct_hits = service.search_recent_evidence(scope, "parser repair")
        self.assertEqual(1, len(direct_hits))
        self.assertEqual("historical_evidence", direct_hits[0]["kind"])
        self.assertNotIn(
            "synthetic-secret-alpha-1234567890", direct_hits[0]["content"]
        )

        # The live run object is intentionally absent now; only its immutable
        # receipt remains.  A process restart must not discard that receipt.
        self.memory_store.close()
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()
        reopened = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )
        restored = reopened.get_trajectory(trajectory["trajectory_id"])

        self.assertEqual(trajectory["trajectory_id"], restored["trajectory_id"])
        self.assertEqual(card["content_hash"], restored["task_card_digest"])
        self.assertEqual(decision["decision_id"], restored["decision_id"])
        self.assertEqual(outcome["outcome_id"], restored["outcome_id"])
        self.assertEqual(review["review_receipt_id"], restored["review_receipt_id"])
        self.assertEqual(review["content_hash"], restored["review_receipt_digest"])
        self.assertEqual(("check://parser", "review://run/retired-run-1"), tuple(restored["evidence_refs"]))
        self.assertEqual(
            ("evidence://protected/parser-log",),
            tuple(restored["protected_source_refs"]),
        )

        with self.assertRaisesRegex(contracts.ContractError, "only ROOT may review"):
            contracts.make_review_receipt(
                review_id="untrusted-review",
                outcome=outcome,
                decision=decision,
                task_card=card,
                plan=plan,
                reviewed_by="worker",
                evidence_refs=("review://run/retired-run-1",),
                raw_evidence="An untrusted reviewer cannot bind this evidence.",
            )

        # A syntactically valid record cannot replace the already durable run
        # binding with a caller-supplied lookalike outcome.
        forged_outcome = dict(outcome)
        forged_outcome["linked_run_id"] = "wrong-run"
        forged_outcome["content_hash"] = contracts.content_hash(forged_outcome)
        forged_review = contracts.make_review_receipt(
            review_id="forged-run-review",
            outcome=forged_outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://wrong-run",),
            raw_evidence="A forged event must not be stored.",
        )
        forged_trajectory = contracts.make_reviewed_trajectory(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=forged_outcome,
            review_receipt=forged_review,
            scope=scope.to_record(),
        )
        with self.assertRaisesRegex(store.TrajectoryConflictError, "durable outcome"):
            self.memory_store.record_reviewed_trajectory(forged_trajectory)


if __name__ == "__main__":
    unittest.main()
