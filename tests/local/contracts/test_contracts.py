from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts


class ContractBasics(unittest.TestCase):
    def test_task_card_and_memory_handoff_round_trip(self) -> None:
        plan = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "implement"]},
            accepted_by="ROOT",
        )
        handoff = contracts.make_memory_handoff(
            objective_id="objective-1",
            route="ordinary",
            plan=plan,
            configuration={"strategy": "standard"},
        )
        card = contracts.make_task_card(
            task="Fix the failing regression",
            base_commit="abc123",
            branch="lane/test",
            memory_handoff=handoff,
        )
        contracts.validate_task_card(card)
        contracts.validate_memory_handoff(card["memory_handoff"])
        self.assertEqual(contracts.TASK_CARD_SCHEMA, card["schema"])
        self.assertEqual(contracts.MEMORY_HANDOFF_SCHEMA, card["memory_handoff"]["schema"])

    def test_invalid_task_card_hash_is_rejected(self) -> None:
        card = contracts.make_task_card(task="Task", base_commit="abc")
        card["task"] = "Changed task"
        with self.assertRaisesRegex(ValueError, "content hash"):
            contracts.validate_task_card(card)

    def test_accepted_plan_validation_rejects_wrong_identity(self) -> None:
        plan = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": []},
            accepted_by="ROOT",
        )
        with self.assertRaisesRegex(ValueError, "objective"):
            contracts.validate_plan(plan, expected_objective_id="objective-2")
        with self.assertRaisesRegex(ValueError, "route"):
            contracts.validate_plan(plan, expected_route="problem_focused")
        with self.assertRaisesRegex(ValueError, "state"):
            contracts.validate_plan(plan, expected_state="candidate")

    def test_missing_stale_or_unreadable_plan_is_rejected(self) -> None:
        base = {
            "schema": contracts.PLAN_SCHEMA,
            "plan_id": "plan-1",
            "objective_id": "objective-1",
            "route": "ordinary",
            "state": "accepted",
            "revision": 1,
            "content": {},
            "accepted_by": "ROOT",
        }
        base["content_hash"] = contracts.content_hash(base)
        missing = dict(base)
        del missing["plan_id"]
        with self.assertRaisesRegex(ValueError, "plan_id"):
            contracts.validate_plan(missing)

        stale = dict(base)
        stale["state"] = "stale"
        stale["content_hash"] = contracts.content_hash(stale)
        with self.assertRaisesRegex(ValueError, "state"):
            contracts.validate_plan(stale)

        unreadable = dict(base)
        unreadable["content_hash"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "content hash"):
            contracts.validate_plan(unreadable)

    def test_root_revision_creates_a_new_accepted_identity(self) -> None:
        old = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )
        revised = contracts.revise_plan(
            old,
            new_plan_id="plan-2",
            new_content={"steps": ["inspect", "implement", "verify"]},
        )
        contracts.validate_plan(revised, expected_state="accepted")
        self.assertEqual("plan-2", revised["plan_id"])
        self.assertEqual("plan-1", revised["supersedes"])
        self.assertEqual(2, revised["revision"])
        self.assertNotEqual(old["content_hash"], revised["content_hash"])

    def test_task_card_cannot_be_bound_to_a_different_valid_plan(self) -> None:
        first = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["first"]},
            accepted_by="ROOT",
        )
        second = contracts.make_plan(
            plan_id="plan-2",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["second"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Task",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=first
            ),
        )
        with self.assertRaisesRegex(ValueError, "task card plan"):
            contracts.make_decision(card, second)


class DispatchEnvelopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "implement"]},
            accepted_by="ROOT",
        )
        self.handoff = contracts.make_memory_handoff(
            objective_id="objective-1",
            route="ordinary",
            plan=self.plan,
        )
        self.card = contracts.make_task_card(
            task="Fix the regression",
            base_commit="base-1",
            branch="lane/test",
            memory_handoff=self.handoff,
        )
        self.decision = contracts.make_decision(self.card, self.plan)
        self.envelope = contracts.make_envelope(
            task_card=self.card,
            plan=self.plan,
            decision_id=self.decision["decision_id"],
            lane_id="lane-1",
            run_id="run-1",
            worktree_path="C:/worktree",
            base_commit="base-1",
            mandatory_content=[{"id": "task", "kind": "task", "content": "Fix the regression"}],
            optional_content=[{"id": "memory", "kind": "evidence", "content": "historical note"}],
        )

    def test_envelope_validates_against_exact_task_and_plan(self) -> None:
        contracts.validate_envelope(
            self.envelope,
            task_card=self.card,
            plan=self.plan,
            lane_id="lane-1",
            run_id="run-1",
            base_commit="base-1",
            worktree_path="C:/worktree",
        )

    def test_changed_task_or_base_is_rejected(self) -> None:
        changed_card = contracts.make_task_card(
            task="Different task",
            base_commit="base-1",
            memory_handoff=self.handoff,
        )
        with self.assertRaisesRegex(ValueError, "task card"):
            contracts.validate_envelope(
                self.envelope,
                task_card=changed_card,
                plan=self.plan,
                lane_id="lane-1",
                run_id="run-1",
                base_commit="base-1",
                worktree_path="C:/worktree",
            )

    def test_envelope_cannot_be_prepared_for_a_different_task_base(self) -> None:
        with self.assertRaisesRegex(ValueError, "task card base"):
            contracts.make_envelope(
                task_card=self.card,
                plan=self.plan,
                decision_id=self.decision["decision_id"],
                lane_id="lane-1",
                run_id="run-1",
                worktree_path="C:/worktree",
                base_commit="base-2",
            )
        with self.assertRaisesRegex(ValueError, "base"):
            contracts.validate_envelope(
                self.envelope,
                task_card=self.card,
                plan=self.plan,
                lane_id="lane-1",
                run_id="run-1",
                base_commit="base-2",
                worktree_path="C:/worktree",
            )

    def test_every_meaning_bearing_envelope_field_is_bound(self) -> None:
        fields = [
            "lane_id",
            "run_id",
            "decision_id",
            "configuration_digest",
            "objective_id",
            "route",
            "plan_id",
            "plan_state",
            "plan_digest",
            "base_commit",
            "worktree_path",
            "mandatory_digest",
            "optional_digest",
        ]
        for field in fields:
            mutated = dict(self.envelope)
            mutated[field] = "mutated-" + field
            mutated["content_hash"] = contracts.content_hash(mutated)
            with self.assertRaisesRegex(ValueError, field.replace("_", " ")):
                contracts.validate_envelope(
                    mutated,
                    task_card=self.card,
                    plan=self.plan,
                    lane_id="lane-1",
                    run_id="run-1",
                    base_commit="base-1",
                    worktree_path="C:/worktree",
                )

    def test_delivery_mutation_is_rejected_even_after_rehash(self) -> None:
        mutated = dict(self.envelope)
        delivery = dict(mutated["delivery"])
        delivery["optional"] = ["not-the-original-item"]
        mutated["delivery"] = delivery
        mutated["content_hash"] = contracts.content_hash(mutated)
        with self.assertRaisesRegex(ValueError, "delivery"):
            contracts.validate_envelope(
                mutated,
                task_card=self.card,
                plan=self.plan,
                lane_id="lane-1",
                run_id="run-1",
                base_commit="base-1",
                worktree_path="C:/worktree",
            )


if __name__ == "__main__":
    unittest.main()
