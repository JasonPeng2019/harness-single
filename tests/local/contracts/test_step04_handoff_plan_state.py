from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts


class HandoffPlanStateTests(unittest.TestCase):
    """STEP-04: the enhanced handoff states the exact current-plan state."""

    def _accepted(self) -> dict:
        return contracts.make_plan(
            plan_id="accepted-plan",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["execute"]},
            accepted_by="ROOT",
        )

    def _candidate(self, *, state: str = "candidate") -> dict:
        return contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route="ordinary",
            state=state,
            content={"steps": ["draft"]},
        )

    def test_accepted_handoff_declares_execution_accepted(self) -> None:
        handoff = contracts.make_memory_handoff(
            objective_id="objective-1", route="ordinary", plan=self._accepted()
        )
        self.assertEqual("execution_accepted", handoff["plan_state"])
        self.assertEqual("accepted", handoff["plan"]["state"])
        contracts.validate_memory_handoff(handoff)
        self.assertEqual(
            "execution_accepted",
            contracts.classify_current_plan(
                handoff["plan"], expected_objective_id="objective-1"
            ),
        )

    def test_candidate_handoff_declares_candidate_review(self) -> None:
        handoff = contracts.make_memory_handoff(
            objective_id="objective-1", route="ordinary", plan=self._candidate()
        )
        self.assertEqual("candidate_review", handoff["plan_state"])
        self.assertEqual(
            "candidate_review",
            contracts.classify_current_plan(
                handoff["plan"], expected_objective_id="objective-1"
            ),
        )

    def test_truly_absent_plan_is_explicit_and_distinct(self) -> None:
        absent = contracts.make_memory_handoff(
            objective_id="objective-1", route="ordinary", plan=None
        )
        self.assertEqual("absent", absent["plan_state"])
        self.assertIsNone(absent["plan"])
        self.assertEqual(
            "absent",
            contracts.classify_current_plan(
                None, expected_objective_id="objective-1"
            ),
        )
        self.assertNotEqual("candidate_review", absent["plan_state"])
        self.assertNotEqual("execution_accepted", absent["plan_state"])

    def test_absent_handoff_rejects_a_nonempty_plan_reference(self) -> None:
        handoff = contracts.make_memory_handoff(
            objective_id="objective-1", route="ordinary", plan=None
        )
        handoff["plan"] = self._accepted()
        handoff["content_hash"] = contracts.content_hash(handoff)
        with self.assertRaisesRegex(ValueError, "absent"):
            contracts.validate_memory_handoff(handoff)

    def test_declared_state_must_match_the_exact_plan(self) -> None:
        with self.assertRaisesRegex(ValueError, "current plan state"):
            contracts.make_memory_handoff(
                objective_id="objective-1",
                route="ordinary",
                plan=self._accepted(),
                plan_state="candidate_review",
            )

    def test_nonaccepted_plan_cannot_declare_execution_accepted(self) -> None:
        with self.assertRaisesRegex(ValueError, "current plan state"):
            contracts.make_memory_handoff(
                objective_id="objective-1",
                route="ordinary",
                plan=self._candidate(),
                plan_state="execution_accepted",
            )

    def test_nonempty_plan_for_another_objective_still_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "objective"):
            contracts.make_memory_handoff(
                objective_id="objective-other",
                route="ordinary",
                plan=self._accepted(),
            )

    def test_nonempty_plan_for_another_route_still_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "route"):
            contracts.make_memory_handoff(
                objective_id="objective-1", route="deeper", plan=self._accepted()
            )

    def test_unreadable_nonempty_plan_still_fails(self) -> None:
        broken = self._accepted()
        broken["content_hash"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "content hash"):
            contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=broken
            )

    def test_absent_handoff_carries_no_task_plan_binding(self) -> None:
        handoff = contracts.make_memory_handoff(
            objective_id="objective-1", route="ordinary", plan=None
        )
        self.assertIsNone(contracts.handoff_plan(handoff))
        card = contracts.make_task_card(
            task="Plan a fresh change",
            base_commit="base-1",
            memory_handoff=handoff,
        )
        contracts.validate_task_card(card)

    def test_bound_handoff_returns_its_exact_plan(self) -> None:
        plan = self._accepted()
        handoff = contracts.make_memory_handoff(
            objective_id="objective-1", route="ordinary", plan=plan
        )
        self.assertEqual(
            plan["content_hash"], contracts.handoff_plan(handoff)["content_hash"]
        )


if __name__ == "__main__":
    unittest.main()
