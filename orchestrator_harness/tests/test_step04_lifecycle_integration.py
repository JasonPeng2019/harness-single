"""STEP-04 lifecycle integration through bootstrap, resume, and launch.

Bootstrap and resume must run one bounded preparation through the STEP-04
coordinator over the real task card and write one exact finalized envelope;
launch validation and dispatch must bind that envelope to the actual task,
base, worktree, lane, and run before any invocation is recorded.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from orchestrator_harness import memory_handoff
from memory_harness import config, contracts, context, runtime, store


def _accepted_card(configuration: dict | None = None) -> tuple[dict, dict]:
    plan = contracts.make_plan(
        plan_id="plan-1",
        objective_id="objective-1",
        route="ordinary",
        state="accepted",
        content={"steps": ["inspect", "implement", "verify"]},
        accepted_by="ROOT",
    )
    handoff = contracts.make_memory_handoff(
        objective_id="objective-1",
        route="ordinary",
        plan=plan,
        configuration=configuration,
        checkpoint="checkpoint-1",
    )
    card = contracts.make_task_card(
        task="Fix the regression and verify it",
        base_commit="base-1",
        branch="lane/memory",
        memory_handoff=handoff,
    )
    return card, plan


def _candidate_card() -> tuple[dict, dict]:
    card, _ = _accepted_card()
    candidate = contracts.make_plan(
        plan_id="candidate-plan",
        objective_id="objective-1",
        route="ordinary",
        state="candidate",
        content={"steps": ["draft"]},
    )
    handoff = contracts.make_memory_handoff(
        objective_id="objective-1",
        route="ordinary",
        plan=candidate,
    )
    return (
        contracts.make_task_card(
            task=card["task"],
            base_commit=card["base_commit"],
            branch=card.get("branch"),
            memory_handoff=handoff,
        ),
        candidate,
    )


class Step04LifecycleIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.worktree = self.root / "worktree"
        self.worktree.mkdir()
        self.card, self.plan = _accepted_card()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _open_store(self):
        store_path, _ = memory_handoff.memory_paths(self.worktree)
        memory_store = store.MemoryStore(store_path)
        memory_store.initialize()
        return memory_store

    def test_bootstrap_runs_bounded_preparation_and_finalizes_accepted_plan(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        self.assertIsNotNone(envelope)
        self.assertEqual("accepted", envelope["plan_state"])
        mandatory_ids = [item["id"] for item in envelope["mandatory_content"]]
        self.assertIn("task", mandatory_ids)
        self.assertIn("accepted-plan", mandatory_ids)

        memory_store = self._open_store()
        try:
            preparations = memory_store.list_preparations(envelope["decision_id"])
            self.assertEqual(1, len(preparations))
            self.assertEqual("execution_accepted", preparations[0]["current_plan_state"])
            trace = memory_store.get_search_trace(preparations[0]["preparation_id"])
            self.assertIn(trace["outcome"], {"no_optional_memory", "optional_memory"})
            disposition = memory_store.list_plan_dispositions(envelope["decision_id"])
            self.assertEqual("preserved_accepted", disposition[-1]["branch"])
            final_context = memory_store.get_final_context_for_decision(
                envelope["decision_id"]
            )
            self.assertIsNotNone(final_context)
            self.assertEqual(self.plan["content_hash"], final_context["plan_digest"])
            self.assertEqual(self.card["content_hash"], final_context["task_card_digest"])
        finally:
            memory_store.close()

    def test_bootstrap_keeps_a_candidate_plan_out_of_dispatch(self) -> None:
        card, candidate = _candidate_card()
        self.assertIsNone(
            memory_handoff.prepare_bootstrap_envelope(
                task_card=card,
                lane_id="lane-candidate",
                run_id="run-candidate",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))
        memory_store = self._open_store()
        try:
            decisions = memory_store.list_preparations(
                contracts.make_decision(
                    card,
                    candidate,
                    strategy="standard",
                    configuration=config.resolve_config(None).__dict__,
                )["decision_id"]
            )
            self.assertTrue(decisions)
            self.assertEqual("candidate_review", decisions[0]["current_plan_state"])
        finally:
            memory_store.close()

    def test_resume_reuses_decision_and_never_replenishes_the_deadline(self) -> None:
        first = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        resumed = memory_handoff.prepare_resume_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-2",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        self.assertEqual("run-2", resumed["run_id"])
        self.assertEqual(first["decision_id"], resumed["decision_id"])
        memory_store = self._open_store()
        try:
            preparations = memory_store.list_preparations(first["decision_id"])
            self.assertGreaterEqual(len(preparations), 2)
            deadlines = {item["deadline_monotonic"] for item in preparations}
            self.assertEqual(1, len(deadlines))
        finally:
            memory_store.close()

    def test_launch_validation_and_dispatch_bind_the_finalized_context(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        memory_store = self._open_store()
        try:
            final_context = memory_store.get_final_context_for_decision(
                envelope["decision_id"]
            )
        finally:
            memory_store.close()
        memory_handoff.validate_final_context_for_launch(
            context=final_context,
            envelope=envelope,
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        changed = contracts.make_task_card(
            task="A different task",
            base_commit="base-1",
            memory_handoff=self.card["memory_handoff"],
        )
        with self.assertRaises(memory_handoff.MemoryHandoffError):
            memory_handoff.validate_final_context_for_launch(
                context=final_context,
                envelope=envelope,
                task_card=changed,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )

    def test_all_off_still_bypasses_the_preparation_pipeline(self) -> None:
        card, _ = _accepted_card({"all_features": False})
        self.assertIsNone(
            memory_handoff.prepare_bootstrap_envelope(
                task_card=card,
                lane_id="lane-off",
                run_id="run-off",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        )
        store_path, envelope_path = memory_handoff.memory_paths(self.worktree)
        self.assertFalse(store_path.exists())
        self.assertFalse(envelope_path.exists())


if __name__ == "__main__":
    unittest.main()

