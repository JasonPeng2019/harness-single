from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from orchestrator_harness import bootstrap, controller, memory_handoff
from orchestrator_harness.records import atomic_write_json
from memory_harness import context, contracts, runtime, store


def _task_card(
    configuration: dict | None = None, *, checkpoint: str | None = "checkpoint-1"
) -> tuple[dict, dict]:
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
        checkpoint=checkpoint,
    )
    card = contracts.make_task_card(
        task="Fix the regression and verify it",
        base_commit="base-1",
        branch="lane/memory",
        memory_handoff=handoff,
    )
    return card, plan


def _finalized_lane1_fixture(
    card: dict, plan: dict, worktree: Path, *, run_id: str = "run-1",
    optional_items: list[dict] | None = None,
) -> SimpleNamespace:
    """A real Lane 1 finalized record for the explicit test dispatch target."""

    configuration = {"strategy": "standard"}
    decision_id = contracts.make_decision(card, plan)["decision_id"]
    mandatory = [
        {"id": "task", "kind": "task", "content": card["task"]},
        {"id": "accepted-plan", "kind": "accepted-plan", "content": plan["content"]},
        {"id": "base", "kind": "base", "content": card["base_commit"]},
        {"id": "route", "kind": "route", "content": plan["route"]},
        {"id": "checkpoint", "kind": "checkpoint", "content": card["memory_handoff"]["checkpoint"]},
        {"id": "security", "kind": "security", "content": contracts.FINAL_CONTEXT_SECURITY},
    ]
    optional = optional_items if optional_items is not None else [
        {"id": "case-1", "kind": "experience", "origin": "reviewed", "content": "prior failure"}
    ]
    omitted = ["case-2"]
    return context.finalize_context(
        task_card=card,
        plan=plan,
        decision_id=decision_id,
        lane_id="lane-1",
        run_id=run_id,
        worktree_path=str(worktree),
        base_commit=card["base_commit"],
        strategy="standard",
        configuration=configuration,
        checkpoint=card["memory_handoff"]["checkpoint"],
        execution_role="worker",
        invocation_target="orchestrator_harness.controller",
        recipient="worker:lane-1",
        mandatory_content=mandatory,
        optional_items=optional,
        omitted=[] if optional_items is not None else omitted,
        source_recheck=(lambda item: contracts.make_final_source_recheck(
            item=item, status="eligible", observed_revision_id=item["revision_id"],
            observed_content_digest=contracts.sha256_hex(item["content"]),
        )) if optional_items is not None else None,
    )


def _record_domain_fixture(worktree: Path, outcome: SimpleNamespace) -> None:
    """Model the domain write before the harness binds its final envelope."""

    store_path, _ = memory_handoff.memory_paths(worktree)
    memory_store = store.MemoryStore(store_path)
    memory_store.initialize()
    try:
        memory_store.record_final_context(
            outcome.context, envelope_digest=outcome.envelope["content_hash"]
        )
    finally:
        memory_store.close()


def _stored_final_context(worktree: Path, context_id: str) -> tuple[str, dict]:
    store_path, _ = memory_handoff.memory_paths(worktree)
    connection = sqlite3.connect(store_path)
    try:
        row = connection.execute(
            "SELECT envelope_digest, record FROM final_contexts WHERE context_id = ?",
            (context_id,),
        ).fetchone()
        assert row is not None
        return row[0], json.loads(row[1])
    finally:
        connection.close()


class MemoryHandoffSeamTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.worktree = self.root / "worktree"
        self.worktree.mkdir()
        self.card, self.plan = _task_card()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_accepted_target_without_authoritative_checkpoint_fails_closed(self) -> None:
        card, _ = _task_card(checkpoint=None)
        with self.assertRaisesRegex(
            memory_handoff.MemoryHandoffError, "authoritative canonical checkpoint"
        ):
            memory_handoff.prepare_bootstrap_envelope(
                task_card=card, lane_id="lane-1", run_id="run-1",
                worktree_path=self.worktree, base_commit="base-1",
            )
        store_path, envelope_path = memory_handoff.memory_paths(self.worktree)
        self.assertFalse(store_path.exists())
        self.assertFalse(envelope_path.exists())

    def test_bound_checkpoint_reaches_normal_finalized_bootstrap(self) -> None:
        card, _ = _task_card(checkpoint="root-checkpoint-42")
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=card, lane_id="lane-1", run_id="run-1",
            worktree_path=self.worktree, base_commit="base-1",
        )
        self.assertEqual("root-checkpoint-42", envelope["checkpoint"])
        self.assertEqual("root-checkpoint-42", envelope["final_context"]["checkpoint"])
        self.assertEqual(
            "root-checkpoint-42",
            next(item["content"] for item in envelope["mandatory_content"]
                 if item["id"] == "checkpoint"),
        )
        self.assertEqual(envelope, memory_handoff.load_envelope(self.worktree))
        self.assertEqual(
            envelope["final_context"],
            memory_handoff.load_final_context(worktree_path=self.worktree, envelope=envelope),
        )

    def test_legacy_task_card_has_no_memory_envelope(self) -> None:
        legacy = contracts.make_task_card(
            task="ordinary task",
            base_commit="base-1",
        )
        self.assertIsNone(memory_handoff.validate_task_card(legacy))
        self.assertIsNone(
            memory_handoff.prepare_bootstrap_envelope(
                task_card=legacy,
                lane_id="lane-legacy",
                run_id="run-legacy",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_bootstrap_and_resume_use_equivalent_envelope_validation(self) -> None:
        bootstrap_envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        self.assertIsNotNone(bootstrap_envelope)
        self.assertEqual("run-1", bootstrap_envelope["run_id"])
        memory_handoff.validate_envelope_for_launch(
            envelope=bootstrap_envelope,
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )

        resume_envelope = memory_handoff.prepare_resume_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-2",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        self.assertEqual("run-2", resume_envelope["run_id"])
        memory_handoff.validate_envelope_for_launch(
            envelope=resume_envelope,
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-2",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        self.assertEqual(
            bootstrap_envelope["decision_id"], resume_envelope["decision_id"]
        )

    def test_candidate_plan_continues_without_parent_envelope(self) -> None:
        candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )
        candidate_card = contracts.make_task_card(
            task="Fix the regression",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1",
                route="ordinary",
                plan=candidate,
            ),
        )
        self.assertIsNone(
            memory_handoff.prepare_bootstrap_envelope(
                task_card=candidate_card,
                lane_id="lane-candidate",
                run_id="run-candidate",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_all_off_bypasses_memory_and_learned_mode_fails_before_state(self) -> None:
        all_off_card, _ = _task_card({"all_features": False})
        self.assertIsNone(
            memory_handoff.prepare_bootstrap_envelope(
                task_card=all_off_card,
                lane_id="lane-off",
                run_id="run-off",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        )
        store_path, envelope_path = memory_handoff.memory_paths(self.worktree)
        self.assertFalse(store_path.exists())
        self.assertFalse(envelope_path.exists())

        learned_card, _ = _task_card({"strategy": "learned"})
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "deferred"):
            memory_handoff.prepare_bootstrap_envelope(
                task_card=learned_card,
                lane_id="lane-learned",
                run_id="run-learned",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        self.assertFalse(store_path.exists())

    def test_two_source_final_context_reaches_worker_input_and_result_capture(self) -> None:
        sources = [
            {"id": "everos-item", "kind": "procedure", "origin": "everos_generated_skill",
             "source_id": "everos-skill-id", "revision_id": "everos-r1", "freshness": "live",
             "content": {"guidance": "EVEROS_MVP_MARKER"}},
            {"id": "atlas-item", "kind": "procedure", "origin": "atlas_trusted_procedure",
             "source_id": "atlas-procedure-id", "revision_id": "atlas-r1", "freshness": "live",
             "content": {"guidance": "ATLAS_MVP_MARKER"}},
        ]
        finalized = _finalized_lane1_fixture(
            self.card, self.plan, self.worktree, optional_items=sources,
        )
        self.assertEqual(2, len(finalized.context["optional_content"]))
        _record_domain_fixture(self.worktree, finalized)
        memory_store = store.MemoryStore(memory_handoff.memory_paths(self.worktree)[0])
        memory_store.initialize()
        try:
            memory_store.record_decision(contracts.make_decision(self.card, self.plan))
        finally:
            memory_store.close()
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir(exist_ok=True)
        atomic_write_json(workspace / "task-card.json", self.card)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=finalized):
            envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card, lane_id="lane-1", run_id="run-1",
                worktree_path=self.worktree, base_commit="base-1",
            )
        assert envelope is not None
        for field, expected in (
            ("task", self.card["task"]), ("plan_id", self.plan["plan_id"]),
            ("decision_id", finalized.context["decision_id"]),
            ("base_commit", "base-1"), ("run_id", "run-1"),
            ("recipient", "worker:lane-1"),
        ):
            self.assertEqual(expected, envelope[field], field)
        bootstrap._write_worker_prompt(self.worktree, self.card, managed=False)
        invocation = bootstrap._write_invocation(
            self.worktree, lane_id="lane-1", run_id="run-1", provider_id="codex",
            model="test-model", launch_config={"reasoning_effort": "high"},
            exclusive_resources=[], memory_envelope=envelope,
            git_identity={
                "worktree_path": str(self.worktree.resolve()),
                "branch": self.card["branch"],
                "base_commit": self.card["base_commit"],
                "base_tree": "tree-base-1",
            },
        )
        lane = {"lane_id": "lane-1", "run_id": "run-1", "worktree_path": str(self.worktree),
                "provider": invocation["provider"], "memory_plan_state": "execution_accepted",
                "result_path": str(self.worktree / "RESULT.json")}
        self.assertEqual("codex", invocation["provider"]["id"])
        self.assertEqual("test-model", lane["provider"]["model"])
        with patch.dict(controller.os.environ, {
            "USERPROFILE": str(self.root), "CODEX_HOME": str(self.root / "codex-home"),
        }, clear=True):
            self.assertEqual(invocation["dispatch_binding"], controller._validate_enhanced_dispatch(lane, invocation))
        prompt = (workspace / "worker-prompt.md").read_text(encoding="utf-8")
        for source in sources:
            descriptor = next(item for item in envelope["delivery_trace"]["context_delivered"]
                              if item["id"] == source["id"])
            self.assertIn(source["source_id"], prompt)
            self.assertIn(source["revision_id"], prompt)
            self.assertIn(descriptor["content_digest"], prompt)
            self.assertIn(source["content"]["guidance"], prompt)
        self.assertNotIn("control credential", prompt.lower())
        calls = []
        def worker(_intent: dict) -> dict:
            calls.append(prompt)
            result = {"schema": "result/v1", "lane_id": "lane-1", "run_id": "run-1",
                      "outcome": "PASS", "summary": "in-process marker transport",
                      "evidence": [marker for marker in (
                          "EVEROS_MVP_MARKER", "ATLAS_MVP_MARKER"
                      ) if marker in prompt],
                      "completed_at": "2026-09-26T00:00:00Z"}
            result["content_hash"] = contracts.content_hash(result)
            atomic_write_json(self.worktree / "RESULT.json", result)
            return {"invocation_id": "in-process-worker", "pid": 17, "creation_time": "deterministic"}
        delivered = memory_handoff.dispatch(
            worktree_path=self.worktree, envelope=envelope, launcher=worker,
        )
        replay = memory_handoff.dispatch(
            worktree_path=self.worktree, envelope=envelope,
            launcher=lambda _intent: self.fail("duplicate worker intent"),
        )
        self.assertEqual(delivered["operation_id"], replay["operation_id"])
        self.assertEqual(1, len(calls))
        with patch.object(
            controller,
            "validate_merge_ready_git",
            return_value={
                "worktree_path": str(self.worktree.resolve()),
                "branch": self.card["branch"],
                "base_commit": self.card["base_commit"],
                "base_tree": "tree-base-1",
                "head_commit": "head-1",
                "head_tree": "tree-head-1",
                "clean": True,
            },
        ):
            state, result = controller._validate_result(lane)
        self.assertEqual("valid", state)
        self.assertEqual(["EVEROS_MVP_MARKER", "ATLAS_MVP_MARKER"], result["evidence"])

    def test_changed_task_or_base_is_rejected_at_launch(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        changed_card = contracts.make_task_card(
            task="different task",
            base_commit="base-1",
            memory_handoff=self.card["memory_handoff"],
        )
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "task card"):
            memory_handoff.validate_envelope_for_launch(
                envelope=envelope,
                task_card=changed_card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "base"):
            memory_handoff.validate_envelope_for_launch(
                envelope=envelope,
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-2",
            )

    def test_bootstrap_rejects_a_final_context_for_another_base_before_writing(self) -> None:
        wrong_card = contracts.make_task_card(
            task=self.card["task"],
            base_commit="base-2",
            branch=self.card["branch"],
            memory_handoff=self.card["memory_handoff"],
        )
        outcome = _finalized_lane1_fixture(wrong_card, self.plan, self.worktree)
        with patch.object(
            memory_handoff,
            "_prepare_memory_outcome",
            return_value=outcome,
        ):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "task card|base"):
                memory_handoff.prepare_bootstrap_envelope(
                    task_card=self.card,
                    lane_id="lane-1",
                    run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_absent_and_proposed_plans_remain_in_root_planning_or_review(self) -> None:
        for state in ("absent", "proposed", "candidate"):
            with self.subTest(state=state):
                plan = None if state == "absent" else contracts.make_plan(
                    plan_id=f"{state}-plan",
                    objective_id="objective-1",
                    route="ordinary",
                    state=state,
                    content={"steps": ["draft"]},
                )
                card = contracts.make_task_card(
                    task="Plan and review the regression",
                    base_commit="base-1",
                    memory_handoff=contracts.make_memory_handoff(
                        objective_id="objective-1", route="ordinary", plan=plan
                    ),
                )
                worktree = self.root / state
                worktree.mkdir()
                prepared = memory_handoff.prepare_lane_memory(
                    task_card=card,
                    lane_id=f"lane-{state}",
                    run_id=f"run-{state}",
                    worktree_path=worktree,
                    base_commit="base-1",
                )
                self.assertEqual(
                    "absent" if state == "absent" else "candidate_review",
                    prepared.state,
                )
                self.assertTrue(prepared.pending_plan)
                self.assertIsNone(prepared.envelope)
                self.assertIsNone(memory_handoff.load_envelope(worktree))

    def test_accepted_fixture_carries_exact_finalized_meaning_without_launch_claim(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        self.assertEqual(self.card["task"], envelope["task"])
        self.assertEqual(self.plan["revision"], envelope["plan_revision"])
        self.assertEqual(self.plan["content_hash"], envelope["plan_digest"])
        self.assertEqual(outcome.context["integrity"], envelope["final_context_integrity"])
        self.assertEqual(outcome.context["context_id"], envelope["final_context_id"])
        self.assertEqual(outcome.context, envelope["final_context"])
        for field in ("execution_role", "invocation_target", "recipient", "delivery_trace"):
            self.assertEqual(outcome.context[field], envelope[field], field)
        trace = envelope["delivery_trace"]
        self.assertEqual(["case-2", "case-1"], [item["id"] for item in trace["selected"]])
        self.assertEqual(["case-1"], [item["id"] for item in trace["packed"]])
        self.assertEqual(["case-2"], [item["id"] for item in trace["omitted"]])
        self.assertEqual(trace["packed"], trace["context_delivered"])
        self.assertNotIn("observed_invocation", envelope)
        self.assertEqual(envelope, memory_handoff.load_envelope(self.worktree))
        prompt_context = "\n".join(bootstrap._render_memory_context(self.worktree))
        self.assertIn("base-1", prompt_context)
        self.assertIn("ordinary", prompt_context)
        self.assertIn("control_plane", prompt_context)
        self.assertIn("prior failure", prompt_context)

    def test_bound_envelope_relinks_the_durable_final_context_digest(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        _record_domain_fixture(self.worktree, outcome)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        written = memory_handoff.load_envelope(self.worktree)
        stored_digest, stored_context = _stored_final_context(
            self.worktree, outcome.context["context_id"]
        )
        self.assertEqual(outcome.envelope["content_hash"], written["content_hash"])
        self.assertEqual(envelope["content_hash"], written["content_hash"])
        self.assertEqual(written["content_hash"], stored_digest)
        self.assertEqual(outcome.context, stored_context)

    def test_finalized_envelope_rejects_observed_claim_in_both_shapes(self) -> None:
        for enriched in (False, True):
            with self.subTest(enriched=enriched):
                if enriched:
                    outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
                    with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
                        envelope = memory_handoff.prepare_bootstrap_envelope(
                            task_card=self.card, lane_id="lane-1", run_id="run-1",
                            worktree_path=self.worktree, base_commit="base-1",
                        )
                else:
                    envelope = memory_handoff.prepare_bootstrap_envelope(
                        task_card=self.card, lane_id="lane-1", run_id="run-1",
                        worktree_path=self.worktree, base_commit="base-1",
                    )
                claimed = dict(envelope)
                claimed["observed_invocation"] = {"invocation_id": "fictional"}
                claimed["content_hash"] = contracts.content_hash(claimed)
                with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "observed launch"):
                    memory_handoff.validate_envelope_for_launch(
                        envelope=claimed, task_card=self.card, lane_id="lane-1",
                        run_id="run-1", worktree_path=self.worktree, base_commit="base-1",
                    )

    def test_finalized_context_rejects_observed_claim_in_both_shapes(self) -> None:
        for enriched in (False, True):
            with self.subTest(enriched=enriched):
                if enriched:
                    outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
                    context = outcome.context
                    with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
                        envelope = memory_handoff.prepare_bootstrap_envelope(
                            task_card=self.card, lane_id="lane-1", run_id="run-1",
                            worktree_path=self.worktree, base_commit="base-1",
                        )
                else:
                    envelope = memory_handoff.prepare_bootstrap_envelope(
                        task_card=self.card, lane_id="lane-1", run_id="run-1",
                        worktree_path=self.worktree, base_commit="base-1",
                    )
                    context = memory_handoff.load_final_context(
                        worktree_path=self.worktree, envelope=envelope
                    )
                claimed = dict(context)
                claimed["observed_invocation"] = {"invocation_id": "fictional"}
                claimed["content_hash"] = contracts.content_hash(claimed)
                with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "observed launch"):
                    memory_handoff.validate_final_context_for_launch(
                        context=claimed, envelope=envelope, task_card=self.card,
                        lane_id="lane-1", run_id="run-1", worktree_path=self.worktree,
                        base_commit="base-1",
                    )

    def test_resume_rejects_an_unreadable_prior_handoff(self) -> None:
        memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        _, envelope_path = memory_handoff.memory_paths(self.worktree)
        envelope_path.write_text("{unreadable", encoding="utf-8")
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "cannot read"):
            memory_handoff.prepare_resume_envelope(
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-2",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        self.assertEqual("{unreadable", envelope_path.read_text(encoding="utf-8"))

    def test_enriched_handoff_rejects_revocation_and_wrong_execution_recipient(self) -> None:
        for change, expected in (
            (lambda context: context["freshness"].update(mode="revoked"), "differs"),
            (lambda context: context.update(execution_role="reviewer"), "differs"),
            (lambda context: context.update(invocation_target="another.controller"), "differs"),
            (lambda context: context.update(recipient="worker:another-lane"), "differs"),
            (
                lambda context: context["role_separation"].update(control_plane="included"),
                "differs",
            ),
        ):
            with self.subTest(expected=expected):
                outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
                change(outcome.context)
                outcome.context["content_hash"] = contracts.content_hash(outcome.context)
                with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
                    with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, expected):
                        memory_handoff.prepare_bootstrap_envelope(
                            task_card=self.card,
                            lane_id="lane-1",
                            run_id="run-1",
                            worktree_path=self.worktree,
                            base_commit="base-1",
                        )
                self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_enriched_handoff_preserves_mandatory_security(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        mandatory = [
            item for item in outcome.envelope["mandatory_content"] if item["id"] != "security"
        ]
        outcome.envelope["mandatory_content"] = mandatory
        outcome.envelope["mandatory_digest"] = contracts.sha256_hex(mandatory)
        outcome.envelope["delivery"]["mandatory"] = [item["id"] for item in mandatory]
        outcome.envelope["content_hash"] = contracts.content_hash(outcome.envelope)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "mandatory security|context binding"):
                memory_handoff.prepare_bootstrap_envelope(
                    task_card=self.card,
                    lane_id="lane-1",
                    run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_enriched_handoff_rejects_duplicate_mandatory_identity(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        mandatory = [
            {"id": "task", "kind": "task", "content": "untrusted duplicate"},
            *outcome.envelope["mandatory_content"],
        ]
        outcome.envelope["mandatory_content"] = mandatory
        outcome.envelope["mandatory_digest"] = contracts.sha256_hex(mandatory)
        outcome.envelope["delivery"]["mandatory"] = [item["id"] for item in mandatory]
        outcome.envelope["content_hash"] = contracts.content_hash(outcome.envelope)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "duplicate"):
                memory_handoff.prepare_bootstrap_envelope(
                    task_card=self.card,
                    lane_id="lane-1",
                    run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_mandatory_overflow_remains_an_explicit_failure(self) -> None:
        from memory_harness.context import MandatoryOverflowError

        with patch.object(
            memory_handoff,
            "_prepare_memory_outcome",
            side_effect=MandatoryOverflowError("mandatory task and plan overflow"),
        ):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "mandatory task and plan overflow"):
                memory_handoff.prepare_bootstrap_envelope(
                    task_card=self.card,
                    lane_id="lane-1",
                    run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_enriched_handoff_detects_changed_payload_after_finalization(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        changed = dict(envelope)
        changed["optional_content"][0]["content"] = "changed after finalization"
        changed["content_hash"] = contracts.content_hash(changed)
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "optional digest|context binding"):
            memory_handoff.validate_final_context_for_launch(
                context=outcome.context,
                envelope=changed,
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        changed_context = json.loads(json.dumps(outcome.context))
        changed_context["integrity"] = "changed-integrity"
        changed_context["content_hash"] = contracts.content_hash(changed_context)
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "integrity|record does not match"):
            memory_handoff.validate_final_context_for_launch(
                context=changed_context,
                envelope=envelope,
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )

    def test_enriched_handoff_rejects_delivery_trace_that_omits_rendered_item(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        changed = json.loads(json.dumps(outcome.envelope))
        changed["delivery_trace"]["context_delivered"] = []
        changed["content_hash"] = contracts.content_hash(changed)
        with patch.object(
            memory_handoff, "_prepare_memory_outcome",
            return_value=SimpleNamespace(envelope=changed, context=outcome.context),
        ):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "context-delivered"):
                memory_handoff.prepare_bootstrap_envelope(
                    task_card=self.card,
                    lane_id="lane-1",
                    run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_enriched_handoff_rejects_packed_item_also_marked_omitted(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        outcome.envelope["delivery"]["omitted"] = ["case-1", "case-2"]
        outcome.envelope["content_hash"] = contracts.content_hash(outcome.envelope)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "omitted|partition"):
                memory_handoff.prepare_bootstrap_envelope(
                    task_card=self.card,
                    lane_id="lane-1",
                    run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )
        self.assertIsNone(memory_handoff.load_envelope(self.worktree))

    def test_resume_checks_the_durable_enriched_context_fixture(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        _record_domain_fixture(self.worktree, outcome)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        store_path, _ = memory_handoff.memory_paths(self.worktree)
        self.assertEqual(
            envelope["content_hash"],
            _stored_final_context(self.worktree, outcome.context["context_id"])[0],
        )
        memory_handoff.validate_resume_handoff(
            task_card=self.card,
            lane_id="lane-1",
            prior_run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )

        changed_context = json.loads(json.dumps(outcome.context))
        changed_context["optional_content"][0]["content"] = "changed after finalization"
        changed_context["content_hash"] = contracts.content_hash(changed_context)
        with patch.object(memory_handoff, "load_final_context", return_value=changed_context):
            with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "finalized context|integrity"):
                memory_handoff.validate_resume_handoff(
                    task_card=self.card,
                    lane_id="lane-1",
                    prior_run_id="run-1",
                    worktree_path=self.worktree,
                    base_commit="base-1",
                )

    def test_resume_rejects_revised_plan_and_changed_prior_payload(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        revised = contracts.revise_plan(
            self.plan, new_plan_id="plan-2", new_content={"steps": ["revised"]}
        )
        changed_card = contracts.make_task_card(
            task=self.card["task"],
            base_commit="base-1",
            branch="lane/memory",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=revised,
                checkpoint=self.card["memory_handoff"]["checkpoint"],
            ),
        )
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "task card"):
            memory_handoff.prepare_resume_envelope(
                task_card=changed_card,
                lane_id="lane-1",
                run_id="run-2",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        self.assertEqual(envelope, memory_handoff.load_envelope(self.worktree))

        all_off_card, _ = _task_card({"all_features": False})
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "non-dispatchable"):
            memory_handoff.prepare_resume_envelope(
                task_card=all_off_card,
                lane_id="lane-1",
                run_id="run-2",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        self.assertEqual(envelope, memory_handoff.load_envelope(self.worktree))

        changed = json.loads(json.dumps(envelope))
        changed["mandatory_content"][0]["content"] = "different task payload"
        changed["mandatory_digest"] = contracts.sha256_hex(changed["mandatory_content"])
        changed["content_hash"] = contracts.content_hash(changed)
        _, envelope_path = memory_handoff.memory_paths(self.worktree)
        envelope_path.write_text(json.dumps(changed), encoding="utf-8")
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "finalized mandatory task|finalized context"):
            memory_handoff.prepare_resume_envelope(
                task_card=self.card,
                lane_id="lane-1",
                run_id="run-2",
                worktree_path=self.worktree,
                base_commit="base-1",
            )
        self.assertEqual(changed, memory_handoff.load_envelope(self.worktree))

    def test_copied_checkpoint_card_cannot_launch_or_resume_saved_context(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card, lane_id="lane-1", run_id="run-1",
            worktree_path=self.worktree, base_commit="base-1",
        )
        copied_card = contracts.make_task_card(
            task=self.card["task"], base_commit="base-1", branch=self.card["branch"],
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=self.plan,
                checkpoint="different-root-checkpoint",
            ),
        )
        with self.assertRaises(memory_handoff.MemoryHandoffError):
            memory_handoff.validate_envelope_for_launch(
                envelope=envelope, task_card=copied_card, lane_id="lane-1",
                run_id="run-1", worktree_path=self.worktree, base_commit="base-1",
            )
        with self.assertRaises(memory_handoff.MemoryHandoffError):
            memory_handoff.prepare_resume_envelope(
                task_card=copied_card, lane_id="lane-1", run_id="run-2",
                worktree_path=self.worktree, base_commit="base-1",
            )
        self.assertEqual(envelope, memory_handoff.load_envelope(self.worktree))

    def test_dispatch_is_idempotent_and_lost_ack_blocks_duplicate(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        observed = {"invocation_id": "controller-1", "pid": 123, "creation_time": "now"}
        delivered = memory_handoff.dispatch(
            worktree_path=self.worktree,
            envelope=envelope,
            launcher=lambda record: observed,
        )
        self.assertEqual("delivered", delivered["status"])
        replay = memory_handoff.dispatch(
            worktree_path=self.worktree,
            envelope=envelope,
            launcher=lambda record: self.fail("duplicate launcher call"),
        )
        self.assertEqual(delivered["operation_id"], replay["operation_id"])

        ambiguous_worktree = self.root / "ambiguous"
        ambiguous_worktree.mkdir()
        ambiguous_envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-2",
            run_id="run-2",
            worktree_path=ambiguous_worktree,
            base_commit="base-1",
        )
        calls: list[object] = []

        def launcher(record: dict) -> None:
            calls.append(record)
            return None

        with self.assertRaisesRegex(runtime.DispatchAmbiguityError, "ambiguous"):
            memory_handoff.dispatch(
                worktree_path=ambiguous_worktree,
                envelope=ambiguous_envelope,
                launcher=launcher,
            )
        with self.assertRaisesRegex(runtime.DispatchAmbiguityError, "ambiguous"):
            memory_handoff.dispatch(
                worktree_path=ambiguous_worktree,
                envelope=ambiguous_envelope,
                launcher=launcher,
            )
        self.assertEqual(1, len(calls))

    def test_split_launch_intent_cannot_overwrite_ambiguous_dispatch(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree,
            envelope=envelope,
        )
        memory_handoff.record_ambiguous_dispatch(
            worktree_path=self.worktree,
            envelope=envelope,
        )
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "ambiguous"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree,
                envelope=envelope,
            )

    def test_conflicting_envelope_cannot_create_another_intent_for_one_run(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        first = memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=envelope
        )
        changed = dict(envelope)
        changed["optional_content"] = [
            {"id": "later", "kind": "memory", "content": "changed"}
        ]
        changed["content_hash"] = contracts.content_hash(changed)
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "conflicting"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=changed
            )
        memory_store = store.MemoryStore(memory_handoff.memory_paths(self.worktree)[0])
        memory_store.initialize()
        try:
            self.assertEqual([first], memory_store.list_operations(envelope["decision_id"]))
        finally:
            memory_store.close()

    def test_changed_decision_cannot_create_another_intent_for_one_run(self) -> None:
        envelope = memory_handoff.prepare_bootstrap_envelope(
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.worktree,
            base_commit="base-1",
        )
        memory_handoff.record_dispatch_intent(
            worktree_path=self.worktree, envelope=envelope
        )
        changed = dict(envelope)
        changed["decision_id"] = "another-decision"
        changed["content_hash"] = contracts.content_hash(changed)
        with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "conflicting"):
            memory_handoff.record_dispatch_intent(
                worktree_path=self.worktree, envelope=changed
            )

    def test_optional_omission_preserves_plan_and_updates_delivery_trace(self) -> None:
        card_with_optional = contracts.make_task_card(
            task="Fix the regression",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1",
                route="ordinary",
                plan=self.plan,
            ),
        )
        # Add one optional item by preparing through the runtime contract.
        from memory_harness import store as memory_store_module

        store_path, _ = memory_handoff.memory_paths(self.worktree)
        memory_store = memory_store_module.MemoryStore(store_path)
        memory_store.initialize()
        try:
            memory_runtime = runtime.MemoryRuntime(memory_store)
            prepared = memory_runtime.prepare(
                plan=self.plan,
                task_card=card_with_optional,
                lane_id="lane-1",
                run_id="run-1",
                worktree_path=self.worktree,
                base_commit="base-1",
                optional_content=[
                    {"id": "history", "kind": "experience", "content": "prior failure"}
                ],
            )
        finally:
            memory_store.close()
        envelope = prepared.envelope
        revised = memory_handoff.omit_optional_content(
            worktree_path=self.worktree,
            envelope=envelope,
            item_id="history",
        )
        self.assertEqual([], revised["optional_content"])
        self.assertIn("history", revised["delivery"]["omitted"])
        self.assertEqual(envelope["plan_id"], revised["plan_id"])
        self.assertEqual(envelope["plan_digest"], revised["plan_digest"])

    def test_enriched_optional_omission_fails_before_file_or_store_mutation(self) -> None:
        outcome = _finalized_lane1_fixture(self.card, self.plan, self.worktree)
        _record_domain_fixture(self.worktree, outcome)
        with patch.object(memory_handoff, "_prepare_memory_outcome", return_value=outcome):
            envelope = memory_handoff.prepare_bootstrap_envelope(
                task_card=self.card, lane_id="lane-1", run_id="run-1",
                worktree_path=self.worktree, base_commit="base-1",
            )
        store_path, envelope_path = memory_handoff.memory_paths(self.worktree)
        before_file = envelope_path.read_bytes()
        before_context = _stored_final_context(self.worktree, outcome.context["context_id"])
        before_store = store_path.read_bytes()
        for candidate in (envelope, outcome.envelope):
            with self.subTest(candidate_is_bound=candidate is envelope):
                with self.assertRaisesRegex(memory_handoff.MemoryHandoffError, "domain finalizer"):
                    memory_handoff.omit_optional_content(
                        worktree_path=self.worktree, envelope=candidate, item_id="case-1"
                    )
                self.assertEqual(before_file, envelope_path.read_bytes())
                self.assertEqual(before_store, store_path.read_bytes())
                self.assertEqual(
                    before_context,
                    _stored_final_context(self.worktree, outcome.context["context_id"]),
                )


if __name__ == "__main__":
    unittest.main()
