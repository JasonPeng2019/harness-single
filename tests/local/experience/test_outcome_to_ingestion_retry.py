"""Faulted reviewed-trajectory ingestion at the existing EverOS boundary.

These fixtures use the accepted local outcome and review contracts. Joined
lane-1 effect scheduling is outside this selector.
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import config, context, contracts, experience, privacy, store
from tests.local.contracts import test_terminal_outcome as native_fixture


SECRET = "synthetic-secret-alpha-1234567890"


class _EverOSSurface:
    def __init__(self) -> None:
        self.memorize_calls: list[dict] = []
        self.search_calls: list[dict] = []
        self.case_results: list[dict] = []
        self.intent_reader: Callable[[], dict | None] | None = None
        self.intent_at_call: dict | None = None
        self.lose_ack = False
        self.readback_unavailable = False
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.memorize_calls.append(dict(payload))
        if self.intent_reader is not None:
            self.intent_at_call = self.intent_reader()
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            await self.release.wait()
        if self.lose_ack:
            raise ConnectionError(f"acknowledgement lost after commit: {SECRET}")
        return {"status": "extracted"}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, request: dict) -> dict:
        self.search_calls.append(request)
        if self.readback_unavailable:
            raise ConnectionError("EverOS readback is unavailable")
        return {"data": {"agent_cases": list(self.case_results), "agent_skills": []}}


class OutcomeToIngestionRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store_path = self.root / "memory.sqlite3"
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()
        self.addCleanup(lambda: self.memory_store.close())
        self.policy = privacy.PrivacyPolicy(known_secrets=(SECRET,))
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="retry-fixture",
            owner="root-agent",
        )
        self.service = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )

    def _capture(self) -> dict:
        plan = contracts.make_plan(
            plan_id="retry-plan",
            objective_id="retry-objective",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Repair parser failure",
            base_commit="retry-base",
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
            evidence_digest="retry-evidence",
            linked_run_id="retry-run",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="retry-review",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://retry-run",),
            protected_source_refs=("evidence://protected/parser",),
            raw_evidence=f"The parser repair failed; the trace contains {SECRET}.",
            failed_hypotheses=("the network caused the parser failure",),
        )
        return self.service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=self.scope,
        )

    def _adapter(self, surface: _EverOSSurface) -> experience.EverOSAdapter:
        base_root = self.root / "everos"
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            base_root, self.scope
        )
        return experience.EverOSAdapter(
            scope=self.scope,
            base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                surface,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
            privacy_policy=self.policy,
        )

    def _restart(self) -> None:
        self.memory_store.close()
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()
        self.service = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )

    @staticmethod
    def _case(adapter: experience.EverOSAdapter, session_id: str) -> dict:
        return {
            "id": "retry-case-1",
            "agent_id": adapter.everos_owner_id,
            "app_id": adapter.everos_application_id,
            "project_id": adapter.everos_project_id,
            "session_id": session_id,
            "task_intent": "repair parser failure",
            "approach": "inspect the failed parser check",
            "quality_score": 0.0,
            "key_insight": "the network hypothesis was disproved",
            "timestamp": "2026-09-21T00:00:00Z",
            "score": 1.0,
        }

    def test_intent_precedes_adapter_call_and_identity_survives_restart(self) -> None:
        trajectory = self._capture()
        surface = _EverOSSurface()
        surface.intent_reader = lambda: self.memory_store.get_experience_ingestion_for_trajectory(
            trajectory["trajectory_id"]
        )
        adapter = self._adapter(surface)

        first = asyncio.run(self.service.extract_trajectory(trajectory["trajectory_id"], adapter))

        self.assertEqual("pending", first["status"])
        self.assertEqual("pending", surface.intent_at_call["status"])
        self.assertEqual(0, surface.intent_at_call["version"])
        self.assertEqual(first["ingestion_id"], surface.intent_at_call["ingestion_id"])
        self.assertEqual(first["session_id"], surface.memorize_calls[0]["session_id"])
        self.assertEqual(first["payload_digest"], contracts.sha256_hex(surface.memorize_calls[0]))
        self.assertNotIn(SECRET, str(surface.memorize_calls[0]))

        self._restart()
        restored = self.service.get_trajectory(trajectory["trajectory_id"])
        recovery_surface = _EverOSSurface()
        recovery_adapter = self._adapter(recovery_surface)
        rebuilt_payload = recovery_adapter.add_payload(
            restored, session_id=first["session_id"]
        )
        replay = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], recovery_adapter)
        )
        self.assertEqual(first, replay)
        self.assertEqual(first["payload_digest"], contracts.sha256_hex(rebuilt_payload))
        self.assertEqual([], recovery_surface.memorize_calls)

    def test_lost_acknowledgement_is_reconciled_by_replay_readback(self) -> None:
        trajectory = self._capture()
        failed_surface = _EverOSSurface()
        failed_surface.lose_ack = True
        uncertain = asyncio.run(
            self.service.extract_trajectory(
                trajectory["trajectory_id"], self._adapter(failed_surface)
            )
        )
        self.assertEqual("uncertain", uncertain["status"])
        self.assertEqual(1, len(failed_surface.memorize_calls))
        self.assertNotIn(SECRET, uncertain["error"])

        self._restart()
        recovery_surface = _EverOSSurface()
        recovery_adapter = self._adapter(recovery_surface)
        recovery_surface.case_results = [self._case(recovery_adapter, "different-session")]
        unresolved = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], recovery_adapter)
        )
        self.assertEqual("uncertain", unresolved["status"])
        self.assertEqual(1, len(recovery_surface.search_calls))
        self.assertEqual([], recovery_surface.memorize_calls)

        recovery_surface.case_results = [
            self._case(recovery_adapter, uncertain["session_id"])
        ]
        confirmed = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], recovery_adapter)
        )
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(uncertain["ingestion_id"], confirmed["ingestion_id"])
        self.assertEqual(uncertain["session_id"], confirmed["session_id"])
        self.assertEqual(uncertain["payload_digest"], confirmed["payload_digest"])
        self.assertEqual(["retry-case-1"], confirmed["case_ids"])
        self.assertEqual(2, len(recovery_surface.search_calls))
        self.assertEqual([], recovery_surface.memorize_calls)
        receipt = self.memory_store.get_case_receipt(self.scope.to_record(), "retry-case-1")
        self.assertEqual(trajectory["review_receipt_id"], receipt["review_receipt_id"])
        self.assertEqual([], self.service.search_recent_evidence(self.scope, "parser"))

    def test_concurrent_and_confirmed_replays_submit_once(self) -> None:
        trajectory = self._capture()
        source = _EverOSSurface()
        adapter = self._adapter(source)

        async def exercise() -> None:
            source.entered = asyncio.Event()
            source.release = asyncio.Event()
            first_task = asyncio.create_task(
                self.service.extract_trajectory(trajectory["trajectory_id"], adapter)
            )
            await source.entered.wait()
            concurrent = await self.service.extract_trajectory(
                trajectory["trajectory_id"], adapter
            )
            self.assertEqual("pending", concurrent["status"])
            self.assertEqual(1, len(source.memorize_calls))
            source.release.set()
            first = await first_task
            self.assertEqual(concurrent["ingestion_id"], first["ingestion_id"])

            source.case_results = [self._case(adapter, first["session_id"])]
            confirmed, replayed = await asyncio.gather(
                self.service.extract_trajectory(trajectory["trajectory_id"], adapter),
                self.service.extract_trajectory(trajectory["trajectory_id"], adapter),
            )
            self.assertEqual("confirmed", confirmed["status"])
            self.assertEqual(confirmed, replayed)
            self.assertEqual(1, len(source.memorize_calls))

        asyncio.run(exercise())

    def test_local_sanitized_reviewed_evidence_survives_everos_outage(self) -> None:
        trajectory = self._capture()
        unavailable = _EverOSSurface()
        unavailable.lose_ack = True
        unavailable.readback_unavailable = True
        uncertain = asyncio.run(
            self.service.extract_trajectory(
                trajectory["trajectory_id"], self._adapter(unavailable)
            )
        )
        self.assertEqual("uncertain", uncertain["status"])

        self._restart()
        recovery = _EverOSSurface()
        recovery.readback_unavailable = True
        replay = asyncio.run(
            self.service.extract_trajectory(trajectory["trajectory_id"], self._adapter(recovery))
        )
        self.assertEqual("uncertain", replay["status"])
        self.assertEqual([], recovery.memorize_calls)
        evidence = self.service.search_recent_evidence(self.scope, "parser")
        self.assertEqual(1, len(evidence))
        self.assertEqual(trajectory["trajectory_id"], evidence[0]["id"])
        self.assertEqual("historical_evidence", evidence[0]["kind"])
        self.assertEqual(trajectory["review_receipt_id"], evidence[0]["review_receipt_id"])
        self.assertIn("parser repair failed", evidence[0]["content"])
        self.assertNotIn(SECRET, str(evidence))

    def test_legacy_confirmed_case_retains_two_distinct_skills(self) -> None:
        trajectory = self._capture()
        surface = _EverOSSurface()
        adapter = self._adapter(surface)
        pending = asyncio.run(self.service.extract_trajectory(trajectory["trajectory_id"], adapter))
        surface.case_results = [self._case(adapter, pending["session_id"])]
        self.assertEqual("confirmed", asyncio.run(self.service.reconcile_extraction(
            trajectory["trajectory_id"], adapter
        ))["status"])
        skills = [
            {
                "id": skill_id, "agent_id": adapter.everos_owner_id,
                "app_id": adapter.everos_application_id,
                "project_id": adapter.everos_project_id,
                "content": content, "source_case_ids": ["retry-case-1"],
            }
            for skill_id, content in (
                ("legacy-skill-a", "Investigate the parser."),
                ("legacy-skill-b", "Check the parser lock."),
            )
        ]
        candidates = [self.service.resolve_generated_skill_candidate(skill, adapter) for skill in skills]
        self.assertNotEqual(candidates[0]["candidate_id"], candidates[1]["candidate_id"])
        self.assertEqual(
            {candidate["candidate_id"] for candidate in candidates},
            {item["candidate_id"] for skill in skills for item in
             self.memory_store.read_generated_skill_candidates_for_scope(skill["id"], self.scope.to_record())},
        )
        self.assertEqual(candidates, [
            self.service.resolve_generated_skill_candidate(skill, adapter) for skill in skills
        ])


class JoinedNativeEffectTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case = native_fixture.TerminalOutcomeTests(
            methodName="test_exact_observed_native_chain_fixes_pass"
        )
        self.case.setUp()

    def tearDown(self) -> None:
        self.case.tearDown()

    def _native(
        self, *, captured_config: dict | None = None
    ) -> tuple[dict, dict, experience.ExperienceScope, experience.ReviewedExperienceService]:
        case = self.case
        if captured_config is not None:
            case.configuration = captured_config
            case.decision = contracts.make_decision(
                case.card, case.plan, configuration=captured_config
            )
            case.state.record_decision(case.decision)
            case.final = context.finalize_context(
                task_card=case.card, plan=case.plan,
                decision_id=case.decision["decision_id"], lane_id=case.lane_id,
                run_id=case.run_id, worktree_path=str(case.root / "worktree"),
                base_commit="base-1", strategy="standard",
                configuration=captured_config, checkpoint="checkpoint-1",
                execution_role="worker", invocation_target="harness:worker",
                recipient="worker:lane-1",
                mandatory_content=case.final.envelope["mandatory_content"],
                limits=config.resolve_limits({"context_char_limit": 4000}),
            )
            case.state.record_final_context(
                case.final.context, envelope_digest=case.final.envelope["content_hash"]
            )
        case.observe()
        fixed = case.runtime.record_terminal_outcome(case.bundle())
        outcome = contracts.make_outcome(
            decision_id=fixed["decision_id"], plan_id=fixed["plan_id"],
            plan_digest=fixed["plan_digest"], status=fixed["status"],
            evidence_digest=fixed["evidence_digest"], linked_run_id=fixed["linked_run_id"],
            task_card_digest=fixed["task_card_digest"], objective_id=fixed["objective_id"],
            observed_at=fixed["observed_at"],
        )
        review = contracts.make_review_receipt(
            review_id="joined-review", outcome=outcome, decision=case.decision,
            task_card=case.card, plan=case.plan, reviewed_by="ROOT",
            evidence_refs=("review://joined",), raw_evidence="ROOT reviewed exact native run.",
        )
        scope = experience.ExperienceScope(
            application="harness", project="product", namespace="isolated", owner="root-agent"
        )
        service = experience.ReviewedExperienceService(case.state)
        trajectory = service.capture(
            task_card=case.card, plan=case.plan, decision=case.decision,
            outcome=outcome, review_receipt=review, scope=scope,
        )
        return outcome, trajectory, scope, service

    def _adapter(self, scope: experience.ExperienceScope, surface: _EverOSSurface) -> experience.EverOSAdapter:
        base_root = self.case.root / "everos"
        memory_root = experience.EverOSAdapter.memory_root_for_scope(base_root, scope)
        return experience.EverOSAdapter(
            scope=scope, base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                surface, memory_root=memory_root, resolve_memory_root=lambda: memory_root,
            ),
        )

    def test_native_ingestion_claims_exact_effect_before_everos_call(self) -> None:
        case = self.case
        outcome, trajectory, scope, service = self._native()
        operation_id = contracts.effect_operation_id(outcome["outcome_id"], "experience_ingestion")
        observed = []

        class Surface(_EverOSSurface):
            async def memorize(self, payload: dict, **kwargs: object) -> dict:
                observed.append(case.state.get_effect_operation(operation_id))
                return await super().memorize(payload, **kwargs)

        surface = Surface()
        adapter = self._adapter(scope, surface)
        ingestion = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        self.assertEqual(1, len(surface.memorize_calls))
        self.assertEqual("in_flight", observed[0]["status"])
        self.assertEqual(trajectory, observed[0]["source_record"])
        self.assertEqual(surface.memorize_calls[0], observed[0]["payload_record"])
        self.assertEqual(ingestion["ingestion_id"], observed[0]["adapter_operation_id"])
        search_count = len(surface.search_calls)
        self.assertEqual(ingestion, asyncio.run(service.extract_trajectory(
            trajectory["trajectory_id"], adapter,
            current_config=config.resolve_config({"experience_write": False}),
        )))
        self.assertEqual(1, len(surface.memorize_calls))
        self.assertEqual(search_count, len(surface.search_calls))

    def test_native_lost_ack_restart_reuses_effect_and_ingestion_identity(self) -> None:
        outcome, trajectory, scope, service = self._native()
        surface = _EverOSSurface()
        surface.lose_ack = True
        adapter = self._adapter(scope, surface)
        uncertain = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        operation_id = contracts.effect_operation_id(outcome["outcome_id"], "experience_ingestion")
        original = self.case.state.get_effect_operation(operation_id)
        self.assertEqual("uncertain", original["status"])
        self.assertEqual(uncertain["ingestion_id"], original["adapter_operation_id"])
        self.assertEqual(uncertain, asyncio.run(service.extract_trajectory(
            trajectory["trajectory_id"], adapter,
            current_config=config.resolve_config({"experience_write": False}),
        )))
        self.assertEqual("uncertain", self.case.state.get_effect_operation(operation_id)["status"])
        self.assertEqual(1, len(surface.memorize_calls))
        self.case.state.close()
        self.case.state = store.MemoryStore(self.case.root / "state.sqlite3")
        self.case.state.initialize()
        service = experience.ReviewedExperienceService(self.case.state)
        recovery = _EverOSSurface()
        adapter = self._adapter(scope, recovery)
        recovery.case_results = [OutcomeToIngestionRetryTests._case(adapter, uncertain["session_id"])]
        confirmed = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        effect = self.case.state.get_effect_operation(operation_id)
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(original["operation_id"], effect["operation_id"])
        self.assertEqual(original["payload_record"], effect["payload_record"])
        self.assertEqual(original["adapter_operation_id"], effect["adapter_operation_id"])
        self.assertEqual([], recovery.memorize_calls)
        self.assertNotIn(operation_id, {
            item["operation_id"] for item in self.case.state.list_effect_operations(
                outcome["outcome_id"], actionable_only=True
            )
        })
        self.assertEqual(trajectory, service.get_trajectory(trajectory["trajectory_id"]))

    def test_current_write_off_preserves_native_local_evidence(self) -> None:
        outcome, trajectory, scope, service = self._native()
        surface = _EverOSSurface()
        adapter = self._adapter(scope, surface)
        off = config.resolve_config({"experience_write": False})
        self.assertIsNone(asyncio.run(service.extract_trajectory(
            trajectory["trajectory_id"], adapter, current_config=off
        )))
        self.assertEqual([], surface.memorize_calls)
        self.assertIsNone(self.case.state.get_experience_ingestion_for_trajectory(
            trajectory["trajectory_id"]
        ))
        effects = {effect["kind"]: effect for effect in self.case.state.list_effect_operations(
            outcome["outcome_id"]
        )}
        self.assertEqual("confirmed", effects["review_receipt"]["status"])
        self.assertEqual("confirmed", effects["recent_evidence"]["status"])
        self.assertEqual("waiting_payload", effects["experience_ingestion"]["status"])
        self.assertEqual(trajectory, effects["recent_evidence"]["source_record"])

    def test_captured_write_off_keeps_native_review_without_optional_work(self) -> None:
        outcome, trajectory, scope, service = self._native(captured_config={
            "strategy": "standard", "experience_write": False,
        })
        surface = _EverOSSurface()
        self.assertIsNone(asyncio.run(service.extract_trajectory(
            trajectory["trajectory_id"], self._adapter(scope, surface)
        )))
        self.assertEqual([], surface.memorize_calls)
        effects = self.case.state.list_effect_operations(outcome["outcome_id"])
        self.assertEqual({"review_receipt", "recent_evidence"}, {
            effect["kind"] for effect in effects
        })
        self.assertTrue(all(effect["status"] == "confirmed" for effect in effects))

    def test_captured_generation_off_keeps_ingestion_but_no_candidate_effect(self) -> None:
        outcome, trajectory, scope, service = self._native(captured_config={
            "strategy": "standard", "experience_write": True,
            "generated_skill_creation": False,
        })
        surface = _EverOSSurface()
        adapter = self._adapter(scope, surface)
        pending = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        self.assertEqual("pending", pending["status"])
        surface.case_results = [OutcomeToIngestionRetryTests._case(adapter, pending["session_id"])]
        asyncio.run(service.reconcile_extraction(trajectory["trajectory_id"], adapter))
        self.assertIsNone(service.resolve_generated_skill_candidate({
            "id": "captured-off-skill", "agent_id": adapter.everos_owner_id,
            "app_id": adapter.everos_application_id,
            "project_id": adapter.everos_project_id,
            "content": "Retain reviewed evidence.",
            "source_case_ids": ["retry-case-1"],
        }, adapter))
        self.assertEqual({"review_receipt", "recent_evidence", "experience_ingestion"}, {
            effect["kind"] for effect in self.case.state.list_effect_operations(outcome["outcome_id"])
        })

    def test_generated_candidate_uses_same_native_effect_and_current_off_gate(self) -> None:
        outcome, trajectory, scope, service = self._native()
        surface = _EverOSSurface()
        adapter = self._adapter(scope, surface)
        pending = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        surface.case_results = [OutcomeToIngestionRetryTests._case(adapter, pending["session_id"])]
        asyncio.run(service.reconcile_extraction(trajectory["trajectory_id"], adapter))
        skill = {
            "id": "joined-skill", "agent_id": adapter.everos_owner_id,
            "app_id": adapter.everos_application_id,
            "project_id": adapter.everos_project_id,
            "content": "Use the reviewed parser evidence.",
            "source_case_ids": ["retry-case-1"],
        }
        operation_id = contracts.effect_operation_id(outcome["outcome_id"], "generated_skill_creation")
        off = config.resolve_config({"generated_skill_creation": False})
        self.assertIsNone(service.resolve_generated_skill_candidate(
            skill, adapter, current_config=off
        ))
        self.assertIsNone(service.resolve_generated_skill_candidate(
            skill, adapter, current_config=config.resolve_config({"experience_write": False})
        ))
        self.assertEqual("waiting_payload", self.case.state.get_effect_operation(operation_id)["status"])
        candidate = service.resolve_generated_skill_candidate(skill, adapter)
        effect = self.case.state.get_effect_operation(operation_id)
        self.assertEqual("proposed", candidate["state"])
        self.assertEqual("confirmed", effect["status"])
        self.assertEqual(
            {"source_trajectory_id": trajectory["trajectory_id"]},
            effect["payload_record"],
        )
        self.assertEqual(effect["payload_record"], effect["acknowledgement"])
        self.assertEqual(trajectory, effect["source_record"])
        self.assertEqual(candidate, service.resolve_generated_skill_candidate(skill, adapter))
        self.assertEqual("confirmed", self.case.state.get_effect_operation(operation_id)["status"])

    def test_two_generated_skills_share_fixed_native_outcome_effect(self) -> None:
        outcome, trajectory, scope, service = self._native()
        surface = _EverOSSurface()
        adapter = self._adapter(scope, surface)
        pending = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        surface.case_results = [
            OutcomeToIngestionRetryTests._case(adapter, pending["session_id"])
        ]
        self.assertEqual("confirmed", asyncio.run(service.reconcile_extraction(
            trajectory["trajectory_id"], adapter
        ))["status"])
        receipt = self.case.state.get_case_receipt(scope.to_record(), "retry-case-1")
        original_outcome = self.case.state.get_outcome(outcome["decision_id"])
        original_trajectory = service.get_trajectory(trajectory["trajectory_id"])
        skills = [
            {
                "id": skill_id, "agent_id": adapter.everos_owner_id,
                "app_id": adapter.everos_application_id,
                "project_id": adapter.everos_project_id,
                "content": content, "source_case_ids": ["retry-case-1"],
            }
            for skill_id, content in (
                ("native-skill-a", "Investigate the parser."),
                ("native-skill-b", "Check the parser lock."),
            )
        ]
        operation_id = contracts.effect_operation_id(outcome["outcome_id"], "generated_skill_creation")
        first = service.resolve_generated_skill_candidate(skills[0], adapter)
        first_effect = self.case.state.get_effect_operation(operation_id)
        for switch in ("experience_write", "generated_skill_creation"):
            self.assertIsNone(service.resolve_generated_skill_candidate(
                skills[1], adapter, current_config=config.resolve_config({switch: False})
            ))
        self.assertEqual([], self.case.state.read_generated_skill_candidates_for_scope(
            skills[1]["id"], scope.to_record()
        ))
        self.assertEqual(first_effect, self.case.state.get_effect_operation(operation_id))
        second = service.resolve_generated_skill_candidate(skills[1], adapter)
        effect = self.case.state.get_effect_operation(operation_id)
        self.assertEqual("confirmed", effect["status"])
        self.assertEqual(first_effect, effect)
        self.assertEqual(
            {"source_trajectory_id": trajectory["trajectory_id"]}, effect["payload_record"]
        )
        self.assertEqual(first_effect["payload_record"], effect["payload_record"])
        self.assertEqual(first_effect["acknowledgement"], effect["acknowledgement"])
        self.assertEqual(trajectory, effect["source_record"])
        self.assertEqual(1, len([
            item for item in self.case.state.list_effect_operations(outcome["outcome_id"])
            if item["kind"] == "generated_skill_creation"
        ]))
        self.assertNotEqual(first["candidate_id"], second["candidate_id"])
        for skill, candidate in zip(skills, (first, second)):
            self.assertEqual("proposed", candidate["state"])
            self.assertEqual([{
                "case_id": "retry-case-1",
                "case_receipt_id": receipt["case_receipt_id"],
                "trajectory_id": trajectory["trajectory_id"],
                "review_receipt_id": trajectory["review_receipt_id"],
                "review_receipt_digest": trajectory["review_receipt_digest"],
            }], candidate["source_cases"])
            self.assertEqual(contracts.make_generated_skill_candidate(
                scope=scope.to_record(), skill_id=skill["id"],
                content=skill["content"], source_cases=candidate["source_cases"],
            )["candidate_id"], candidate["candidate_id"])
            self.assertEqual(candidate, self.case.state.get_generated_skill_candidate(
                candidate["candidate_id"]
            ))
            self.assertEqual(candidate, service.resolve_generated_skill_candidate(skill, adapter))
        self.assertEqual(original_outcome, self.case.state.get_outcome(outcome["decision_id"]))
        self.assertEqual(original_trajectory, service.get_trajectory(trajectory["trajectory_id"]))

    def test_old_candidate_specific_effect_recovers_then_accepts_another_skill(self) -> None:
        outcome, trajectory, scope, service = self._native()
        surface = _EverOSSurface()
        adapter = self._adapter(scope, surface)
        pending = asyncio.run(service.extract_trajectory(trajectory["trajectory_id"], adapter))
        surface.case_results = [OutcomeToIngestionRetryTests._case(adapter, pending["session_id"])]
        asyncio.run(service.reconcile_extraction(trajectory["trajectory_id"], adapter))
        receipt = self.case.state.get_case_receipt(scope.to_record(), "retry-case-1")
        source = [{
            "case_id": receipt["case_id"],
            "case_receipt_id": receipt["case_receipt_id"],
            "trajectory_id": trajectory["trajectory_id"],
            "review_receipt_id": trajectory["review_receipt_id"],
            "review_receipt_digest": trajectory["review_receipt_digest"],
        }]
        first = contracts.make_generated_skill_candidate(
            scope=scope.to_record(), skill_id="old-skill", content="Check the parser.",
            source_cases=source,
        )
        operation_id = contracts.effect_operation_id(outcome["outcome_id"], "generated_skill_creation")
        self.case.state.bind_effect_source(operation_id, trajectory["trajectory_id"], trajectory)
        self.case.state.bind_effect_payload(operation_id, {
            key: first[key] for key in
            ("candidate_id", "skill_id", "content_digest", "source_cases", "metadata")
        })
        self.case.state.claim_effect_operation(operation_id, current_config=config.MemoryConfig())
        first_skill = {
            "id": "old-skill", "agent_id": adapter.everos_owner_id,
            "app_id": adapter.everos_application_id,
            "project_id": adapter.everos_project_id,
            "content": "Check the parser.", "source_case_ids": ["retry-case-1"],
        }
        self.assertEqual(first["candidate_id"], service.resolve_generated_skill_candidate(
            first_skill, adapter
        )["candidate_id"])
        original_effect = self.case.state.get_effect_operation(operation_id)
        self.assertEqual({"candidate_id": first["candidate_id"]}, original_effect["acknowledgement"])
        skill = {
            "id": "new-skill", "agent_id": adapter.everos_owner_id,
            "app_id": adapter.everos_application_id,
            "project_id": adapter.everos_project_id,
            "content": "Investigate parser history.", "source_case_ids": ["retry-case-1"],
        }
        second = service.resolve_generated_skill_candidate(skill, adapter)
        self.assertNotEqual(first["candidate_id"], second["candidate_id"])
        self.assertEqual("proposed", second["state"])
        self.assertEqual(original_effect, self.case.state.get_effect_operation(operation_id))
        self.assertEqual(second, service.resolve_generated_skill_candidate(skill, adapter))

    def test_generated_effect_identity_reopen_matrix(self) -> None:
        # Each row has a fresh fixed outcome and SQLite file. The legacy rows
        # model the payload left by the original candidate-specific writer.
        rows = [("umbrella", "new-first")]
        rows += [
            (status, order)
            for status in ("pending", "in_flight", "uncertain")
            for order in ("old-first", "different-first")
        ]
        rows.append(("confirmed", "different-first"))
        for index, (status, order) in enumerate(rows):
            with self.subTest(status=status, order=order):
                if index:
                    self.case.tearDown()
                    self.case = native_fixture.TerminalOutcomeTests(
                        methodName="test_exact_observed_native_chain_fixes_pass"
                    )
                    self.case.setUp()
                outcome, trajectory, scope, service = self._native()
                surface = _EverOSSurface()
                adapter = self._adapter(scope, surface)
                ingestion = asyncio.run(service.extract_trajectory(
                    trajectory["trajectory_id"], adapter
                ))
                surface.case_results = [OutcomeToIngestionRetryTests._case(
                    adapter, ingestion["session_id"]
                )]
                asyncio.run(service.reconcile_extraction(trajectory["trajectory_id"], adapter))
                receipt = self.case.state.get_case_receipt(scope.to_record(), "retry-case-1")
                source = [{
                    "case_id": receipt["case_id"],
                    "case_receipt_id": receipt["case_receipt_id"],
                    "trajectory_id": trajectory["trajectory_id"],
                    "review_receipt_id": trajectory["review_receipt_id"],
                    "review_receipt_digest": trajectory["review_receipt_digest"],
                }]
                skills = [
                    {
                        "id": skill_id, "agent_id": adapter.everos_owner_id,
                        "app_id": adapter.everos_application_id,
                        "project_id": adapter.everos_project_id,
                        "content": content, "source_case_ids": ["retry-case-1"],
                    }
                    for skill_id, content in (
                        ("old-skill", "Check the parser."),
                        ("different-skill", "Investigate parser history."),
                    )
                ]
                expected = [contracts.make_generated_skill_candidate(
                    scope=scope.to_record(), skill_id=skill["id"],
                    content=skill["content"], source_cases=source,
                ) for skill in skills]
                operation_id = contracts.effect_operation_id(
                    outcome["outcome_id"], "generated_skill_creation"
                )
                if status != "umbrella":
                    self.case.state.bind_effect_source(
                        operation_id, trajectory["trajectory_id"], trajectory
                    )
                    self.case.state.bind_effect_payload(operation_id, {
                        key: expected[0][key] for key in (
                            "candidate_id", "skill_id", "content_digest",
                            "source_cases", "metadata",
                        )
                    })
                    if status != "pending":
                        self.case.state.claim_effect_operation(
                            operation_id, current_config=config.MemoryConfig()
                        )
                    if status == "uncertain":
                        self.case.state.mark_effect_uncertain(
                            operation_id, "acknowledgement unavailable"
                        )
                    if status == "confirmed":
                        self.case.state.record_generated_skill_candidate(expected[0])
                        self.case.state.confirm_effect_operation(
                            operation_id, {"candidate_id": expected[0]["candidate_id"]}
                        )
                    self.case.state.close()
                    self.case.state = store.MemoryStore(self.case.root / "state.sqlite3")
                    self.case.state.initialize()
                    service = experience.ReviewedExperienceService(self.case.state)
                before = self.case.state.get_effect_operation(operation_id)
                original_outcome = self.case.state.get_outcome(outcome["decision_id"])
                if order == "different-first":
                    if status == "in_flight":
                        with self.assertRaises(experience.ProvenanceError):
                            service.resolve_generated_skill_candidate(
                                {**skills[1], "source_case_ids": ["foreign-case"]}, adapter
                            )
                        with self.assertRaises(experience.ScopeBoundaryError):
                            service.resolve_generated_skill_candidate(
                                {**skills[1], "agent_id": "foreign-owner"}, adapter
                            )
                    for switch in ("experience_write", "generated_skill_creation"):
                        self.assertIsNone(service.resolve_generated_skill_candidate(
                            skills[1], adapter,
                            current_config=config.resolve_config({switch: False}),
                        ))
                    self.assertEqual(before, self.case.state.get_effect_operation(operation_id))
                    self.assertEqual([], self.case.state.read_generated_skill_candidates_for_scope(
                        skills[1]["id"], scope.to_record()
                    ))
                    different = service.resolve_generated_skill_candidate(skills[1], adapter)
                    self.assertEqual(expected[1]["candidate_id"], different["candidate_id"])
                    self.assertEqual(source, different["source_cases"])
                    self.assertEqual("proposed", different["state"])
                    self.assertEqual(before, self.case.state.get_effect_operation(operation_id))
                    if status != "confirmed":
                        self.assertIn(operation_id, {
                            effect["operation_id"] for effect in
                            self.case.state.list_effect_operations(
                                outcome["outcome_id"], actionable_only=True
                            )
                        })
                    old = service.resolve_generated_skill_candidate(skills[0], adapter)
                else:
                    old = service.resolve_generated_skill_candidate(skills[0], adapter)
                    different = service.resolve_generated_skill_candidate(skills[1], adapter)
                self.assertEqual(expected[0]["candidate_id"], old["candidate_id"])
                self.assertEqual(expected[1]["candidate_id"], different["candidate_id"])
                self.assertEqual(old, service.resolve_generated_skill_candidate(skills[0], adapter))
                self.assertEqual(different, service.resolve_generated_skill_candidate(
                    skills[1], adapter
                ))
                after = self.case.state.get_effect_operation(operation_id)
                self.assertEqual("confirmed", after["status"])
                self.assertEqual(before["operation_id"], after["operation_id"])
                self.assertEqual(before["payload_record"] if status != "umbrella" else
                                 {"source_trajectory_id": trajectory["trajectory_id"]},
                                 after["payload_record"])
                self.assertEqual(
                    {"candidate_id": old["candidate_id"]} if status != "umbrella" else
                    after["payload_record"], after["acknowledgement"]
                )
                self.assertEqual(1, len([
                    effect for effect in self.case.state.list_effect_operations(outcome["outcome_id"])
                    if effect["kind"] == "generated_skill_creation"
                ]))
                self.assertEqual(original_outcome, self.case.state.get_outcome(
                    outcome["decision_id"]
                ))


if __name__ == "__main__":
    unittest.main()
