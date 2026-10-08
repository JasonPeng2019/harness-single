"""Caller-owned synthetic EverOS stored-skill lineage for focused MVP proof.

The caller owns the MemoryStore and temporary root. No global service or file is
retained after that caller cleans up.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from unittest.mock import patch

from memory_harness import config, contracts, experience, privacy, procedures, store, templates

MARKER = "EVEROS_MVP_MARKER"
CONTENT = f"Inspect the parser lock first; record {MARKER} before recovery."


class _CaseSurface:
    """Only the STEP-01 reviewed-case ingestion fixture is simulated."""

    def __init__(self) -> None:
        self.cases: list[dict] = []

    async def memorize(self, payload: dict, **_: object) -> dict:
        return {"status": "extracted", "message_count": len(payload["messages"])}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, _: object) -> dict:
        return {"data": {"agent_cases": self.cases, "agent_skills": []}}


class _RootVerifier:
    def verify_generated_skill_approval(
        self, *, issuer: str, candidate: dict, scope: dict, recipients: tuple[str, ...]
    ) -> experience.VerifiedApproval:
        if issuer != "ROOT" or recipients != (scope["owner"],):
            raise experience.ApprovalError("synthetic ROOT policy rejected approval")
        return experience.VerifiedApproval(
            issuer=issuer,
            candidate_id=candidate["candidate_id"],
            scope=scope,
            recipients=recipients,
            authority_evidence={"policy_id": "mvp-root/v1", "subject": "synthetic-root"},
        )


class EverOSFixture:
    """Stage a reviewed skill into caller-owned product storage and EverOS root."""

    def __init__(self, *, root: Path, memory_store: store.MemoryStore,
                 scope: experience.ExperienceScope, policy: privacy.PrivacyPolicy) -> None:
        self.root = root
        self.memory_store = memory_store
        self.policy = policy
        self.scope = scope
        self.receiver = self.scope.to_record()
        self.base_root = self.root / "everos"
        self.memory_root = experience.EverOSAdapter.memory_root_for_scope(
            self.base_root, self.scope
        )
        self.case_surface = _CaseSurface()
        self.case_adapter = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.base_root,
            surface=experience.EverOSPublicSurface.from_object(
                self.case_surface,
                memory_root=self.memory_root,
                resolve_memory_root=lambda: self.memory_root,
            ),
            privacy_policy=self.policy,
        )
        self.experience_service = experience.ReviewedExperienceService(
            self.memory_store,
            privacy_policy=self.policy,
            approval_verifier=_RootVerifier(),
        )
        self.procedure_service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}, privacy_policy=self.policy
        )
        self.partition = {
            "scope": "project",
            "application": self.scope.application,
            "project": self.scope.project,
            "namespace": self.scope.namespace,
            "recipients": [self.receiver],
        }
        self.limits = config.PreparationLimits()

    def _confirmed_case(self) -> dict:
        plan = contracts.make_plan(
            plan_id="mvp-source-plan", objective_id="mvp-source-objective",
            route="ordinary", state="accepted", content={"steps": ["inspect", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Review parser lock recovery", base_commit="synthetic-base",
            memory_handoff=contracts.make_memory_handoff(
                objective_id=plan["objective_id"], route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        self.memory_store.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"], plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"], status="PASS", evidence_digest="mvp-evidence",
            linked_run_id="mvp-source-run", task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="mvp-source-review", outcome=outcome, decision=decision,
            task_card=card, plan=plan, reviewed_by="ROOT",
            evidence_refs=("review://mvp/source",),
            raw_evidence="The parser lock recovery passed its focused check.",
        )
        trajectory = self.experience_service.capture(
            task_card=card, plan=plan, decision=decision, outcome=outcome,
            review_receipt=review, scope=self.scope,
        )
        pending = asyncio.run(self.experience_service.extract_trajectory(
            trajectory["trajectory_id"], self.case_adapter
        ))
        self.case_surface.cases = [{
            "id": "mvp-case", "agent_id": self.case_adapter.everos_owner_id,
            "app_id": self.case_adapter.everos_application_id,
            "project_id": self.case_adapter.everos_project_id,
            "session_id": pending["session_id"],
        }]
        confirmed = asyncio.run(self.experience_service.reconcile_extraction(
            trajectory["trajectory_id"], self.case_adapter
        ))
        if confirmed["status"] != "confirmed":
            raise AssertionError("reviewed source case was not confirmed")
        return trajectory

    def stage_lineage(self, *, designate: bool = True, approve: bool = True) -> tuple[dict, dict | None, dict]:
        self._confirmed_case()
        skill = {
            "id": f"{self.case_adapter.everos_owner_id}_parser_lock_mvp",
            "agent_id": self.case_adapter.everos_owner_id,
            "app_id": self.case_adapter.everos_application_id,
            "project_id": self.case_adapter.everos_project_id,
            "name": "parser_lock_mvp",
            "description": "Inspect parser lock evidence before recovery.",
            "content": CONTENT,
            "confidence": 0.8,
            "maturity_score": 0.6,
            "source_case_ids": ["mvp-case"],
        }
        candidate = self.experience_service.resolve_generated_skill_candidate(
            skill, self.case_adapter
        )
        source_approval = self.experience_service.approve_generated_skill(
            candidate_id=candidate["candidate_id"], scope=self.scope,
            approval_id="mvp-source-approval", issuer="ROOT",
            recipients=(self.scope.owner,), approved_at="2026-09-26T00:00:00Z",
        )
        procedure = self.procedure_service.procedure_from_generated_skill(
            candidate_id=candidate["candidate_id"],
            skill_approval_id=source_approval["approval_id"],
            logical_name="parser-lock-mvp", references=[],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
        )
        approval = None
        if approve:
            approval = self.approve_procedure(procedure, designate=designate)
        return candidate, approval, procedure

    async def seed_stored_skill(
        self, candidate: dict, *, include_foreign_scope_hits: bool = False
    ) -> Path:
        """Write and index this candidate in the caller's fresh EverOS root."""
        from everos.component.tokenizer import build_tokenizer
        from everos.config import load_settings
        from everos.core.persistence import MemoryRoot
        from everos.infra.persistence import index
        from everos.infra.persistence.markdown import AgentSkillFrontmatter, AgentSkillWriter
        from everos.memory.cascade.handlers.agent_skill import AgentSkillHandler
        from everos.memory.cascade.handlers.base import HandlerDeps

        owner = self.case_adapter.everos_owner_id
        app = self.case_adapter.everos_application_id
        project = self.case_adapter.everos_project_id
        if candidate["skill_id"] != f"{owner}_parser_lock_mvp" or candidate["scope"] != self.receiver:
            raise AssertionError("candidate does not belong to this EverOS fixture")

        self.memory_root.mkdir(parents=True, exist_ok=True)
        (self.memory_root / "ome.toml").write_text("# isolated MVP test\n", encoding="utf-8")
        with patch.dict(os.environ, {"EVEROS_ROOT": str(self.memory_root)}):
            load_settings.cache_clear()
            try:
                await index.startup()
                root = MemoryRoot.resolve(explicit_root=str(self.memory_root))
                writer = AgentSkillWriter(root)
                handler = AgentSkillHandler(
                    HandlerDeps(memory_root=root, tokenizer=build_tokenizer())
                )

                async def add(skill_owner: str, name: str, skill_app: str, skill_project: str) -> Path:
                    frontmatter = AgentSkillFrontmatter(
                        id=f"skill_{name}", agent_id=skill_owner, name=name,
                        description="Inspect parser lock evidence before recovery.",
                        confidence=0.8, maturity_score=0.6,
                        source_case_ids=[case["case_id"] for case in candidate["source_cases"]],
                    )
                    path = await writer.write_main(
                        skill_owner, name, frontmatter=frontmatter,
                        body=candidate["content"], app_id=skill_app, project_id=skill_project,
                    )
                    outcome = await handler.handle_added_or_modified(
                        path.relative_to(root.root).as_posix()
                    )
                    if outcome.upserted != 1:
                        raise AssertionError("EverOS did not index the stored skill")
                    return path

                skill_path = await add(owner, "parser_lock_mvp", app, project)
                if include_foreign_scope_hits:
                    await add(owner + "-foreign", "foreign_owner_mvp", app, project)
                    await add(owner, "foreign_project_mvp", app, project + "-foreign")
                return skill_path
            finally:
                await index.shutdown()
                load_settings.cache_clear()

    def approve_procedure(self, procedure: dict, *, designate: bool = True) -> dict:
        approval = self.procedure_service.approve_revision(
            procedure=procedure, approval_id="mvp-procedure-approval", issuer="ROOT",
            recipients=[self.receiver],
            authority_evidence={"policy_id": "mvp-root/v1", "subject": "synthetic-root"},
        )
        identity = templates.representation_identity(limits=self.limits)
        representation = contracts.make_procedure_representation(
            procedure=procedure, model=identity["model"],
            dimensions=identity["dimensions"], metric=identity["metric"],
            sanitizer_version=identity["sanitizer_version"],
            search_text="parser lock evidence recovery inspection",
            vector=[0.01] * int(identity["dimensions"]),
        )
        self.procedure_service.record_representation(representation)
        if designate:
            self.procedure_service.designate(
                procedure=procedure, approval=approval,
                partition=self.partition, issuer="ROOT",
            )
        return approval



__all__ = ["EverOSFixture", "MARKER", "CONTENT"]
