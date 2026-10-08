from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts, experience, store


class RealEverOSAdapterTests(unittest.TestCase):
    def test_real_vendored_add_search_and_restart_are_scoped(self) -> None:
        """Exercise EverOS's public memorize/search DTO path against disk state.

        The test runs with the vendored EverOS runtime when installed (for
        example, its own Python 3.12 virtual environment). The ordinary product
        suite remains dependency-light and reports this integration as skipped
        when that optional runtime is unavailable.
        """

        try:
            import everos  # noqa: F401
            from everalgo.testing.fake_llm import FakeLLMClient
            from everalgo.types import AgentCase
            from everalgo.llm.types import ChatResponse
        except ModuleNotFoundError as exc:
            self.skipTest(f"vendored EverOS runtime is unavailable: {exc.name}")
        asyncio.run(
            self._exercise(
                fake_llm_type=FakeLLMClient,
                agent_case_type=AgentCase,
                chat_response_type=ChatResponse,
            )
        )

    async def _exercise(
        self,
        *,
        fake_llm_type: type,
        agent_case_type: type,
        chat_response_type: type,
    ) -> None:
        from everos.config import load_settings
        from everos.entrypoints.api.app import create_app

        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        state = store.MemoryStore(root / "memory-harness.sqlite3")
        state.initialize()
        scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="everos-isolated",
            owner="root-agent",
        )
        service = experience.ReviewedExperienceService(state)
        old_environment = {
            key: os.environ.get(key)
            for key in (
                "EVEROS_ROOT",
                "EVEROS_MEMORIZE__MODE",
                "EVEROS_LLM__API_KEY",
                "EVEROS_LLM__BASE_URL",
            )
        }
        try:
            memory_root = experience.EverOSAdapter.memory_root_for_scope(
                root / "everos", scope
            )
            os.environ["EVEROS_ROOT"] = str(memory_root)
            os.environ["EVEROS_MEMORIZE__MODE"] = "agent"
            os.environ["EVEROS_LLM__API_KEY"] = "fake-key"
            os.environ["EVEROS_LLM__BASE_URL"] = "https://fake.invalid"
            memory_root.mkdir(parents=True, exist_ok=True)
            (memory_root / "ome.toml").write_text("# isolated test\n")
            load_settings.cache_clear()
            self._reset_everos_singletons()
            surface = experience.load_vendored_everos_public_surface(
                memory_root=memory_root
            )
            self.assertEqual(memory_root.resolve(), surface.memory_root)
            adapter = experience.EverOSAdapter(
                scope=scope,
                base_root=root / "everos",
                surface=surface,
            )

            trajectory = self._capture_reviewed_trajectory(service, state, scope)
            await self._seed_through_public_memorize(
                create_app=create_app,
                adapter=adapter,
                service=service,
                trajectory=trajectory,
                fake_llm_type=fake_llm_type,
                agent_case_type=agent_case_type,
                chat_response_type=chat_response_type,
            )

            # The source representation is durable before restart, but its
            # receipt is deliberately not claimed until a fresh public search
            # resolves the exact scoped case in a new service lifetime.
            self._reset_everos_singletons()
            restarted = create_app()
            async with restarted.router.lifespan_context(restarted):
                confirmed = await self._wait_for_confirmation(
                    service, trajectory["trajectory_id"], adapter
                )
                self.assertEqual("confirmed", confirmed["status"])
                self.assertTrue(confirmed["case_ids"])

                other_owner = experience.EverOSAdapter(
                    scope=experience.ExperienceScope(
                        application="harness",
                        project="product",
                        namespace="everos-isolated",
                        owner="other-agent",
                    ),
                    base_root=root / "everos",
                    surface=surface,
                )
                owner_results = await other_owner.search_representation(
                    session_id=confirmed["session_id"], query="parser failure"
                )
                self.assertEqual([], owner_results["agent_cases"])
                other_project = experience.EverOSAdapter(
                    scope=experience.ExperienceScope(
                        application="harness",
                        project="other-product",
                        namespace="everos-isolated",
                        owner="root-agent",
                    ),
                    base_root=root / "everos",
                    surface=surface,
                )
                project_results = await other_project.search_representation(
                    session_id=confirmed["session_id"], query="parser failure"
                )
                self.assertEqual([], project_results["agent_cases"])
                with self.assertRaises(experience.ScopeBoundaryError):
                    experience.EverOSAdapter(
                        scope=experience.ExperienceScope(
                            application="harness",
                            project="product",
                            namespace="another-namespace",
                            owner="root-agent",
                        ),
                        base_root=root / "everos",
                        surface=surface,
                    )
                with self.assertRaises(experience.ScopeBoundaryError):
                    experience.EverOSAdapter(
                        scope=experience.ExperienceScope(
                            application="another-harness",
                            project="product",
                            namespace="everos-isolated",
                            owner="root-agent",
                        ),
                        base_root=root / "everos",
                        surface=surface,
                    )
        finally:
            for key, value in old_environment.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            load_settings.cache_clear()
            self._reset_everos_singletons()
            state.close()
            temporary.cleanup()

    @staticmethod
    def _reset_everos_singletons() -> None:
        """Test-only reset matching EverOS's documented lazy service pattern."""

        memorize_module = importlib.import_module("everos.service.memorize")
        for name in (
            "_episode_writer",
            "_prompt_loader",
            "_user_pipeline",
            "_agent_pipeline",
            "_ome_engine",
        ):
            setattr(memorize_module, name, None)
        search_module = importlib.import_module("everos.service.search")
        search_module._manager = None
        client_module = importlib.import_module("everos.component.llm.client")
        client_module._llm_client = None
        for module_name, attribute in (
            ("everos.component.embedding.accessor", "_capability"),
            ("everos.component.rerank.accessor", "_capability"),
        ):
            setattr(importlib.import_module(module_name), attribute, None)

    @staticmethod
    def _capture_reviewed_trajectory(
        service: experience.ReviewedExperienceService,
        state: store.MemoryStore,
        scope: experience.ExperienceScope,
    ) -> dict:
        plan = contracts.make_plan(
            plan_id="accepted-plan",
            objective_id="objective-everos",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Repair the parser failure",
            base_commit="base-everos",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-everos", route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        state.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"],
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status="PASS",
            evidence_digest="everos-evidence",
            linked_run_id="everos-run",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        state.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="everos-review",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://everos/run",),
            raw_evidence="The parser repair passed after a discriminating local check.",
            failed_hypotheses=("network timeout",),
        )
        return service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=scope,
        )

    async def _seed_through_public_memorize(
        self,
        *,
        create_app: object,
        adapter: experience.EverOSAdapter,
        service: experience.ReviewedExperienceService,
        trajectory: dict,
        fake_llm_type: type,
        agent_case_type: type,
        chat_response_type: type,
    ) -> None:
        client_module = importlib.import_module("everos.component.llm.client")
        case_module = importlib.import_module("everos.memory.strategies.extract_agent_case")
        atomic_module = importlib.import_module("everos.memory.strategies.extract_atomic_facts")
        foresight_module = importlib.import_module("everos.memory.strategies.extract_foresight")

        def boundary_handler(*_: object, **__: object) -> object:
            return chat_response_type(
                content=json.dumps(
                    {"reasoning": "test", "boundaries": [], "should_wait": False}
                ),
                model="fake",
            )

        fake_llm = fake_llm_type(handler=boundary_handler)

        class DeterministicCaseExtractor:
            def __init__(self, **_: object) -> None:
                pass

            async def aextract(self, _: object) -> list[object]:
                return [
                    agent_case_type(
                        id="generated-case",
                        timestamp=1_790_000_000_000,
                        task_intent="repair parser failure",
                        approach="run discriminating parser check",
                        quality_score=1.0,
                        key_insight="local lock before network investigation",
                    )
                ]

        class EmptyExtractor:
            def __init__(self, **_: object) -> None:
                pass

            async def aextract(self, _: object) -> list[object]:
                return []

        with (
            patch.object(client_module, "_llm_client", fake_llm),
            patch.object(case_module, "AgentCaseExtractor", DeterministicCaseExtractor),
            patch.object(atomic_module, "AtomicFactExtractor", EmptyExtractor),
            patch.object(foresight_module, "ForesightExtractor", EmptyExtractor),
        ):
            app = create_app()
            async with app.router.lifespan_context(app):
                pending = await service.extract_trajectory(
                    trajectory["trajectory_id"], adapter
                )
                self.assertEqual("pending", pending["status"])
                await self._wait_for_searchable_case(adapter, pending["session_id"])

    @staticmethod
    async def _wait_for_searchable_case(
        adapter: experience.EverOSAdapter, session_id: str
    ) -> None:
        async with asyncio.timeout(20):
            while True:
                found = await adapter.search_representation(
                    session_id=session_id, query="repair parser failure"
                )
                if found["agent_cases"]:
                    return
                await asyncio.sleep(0.1)

    @staticmethod
    async def _wait_for_confirmation(
        service: experience.ReviewedExperienceService,
        trajectory_id: str,
        adapter: experience.EverOSAdapter,
    ) -> dict:
        async with asyncio.timeout(20):
            while True:
                record = await service.reconcile_extraction(trajectory_id, adapter)
                if record["status"] == "confirmed":
                    return record
                await asyncio.sleep(0.1)


if __name__ == "__main__":
    unittest.main()
