from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts, experience, store


class _FakeEverOS:
    async def memorize(self, _: dict, **__: object) -> dict:
        return {"status": "extracted"}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, _: object) -> dict:
        return {"data": {"agent_cases": [], "agent_skills": []}}


class _CountingEverOS:
    """Public-surface double that records calls which a root check must block."""

    def __init__(self) -> None:
        self.memorize_calls = 0
        self.search_calls = 0
        self.search_request_calls = 0
        self.get_calls = 0
        self.get_request_calls = 0

    async def memorize(self, _: dict, **__: object) -> dict:
        self.memorize_calls += 1
        return {"status": "extracted"}

    def make_search_request(self, **kwargs: object) -> dict:
        self.search_request_calls += 1
        return dict(kwargs)

    async def search(self, _: object) -> dict:
        self.search_calls += 1
        return {"data": {"agent_cases": [], "agent_skills": []}}

    def make_get_request(self, **kwargs: object) -> dict:
        self.get_request_calls += 1
        return dict(kwargs)

    async def get(self, _: object) -> dict:
        self.get_calls += 1
        return {"data": {"agent_cases": [], "count": 0, "total_count": 0}}


class EverOSRootBindingRegressionTests(unittest.TestCase):
    @staticmethod
    def _dynamic_memory_root_type(default_root: Path) -> type:
        """Build a public ``MemoryRoot`` double that reads current environment."""

        class DynamicMemoryRoot:
            resolve_calls = 0

            def __init__(self, root: str | Path) -> None:
                self.root = Path(root).resolve()

            @classmethod
            def resolve(cls) -> "DynamicMemoryRoot":
                cls.resolve_calls += 1
                return cls(os.environ.get("EVEROS_ROOT") or default_root)

        return DynamicMemoryRoot

    @staticmethod
    def _public_everos_modules(
        memory_root_type: type, public: _CountingEverOS
    ) -> dict[str, types.ModuleType]:
        """Expose only the imports the production loader is allowed to use."""

        everos = types.ModuleType("everos")
        everos.__path__ = []  # type: ignore[attr-defined]
        core = types.ModuleType("everos.core")
        core.__path__ = []  # type: ignore[attr-defined]
        persistence = types.ModuleType("everos.core.persistence")
        persistence.MemoryRoot = memory_root_type
        memory = types.ModuleType("everos.memory")
        memory.__path__ = []  # type: ignore[attr-defined]
        get = types.ModuleType("everos.memory.get")
        get.GetRequest = public.make_get_request
        search = types.ModuleType("everos.memory.search")
        search.SearchRequest = public.make_search_request
        service = types.ModuleType("everos.service")
        service.get = public.get
        service.memorize = public.memorize
        service.search = public.search
        everos.core = core
        everos.memory = memory
        everos.service = service
        core.persistence = persistence
        memory.get = get
        memory.search = search
        return {
            "everos": everos,
            "everos.core": core,
            "everos.core.persistence": persistence,
            "everos.memory": memory,
            "everos.memory.get": get,
            "everos.memory.search": search,
            "everos.service": service,
        }

    def _load_adapter_with_fake_public_surface(
        self,
        *,
        base_root: Path,
        scope: experience.ExperienceScope,
        public: _CountingEverOS,
        memory_root_type: type,
    ) -> experience.EverOSAdapter:
        memory_root = experience.EverOSAdapter.memory_root_for_scope(base_root, scope)
        os.environ["EVEROS_ROOT"] = str(memory_root)
        surface = experience.load_vendored_everos_public_surface(memory_root=memory_root)
        return experience.EverOSAdapter(
            scope=scope,
            base_root=base_root,
            surface=surface,
        )

    def test_loader_fails_closed_when_root_is_absent_or_mismatched(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        expected_root = Path(temporary.name) / "expected"
        wrong_root = Path(temporary.name) / "wrong"

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(experience.ScopeBoundaryError, "EVEROS_ROOT"):
                experience.load_vendored_everos_public_surface(memory_root=expected_root)
        with patch.dict(os.environ, {"EVEROS_ROOT": str(wrong_root)}, clear=True):
            with self.assertRaisesRegex(experience.ScopeBoundaryError, "namespace"):
                experience.load_vendored_everos_public_surface(memory_root=expected_root)

    def test_surface_root_is_captured_and_cannot_be_rebound_by_later_environment(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base_root = Path(temporary.name) / "everos"
        first_scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="one",
            owner="root-agent",
        )
        stale_scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="two",
            owner="root-agent",
        )
        first_root = experience.EverOSAdapter.memory_root_for_scope(base_root, first_scope)
        stale_root = experience.EverOSAdapter.memory_root_for_scope(base_root, stale_scope)
        surface = experience.EverOSPublicSurface.from_object(
            _FakeEverOS(),
            memory_root=first_root,
            resolve_memory_root=lambda: first_root,
        )

        with patch.dict(os.environ, {"EVEROS_ROOT": str(stale_root)}, clear=True):
            bound = experience.EverOSAdapter(
                scope=first_scope,
                base_root=base_root,
                surface=surface,
            )
            self.assertEqual(first_root, bound.memory_root)
            with self.assertRaisesRegex(experience.ScopeBoundaryError, "captured"):
                experience.EverOSAdapter(
                    scope=stale_scope,
                    base_root=base_root,
                    surface=surface,
                )

    def test_root_change_or_unset_blocks_payload_and_public_calls(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base_root = Path(temporary.name) / "everos"
        scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="root-guard",
            owner="root-agent",
        )
        expected_root = experience.EverOSAdapter.memory_root_for_scope(base_root, scope)
        changed_root = Path(temporary.name) / "different-root"
        trajectory = {
            "scope": scope.to_record(),
            "task_text": "reject a changed EverOS root",
            "raw_evidence": "the public operation must not run",
            "failed_hypotheses": [],
            "recorded_at": "2026-09-21T00:00:00Z",
        }

        for replacement in (str(changed_root), None):
            with self.subTest(replacement=replacement):
                public = _CountingEverOS()
                memory_root_type = self._dynamic_memory_root_type(
                    Path(temporary.name) / "everos-default"
                )
                modules = self._public_everos_modules(memory_root_type, public)
                with patch.dict(sys.modules, modules):
                    with patch.object(
                        experience, "_everos_process_root", None, create=True
                    ):
                        with patch.dict(
                            os.environ, {"EVEROS_ROOT": str(expected_root)}, clear=True
                        ):
                            adapter = self._load_adapter_with_fake_public_surface(
                                base_root=base_root,
                                scope=scope,
                                public=public,
                                memory_root_type=memory_root_type,
                            )
                            if replacement is None:
                                os.environ.pop("EVEROS_ROOT")
                            else:
                                os.environ["EVEROS_ROOT"] = replacement

                            with self.assertRaisesRegex(
                                experience.ScopeBoundaryError, "current EverOS root"
                            ):
                                adapter.add_payload(trajectory, session_id="receipt-1")
                            with self.assertRaisesRegex(
                                experience.ScopeBoundaryError, "current EverOS root"
                            ):
                                asyncio.run(adapter.memorize({"session_id": "receipt-1"}))
                            with self.assertRaisesRegex(
                                experience.ScopeBoundaryError, "current EverOS root"
                            ):
                                asyncio.run(
                                    adapter.search_representation(
                                        session_id="receipt-1", query="root guard"
                                    )
                                )
                            with self.assertRaisesRegex(
                                experience.ScopeBoundaryError, "current EverOS root"
                            ):
                                asyncio.run(
                                    adapter.readback_cases(
                                        session_id="receipt-1", query="root guard"
                                    )
                                )
                self.assertEqual(0, public.memorize_calls)
                self.assertEqual(0, public.get_request_calls)
                self.assertEqual(0, public.get_calls)
                self.assertEqual(0, public.search_request_calls)
                self.assertEqual(0, public.search_calls)

    def test_loader_rejects_a_second_different_root_in_one_process(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        first_root = Path(temporary.name) / "first"
        second_root = Path(temporary.name) / "second"
        public = _CountingEverOS()
        memory_root_type = self._dynamic_memory_root_type(
            Path(temporary.name) / "everos-default"
        )
        modules = self._public_everos_modules(memory_root_type, public)

        with patch.dict(sys.modules, modules):
            with patch.object(experience, "_everos_process_root", None, create=True):
                with patch.dict(os.environ, {"EVEROS_ROOT": str(first_root)}, clear=True):
                    surface = experience.load_vendored_everos_public_surface(
                        memory_root=first_root
                    )
                    self.assertEqual(first_root.resolve(), surface.memory_root)
                    os.environ["EVEROS_ROOT"] = str(second_root)
                    with self.assertRaisesRegex(
                        experience.ScopeBoundaryError, "fresh process"
                    ):
                        experience.load_vendored_everos_public_surface(
                            memory_root=second_root
                        )
        self.assertEqual(1, memory_root_type.resolve_calls)


class ReviewReceiptBindingRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.memory_store = store.MemoryStore(self.root / "memory.sqlite3")
        self.memory_store.initialize()
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="receipt-binding",
            owner="root-agent",
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def _records(self) -> tuple[dict, dict, dict, dict]:
        plan = contracts.make_plan(
            plan_id="receipt-plan",
            objective_id="receipt-objective",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Bind review evidence before capture",
            base_commit="receipt-base",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="receipt-objective", route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        self.memory_store.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"],
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status="FAIL",
            evidence_digest="receipt-outcome-evidence",
            linked_run_id="receipt-run",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        return card, plan, decision, outcome

    def test_receipt_durably_binds_exact_evidence_and_hypotheses_before_capture(self) -> None:
        card, plan, decision, outcome = self._records()
        review = contracts.make_review_receipt(
            review_id="receipt-review",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://receipt-run",),
            protected_source_refs=("evidence://protected/receipt",),
            raw_evidence="The first hypothesis failed; retain this exact protected narrative.",
            failed_hypotheses=("the remote service was the cause",),
        )
        self.memory_store.record_review_receipt(review)
        expected = contracts.make_reviewed_trajectory(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=self.scope.to_record(),
        )
        substituted = dict(expected)
        substituted["raw_evidence"] = "A later caller substituted a different narrative."
        substituted["failed_hypotheses"] = ["a later caller changed the hypothesis"]
        substituted["content_hash"] = contracts.content_hash(substituted)
        with self.assertRaisesRegex(store.TrajectoryConflictError, "review receipt"):
            self.memory_store.record_reviewed_trajectory(substituted)

        service = experience.ReviewedExperienceService(self.memory_store)
        captured = service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=self.scope,
        )
        persisted = self.memory_store.get_review_receipt(review["review_receipt_id"])
        self.assertEqual(review, persisted)
        self.assertEqual(review["raw_evidence"], captured["raw_evidence"])
        self.assertEqual(review["failed_hypotheses"], captured["failed_hypotheses"])

        self.memory_store.close()
        self.memory_store = store.MemoryStore(self.root / "memory.sqlite3")
        self.memory_store.initialize()
        restored = self.memory_store.get_review_receipt(review["review_receipt_id"])
        self.assertEqual(review, restored)
        self.assertEqual(captured, self.memory_store.get_reviewed_trajectory(captured["trajectory_id"]))


if __name__ == "__main__":
    unittest.main()
