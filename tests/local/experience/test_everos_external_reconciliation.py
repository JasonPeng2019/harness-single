"""Exact public-get readback of an existing reviewed-experience ingestion."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import config, contracts, experience, store


class GetSurface:
    def __init__(self) -> None:
        self.memorize_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.search_calls: list[dict] = []
        self.cases: list[object] = []
        self.total_count: int | None = None
        self.fail_get = False
        self.lose_ack = False
        self.message_count: int | None = None
        self.before_return = None

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.memorize_calls.append(dict(payload))
        if self.before_return is not None:
            await self.before_return()
        if self.lose_ack:
            raise ConnectionError("acknowledgement lost")
        result = {"status": "extracted"}
        if self.message_count is not None:
            result["message_count"] = self.message_count
        return result

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, request: dict) -> dict:
        self.search_calls.append(request)
        raise AssertionError("ranked search must not reconcile a get-capable surface")

    def make_get_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def get(self, request: dict) -> dict:
        self.get_calls.append(request)
        if self.fail_get:
            raise ConnectionError("readback unavailable")
        total = len(self.cases) if self.total_count is None else self.total_count
        return {"data": {"agent_cases": list(self.cases), "count": len(self.cases),
                         "total_count": total}}


class PaginatingGetSurface(GetSurface):
    def __init__(self) -> None:
        super().__init__()
        self.later_fault: str | None = None

    async def get(self, request: dict) -> dict:
        self.get_calls.append(request)
        page = request["page"]
        page_size = request["page_size"]
        start = (page - 1) * page_size
        cases = list(self.cases[start:start + page_size])
        total = len(self.cases)
        if page == 2:
            if self.later_fault == "failed":
                raise ConnectionError("page 2 unavailable")
            if self.later_fault == "missing":
                cases = []
            elif self.later_fault == "duplicate":
                cases = [self.cases[0]]
            elif self.later_fault == "foreign":
                cases = [{**cases[0], "agent_id": "foreign-owner"}]
            elif self.later_fault == "changed_total":
                total += 1
            elif self.later_fault == "malformed":
                return {"data": {"agent_cases": "invalid", "count": 1,
                                 "total_count": total}}
        return {"data": {"agent_cases": cases, "count": len(cases),
                         "total_count": total}}


class EverOSExternalReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = store.MemoryStore(self.root / "memory.sqlite3")
        self.state.initialize()
        self.addCleanup(self.state.close)
        self.scope = experience.ExperienceScope(
            application="harness", project="product", namespace="readback", owner="ROOT"
        )
        self.service = experience.ReviewedExperienceService(self.state)
        self.trajectory = self._capture()
        self.surface = GetSurface()
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            self.root / "everos", self.scope
        )
        self.adapter = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.root / "everos",
            surface=experience.EverOSPublicSurface.from_object(
                self.surface,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
        )

    def _capture(self) -> dict:
        plan = contracts.make_plan(
            plan_id="readback-plan", objective_id="readback-objective", route="ordinary",
            state="accepted", content={"steps": ["inspect", "repair"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Repair parser failure", base_commit="readback-base",
            memory_handoff=contracts.make_memory_handoff(
                objective_id=plan["objective_id"], route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        self.state.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"], plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"], status="FAIL",
            evidence_digest="readback-evidence", linked_run_id="readback-run",
            task_card_digest=card["content_hash"], objective_id=plan["objective_id"],
        )
        self.state.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="readback-review", outcome=outcome, decision=decision,
            task_card=card, plan=plan, reviewed_by="ROOT",
            evidence_refs=("review://readback-run",),
            raw_evidence="The parser repair failed after a focused local check.",
            failed_hypotheses=("network timeout",),
        )
        return self.service.capture(
            task_card=card, plan=plan, decision=decision, outcome=outcome,
            review_receipt=review, scope=self.scope,
        )

    def _case(self, session_id: str) -> dict:
        return {
            "id": "readback-case", "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "session_id": session_id, "task_intent": "repair parser failure",
            "approach": "inspect parser", "quality_score": 0.0,
            "key_insight": "the check failed", "timestamp": "2026-09-21T00:00:00Z",
        }

    def _extract(self) -> dict:
        return asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter
        ))

    def _claimant(self) -> dict:
        claimant = {"schema": "external-effect-claimant/v1",
                    "native_invocation_id": "everos-readback-call",
                    "pid": 1001, "process_created_at": "native-process-A"}
        claimant["content_hash"] = contracts.content_hash(claimant)
        return claimant

    def _external(self) -> dict:
        scope_key = contracts.sha256_hex({
            "scope": self.trajectory["scope"], "destination": self.adapter.destination,
        })
        return self.state.get_effect_operation(contracts.external_effect_operation_id(
            self.trajectory["trajectory_id"], "experience_ingestion", scope_key,
        ))

    def test_claimed_public_payload_confirms_atomic_pair_without_replay(self) -> None:
        session = self.adapter.session_id_for(self.trajectory["trajectory_id"])
        self.surface.cases = [self._case(session)]
        fixed = self.state.get_outcome(self.trajectory["decision_id"])
        confirmed = asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=self._claimant(),
        ))
        operation = self._external()
        self.assertEqual(("confirmed", "confirmed"),
                         (operation["status"], confirmed["status"]))
        self.assertEqual(self.adapter.add_payload(self.trajectory, session_id=session),
                         operation["payload_record"])
        self.assertEqual(["readback-case"], confirmed["case_ids"])
        self.assertEqual(1, len(self.surface.memorize_calls))
        self.assertEqual(fixed, self.state.get_outcome(self.trajectory["decision_id"]))

    def test_claim_and_usage_start_precede_public_memorize(self) -> None:
        claimant = self._claimant()
        observed = []

        async def inspect_before_return() -> None:
            operation = self._external()
            ingestion = self.state.get_experience_ingestion_for_trajectory(
                self.trajectory["trajectory_id"],
            )
            usage = self.state.get_native_usage(
                "everos:memorize", claimant["native_invocation_id"],
            )
            observed.append((operation, ingestion, usage))

        self.surface.before_return = inspect_before_return
        asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=claimant,
        ))
        self.assertEqual(1, len(observed))
        operation, ingestion, usage = observed[0]
        self.assertEqual(("in_flight", "pending", 1),
                         (operation["status"], ingestion["status"],
                          operation["claim_generation"]))
        self.assertEqual(claimant, operation["claimant_record"])
        self.assertEqual(self.trajectory, operation["source_record"])
        self.assertEqual(self.state.get_decision(self.trajectory["decision_id"])["configuration"],
                         operation["configuration"])
        self.assertEqual(("incomplete", 0), (usage["coverage"], usage["receipt_count"]))

    def test_claimed_lost_response_and_off_readback_preserve_attribution(self) -> None:
        self.surface.lose_ack = True
        claimant = self._claimant()
        uncertain = asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=claimant,
        ))
        operation = self._external()
        self.assertEqual(("uncertain", "uncertain"),
                         (operation["status"], uncertain["status"]))
        off = config.resolve_config({"experience_write": False})
        self.assertEqual("pending_off", self.state.external_effect_off_state(
            operation["operation_id"], current_config=off,
        ))
        self.surface.cases = [self._case(uncertain["session_id"])]
        confirmed = asyncio.run(self.service.reconcile_extraction(
            self.trajectory["trajectory_id"], self.adapter, current_config=off,
        ))
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(operation["configuration"], self._external()["configuration"])
        self.assertEqual(1, len(self.surface.memorize_calls))
        self.assertEqual("off", self.state.external_effect_off_state(
            operation["operation_id"], current_config=off,
        ))

    def test_two_handle_peer_before_post_call_settlement(self) -> None:
        peer = store.MemoryStore(self.root / "memory.sqlite3")
        peer.initialize()
        self.addCleanup(peer.close)
        peer_service = experience.ReviewedExperienceService(peer)
        session = self.adapter.session_id_for(self.trajectory["trajectory_id"])
        self.surface.cases = [self._case(session)]

        async def confirm_on_peer() -> None:
            confirmed = await peer_service.reconcile_extraction(
                self.trajectory["trajectory_id"], self.adapter,
            )
            self.assertEqual("confirmed", confirmed["status"])

        self.surface.before_return = confirm_on_peer
        confirmed = asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=self._claimant(),
        ))
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual("confirmed", self._external()["status"])
        self.assertEqual(1, len(self.surface.memorize_calls))

    def test_two_handle_peer_between_call_and_atomic_settlement(self) -> None:
        session = self.adapter.session_id_for(self.trajectory["trajectory_id"])
        self.surface.cases = [self._case(session)]
        original = self.state.settle_external_experience_ingestion
        dispositions = []

        def settle_after_peer(*args: object, **kwargs: object):
            if not dispositions:
                dispositions.append("peer")
                def confirm_on_peer():
                    peer = store.MemoryStore(self.root / "memory.sqlite3")
                    peer.initialize()
                    try:
                        return asyncio.run(experience.ReviewedExperienceService(
                            peer,
                        ).reconcile_extraction(
                            self.trajectory["trajectory_id"], self.adapter,
                        ))
                    finally:
                        peer.close()
                with ThreadPoolExecutor(max_workers=1) as pool:
                    confirmed = pool.submit(confirm_on_peer).result(timeout=3)
                self.assertEqual("confirmed", confirmed["status"])
            result = original(*args, **kwargs)
            dispositions.append(result[2])
            return result

        with patch.object(self.state, "settle_external_experience_ingestion",
                          side_effect=settle_after_peer):
            confirmed = asyncio.run(self.service.extract_trajectory(
                self.trajectory["trajectory_id"], self.adapter, claimant=self._claimant(),
            ))
        self.assertEqual(["peer", "already_confirmed", "already_confirmed"], dispositions)
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(1, len(self.surface.memorize_calls))

    def test_native_usage_available_absent_and_late(self) -> None:
        claimant = self._claimant()
        self.surface.message_count = 3
        asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=claimant,
        ))
        usage = self.state.get_native_usage("everos:memorize", claimant["native_invocation_id"])
        self.assertEqual(("incomplete", 1, 3), (
            usage["coverage"], usage["receipt_count"],
            usage["measures"]["messages|messages|unknown"]["value"],
        ))
        self.service.record_memorize_usage_result(
            self.trajectory["trajectory_id"], self.adapter, claimant,
            {"message_count": 3},
        )
        self.assertEqual(1, self.state.get_native_usage(
            "everos:memorize", claimant["native_invocation_id"],
        )["receipt_count"])

    def test_missing_counter_stays_incomplete_and_late_source_result_is_once(self) -> None:
        claimant = self._claimant()
        self.surface.lose_ack = True
        asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=claimant,
        ))
        usage = self.state.get_native_usage("everos:memorize", claimant["native_invocation_id"])
        self.assertEqual(("incomplete", 0, {}),
                         (usage["coverage"], usage["receipt_count"], usage["measures"]))
        self.assertIsNone(self.service.record_memorize_usage_result(
            self.trajectory["trajectory_id"], self.adapter, claimant, {},
        ))
        self.assertIsNone(self.service.record_memorize_usage_result(
            self.trajectory["trajectory_id"], self.adapter, claimant,
            {"message_count": -1},
        ))
        for _ in range(2):
            self.service.record_memorize_usage_result(
                self.trajectory["trajectory_id"], self.adapter, claimant,
                {"message_count": 2},
            )
        usage = self.state.get_native_usage("everos:memorize", claimant["native_invocation_id"])
        self.assertEqual((1, 2), (usage["receipt_count"],
                                 usage["measures"]["messages|messages|unknown"]["value"]))
        with self.assertRaises(store.NativeUsageConflictError):
            self.service.record_memorize_usage_result(
                self.trajectory["trajectory_id"], self.adapter, claimant,
                {"message_count": 3},
            )

    def test_off_blocks_new_claim_and_conflicting_claim_cannot_replay(self) -> None:
        off = config.resolve_config({"experience_write": False})
        self.assertIsNone(asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter,
            claimant=self._claimant(), current_config=off,
        )))
        self.assertEqual([], self.surface.memorize_calls)
        self.assertIsNone(self.state.get_experience_ingestion_for_trajectory(
            self.trajectory["trajectory_id"],
        ))
        claimant = self._claimant()
        asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=claimant,
        ))
        different = dict(claimant, native_invocation_id="another-native-call")
        different["content_hash"] = contracts.content_hash(different)
        with self.assertRaises(store.OperationConflictError):
            asyncio.run(self.service.extract_trajectory(
                self.trajectory["trajectory_id"], self.adapter, claimant=different,
            ))
        self.assertEqual(1, len(self.surface.memorize_calls))

    def test_claimed_incomplete_or_foreign_readback_never_confirms(self) -> None:
        session = self.adapter.session_id_for(self.trajectory["trajectory_id"])
        good = self._case(session)
        self.surface.cases = [good]
        self.surface.total_count = 2
        pending = asyncio.run(self.service.extract_trajectory(
            self.trajectory["trajectory_id"], self.adapter, claimant=self._claimant(),
        ))
        self.assertEqual(("pending", "uncertain"),
                         (pending["status"], self._external()["status"]))
        self.surface.total_count = None
        self.surface.cases = [{**good, "agent_id": "foreign"}]
        self.assertEqual("pending", asyncio.run(self.service.reconcile_extraction(
            self.trajectory["trajectory_id"], self.adapter,
        ))["status"])
        self.assertEqual("uncertain", self._external()["status"])
        self.assertEqual(1, len(self.surface.memorize_calls))
        with self.assertRaises(store.StoreError):
            self.state.get_case_receipt(self.scope.to_record(), good["id"])

    def test_public_get_confirms_only_original_scoped_session(self) -> None:
        pending = self._extract()
        self.assertEqual("pending", pending["status"])
        self.surface.cases = [self._case(pending["session_id"])]
        confirmed = self._extract()
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(["readback-case"], confirmed["case_ids"])
        self.assertEqual(1, len(self.surface.memorize_calls))
        self.assertEqual([], self.surface.search_calls)
        self.assertEqual({
            "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "memory_type": "agent_case", "filters": {"session_id": pending["session_id"]},
            "page": 1, "page_size": 100,
        }, self.surface.get_calls[-1])
        receipt = self.state.get_case_receipt(self.scope.to_record(), "readback-case")
        self.assertEqual(self.trajectory["review_receipt_id"], receipt["review_receipt_id"])

    def test_empty_and_outage_leave_original_pending_without_resubmission(self) -> None:
        pending = self._extract()
        self.assertEqual("pending", self._extract()["status"])
        self.surface.fail_get = True
        self.assertEqual("pending", self._extract()["status"])
        self.assertEqual(1, len(self.surface.memorize_calls))
        self.assertEqual(pending["ingestion_id"], self._extract()["ingestion_id"])

    def test_foreign_malformed_and_truncated_readback_never_confirm(self) -> None:
        pending = self._extract()
        good = self._case(pending["session_id"])
        for cases, total in (
            ([{**good, "session_id": "another-session"}], None),
            ([{**good, "agent_id": "another-owner"}], None),
            ([{**good, "project_id": "another-project"}], None),
            ([{**good, "app_id": "another-app"}], None),
            ([{"id": "missing-scope"}], None),
            ([good, {**good, "id": "foreign-case", "agent_id": "another-owner"}], None),
            ([good, good], None),
            ([good], 101),
        ):
            with self.subTest(cases=cases, total=total):
                self.surface.cases = cases
                self.surface.total_count = total
                self.assertEqual("pending", self._extract()["status"])
        self.assertEqual(1, len(self.surface.memorize_calls))

    def test_lost_ack_can_confirm_by_get_but_failed_readback_stays_uncertain(self) -> None:
        self.surface.lose_ack = True
        uncertain = self._extract()
        self.assertEqual("uncertain", uncertain["status"])
        self.surface.fail_get = True
        self.assertEqual("uncertain", self._extract()["status"])
        self.surface.fail_get = False
        self.assertEqual("uncertain", self._extract()["status"])
        self.assertEqual(1, len(self.service.search_recent_evidence(self.scope, "parser")))
        self.surface.cases = [self._case(uncertain["session_id"])]
        confirmed = self._extract()
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(uncertain["ingestion_id"], confirmed["ingestion_id"])
        self.assertEqual(1, len(self.surface.memorize_calls))

    def _paginating_surface(self) -> PaginatingGetSurface:
        surface = PaginatingGetSurface()
        self.surface = surface
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            self.root / "everos", self.scope
        )
        self.adapter = experience.EverOSAdapter(
            scope=self.scope,
            base_root=self.root / "everos",
            surface=experience.EverOSPublicSurface.from_object(
                surface,
                memory_root=memory_root,
                resolve_memory_root=lambda: memory_root,
            ),
        )
        return surface

    def _page_cases(self, session_id: str) -> list[dict]:
        return [
            {**self._case(session_id), "id": f"readback-case-{index:03d}"}
            for index in range(101)
        ]

    def test_public_get_confirms_all_101_cases_across_two_exact_pages(self) -> None:
        surface = self._paginating_surface()
        pending = self._extract()
        surface.cases = self._page_cases(pending["session_id"])
        surface.get_calls.clear()

        confirmed = self._extract()

        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(101, len(confirmed["case_ids"]))
        self.assertEqual({case["id"] for case in surface.cases}, set(confirmed["case_ids"]))
        self.assertEqual([1, 2], [request["page"] for request in surface.get_calls])
        for request in surface.get_calls:
            self.assertEqual({
                "agent_id": self.adapter.everos_owner_id,
                "app_id": self.adapter.everos_application_id,
                "project_id": self.adapter.everos_project_id,
                "memory_type": "agent_case",
                "filters": {"session_id": pending["session_id"]},
                "page_size": 100,
            }, {key: value for key, value in request.items() if key != "page"})
        self.assertEqual(1, len(surface.memorize_calls))
        for case_id in ("readback-case-000", "readback-case-100"):
            receipt = self.state.get_case_receipt(self.scope.to_record(), case_id)
            self.assertEqual(self.trajectory["review_receipt_id"], receipt["review_receipt_id"])

    def test_later_page_faults_cannot_partially_confirm_uncertain_ingestion(self) -> None:
        surface = self._paginating_surface()
        surface.lose_ack = True
        uncertain = self._extract()
        self.assertEqual("uncertain", uncertain["status"])
        surface.cases = self._page_cases(uncertain["session_id"])
        for fault in ("missing", "malformed", "changed_total", "duplicate", "foreign", "failed"):
            with self.subTest(fault=fault):
                surface.later_fault = fault
                surface.get_calls.clear()
                replay = self._extract()
                self.assertEqual("uncertain", replay["status"])
                self.assertEqual(uncertain["ingestion_id"], replay["ingestion_id"])
                self.assertEqual([1, 2], [request["page"] for request in surface.get_calls])
                with self.assertRaises(store.StoreError):
                    self.state.get_case_receipt(
                        self.scope.to_record(), "readback-case-000"
                    )
                self.assertEqual(1, len(surface.memorize_calls))
                self.assertEqual(1, len(self.service.search_recent_evidence(self.scope, "parser")))


if __name__ == "__main__":
    unittest.main()
