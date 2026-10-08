"""Native invocation usage remains attributable through late and repeated receipts."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import contracts, store


class NativeUsageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "usage.sqlite3"
        self.state = store.MemoryStore(self.path)
        self.state.initialize()

    def tearDown(self) -> None:
        self.state.close()
        self.temporary.cleanup()

    def start(self, invocation_id: str, **changes: object) -> dict:
        args = dict(
            source="provider:codex", invocation_id=invocation_id,
            objective_id="objective-a", decision_id=None, maintenance_operation_id=None,
            stage="execution", category="inner_candidate", window_id="run-a",
            binding={"requested": "model-a", "resolved": "model-a", "native": "model-a"},
        )
        args.update(changes)
        record = contracts.make_native_usage_start(**args)
        return self.state.start_native_usage(record)

    def receipt(self, invocation_id: str, receipt_id: str, count: int, **changes: object) -> dict:
        args = dict(
            source="provider:codex", invocation_id=invocation_id, receipt_id=receipt_id,
            objective_id="objective-a", decision_id=None, maintenance_operation_id=None,
            window_id="run-a", mode="cumulative", complete=True,
            stage="execution", category="inner_candidate",
            binding={"requested": "model-a", "resolved": "model-a", "native": "model-a"},
            measures={"input": {"value": count, "unit": "tokens", "included_in_total": True}},
        )
        args.update(changes)
        return contracts.make_native_usage_receipt(**args)

    def test_started_missing_late_replay_cumulative_and_distinct_calls(self) -> None:
        first = self.start("call-1")
        self.assertEqual("incomplete", first["coverage"])
        self.assertEqual({}, first["measures"])
        self.start("call-2")
        self.state.record_native_usage_receipt(self.receipt("call-1", "r1", 4))
        self.state.record_native_usage_receipt(self.receipt("call-1", "r1", 4))
        self.state.record_native_usage_receipt(self.receipt("call-1", "r2", 9))
        self.state.record_native_usage_receipt(self.receipt("call-2", "r3", 9))
        summary = self.state.aggregate_native_usage(objective_id="objective-a")
        self.assertEqual(2, summary["invocations"])
        self.assertEqual(18, summary["measures"]["input|tokens|included"]["value"])
        self.assertEqual([], summary["incomplete_invocations"])

    def test_partial_incremental_units_total_and_late_owner_conflict(self) -> None:
        self.start("old")
        self.start("current", objective_id="objective-b", window_id="run-b")
        partial = self.receipt("old", "part-1", 3, mode="incremental", complete=False)
        self.state.record_native_usage_receipt(partial)
        self.assertEqual("incomplete", self.state.get_native_usage("provider:codex", "old")["coverage"])
        self.state.record_native_usage_receipt(self.receipt(
            "old", "part-2", 4, mode="incremental", complete=True,
            measures={
                "input": {"value": 4, "unit": "tokens", "included_in_total": True},
                "output": {"value": 2, "unit": "tokens", "included_in_total": True},
                "cache_read": {"value": 1, "unit": "tokens", "included_in_total": True},
                "reasoning": {"value": 1, "unit": "tokens", "included_in_total": True},
                "total": {"value": 10, "unit": "tokens", "included_in_total": None},
                "cost": {"value": 0.03, "unit": "USD", "included_in_total": None},
            },
        ))
        old = self.state.get_native_usage("provider:codex", "old")
        self.assertEqual(7, old["measures"]["input|tokens|included"]["value"])
        self.assertEqual(10, old["measures"]["total|tokens|unknown"]["value"])
        self.assertEqual(0.03, old["measures"]["cost|USD|unknown"]["value"])
        self.assertEqual(0, len(self.state.aggregate_native_usage(objective_id="objective-b")["measures"]))
        self.assertEqual("incomplete", self.state.aggregate_native_usage(objective_id="objective-b")["coverage"])
        with self.assertRaises(store.NativeUsageConflictError):
            self.state.record_native_usage_receipt(self.receipt(
                "old", "wrong-owner", 20, objective_id="objective-b", window_id="run-b"))
        with self.assertRaises(store.NativeUsageConflictError):
            self.start("old", objective_id="objective-b", window_id="run-b")
        self.assertEqual(old, self.state.get_native_usage("provider:codex", "old"))

    def test_apc_parent_child_replay_and_failed_reuse_fallback(self) -> None:
        self.start("adapt", source="harness:apc", category="online_adaptation", stage="adaptation")
        child = self.start("child", category="online_adaptation", stage="adaptation",
                           parent_invocation_id="adapt", parent_source="harness:apc",
                           accepted_support=True)
        self.assertTrue(child["accepted_support"])
        self.start("child", category="online_adaptation", stage="adaptation",
                   parent_invocation_id="adapt", parent_source="harness:apc",
                   accepted_support=True)
        with self.assertRaises(store.NativeUsageConflictError):
            self.start("another-child", category="online_adaptation", stage="adaptation",
                       parent_invocation_id="adapt", parent_source="harness:apc",
                       accepted_support=True)
        self.start("reuse-failed", category="online_planning", stage="reuse")
        self.start("fallback", category="online_planning", stage="fallback")
        for name, category, stage in (("child", "online_adaptation", "adaptation"),
                                       ("reuse-failed", "online_planning", "reuse"),
                                       ("fallback", "online_planning", "fallback")):
            self.state.record_native_usage_receipt(self.receipt(
                name, f"receipt-{name}", 5, category=category, stage=stage))
        result = self.state.aggregate_native_usage(objective_id="objective-a")
        self.assertEqual(4, result["invocations"])
        self.assertEqual(15, result["measures"]["input|tokens|included"]["value"])
        self.assertEqual(1, len(result["incomplete_invocations"]))

    def test_correction_resume_nested_categories_and_incompatible_units(self) -> None:
        for name, category, window, unit in (
            ("outer", "outer_implementation", "outer-run", "tokens"),
            ("root", "product_test_root", "outer-run", "tokens"),
            ("inner", "inner_candidate", "inner-run", "tokens"),
            ("correction", "inner_candidate", "inner-run", "tokens"),
            ("resume", "inner_candidate", "resumed-run", "tokens"),
            ("embedding", "embedding", "inner-run", "vectors"),
        ):
            self.start(name, category=category, window_id=window)
            self.state.record_native_usage_receipt(self.receipt(
                name, f"r-{name}", 2, category=category, window_id=window,
                measures={"input": {"value": 2, "unit": unit, "included_in_total": True}}))
        result = self.state.aggregate_native_usage(objective_id="objective-a")
        self.assertEqual(6, result["invocations"])
        self.assertEqual(10, result["measures"]["input|tokens|included"]["value"])
        self.assertEqual(2, result["measures"]["input|vectors|included"]["value"])
        self.assertEqual(3, result["by_category"]["inner_candidate"]["invocations"])
        self.assertEqual(6, result["by_category"]["inner_candidate"]["measures"]["input|tokens|included"]["value"])
        self.assertEqual("complete", result["coverage"])

    def test_receipt_identity_and_modes_conflict_without_persisting(self) -> None:
        self.start("call")
        self.state.record_native_usage_receipt(self.receipt("call", "r1", 2))
        with self.assertRaises(store.NativeUsageConflictError):
            self.state.record_native_usage_receipt(self.receipt("call", "r1", 9))
        with self.assertRaises(store.NativeUsageConflictError):
            self.state.record_native_usage_receipt(self.receipt("call", "r2", 2, mode="incremental"))
        self.assertEqual(1, self.state.get_native_usage("provider:codex", "call")["receipt_count"])

    def test_late_usage_does_not_revise_fixed_quality(self) -> None:
        plan = contracts.make_plan(
            plan_id="plan", objective_id="objective-a", route="ordinary",
            state="accepted", accepted_by="ROOT", content={"steps": ["execute"]})
        card = contracts.make_task_card(
            task="execute", base_commit="base",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-a", route="ordinary", plan=plan, checkpoint="checkpoint"))
        decision = contracts.make_decision(card, plan, configuration={"strategy": "standard"})
        self.state.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"], plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"], status="FAIL", evidence_digest="evidence",
            linked_run_id="run-a", task_card_digest=card["content_hash"],
            objective_id="objective-a")
        before = self.state.record_outcome(outcome)
        self.start("late", decision_id=decision["decision_id"])
        self.state.record_native_usage_receipt(self.receipt(
            "late", "late-receipt", 7, decision_id=decision["decision_id"]))
        self.assertEqual(before, self.state.get_outcome(decision["decision_id"]))

    def test_started_usage_survives_restart_before_late_receipt(self) -> None:
        self.start("restart")
        self.state.close()
        self.state = store.MemoryStore(self.path)
        self.state.initialize()
        self.assertEqual("incomplete", self.state.get_native_usage("provider:codex", "restart")["coverage"])
        self.state.record_native_usage_receipt(self.receipt("restart", "later", 11))
        self.assertEqual(11, self.state.aggregate_native_usage(
            objective_id="objective-a")["measures"]["input|tokens|included"]["value"])

    def test_maintenance_owner_and_currency_stay_separate(self) -> None:
        self.start("maintenance", objective_id=None, maintenance_operation_id="publish-1",
                   category="maintenance", stage="publication")
        self.state.record_native_usage_receipt(self.receipt(
            "maintenance", "m1", 2, objective_id=None,
            maintenance_operation_id="publish-1", category="maintenance", stage="publication",
            measures={"cost": {"value": 2, "unit": "USD", "included_in_total": None}}))
        self.state.record_native_usage_receipt(self.receipt(
            "maintenance", "m2", 3, objective_id=None,
            maintenance_operation_id="publish-1", category="maintenance", stage="publication",
            measures={"cost": {"value": 3, "unit": "EUR", "included_in_total": None}}))
        summary = self.state.aggregate_native_usage(maintenance_operation_id="publish-1")
        self.assertEqual(2, summary["measures"]["cost|USD|unknown"]["value"])
        self.assertEqual(3, summary["measures"]["cost|EUR|unknown"]["value"])
        self.assertEqual(0, self.state.aggregate_native_usage(objective_id="objective-a")["invocations"])

    def test_late_native_binding_refines_projection_without_changing_start(self) -> None:
        start = contracts.make_native_usage_start(
            source="provider:codex", invocation_id="late-binding",
            objective_id="objective-a", decision_id=None, maintenance_operation_id=None,
            stage="execution", category="inner_candidate", window_id="run-a",
            binding={"requested": "model-a", "resolved": "model-a", "native": None},
        )
        self.state.start_native_usage(start)
        self.state.close()
        self.state = store.MemoryStore(self.path)
        self.state.initialize()
        receipt = self.receipt("late-binding", "final", 7)
        usage = self.state.record_native_usage_receipt(receipt)
        self.assertEqual("complete", usage["coverage"])
        self.assertEqual(start["binding"], usage["binding"])
        self.assertEqual(start["content_hash"], usage["content_hash"])
        self.assertEqual("model-a", usage["effective_binding"]["native"])
        self.assertEqual(7, usage["measures"]["input|tokens|included"]["value"])
        self.assertEqual(usage, self.state.record_native_usage_receipt(receipt))
        self.assertEqual(usage, self.state.start_native_usage(start))
        with self.assertRaises(store.NativeUsageConflictError):
            self.state.record_native_usage_receipt(self.receipt(
                "late-binding", "contradiction", 9,
                binding={"requested": "model-a", "resolved": "model-a", "native": "model-b"}))
        self.assertEqual(usage, self.state.get_native_usage("provider:codex", "late-binding"))

    def test_aggregate_cost_coverage_exposes_unpriced_complete_call(self) -> None:
        self.start("token-only")
        self.state.record_native_usage_receipt(self.receipt("token-only", "final", 8))
        self.start("priced")
        self.state.record_native_usage_receipt(self.receipt(
            "priced", "final", 0,
            measures={"cost": {"value": 1.25, "unit": "USD", "included_in_total": None}}))
        aggregate = self.state.aggregate_native_usage(objective_id="objective-a")
        self.assertEqual("complete", aggregate["coverage"])
        self.assertEqual(1.25, aggregate["measures"]["cost|USD|unknown"]["value"])
        self.assertEqual("incomplete", aggregate["measure_coverage"]["cost|USD|unknown"])
        self.assertEqual("incomplete", aggregate["by_category"]["inner_candidate"]
                         ["measure_coverage"]["cost|USD|unknown"])
        self.state.record_native_usage_receipt(self.receipt(
            "token-only", "late-cost", 0,
            measures={"cost": {"value": 0.5, "unit": "USD", "included_in_total": None}}))
        aggregate = self.state.aggregate_native_usage(objective_id="objective-a")
        self.assertEqual(1.75, aggregate["measures"]["cost|USD|unknown"]["value"])
        self.assertEqual("complete", aggregate["measure_coverage"]["cost|USD|unknown"])

    def test_cumulative_inclusion_refines_one_measure_and_rejects_contradiction(self) -> None:
        self.start("inclusion")
        self.state.record_native_usage_receipt(self.receipt(
            "inclusion", "partial", 0, complete=False,
            measures={"output": {"value": 3, "unit": "tokens", "included_in_total": None}}))
        usage = self.state.record_native_usage_receipt(self.receipt(
            "inclusion", "final", 0,
            measures={"output": {"value": 7, "unit": "tokens", "included_in_total": True}}))
        self.assertEqual({"output|tokens|included"}, set(usage["measures"]))
        self.assertEqual(7, usage["measures"]["output|tokens|included"]["value"])
        with self.assertRaises(store.NativeUsageConflictError):
            self.state.record_native_usage_receipt(self.receipt(
                "inclusion", "contradiction", 0,
                measures={"output": {"value": 8, "unit": "tokens", "included_in_total": False}}))
        self.assertEqual(2, self.state.get_native_usage("provider:codex", "inclusion")["receipt_count"])
        self.state.record_native_usage_receipt(self.receipt(
            "inclusion", "other-unit", 0,
            measures={"output": {"value": 2, "unit": "vectors", "included_in_total": False}}))
        self.assertEqual({"output|tokens|included", "output|vectors|excluded"},
                         set(self.state.get_native_usage("provider:codex", "inclusion")["measures"]))


if __name__ == "__main__":
    unittest.main()
