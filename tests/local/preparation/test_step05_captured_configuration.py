"""STEP-05-1: one durable logical decision survives policy and process drift."""

from __future__ import annotations

import concurrent.futures
import json
import sys
import tempfile
import threading
import unittest
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, contracts, preparation, search, store


class Clock:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value


class CapturedConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "memory.sqlite3"
        self.state = store.MemoryStore(self.path)
        self.state.initialize()
        self.clock = Clock()
        self.limits = config.resolve_limits({"default_deadline_seconds": 300.0})
        self.card = contracts.make_task_card(task="Fix parser", base_commit="base-1")
        self.plan = contracts.make_plan(
            plan_id="plan-1", objective_id="objective-1", route="ordinary",
            state="candidate", content={"steps": ["repair"]},
        )

    def tearDown(self) -> None:
        self.state.close()
        self.temporary.cleanup()

    def prepare(self, service: preparation.PreparationService, **changes):
        arguments = {
            "task_card": self.card, "plan": self.plan, "objective_id": "objective-1",
            "deadline": 1300.0,
        }
        arguments.update(changes)
        return service.prepare(**arguments)

    def service(self, state=None, network_resolver=None, **raw):
        return preparation.PreparationService(
            store=state or self.state, config=config.resolve_config(raw),
            limits=self.limits, clock=self.clock, network_resolver=network_resolver,
        )

    def first(self):
        return self.prepare(
            self.service(strategy="problem_focused", template_memory=False,
                         atlas_shared_retrieval=False, experience_read=False),
            failure_context="parser failed", network_mode="restricted_local",
        )

    def test_restart_reuses_decision_configuration_network_and_spent_budget(self) -> None:
        first = self.first()
        self.state.close()
        self.clock.value += 45.0
        self.state = store.MemoryStore(self.path)
        self.state.initialize()
        optional_calls: list[object] = []
        restarted = self.service(strategy="deeper", template_memory=True,
                                 atlas_shared_retrieval=True, experience_read=True)
        second = self.prepare(
            restarted, deadline=None,
            stores=[search.SearchStore(
                store_id="templates", kind="template",
                query=lambda query: optional_calls.append(query) or [],
            )],
        )
        self.assertEqual(first.decision["decision_id"], second.decision["decision_id"])
        self.assertEqual(first.decision, second.decision)
        self.assertEqual(first.preparation["configuration"], second.preparation["configuration"])
        self.assertEqual("problem_focused", second.preparation["requested_strategy"])
        self.assertEqual("problem_focused", second.preparation["strategy"])
        self.assertEqual("restricted_local", second.preparation["network_mode"])
        self.assertEqual(1300.0, second.preparation["deadline_monotonic"])
        self.assertEqual(255.0, second.preparation["remaining_seconds"])
        self.assertEqual(45.0, second.preparation["spent_seconds"])
        self.assertEqual(2, second.preparation["attempt"])
        self.assertEqual([], optional_calls)

    def test_rich_network_resolution_is_captured_and_explicit_drift_rejected(self) -> None:
        evidence = {
            "launched_payload": {"verified": True, "evidence_id": "payload-7"},
            "unrelated_destination_block": {
                "blocked": True, "independent": True,
                "source_kind": "independent_network_boundary", "evidence_id": "egress-4",
            },
        }
        verifier_calls: list[object] = []

        def verify(evidence, context):
            verifier_calls.append((evidence, context))
            return (
                context["objective_id"] == "objective-1"
                and context["task_card_digest"] == self.card["content_hash"]
                and context["plan_digest"] == self.plan["content_hash"]
                and bool(context["decision_id"])
            )
        first = self.prepare(self.service(network_resolver=config.NetworkResolver(verify)),
                             network_mode="atlas_memory_only",
                             network_evidence=evidence)
        self.assertEqual("atlas_memory_only", first.preparation["network_mode"])
        captured = first.preparation["network_resolution"]
        self.assertEqual("atlas_memory_only", captured["requested_mode"])
        self.assertEqual(evidence, captured["input_evidence"])
        self.assertIs(captured["verification_result"], True)
        self.assertEqual(first.decision["decision_id"], captured["context"]["decision_id"])
        self.assertEqual(self.card["content_hash"], captured["context"]["task_card_digest"])
        self.assertEqual(self.plan["content_hash"], captured["context"]["plan_digest"])
        self.assertEqual(captured["context"], verifier_calls[0][1])
        self.assertEqual(1, len(verifier_calls))
        tampered = dict(first.preparation)
        tampered["network_resolution"] = {**captured, "context": {
            **captured["context"], "decision_id": "other",
        }}
        tampered["content_hash"] = contracts.content_hash(tampered)
        with self.assertRaises(contracts.ContractError):
            contracts.validate_preparation(tampered)
        self.assertEqual(first.decision["configuration_digest"],
                         first.preparation["configuration_digest"])
        old_shape = dict(first.preparation)
        old_shape.pop("network_resolution")
        self.assertEqual(first.preparation["preparation_id"],
                         contracts.sha256_hex({
                             "domain": "memory-preparation/v1",
                             "task_card_digest": old_shape["task_card_digest"],
                             "decision_id": old_shape["decision_id"],
                             "objective_id": old_shape["objective_id"],
                             "route": old_shape["route"],
                             "strategy": old_shape["strategy"],
                             "configuration_digest": old_shape["configuration_digest"],
                             "supersedes": old_shape["supersedes"],
                             "attempt": old_shape["attempt"],
                         }))
        self.clock.value += 1
        self.state.close()
        self.state = store.MemoryStore(self.path)
        self.state.initialize()
        retry = self.prepare(self.service(), deadline=None)
        self.assertEqual(captured, retry.preparation["network_resolution"])
        self.assertEqual(1, len(verifier_calls))
        self.assertEqual(first.decision, retry.decision)
        for changed in (
            {"network_mode": "soft_guardrail_network"},
            {"network_mode": "atlas_memory_only", "network_evidence": {
                **evidence, "unrelated_destination_block": {
                    "blocked": True, "independent": True,
                    "source_kind": "independent_network_boundary", "evidence_id": "egress-5",
                },
            }},
        ):
            with self.subTest(changed=changed), self.assertRaises(
                preparation.MandatoryStateFailure
            ):
                self.prepare(self.service(), **changed)
        self.assertEqual(2, len(self.state.list_preparations(first.decision["decision_id"])))

    def test_invented_network_evidence_cannot_authorize_normal_preparation(self) -> None:
        evidence = {
            "launched_payload": {"verified": True, "evidence_id": "invented-payload"},
            "unrelated_destination_block": {
                "blocked": True, "independent": True,
                "source_kind": "independent_network_boundary", "evidence_id": "invented-egress",
            },
        }
        first = self.prepare(self.service(), network_mode="atlas_memory_only",
                             network_evidence=evidence)
        self.assertEqual("soft_guardrail_network", first.preparation["network_mode"])
        resolution = first.preparation["network_resolution"]
        self.assertIsNone(resolution["verification_result"])
        self.assertNotIn("verified_launched_payload:invented-payload",
                         resolution["enforcement_sources"])
        self.assertIn("missing or unverified", " ".join(resolution["disclosed_limits"]))
        self.assertIn("shell egress", " ".join(resolution["disclosed_limits"]))
        self.assertEqual(first.decision["decision_id"],
                         first.preparation["network_resolution"]["context"]["decision_id"])
        self.clock.value += 1
        retry = self.prepare(self.service(), deadline=None)
        self.assertEqual(first.preparation["network_resolution"], retry.preparation["network_resolution"])

    def test_legacy_normal_reopens_without_rewriting_record(self) -> None:
        first = self.prepare(self.service(), network_mode="normal")
        legacy = dict(first.preparation)
        legacy.pop("network_resolution")
        legacy["content_hash"] = contracts.content_hash(legacy)
        self.state.connection.execute(
            "UPDATE preparations SET record=? WHERE preparation_id=?",
            (json.dumps(legacy, sort_keys=True), legacy["preparation_id"]),
        )
        self.state.connection.commit()
        self.assertEqual("normal", contracts.preparation_network_resolution(legacy)["effective_mode"])
        for mode in ("soft_guardrail_network", "atlas_memory_only", "restricted_local"):
            projected = {**legacy, "network_mode": mode}
            projected["content_hash"] = contracts.content_hash(projected)
            self.assertEqual(mode, contracts.preparation_network_resolution(projected)["effective_mode"])
            self.assertEqual(["legacy_captured_mode"],
                             contracts.preparation_network_resolution(projected)["enforcement_sources"])
        retry = self.prepare(self.service(), deadline=None)
        self.assertEqual("normal", retry.preparation["network_mode"])
        self.assertNotIn("network_resolution", retry.preparation)
        self.assertEqual(legacy, self.state.get_preparation(legacy["preparation_id"]))

    def test_downgraded_atlas_claim_stays_captured_on_retry_and_level_zero(self) -> None:
        first = self.prepare(self.service(), network_mode="atlas_memory_only",
                             network_evidence={
                                 "launched_payload": {
                                     "verified": True, "evidence_id": "payload-7",
                                 },
                             })
        captured = first.preparation["network_resolution"]
        self.assertEqual("atlas_memory_only", captured["requested_mode"])
        self.assertEqual("soft_guardrail_network", captured["effective_mode"])
        self.assertEqual("soft_guardrail_network", first.preparation["network_mode"])
        retry = self.prepare(self.service(), deadline=None,
                             network_mode="atlas_memory_only")
        self.assertEqual(captured, retry.preparation["network_resolution"])
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(self.service(), deadline=None, network_mode="atlas_memory_only",
                         network_evidence={
                             "launched_payload": {
                                 "verified": True, "evidence_id": "payload-7",
                             },
                             "unrelated_destination_block": {
                                 "blocked": True, "independent": True,
                                 "source_kind": "independent_network_boundary",
                                 "evidence_id": "egress-4",
                             },
                         })
        corrected = self.service().apply_level_zero(
            preparation=retry.preparation, decision=retry.decision,
            task_card=self.card, plan=retry.plan, objective_id="objective-1",
        )
        self.assertEqual(captured, corrected.preparation["network_resolution"])
        self.assertEqual("soft_guardrail_network", corrected.preparation["network_mode"])

    def test_explicit_conflicting_policy_fails_without_writes_or_optional_calls(self) -> None:
        first = self.first()
        calls: list[object] = []
        template_store = search.SearchStore(
            store_id="templates", kind="template",
            query=lambda query: calls.append(query) or [],
        )
        for override in ({"request": {"template_memory": True}},
                         {"request": {"all_features": True}},
                         {"network_mode": "normal"}):
            with self.subTest(override=override):
                with self.assertRaisesRegex(preparation.MandatoryStateFailure,
                                            "recover|new decision"):
                    self.prepare(self.service(), stores=[template_store], **override)
                self.assertEqual([], calls)
                self.assertEqual([first.preparation], self.state.list_preparations(
                    first.decision["decision_id"]
                ))
                self.assertEqual(1, self.state.connection.execute(
                    "SELECT COUNT(*) FROM decisions"
                ).fetchone()[0])

    def test_exact_mandatory_identity_separates_decisions(self) -> None:
        first = self.first()
        changed = [
            {"task_card": contracts.make_task_card(task="Fix another parser", base_commit="base-1")},
            {"task_card": contracts.make_task_card(task="Fix parser", base_commit="base-2")},
            {"plan": contracts.make_plan(
                plan_id="plan-1", objective_id="objective-2", route="ordinary",
                state="candidate", content={"steps": ["repair"]},
            ), "objective_id": "objective-2"},
            {"plan": contracts.make_plan(
                plan_id="plan-1", objective_id="objective-1", route="deeper",
                state="candidate", content={"steps": ["repair"]},
            ), "route": "deeper"},
            {"plan": contracts.make_plan(
                plan_id="plan-2", objective_id="objective-1", route="ordinary",
                state="candidate", content={"steps": ["repair"]},
            )},
            {"plan": contracts.make_plan(
                plan_id="plan-1", objective_id="objective-1", route="ordinary",
                state="candidate", content={"steps": ["different"]},
            )},
            {"plan": contracts.make_plan(
                plan_id="plan-1", objective_id="objective-1", route="ordinary",
                state="accepted", accepted_by="ROOT", content={"steps": ["repair"]},
            )},
        ]
        for change in changed:
            with self.subTest(change=change):
                outcome = self.prepare(
                    self.service(strategy="problem_focused", template_memory=False,
                                 atlas_shared_retrieval=False, experience_read=False),
                    failure_context="parser failed", network_mode="restricted_local",
                    **change,
                )
                self.assertNotEqual(first.decision["decision_id"], outcome.decision["decision_id"])

    def test_unreadable_or_ambiguous_lookup_fails_closed(self) -> None:
        first = self.first()

        class FailedLookup:
            def __init__(self, inner):
                self.inner = inner

            def find_logical_decision(self, identity):
                raise OSError("read failed")

            def __getattr__(self, name):
                return getattr(self.inner, name)

        failing = self.service(state=FailedLookup(self.state))
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(failing)
        second_decision = contracts.make_decision(
            self.card, self.plan, strategy="deeper",
            configuration={"strategy": "deeper"},
        )
        self.state.record_decision(second_decision)
        with self.assertRaisesRegex(preparation.MandatoryStateFailure, "ambiguous"):
            self.prepare(self.service())
        self.assertEqual([first.preparation], self.state.list_preparations(
            first.decision["decision_id"]
        ))

    def test_malformed_durable_decision_fails_closed(self) -> None:
        first = self.first()
        self.state.connection.execute(
            "UPDATE decisions SET configuration = ? WHERE decision_id = ?",
            ("{broken", first.decision["decision_id"]),
        )
        self.state.connection.commit()
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(self.service())
        self.assertEqual(1, len(self.state.list_preparations(first.decision["decision_id"])))

    def test_corrupt_identity_column_cannot_mint_a_drifted_second_budget(self) -> None:
        first = self.first()
        self.state.connection.execute(
            "UPDATE decisions SET plan_digest = ? WHERE decision_id = ?",
            ("corrupt-digest", first.decision["decision_id"]),
        )
        self.state.connection.commit()
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(self.service(strategy="deeper"))
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual([first.preparation], self.state.list_preparations(
            first.decision["decision_id"]
        ))

    def test_two_corrupt_identity_columns_fail_before_distance_filter(self) -> None:
        first = self.first()
        self.state.connection.execute(
            "UPDATE decisions SET objective_id=?, plan_digest=? WHERE decision_id=?",
            ("damaged-objective", "damaged-plan", first.decision["decision_id"]),
        )
        self.state.connection.commit()
        calls: list[object] = []
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(
                self.service(strategy="deeper"),
                stores=[search.SearchStore(
                    store_id="everos", kind="historical_evidence",
                    query=lambda query: calls.append(query) or [],
                )],
            )
        self.assertEqual([], calls)
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual([first.preparation], self.state.list_preparations(
            first.decision["decision_id"]
        ))

    def test_preparation_identity_recovers_owner_after_three_decision_columns_corrupt(self) -> None:
        first = self.first()
        self.state.connection.execute(
            """UPDATE decisions SET task_card_digest=?, objective_id=?, plan_digest=?
               WHERE decision_id=?""",
            ("damaged-task", "damaged-objective", "damaged-plan",
             first.decision["decision_id"]),
        )
        self.state.connection.commit()
        calls: list[object] = []
        with self.assertRaisesRegex(preparation.MandatoryStateFailure,
                                    "logical decision|recover mandatory state"):
            self.prepare(
                self.service(strategy="deeper"),
                stores=[search.SearchStore(
                    store_id="everos", kind="historical_evidence",
                    query=lambda query: calls.append(query) or [],
                )],
            )
        self.assertEqual([], calls)
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual([first.preparation], self.state.list_preparations(
            first.decision["decision_id"]
        ))

    def test_corrupt_preparation_index_fails_before_optional_call_or_write(self) -> None:
        first = self.prepare(self.service())
        self.state.connection.execute(
            "UPDATE preparations SET task_card_digest=? WHERE preparation_id=?",
            ("damaged-index", first.preparation["preparation_id"]),
        )
        self.state.connection.commit()
        decision_row = tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (first.decision["decision_id"],)
        ).fetchone())
        preparation_row = tuple(self.state.connection.execute(
            "SELECT * FROM preparations WHERE preparation_id=?",
            (first.preparation["preparation_id"],),
        ).fetchone())
        writes_before = self.state.connection.total_changes
        calls: list[object] = []
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(
                self.service(strategy="deeper"),
                stores=[search.SearchStore(
                    store_id="everos", kind="historical_evidence",
                    query=lambda query: calls.append(query) or [],
                )],
            )
        self.assertEqual([], calls)
        self.assertEqual(writes_before, self.state.connection.total_changes)
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM preparations"
        ).fetchone()[0])
        self.assertEqual(decision_row, tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (first.decision["decision_id"],)
        ).fetchone()))
        self.assertEqual(preparation_row, tuple(self.state.connection.execute(
            "SELECT * FROM preparations WHERE preparation_id=?",
            (first.preparation["preparation_id"],),
        ).fetchone()))

    def test_corrupt_preparation_index_and_decision_cannot_mint_budget(self) -> None:
        first = self.prepare(self.service())
        self.state.connection.execute(
            "UPDATE preparations SET task_card_digest=? WHERE preparation_id=?",
            ("damaged-index", first.preparation["preparation_id"]),
        )
        self.state.connection.execute(
            """UPDATE decisions SET task_card_digest=?, objective_id=?, plan_digest=?
               WHERE decision_id=?""",
            ("damaged-task", "damaged-objective", "damaged-plan",
             first.decision["decision_id"]),
        )
        self.state.connection.commit()
        decision_row = tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (first.decision["decision_id"],)
        ).fetchone())
        preparation_row = tuple(self.state.connection.execute(
            "SELECT * FROM preparations WHERE preparation_id=?",
            (first.preparation["preparation_id"],),
        ).fetchone())
        writes_before = self.state.connection.total_changes
        calls: list[object] = []
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(
                self.service(strategy="deeper"),
                stores=[search.SearchStore(
                    store_id="everos", kind="historical_evidence",
                    query=lambda query: calls.append(query) or [],
                )],
            )
        self.assertEqual([], calls)
        self.assertEqual(writes_before, self.state.connection.total_changes)
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM preparations"
        ).fetchone()[0])
        self.assertEqual(decision_row, tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (first.decision["decision_id"],)
        ).fetchone()))
        self.assertEqual(preparation_row, tuple(self.state.connection.execute(
            "SELECT * FROM preparations WHERE preparation_id=?",
            (first.preparation["preparation_id"],),
        ).fetchone()))

    def test_malformed_related_preparation_identity_blocks_corrupt_owner_recovery(self) -> None:
        first = self.first()
        packet = dict(first.preparation)
        packet["plan_digest"] = "damaged-preparation-plan"
        self.state.connection.execute(
            "UPDATE preparations SET record=? WHERE preparation_id=?",
            (json.dumps(packet), first.preparation["preparation_id"]),
        )
        self.state.connection.execute(
            """UPDATE decisions SET task_card_digest=?, objective_id=?, plan_digest=?
               WHERE decision_id=?""",
            ("damaged-task", "damaged-objective", "damaged-plan",
             first.decision["decision_id"]),
        )
        self.state.connection.commit()
        calls: list[object] = []
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(
                self.service(strategy="deeper"),
                stores=[search.SearchStore(
                    store_id="everos", kind="historical_evidence",
                    query=lambda query: calls.append(query) or [],
                )],
            )
        self.assertEqual([], calls)
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM preparations"
        ).fetchone()[0])

    def test_unrelated_malformed_preparation_does_not_block_exact_owner(self) -> None:
        first = self.first()
        other_card = contracts.make_task_card(task="Unrelated task", base_commit="base-2")
        other = self.service().prepare(
            task_card=other_card, plan=self.plan, objective_id="objective-1",
            deadline=1300.0,
        )
        self.state.connection.execute(
            "UPDATE preparations SET record=? WHERE preparation_id=?",
            ("{broken", other.preparation["preparation_id"]),
        )
        self.state.connection.commit()
        continued = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision, continued.decision)
        self.assertEqual(2, continued.preparation["attempt"])
        self.assertEqual(2, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])

    def test_distinct_plan_malformed_preparation_does_not_block_exact_owner(self) -> None:
        first = self.first()
        other_plan = contracts.make_plan(
            plan_id="another-plan", objective_id="objective-1", route="ordinary",
            state="candidate", content={"steps": ["unrelated revision"]},
        )
        other = self.prepare(self.service(), plan=other_plan)
        self.state.connection.execute(
            "UPDATE preparations SET record=? WHERE preparation_id=?",
            ("{broken", other.preparation["preparation_id"]),
        )
        self.state.connection.commit()
        continued = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision, continued.decision)
        self.assertEqual(2, continued.preparation["attempt"])
        self.assertEqual(2, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])

    def test_distinct_plan_corrupt_preparation_column_does_not_block_owner(self) -> None:
        first = self.first()
        other_plan = contracts.make_plan(
            plan_id="another-plan", objective_id="objective-1", route="ordinary",
            state="candidate", content={"steps": ["unrelated revision"]},
        )
        other = self.prepare(self.service(), plan=other_plan)
        self.state.connection.execute(
            "UPDATE preparations SET strategy=? WHERE preparation_id=?",
            ("damaged", other.preparation["preparation_id"]),
        )
        self.state.connection.commit()
        continued = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision, continued.decision)
        self.assertEqual(2, continued.preparation["attempt"])

    def test_distinct_plan_corrupt_decision_does_not_block_prepared_owner(self) -> None:
        first = self.first()
        other_plan = contracts.make_plan(
            plan_id="another-plan", objective_id="objective-1", route="ordinary",
            state="candidate", content={"steps": ["unrelated revision"]},
        )
        other = contracts.make_decision(self.card, other_plan)
        self.state.record_decision(other)
        self.state.connection.execute(
            "UPDATE decisions SET configuration=? WHERE decision_id=?",
            ("{broken", other["decision_id"]),
        )
        self.state.connection.commit()
        continued = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision, continued.decision)
        self.assertEqual(2, continued.preparation["attempt"])

    def test_distinct_prepared_plan_corrupt_decision_does_not_block_owner(self) -> None:
        first = self.first()
        other_plan = contracts.make_plan(
            plan_id="another-plan", objective_id="objective-1", route="ordinary",
            state="candidate", content={"steps": ["unrelated revision"]},
        )
        other = self.prepare(self.service(), plan=other_plan)
        self.state.connection.execute(
            "UPDATE decisions SET configuration=? WHERE decision_id=?",
            ("{broken", other.decision["decision_id"]),
        )
        self.state.connection.commit()
        continued = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision, continued.decision)
        self.assertEqual(2, continued.preparation["attempt"])

    def test_distinct_plan_corrupt_decision_does_not_block_standalone_owner(self) -> None:
        standalone = contracts.make_decision(self.card, self.plan)
        self.state.record_decision(standalone)
        other_plan = contracts.make_plan(
            plan_id="another-plan", objective_id="objective-1", route="ordinary",
            state="candidate", content={"steps": ["unrelated revision"]},
        )
        other = contracts.make_decision(self.card, other_plan)
        self.state.record_decision(other)
        self.state.connection.execute(
            "UPDATE decisions SET configuration=? WHERE decision_id=?",
            ("{broken", other["decision_id"]),
        )
        self.state.connection.commit()
        first = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(standalone, first.decision)
        self.assertEqual(1, first.preparation["attempt"])

    def test_explicit_dependent_gates_compare_normalized_policy(self) -> None:
        first = self.prepare(self.service(template_memory=False, experience_write=False))
        self.assertFalse(first.preparation["configuration"]["apc"])
        self.assertFalse(first.preparation["configuration"]["generated_skill_creation"])
        matching = self.prepare(
            self.service(strategy="deeper"),
            request={
                "template_memory": False, "apc": True,
                "experience_write": False, "generated_skill_creation": True,
            },
        )
        self.assertEqual(first.decision["decision_id"], matching.decision["decision_id"])
        self.assertEqual(first.preparation["configuration"], matching.preparation["configuration"])
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(
                self.service(), request={"template_memory": True, "apc": True}
            )
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual(2, len(self.state.list_preparations(first.decision["decision_id"])))

    def test_standalone_decision_claims_first_policy_without_changing_id(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration={"strategy": "standard"},
        )
        self.state.record_decision(standalone)
        first = self.prepare(self.service(template_memory=False))
        self.assertEqual(standalone["decision_id"], first.decision["decision_id"])
        self.assertEqual(1, first.preparation["attempt"])
        self.assertTrue(first.preparation["configuration"]["template_memory"])
        captured = self.state.get_decision(standalone["decision_id"])
        self.assertEqual(standalone["configuration"], captured["configuration"])
        self.assertEqual(standalone["content_hash"], captured["content_hash"])
        self.assertEqual(standalone, first.decision)
        retry = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision["decision_id"], retry.decision["decision_id"])
        self.assertTrue(retry.preparation["configuration"]["template_memory"])
        self.assertEqual(2, retry.preparation["attempt"])

    def test_standalone_complete_policy_and_delivered_links_remain_immutable(self) -> None:
        recorded = asdict(config.resolve_config({
            "strategy": "standard", "template_memory": False,
            "experience_read": False,
        }))
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard", configuration=recorded,
        )
        self.state.record_decision(standalone)
        original_row = tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (standalone["decision_id"],)
        ).fetchone())
        operation = contracts.make_operation(
            kind="dispatch", envelope={
                "content_hash": "delivered-envelope", "decision_id": standalone["decision_id"],
                "run_id": "run-1",
            }, status="delivered", observed_invocation={"receipt": "delivered"},
        )
        self.state.create_operation(contracts.make_operation(
            kind="dispatch", envelope={
                "content_hash": "delivered-envelope", "decision_id": standalone["decision_id"],
                "run_id": "run-1",
            },
        ))
        self.state.record_operation(operation)
        outcome = contracts.make_outcome(
            decision_id=standalone["decision_id"], plan_id=self.plan["plan_id"],
            plan_digest=self.plan["content_hash"], status="PASS",
            evidence_digest="delivered-evidence", linked_run_id="run-1",
            task_card_digest=self.card["content_hash"], objective_id="objective-1",
        )
        self.state.record_outcome(outcome)
        stored_operation = self.state.get_operation(operation["operation_id"])
        stored_outcome = self.state.get_outcome(standalone["decision_id"])
        calls: list[object] = []
        first = self.prepare(
            self.service(strategy="deeper", template_memory=True, experience_read=True),
            stores=[search.SearchStore(
                store_id="templates", kind="template",
                query=lambda query: calls.append(query) or [],
            )],
        )
        self.assertEqual(standalone, first.decision)
        self.assertEqual(standalone["configuration"], self.state.get_decision(
            standalone["decision_id"]
        )["configuration"])
        self.assertEqual(standalone["content_hash"], self.state.get_decision(
            standalone["decision_id"]
        )["content_hash"])
        self.assertFalse(first.preparation["configuration"]["template_memory"])
        self.assertFalse(first.preparation["configuration"]["experience_read"])
        self.assertEqual([], calls)
        self.assertEqual(stored_operation, self.state.get_operation(operation["operation_id"]))
        self.assertEqual(stored_outcome, self.state.get_outcome(standalone["decision_id"]))
        self.assertEqual(original_row, tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (standalone["decision_id"],)
        ).fetchone()))

    def test_standalone_minimal_deeper_and_known_policy_conflicts(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="deeper",
            configuration={"strategy": "deeper", "template_memory": False},
        )
        self.state.record_decision(standalone)
        calls: list[object] = []
        candidate_store = search.SearchStore(
            store_id="templates", kind="template",
            query=lambda query: calls.append(query) or [],
        )
        for request in ({"strategy": "standard"}, {"template_memory": True}):
            with self.subTest(request=request):
                with self.assertRaisesRegex(preparation.MandatoryStateFailure,
                                            "recover|new decision"):
                    self.prepare(self.service(), request=request, stores=[candidate_store])
                self.assertEqual([], self.state.list_preparations(standalone["decision_id"]))
                self.assertEqual([], calls)
        first = self.prepare(
            self.service(strategy="standard", template_memory=True), stores=[candidate_store]
        )
        self.assertEqual(standalone, first.decision)
        self.assertEqual("deeper", first.preparation["requested_strategy"])
        self.assertEqual("deeper", first.preparation["strategy"])
        self.assertFalse(first.preparation["configuration"]["template_memory"])
        self.assertTrue(first.preparation["configuration"]["deeper"])
        self.assertEqual([], calls)
        retry = self.prepare(self.service(strategy="standard", template_memory=True))
        self.assertEqual(standalone, retry.decision)
        self.assertEqual(first.preparation["configuration"], retry.preparation["configuration"])
        self.assertEqual(2, retry.preparation["attempt"])

    def test_standalone_inconsistent_recorded_gate_fails_before_claim(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration={
                "strategy": "standard", "template_memory": False, "apc": True,
            },
        )
        self.state.record_decision(standalone)
        original_row = tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (standalone["decision_id"],)
        ).fetchone())
        calls: list[object] = []
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(
                self.service(), stores=[search.SearchStore(
                    store_id="templates", kind="template",
                    query=lambda query: calls.append(query) or [],
                )],
            )
        self.assertEqual([], calls)
        self.assertEqual([], self.state.list_preparations(standalone["decision_id"]))
        self.assertEqual(original_row, tuple(self.state.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=?", (standalone["decision_id"],)
        ).fetchone()))

    def test_standalone_missing_gate_accepts_explicit_first_policy(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration={"strategy": "standard"},
        )
        self.state.record_decision(standalone)
        first = self.prepare(
            self.service(template_memory=True), request={"template_memory": False}
        )
        self.assertEqual(standalone, first.decision)
        self.assertFalse(first.preparation["configuration"]["template_memory"])
        self.assertEqual(standalone["content_hash"], self.state.get_decision(
            standalone["decision_id"]
        )["content_hash"])
        retry = self.prepare(self.service(template_memory=True))
        self.assertFalse(retry.preparation["configuration"]["template_memory"])

    def test_standalone_all_off_policy_survives_service_default_drift(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration=asdict(config.resolve_config({"all_features": False})),
        )
        self.state.record_decision(standalone)
        calls: list[object] = []
        outcome = self.prepare(
            self.service(), stores=[search.SearchStore(
                store_id="everos", kind="historical_evidence",
                query=lambda query: calls.append(query) or [],
            )],
        )
        self.assertEqual("inherited", outcome.mode)
        self.assertEqual([], calls)
        self.assertEqual(standalone["content_hash"], self.state.get_decision(
            standalone["decision_id"]
        )["content_hash"])
        self.assertEqual([], self.state.list_preparations(standalone["decision_id"]))
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])

    def test_standalone_all_off_service_keeps_recorded_enabled_gate(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration={"strategy": "standard", "template_memory": True},
        )
        self.state.record_decision(standalone)
        first = self.prepare(self.service(all_features=False))
        self.assertEqual(standalone, first.decision)
        self.assertTrue(first.preparation["configuration"]["template_memory"])
        self.assertEqual(1, first.preparation["attempt"])

    def test_standalone_minimal_standard_all_off_service_stays_inherited(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration={"strategy": "standard"},
        )
        self.state.record_decision(standalone)
        outcome = self.prepare(self.service(all_features=False))
        self.assertEqual("inherited", outcome.mode)
        self.assertEqual([], self.state.list_preparations(standalone["decision_id"]))
        self.assertEqual(standalone["content_hash"], self.state.get_decision(
            standalone["decision_id"]
        )["content_hash"])

    def test_standalone_claim_rolls_back_and_racing_caller_reuses_winner(self) -> None:
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration={"strategy": "standard"},
        )
        self.state.record_decision(standalone)
        self.state.connection.execute(
            """CREATE TRIGGER fail_first_prep BEFORE INSERT ON preparations
               BEGIN SELECT RAISE(ABORT, 'synthetic preparation write failure'); END"""
        )
        with self.assertRaises(preparation.MandatoryStateFailure):
            self.prepare(self.service(template_memory=False))
        rolled_back = self.state.get_decision(standalone["decision_id"])
        self.assertEqual(standalone["configuration"], rolled_back["configuration"])
        self.assertEqual(standalone["content_hash"], rolled_back["content_hash"])
        self.assertEqual([], self.state.list_preparations(standalone["decision_id"]))
        self.state.connection.execute("DROP TRIGGER fail_first_prep")
        self.state.connection.commit()

        barrier = threading.Barrier(2)

        class RacingStore:
            def __init__(self, inner):
                self.inner = inner
                self.first = True

            def list_captured_preparations(self, decision_id):
                prior = self.inner.list_captured_preparations(decision_id)
                if self.first:
                    self.first = False
                    barrier.wait(timeout=5)
                return prior

            def __getattr__(self, name):
                return getattr(self.inner, name)

        def worker(template_memory: bool):
            own_store = store.MemoryStore(self.path)
            own_store.initialize()
            try:
                return self.prepare(self.service(
                    state=RacingStore(own_store), template_memory=template_memory
                ))
            finally:
                own_store.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker, value) for value in (False, True)]
            results = [future.result(timeout=10) for future in futures]
        self.assertEqual({standalone["decision_id"]}, {
            item.decision["decision_id"] for item in results
        })
        self.assertEqual({1, 2}, {item.preparation["attempt"] for item in results})
        self.assertEqual(1, len({
            item.preparation["configuration"]["template_memory"] for item in results
        }))
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual(standalone["content_hash"], self.state.get_decision(
            standalone["decision_id"]
        )["content_hash"])

    def test_standalone_identifier_collision_preserves_the_other_plan(self) -> None:
        captured_config = asdict(config.resolve_config({}))
        standalone = contracts.make_decision(
            self.card, self.plan, strategy="standard",
            configuration=captured_config,
        )
        self.state.record_decision(standalone)
        revised_plan = contracts.make_plan(
            plan_id=self.plan["plan_id"], objective_id="objective-1",
            route="ordinary", state="candidate", content={"steps": ["revised"]},
        )
        self.assertEqual(standalone["decision_id"], contracts.make_decision(
            self.card, revised_plan, strategy="standard",
            configuration=captured_config,
        )["decision_id"])
        other = self.prepare(self.service(), plan=revised_plan)
        self.assertNotEqual(standalone["decision_id"], other.decision["decision_id"])
        self.assertEqual(standalone["configuration"], self.state.get_decision(
            standalone["decision_id"]
        )["configuration"])
        self.assertEqual([], self.state.list_preparations(standalone["decision_id"]))
        self.assertEqual(1, other.preparation["attempt"])

    def test_unknown_time_restart_never_grants_a_second_cheap_pass(self) -> None:
        calls: list[object] = []
        launches: list[object] = []
        candidate_store = search.SearchStore(
            store_id="everos", kind="historical_evidence",
            query=lambda query: calls.append(query) or [],
        )
        first = self.prepare(
            self.service(), deadline=None, unknown_time=True,
            stores=[candidate_store],
        )
        self.assertEqual(1, len(calls))
        self.state.close()
        self.state = store.MemoryStore(self.path)
        self.state.initialize()
        second = self.prepare(
            self.service(strategy="deeper"), deadline=None, unknown_time=True,
            stores=[candidate_store],
            apc_launcher=lambda request: launches.append(request) or None,
        )
        self.assertEqual(first.decision["decision_id"], second.decision["decision_id"])
        self.assertEqual(2, second.preparation["attempt"])
        self.assertEqual("unknown_time", second.preparation["budget_source"])
        self.assertIsNone(second.preparation["deadline_monotonic"])
        self.assertIsNone(second.preparation["remaining_seconds"])
        self.assertEqual(0.0, second.preparation["stage_allowance_seconds"])
        self.assertEqual(1, len(calls))
        self.assertEqual([], launches)
        self.assertEqual("no_optional_memory", second.trace["outcome"])

    def test_unknown_time_level_zero_does_not_reopen_optional_search(self) -> None:
        calls: list[object] = []
        candidate_store = search.SearchStore(
            store_id="everos", kind="historical_evidence",
            query=lambda query: calls.append(query) or [],
        )
        first = self.prepare(
            self.service(), deadline=None, unknown_time=True,
            stores=[candidate_store],
        )
        self.assertEqual(1, len(calls))
        corrected = self.service().apply_level_zero(
            preparation=first.preparation, decision=first.decision,
            task_card=self.card, plan=first.plan, objective_id="objective-1",
            stores=[candidate_store],
        )
        self.assertEqual(1, len(calls))
        self.assertEqual(0.0, corrected.preparation["stage_allowance_seconds"])
        self.assertEqual("unknown_time", corrected.preparation["budget_source"])
        self.assertEqual("no_optional_memory", corrected.trace["outcome"])

    def test_unknown_time_first_preparation_can_be_rewritten_without_new_pass(self) -> None:
        first = self.prepare(self.service(), deadline=None, unknown_time=True)
        preparation_id = first.preparation["preparation_id"]
        self.assertEqual(first.preparation, self.state.record_preparation(first.preparation))
        superseded = self.state.mark_preparation_superseded(
            preparation_id, superseded_by="next-preparation"
        )
        self.assertEqual("superseded", superseded["status"])
        self.assertEqual("next-preparation", superseded["superseded_by"])
        self.assertEqual(1, len(self.state.list_preparations(first.decision["decision_id"])))

    def test_unrelated_corrupt_decision_does_not_block_exact_owner(self) -> None:
        first = self.first()
        other_plan = contracts.make_plan(
            plan_id="another-plan", objective_id="another-objective",
            route="ordinary", state="candidate", content={"steps": ["other"]},
        )
        other = self.prepare(
            self.service(), plan=other_plan, objective_id="another-objective"
        )
        self.state.connection.execute(
            "UPDATE decisions SET configuration = ? WHERE decision_id = ?",
            ("{broken", other.decision["decision_id"]),
        )
        self.state.connection.commit()
        self.clock.value += 10.0
        retry = self.prepare(self.service(strategy="deeper"))
        self.assertEqual(first.decision["decision_id"], retry.decision["decision_id"])
        self.assertEqual(2, retry.preparation["attempt"])

    def test_concurrent_first_writers_share_captured_policy_and_attempt_sequence(self) -> None:
        barrier = threading.Barrier(2)

        class RacingStore:
            def __init__(self, inner):
                self.inner = inner
                self.first_lookup = True

            def find_logical_decision(self, identity):
                result = self.inner.find_logical_decision(identity)
                if self.first_lookup:
                    self.first_lookup = False
                    barrier.wait(timeout=5)
                return result

            def __getattr__(self, name):
                return getattr(self.inner, name)

        def worker(template_memory: bool):
            own_store = store.MemoryStore(self.path)
            own_store.initialize()
            try:
                service = self.service(state=RacingStore(own_store),
                                       template_memory=template_memory)
                return self.prepare(service)
            finally:
                own_store.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            future_a = pool.submit(worker, False)
            future_b = pool.submit(worker, True)
            results = [future_a.result(timeout=10), future_b.result(timeout=10)]
        ids = {outcome.decision["decision_id"] for outcome in results}
        self.assertEqual(1, len(ids))
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual({1, 2}, {item.preparation["attempt"] for item in results})
        self.assertEqual(1, len({item.preparation["configuration"]["template_memory"]
                                 for item in results}))
        self.assertEqual(2, len(self.state.list_preparations(results[0].decision["decision_id"])))

    def test_concurrent_conflicting_network_first_writer_fails_closed(self) -> None:
        barrier = threading.Barrier(2)

        class RacingStore:
            def __init__(self, inner):
                self.inner = inner
                self.wait_once = True

            def find_logical_decision(self, identity):
                result = self.inner.find_logical_decision(identity)
                if self.wait_once:
                    self.wait_once = False
                    barrier.wait(timeout=5)
                return result

            def __getattr__(self, name):
                return getattr(self.inner, name)

        def worker(network_mode: str):
            own_store = store.MemoryStore(self.path)
            own_store.initialize()
            try:
                service = self.service(state=RacingStore(own_store))
                return self.prepare(service, network_mode=network_mode)
            finally:
                own_store.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker, mode) for mode in
                       ("normal", "restricted_local")]
            results = []
            failures = []
            for future in futures:
                try:
                    results.append(future.result(timeout=10))
                except preparation.MandatoryStateFailure as exc:
                    failures.append(str(exc))
        self.assertEqual(1, len(results))
        self.assertEqual(1, len(failures))
        self.assertIn("network policy", failures[0])
        self.assertEqual(1, self.state.connection.execute(
            "SELECT COUNT(*) FROM decisions"
        ).fetchone()[0])
        self.assertEqual([results[0].preparation], self.state.list_preparations(
            results[0].decision["decision_id"]
        ))


if __name__ == "__main__":
    unittest.main()
