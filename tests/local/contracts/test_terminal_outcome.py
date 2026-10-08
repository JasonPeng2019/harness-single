"""The enhanced parent fixes quality only from its exact native terminal chain."""

from __future__ import annotations

import sys
import tempfile
import unittest
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, context, contracts, runtime, store


class TerminalOutcomeTests(unittest.TestCase):
    def setUp(self) -> None:
        # SQLite WAL cleanup needs a local filesystem on NFS-backed runners.
        local_tmp = Path("/dev/shm")
        self.temporary = tempfile.TemporaryDirectory(
            dir=local_tmp if local_tmp.is_dir() and local_tmp.exists() else None
        )
        self.root = Path(self.temporary.name)
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        self.prepare_case()

    def prepare_case(self, *, plan_id: str = "plan-1", run_id: str = "run-1",
                     lane_id: str = "lane-1", supersedes: str | None = None) -> None:
        self.run_id, self.lane_id = run_id, lane_id
        self.plan = contracts.make_plan(
            plan_id=plan_id, objective_id="objective-1", route="ordinary",
            state="accepted", accepted_by="ROOT", content={"steps": ["verify", plan_id]},
            supersedes=supersedes,
        )
        self.card = contracts.make_task_card(
            task="Verify the change", base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id="objective-1", route="ordinary", plan=self.plan,
                checkpoint="checkpoint-1",
            ),
        )
        self.configuration = {"strategy": "standard"}
        self.decision = contracts.make_decision(
            self.card, self.plan, configuration=self.configuration,
        )
        self.state.record_decision(self.decision)
        self.final = context.finalize_context(
            task_card=self.card, plan=self.plan,
            decision_id=self.decision["decision_id"], lane_id=lane_id, run_id=run_id,
            worktree_path=str(self.root / "worktree"), base_commit="base-1",
            strategy="standard", configuration=self.configuration,
            checkpoint="checkpoint-1", execution_role="worker",
            invocation_target="harness:worker", recipient="worker:lane-1",
            mandatory_content=[
                {"id": "task", "kind": "task", "content": self.card["task"]},
                {"id": "accepted-plan", "kind": "accepted-plan", "content": self.plan["content"]},
                {"id": "base", "kind": "base", "content": "base-1"},
                {"id": "route", "kind": "route", "content": "ordinary"},
                {"id": "checkpoint", "kind": "checkpoint", "content": "checkpoint-1"},
                {"id": "security", "kind": "security", "content": context.ROLE_SEPARATION},
            ],
            limits=config.resolve_limits({"context_char_limit": 4000}),
        )
        self.state.record_final_context(
            self.final.context, envelope_digest=self.final.envelope["content_hash"],
        )

    def tearDown(self) -> None:
        self.state.close()
        self.temporary.cleanup()

    def observe(self) -> dict:
        self.runtime.record_dispatch_intent(self.final.envelope)
        receipt = {
            "invocation_id": "controller:7:now", "pid": 7, "creation_time": "now",
            "task_card_digest": self.card["content_hash"],
            "decision_id": self.decision["decision_id"],
            "plan_id": self.plan["plan_id"], "plan_digest": self.plan["content_hash"],
            "envelope_digest": self.final.envelope["content_hash"],
            "lane_id": self.lane_id, "run_id": self.run_id, "base_commit": "base-1",
            "route": "ordinary", "configuration_digest": self.decision["configuration_digest"],
            "context_id": self.final.context["context_id"],
            "context_digest": self.final.context["content_hash"],
        }
        self.runtime.record_observed_dispatch(self.final.envelope, receipt)
        return receipt

    def bundle(self, *, status: str = "PASS") -> dict:
        operation = self.state.get_operation(contracts.make_operation(
            kind="dispatch", envelope=self.final.envelope,
        )["operation_id"])
        native_operation = {key: operation[key] for key in (
            "operation_id", "decision_id", "envelope_digest", "run_id", "kind",
            "status", "observed_invocation", "created_at", "updated_at",
        )}
        result = {
            "schema": "result/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "outcome": status,
        }
        result["content_hash"] = contracts.content_hash(result)
        review = {
            "schema": "completion-review/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "review_outcome": status, "task_card_id": self.card["content_hash"],
            "task_card_hash": self.card["content_hash"], "result_id": self.run_id,
            "result_hash": result["content_hash"], "commit": "commit-1",
            "reviewed_at": "2026-09-26T00:00:00Z",
        }
        review["content_hash"] = contracts.content_hash(review)
        acceptance = {
            "schema": "orchestrator-acceptance/v1", "lane_id": self.lane_id, "run_id": self.run_id,
            "approval": "ACCEPTED" if status == "PASS" else "REJECTED", "accepted_by": "ROOT",
            "review_ref": review["content_hash"], "task_card_id": self.card["content_hash"],
            "task_card_hash": self.card["content_hash"], "result_id": self.run_id,
            "result_hash": result["content_hash"], "commit": "commit-1",
            "decided_at": "2026-09-26T00:00:00Z",
        }
        acceptance["content_hash"] = contracts.content_hash(acceptance)
        bundle = {
            "schema": "native-terminal-evidence/v1", "epoch_id": "epoch-1",
            "lane_id": self.lane_id, "run_id": self.run_id, "task_card": self.card,
            "accepted_plan": self.plan, "objective_id": "objective-1",
            "decision_id": self.decision["decision_id"], "decision": self.decision,
            "final_context": self.final.context,
            "dispatch": {
                "operation": native_operation,
                "operation_digest": contracts.sha256_hex(native_operation),
                "envelope_digest": self.final.envelope["content_hash"],
                "envelope": self.final.envelope,
                "observed_invocation": operation["observed_invocation"],
            },
            "configuration": self.configuration,
            "configuration_digest": contracts.sha256_hex(self.configuration),
            "result": result, "review": review, "acceptance": acceptance,
            "terminal_proof": None,
        }
        bundle["content_hash"] = contracts.content_hash(bundle)
        return bundle

    @staticmethod
    def exceptionally_accept(evidence: dict) -> dict:
        evidence["acceptance"]["approval"] = "ACCEPTED"
        evidence["acceptance"]["force_accept_reason"] = "ROOT exception"
        evidence["acceptance"]["content_hash"] = contracts.content_hash(evidence["acceptance"])
        evidence["content_hash"] = contracts.content_hash(evidence)
        return evidence

    def test_exact_observed_native_chain_fixes_pass(self) -> None:
        self.observe()
        outcome = self.runtime.record_terminal_outcome(self.bundle())
        self.assertEqual("PASS", outcome["status"])
        self.assertEqual("ACCEPTED", outcome["acceptance_status"])
        self.assertFalse(outcome["exceptional_acceptance"])

    def next_run(self, run_id: str, *, recipient: str = "worker:lane-1") -> None:
        self.run_id = run_id
        self.final = context.finalize_context(
            task_card=self.card, plan=self.plan,
            decision_id=self.decision["decision_id"], lane_id=self.lane_id,
            run_id=run_id, worktree_path=str(self.root / "worktree"),
            base_commit="base-1", strategy="standard",
            configuration=self.configuration, checkpoint="checkpoint-1",
            execution_role="worker", invocation_target="harness:worker",
            recipient=recipient,
            mandatory_content=self.final.envelope["mandatory_content"],
            limits=config.resolve_limits({"context_char_limit": 4000}),
        )
        self.state.record_final_context(
            self.final.context, envelope_digest=self.final.envelope["content_hash"],
        )

    def test_rejected_attempt_authorizes_one_corrected_run_and_final_quality(self) -> None:
        self.observe()
        first = self.bundle(status="FAIL")
        with self.assertRaisesRegex(store.OutcomeConflictError, "ROOT ACCEPTED"):
            self.runtime.record_terminal_outcome(first)
        rejected = self.runtime.record_rejected_native_attempt(first)
        self.assertEqual(rejected, self.runtime.record_rejected_native_attempt(first))
        self.assertEqual(first, rejected["terminal_evidence"])
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])

        self.next_run("run-2")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(self.final.envelope)
        intent = self.runtime.record_dispatch_intent(
            self.final.envelope,
            supersedes_rejected_attempt_id=rejected["rejected_attempt_id"],
        )
        self.assertEqual(rejected["rejected_attempt_id"], intent["supersedes_rejected_attempt_id"])
        with self.assertRaises(runtime.DispatchAmbiguityError):
            self.runtime.record_dispatch_intent(
                self.final.envelope,
                supersedes_rejected_attempt_id=rejected["rejected_attempt_id"],
            )
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id="different-attempt",
            )
        self.runtime.record_observed_dispatch(self.final.envelope, self.native_receipt())
        second = self.bundle(status="BLOCKED")
        rejected_second = self.runtime.record_rejected_native_attempt(second)
        self.assertNotEqual(rejected["rejected_attempt_id"], rejected_second["rejected_attempt_id"])
        self.next_run("run-3")
        self.runtime.record_dispatch_intent(
            self.final.envelope,
            supersedes_rejected_attempt_id=rejected_second["rejected_attempt_id"],
        )
        self.runtime.record_observed_dispatch(self.final.envelope, self.native_receipt())
        fixed = self.runtime.record_terminal_outcome(self.bundle())
        self.assertEqual("PASS", fixed["status"])
        self.assertEqual("ACCEPTED", fixed["acceptance_status"])
        self.assertEqual(rejected, self.state.get_rejected_native_attempt(rejected["rejected_attempt_id"]))
        self.assertEqual(rejected_second, self.state.get_rejected_native_attempt(rejected_second["rejected_attempt_id"]))
        self.assertNotIn("effect_status", fixed)
        self.assertEqual(rejected, self.runtime.record_rejected_native_attempt(first))

    def test_rejection_replay_conflict_and_restart_are_atomic(self) -> None:
        self.observe()
        original = self.bundle(status="FAIL")
        self.state.close()
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])
        attempt = self.runtime.record_rejected_native_attempt(original)
        self.state.close()
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        self.assertEqual(attempt, self.runtime.record_rejected_native_attempt(original))
        contradiction = deepcopy(original)
        contradiction["review"]["commit"] = "different"
        contradiction["review"]["content_hash"] = contracts.content_hash(contradiction["review"])
        contradiction["acceptance"]["commit"] = "different"
        contradiction["acceptance"]["review_ref"] = contradiction["review"]["content_hash"]
        contradiction["acceptance"]["content_hash"] = contracts.content_hash(contradiction["acceptance"])
        contradiction["content_hash"] = contracts.content_hash(contradiction)
        with self.assertRaises(store.OutcomeConflictError):
            self.runtime.record_rejected_native_attempt(contradiction)
        accepted_same_run = deepcopy(original)
        accepted_same_run["acceptance"]["approval"] = "ACCEPTED"
        accepted_same_run["acceptance"]["force_accept_reason"] = "late override"
        accepted_same_run["acceptance"]["content_hash"] = contracts.content_hash(
            accepted_same_run["acceptance"]
        )
        accepted_same_run["content_hash"] = contracts.content_hash(accepted_same_run)
        with self.assertRaises(store.OutcomeConflictError):
            self.runtime.record_terminal_outcome(accepted_same_run)
        self.assertEqual(attempt, self.state.get_rejected_native_attempt(attempt["rejected_attempt_id"]))
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])

    def test_correction_requires_latest_exact_rejected_attempt_and_one_use(self) -> None:
        self.observe()
        first = self.runtime.record_rejected_native_attempt(self.bundle(status="FAIL"))
        self.next_run("run-2")
        for wrong in ("unknown-attempt", None):
            with self.subTest(wrong=wrong), self.assertRaises(store.OperationConflictError):
                self.runtime.record_dispatch_intent(
                    self.final.envelope, supersedes_rejected_attempt_id=wrong,
                )
        self.runtime.record_dispatch_intent(
            self.final.envelope, supersedes_rejected_attempt_id=first["rejected_attempt_id"],
        )
        self.runtime.record_observed_dispatch(self.final.envelope, self.native_receipt())
        self.next_run("run-3")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id=first["rejected_attempt_id"],
            )
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(self.final.envelope)

    def test_failed_pre_spawn_transfers_one_rejected_authorization_across_restart(self) -> None:
        self.observe()
        rejected = self.runtime.record_rejected_native_attempt(self.bundle(status="FAIL"))
        token = rejected["rejected_attempt_id"]
        history = []
        for run_id in ("run-2", "run-3"):
            self.next_run(run_id)
            intent = self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id=token,
            )
            history.append(intent["operation_id"])
            self.runtime.mark_dispatch_pre_spawn_failed(self.final.envelope)
            self.state.close()
            self.state = store.MemoryStore(self.root / "state.sqlite3")
            self.state.initialize()
            self.runtime = runtime.MemoryRuntime(self.state)
        self.next_run("run-4")
        active = self.runtime.record_dispatch_intent(
            self.final.envelope, supersedes_rejected_attempt_id=token,
        )
        self.assertNotIn(active["operation_id"], history)
        self.assertEqual(
            ["failed_pre_spawn", "failed_pre_spawn"],
            [self.state.get_operation(operation_id)["status"] for operation_id in history],
        )
        self.assertTrue(all(
            self.state.get_operation(operation_id)["supersedes_rejected_attempt_id"] == token
            for operation_id in history
        ))
        self.runtime.record_observed_dispatch(self.final.envelope, self.native_receipt())
        self.next_run("run-5")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id=token,
            )
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(self.final.envelope)

    def test_corrected_pending_and_ambiguous_replay_never_grants_second_launch(self) -> None:
        self.observe()
        token = self.runtime.record_rejected_native_attempt(
            self.bundle(status="FAIL")
        )["rejected_attempt_id"]
        self.next_run("run-2")
        intent = self.runtime.record_dispatch_intent(
            self.final.envelope, supersedes_rejected_attempt_id=token,
        )
        for status in ("pending", "ambiguous"):
            with self.subTest(status=status), self.assertRaises(runtime.DispatchAmbiguityError):
                self.runtime.record_dispatch_intent(
                    self.final.envelope, supersedes_rejected_attempt_id=token,
                )
            self.assertEqual(status, self.state.get_operation(intent["operation_id"])["status"])
            if status == "pending":
                self.runtime.mark_dispatch_ambiguous(self.final.envelope)
        self.next_run("run-3")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id=token,
            )

    def test_abandoned_corrected_intent_does_not_release_rejected_authorization(self) -> None:
        self.observe()
        token = self.runtime.record_rejected_native_attempt(
            self.bundle(status="FAIL")
        )["rejected_attempt_id"]
        self.next_run("run-2")
        self.runtime.record_dispatch_intent(
            self.final.envelope, supersedes_rejected_attempt_id=token,
        )
        self.runtime.abandon_dispatch_intent(self.final.envelope)
        self.next_run("run-3")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id=token,
            )

    def test_concurrent_corrected_claims_leave_one_active_owner(self) -> None:
        self.observe()
        token = self.runtime.record_rejected_native_attempt(
            self.bundle(status="FAIL")
        )["rejected_attempt_id"]
        self.next_run("run-2")
        envelope = self.final.envelope
        path = self.root / "state.sqlite3"

        def claim() -> str:
            with store.MemoryStore(path) as state:
                try:
                    runtime.MemoryRuntime(state).record_dispatch_intent(
                        envelope, supersedes_rejected_attempt_id=token,
                    )
                    return "created"
                except runtime.DispatchAmbiguityError:
                    return "owned"

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: claim(), range(2)))
        self.assertCountEqual(["created", "owned"], results)
        rows = self.state.connection.execute(
            "SELECT operations.status FROM dispatch_bindings JOIN operations USING(operation_id) "
            "WHERE supersedes_rejected_attempt_id=?", (token,),
        ).fetchall()
        self.assertEqual(["pending"], [row["status"] for row in rows])

    def test_legacy_unique_binding_migrates_without_losing_history_or_ownership(self) -> None:
        self.observe()
        token = self.runtime.record_rejected_native_attempt(
            self.bundle(status="FAIL")
        )["rejected_attempt_id"]
        self.next_run("run-2")
        pending = self.runtime.record_dispatch_intent(
            self.final.envelope, supersedes_rejected_attempt_id=token,
        )
        self.state.close()
        with sqlite3.connect(self.root / "state.sqlite3") as legacy:
            legacy.execute("DROP TABLE IF EXISTS rejected_authorizations")
            legacy.execute("DROP INDEX IF EXISTS dispatch_rejected_authorization")
            legacy.execute(
                "CREATE TABLE dispatch_bindings_legacy ("
                "operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id), "
                "lane_id TEXT NOT NULL, envelope_record TEXT NOT NULL, "
                "native_receipt_required INTEGER NOT NULL, "
                "supersedes_rejected_attempt_id TEXT UNIQUE)"
            )
            legacy.execute(
                "INSERT INTO dispatch_bindings_legacy SELECT operation_id, lane_id, "
                "envelope_record, native_receipt_required, supersedes_rejected_attempt_id "
                "FROM dispatch_bindings"
            )
            legacy.execute("DROP TABLE dispatch_bindings")
            legacy.execute("ALTER TABLE dispatch_bindings_legacy RENAME TO dispatch_bindings")
            legacy.execute(
                "CREATE UNIQUE INDEX dispatch_rejected_authorization "
                "ON dispatch_bindings(supersedes_rejected_attempt_id)"
            )
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        with self.assertRaises(runtime.DispatchAmbiguityError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id=token,
            )
        self.runtime.mark_dispatch_pre_spawn_failed(self.final.envelope)
        self.state.close()
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        self.next_run("run-3")
        fresh = self.runtime.record_dispatch_intent(
            self.final.envelope, supersedes_rejected_attempt_id=token,
        )
        self.assertEqual(token, self.state.get_operation(pending["operation_id"])[
            "supersedes_rejected_attempt_id"
        ])
        self.assertNotEqual(pending["operation_id"], fresh["operation_id"])
        indexes = self.state.connection.execute(
            "PRAGMA index_list(dispatch_bindings)"
        ).fetchall()
        authorization_index_found = False
        for index in indexes:
            columns = [column["name"] for column in self.state.connection.execute(
                f"PRAGMA index_info({index['name']})"
            ).fetchall()]
            if columns == ["supersedes_rejected_attempt_id"]:
                authorization_index_found = True
                self.assertFalse(index["unique"])
        self.assertTrue(authorization_index_found)
        self.assertEqual([], self.state.connection.execute("PRAGMA foreign_key_check").fetchall())

    def test_accepted_run_cannot_authorize_a_new_same_decision_dispatch(self) -> None:
        self.observe()
        accepted = self.bundle()
        self.runtime.record_terminal_outcome(accepted)
        with self.assertRaises(contracts.ContractError):
            self.runtime.record_rejected_native_attempt(accepted)
        self.next_run("run-2")
        for attempt_id in (None, "invented-id"):
            with self.subTest(attempt_id=attempt_id), self.assertRaises(store.OperationConflictError):
                self.runtime.record_dispatch_intent(
                    self.final.envelope, supersedes_rejected_attempt_id=attempt_id,
                )

    def test_unreviewed_pending_ambiguous_and_wrong_recipient_stay_blocked(self) -> None:
        self.observe()
        self.next_run("run-2")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(self.final.envelope)
        # A caller cannot turn a delivered but unreviewed run into authority.
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope, supersedes_rejected_attempt_id="caller-assertion",
            )

        self.prepare_case(plan_id="plan-pending", run_id="pending", lane_id="lane-pending")
        self.runtime.record_dispatch_intent(self.final.envelope)
        self.next_run("pending-2")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(self.final.envelope)

        self.prepare_case(plan_id="plan-ambiguous", run_id="ambiguous", lane_id="lane-ambiguous")
        self.runtime.record_dispatch_intent(self.final.envelope)
        self.runtime.mark_dispatch_ambiguous(self.final.envelope)
        self.next_run("ambiguous-2")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(self.final.envelope)

        self.prepare_case(plan_id="plan-recipient", run_id="recipient-1", lane_id="lane-recipient")
        self.observe()
        rejected = self.runtime.record_rejected_native_attempt(self.bundle(status="FAIL"))
        self.next_run("recipient-2", recipient="worker:someone-else")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope,
                supersedes_rejected_attempt_id=rejected["rejected_attempt_id"],
            )
        self.prepare_case(plan_id="other-plan", run_id="other-run", lane_id="other-lane")
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_dispatch_intent(
                self.final.envelope,
                supersedes_rejected_attempt_id=rejected["rejected_attempt_id"],
            )

    def test_quality_statuses_and_exceptional_acceptance_stay_distinct(self) -> None:
        for status in ("PASS", "FAIL", "BLOCKED"):
            with self.subTest(status=status):
                self.prepare_case(plan_id=f"plan-{status}", run_id=f"run-{status}",
                                  lane_id=f"lane-{status}")
                self.observe()
                evidence = self.bundle(status=status)
                if status != "PASS":
                    evidence["acceptance"]["approval"] = "ACCEPTED"
                    evidence["acceptance"]["force_accept_reason"] = "ROOT exception"
                    evidence["acceptance"]["content_hash"] = contracts.content_hash(evidence["acceptance"])
                    evidence["content_hash"] = contracts.content_hash(evidence)
                fixed = self.runtime.record_terminal_outcome(evidence)
                self.assertEqual(status, fixed["status"])
                self.assertEqual(status != "PASS", fixed["exceptional_acceptance"])
                self.assertEqual("ACCEPTED", fixed["acceptance_status"])
                self.assertNotIn("effect_status", fixed)
                self.assertNotIn("usage_status", fixed)

    def test_terminal_unknown_requires_no_result_proof_and_exception(self) -> None:
        self.observe()
        evidence = self.bundle()
        proof = {
            "schema": "controller-status/v1", "lane_id": "lane-1", "run_id": "run-1",
            "controller_state": "exited", "provider_state": {"state": "exited"},
            "result_state": "absent", "recorded_status": "provider_exited_no_result",
            "cleanup_proven": True,
        }
        evidence["result"] = None
        evidence["terminal_proof"] = proof
        evidence["review"].update({
            "review_outcome": "UNKNOWN", "result_id": None, "result_hash": None,
            "terminal_proof_digest": contracts.sha256_hex(proof),
        })
        evidence["review"]["content_hash"] = contracts.content_hash(evidence["review"])
        evidence["acceptance"].update({
            "result_id": None, "result_hash": None,
            "review_ref": evidence["review"]["content_hash"],
            "terminal_proof_digest": contracts.sha256_hex(proof),
            "force_accept_reason": "No result after provider exited",
        })
        evidence["acceptance"]["content_hash"] = contracts.content_hash(evidence["acceptance"])
        evidence["content_hash"] = contracts.content_hash(evidence)
        invalid = deepcopy(evidence)
        invalid["terminal_proof"] = None
        invalid["content_hash"] = contracts.content_hash(invalid)
        with self.assertRaises(ValueError):
            self.runtime.record_terminal_outcome(invalid)
        fixed = self.runtime.record_terminal_outcome(evidence)
        self.assertEqual("UNKNOWN", fixed["status"])
        self.assertTrue(fixed["exceptional_acceptance"])

    def test_missing_wrong_and_apc_evidence_never_fix(self) -> None:
        with self.assertRaises(ValueError):
            self.runtime.record_terminal_outcome(None)
        with self.assertRaises(ValueError):
            self.runtime.record_terminal_outcome({"schema": "result/v1", "outcome": "PASS"})
        self.runtime.record_dispatch_intent(self.final.envelope)
        pending = self.bundle()
        with self.assertRaises(ValueError):
            self.runtime.record_terminal_outcome(pending)
        self.runtime.record_observed_dispatch(self.final.envelope, self.native_receipt())
        valid = self.bundle()
        for field in ("review", "acceptance", "result", "dispatch"):
            with self.subTest(field=field):
                invalid = deepcopy(valid)
                invalid[field] = None
                invalid["content_hash"] = contracts.content_hash(invalid)
                with self.assertRaises(ValueError):
                    self.runtime.record_terminal_outcome(invalid)
        for field in ("run_id", "decision_id", "objective_id", "accepted_plan", "task_card"):
            with self.subTest(field=field):
                invalid = deepcopy(valid)
                invalid[field] = "wrong"
                invalid["content_hash"] = contracts.content_hash(invalid)
                with self.assertRaises(ValueError):
                    self.runtime.record_terminal_outcome(invalid)
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])

    def test_changed_native_observation_cannot_replace_durable_dispatch(self) -> None:
        self.observe()
        evidence = self.bundle()
        operation_id = evidence["dispatch"]["operation"]["operation_id"]
        durable = self.state.get_operation(operation_id)
        forged = deepcopy(evidence)
        observed = forged["dispatch"]["observed_invocation"]
        observed["pid"] = 8
        observed["invocation_id"] = "controller:8:now"
        forged["dispatch"]["operation"]["observed_invocation"] = observed
        forged["dispatch"]["operation_digest"] = contracts.sha256_hex(forged["dispatch"]["operation"])
        forged["content_hash"] = contracts.content_hash(forged)
        with self.assertRaises(store.OperationConflictError):
            self.runtime.record_terminal_outcome(forged)
        self.assertEqual(durable, self.state.get_operation(operation_id))
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])

    def native_receipt(self) -> dict:
        return {
            "invocation_id": "controller:7:now", "pid": 7, "creation_time": "now",
            "task_card_digest": self.card["content_hash"],
            "decision_id": self.decision["decision_id"],
            "plan_id": self.plan["plan_id"], "plan_digest": self.plan["content_hash"],
            "envelope_digest": self.final.envelope["content_hash"],
            "lane_id": self.lane_id, "run_id": self.run_id, "base_commit": "base-1",
            "route": "ordinary", "configuration_digest": self.decision["configuration_digest"],
            "context_id": self.final.context["context_id"],
            "context_digest": self.final.context["content_hash"],
        }

    def test_crash_before_after_and_conflict_isolation(self) -> None:
        self.observe()
        evidence = self.bundle()
        self.state.close()
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])
        fixed = self.runtime.record_terminal_outcome(evidence)
        self.state.close()
        self.state = store.MemoryStore(self.root / "state.sqlite3")
        self.state.initialize()
        self.runtime = runtime.MemoryRuntime(self.state)
        self.assertEqual(fixed, self.runtime.record_terminal_outcome(evidence))
        second = deepcopy(evidence)
        second["review"]["review_outcome"] = "FAIL"
        second["review"]["content_hash"] = contracts.content_hash(second["review"])
        second["acceptance"]["review_ref"] = second["review"]["content_hash"]
        second["acceptance"]["approval"] = "REJECTED"
        second["acceptance"]["content_hash"] = contracts.content_hash(second["acceptance"])
        second["content_hash"] = contracts.content_hash(second)
        original_operation = self.state.get_operation(evidence["dispatch"]["operation"]["operation_id"])
        with self.assertRaises(store.OutcomeConflictError):
            self.runtime.record_terminal_outcome(second)
        self.assertEqual(fixed, self.state.get_outcome(fixed["decision_id"]))
        self.assertEqual(original_operation, self.state.get_operation(original_operation["operation_id"]))
        self.prepare_case(plan_id="unrelated", run_id="run-2", lane_id="lane-2")
        self.observe()
        other = self.runtime.record_terminal_outcome(
            self.exceptionally_accept(self.bundle(status="BLOCKED"))
        )
        self.assertEqual("BLOCKED", other["status"])
        self.assertEqual(fixed, self.state.get_outcome(fixed["decision_id"]))

    def test_explicit_plan_supersession_retains_predecessor(self) -> None:
        self.observe()
        old = self.runtime.record_terminal_outcome(self.bundle())
        self.prepare_case(plan_id="corrected-plan", run_id="run-2", lane_id="lane-2",
                          supersedes="plan-1")
        self.observe()
        corrected = self.runtime.record_terminal_outcome(
            self.exceptionally_accept(self.bundle(status="FAIL")),
            supersedes_outcome_id=old["outcome_id"],
        )
        self.assertEqual(old["outcome_id"], corrected["supersedes_outcome_id"])
        self.assertEqual("PASS", self.state.get_outcome(old["decision_id"])["status"])

    def test_supersession_requires_explicit_accepted_plan_link(self) -> None:
        self.observe()
        old = self.runtime.record_terminal_outcome(self.bundle())
        self.prepare_case(plan_id="unlinked-plan", run_id="run-2", lane_id="lane-2")
        self.observe()
        with self.assertRaises(store.OutcomeConflictError):
            self.runtime.record_terminal_outcome(
                self.bundle(), supersedes_outcome_id=old["outcome_id"],
            )
        with self.assertRaises(store.StoreError):
            self.state.get_outcome(self.decision["decision_id"])

    def test_generic_caller_cannot_bypass_finalized_guard(self) -> None:
        self.observe()
        with self.assertRaisesRegex(store.OperationConflictError, "native terminal evidence"):
            self.runtime.record_outcome(
                decision_id=self.decision["decision_id"], plan_id=self.plan["plan_id"],
                plan_digest=self.plan["content_hash"], status="PASS",
                evidence_digest="caller-supplied", linked_run_id=self.run_id,
            )


if __name__ == "__main__":
    unittest.main()
