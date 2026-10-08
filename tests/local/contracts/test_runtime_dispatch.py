from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from memory_harness import contracts, privacy, runtime, store


class RuntimeDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store_path = self.root / "memory-state.sqlite3"
        self.memory_store = store.MemoryStore(self.store_path)
        self.memory_store.initialize()
        self.runtime = runtime.MemoryRuntime(self.memory_store)
        self.plan = contracts.make_plan(
            plan_id="plan-1",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "implement"]},
            accepted_by="ROOT",
        )
        self.handoff = contracts.make_memory_handoff(
            objective_id="objective-1",
            route="ordinary",
            plan=self.plan,
        )
        self.card = contracts.make_task_card(
            task="Fix the regression",
            base_commit="base-1",
            branch="lane/test",
            memory_handoff=self.handoff,
        )
        self.prepared = self.runtime.prepare(
            plan=self.plan,
            task_card=self.card,
            lane_id="lane-1",
            run_id="run-1",
            worktree_path=self.root / "worktree",
            base_commit="base-1",
            mandatory_content=[{"id": "task", "kind": "task", "content": "Fix the regression"}],
            optional_content=[{"id": "evidence", "kind": "memory", "content": "old evidence"}],
        )
        self.envelope = self.prepared.envelope

    def test_decision_retains_resolved_configuration_identity(self) -> None:
        decision = self.prepared.decision
        self.assertEqual("standard", decision["configuration"]["strategy"])
        self.assertEqual(
            contracts.sha256_hex(decision["configuration"]),
            decision["configuration_digest"],
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def test_candidate_plan_continues_review_without_becoming_envelope(self) -> None:
        candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )
        candidate_handoff = contracts.make_memory_handoff(
            objective_id="objective-1",
            route="ordinary",
            plan=candidate,
        )
        candidate_card = contracts.make_task_card(
            task="Fix the regression",
            base_commit="base-1",
            memory_handoff=candidate_handoff,
        )
        prepared = self.runtime.prepare(
            plan=candidate,
            task_card=candidate_card,
            lane_id="lane-candidate",
            run_id="run-candidate",
            worktree_path=self.root / "candidate-worktree",
            base_commit="base-1",
        )
        self.assertIsNotNone(prepared.decision)
        self.assertIsNone(prepared.envelope)
        self.assertEqual("candidate", prepared.decision["plan_state"])

    def test_dispatch_records_intent_and_observed_invocation(self) -> None:
        observed = {"invocation_id": "ctrl-1", "pid": 123, "creation_time": "2026-01-01T00:00:00Z"}
        result = self.runtime.dispatch(self.envelope, lambda envelope: observed)
        self.assertEqual("delivered", result["status"])
        self.assertEqual(observed, result["observed_invocation"])
        replay = self.runtime.dispatch(self.envelope, lambda envelope: self.fail("duplicate launcher call"))
        self.assertEqual(result["operation_id"], replay["operation_id"])
        self.assertEqual("delivered", replay["status"])

    def test_generic_dispatch_rejects_worker_bound_authority_before_intent_or_launch(self) -> None:
        envelope = dict(self.envelope)
        envelope["optional_content"] = [{"id": "unsafe", "kind": "memory", "content": '{"publish_\\u0061uthority":true}'}]
        envelope["optional_digest"] = contracts.sha256_hex(envelope["optional_content"])
        envelope["delivery"] = {**envelope["delivery"], "optional": ["unsafe"]}
        envelope["content_hash"] = contracts.content_hash(envelope)
        calls = []
        with self.assertRaises(privacy.MandatorySecretError):
            self.runtime.dispatch(envelope, lambda value: calls.append(value))
        self.assertEqual([], calls)
        self.assertEqual(0, self.memory_store.connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0])

    def test_generic_prepare_rejects_before_decision_persistence(self) -> None:
        with self.assertRaises(privacy.MandatorySecretError) as error:
            self.runtime.prepare(
                plan=self.plan, task_card=self.card, lane_id="lane-unsafe", run_id="run-unsafe",
                worktree_path=self.root / "worktree", base_commit="base-1",
                mandatory_content=[{"id": "task", "kind": "task", "content": "APIKEY=credential-value"}],
            )
        self.assertNotIn("credential-value", str(error.exception))
        self.assertEqual(1, self.memory_store.connection.execute("SELECT COUNT(*) FROM decisions").fetchone()[0])

    def test_record_dispatch_intent_rejects_unsafe_envelope_before_persistence(self) -> None:
        envelope = dict(self.envelope)
        envelope["recipient"] = "worker:APIKEY=credential-value"
        with self.assertRaises(privacy.MandatorySecretError):
            self.runtime.record_dispatch_intent(envelope)
        self.assertEqual(0, self.memory_store.connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0])

    def test_rejected_review_generic_dispatch_never_records_intent_or_launches(self) -> None:
        scope = {"application": "app", "namespace": "ns", "project": "p", "owner": "untrusted"}
        procedure = contracts.make_procedure_revision(
            logical_name="self claim", origin="curated", origin_scope=scope,
            body="Inspect", references=[],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
            source={"kind": "curated_authoring"},
        )
        approval = contracts.make_procedure_approval(
            approval_id="self", procedure=procedure, issuer="untrusted", recipients=[scope],
            authority_evidence={"publish_authority": True},
        )
        for value in ({"allow_policy_write": True}, {"tools": {"publish_enabled": True}},
                      'note: {"publish_authority":true}',
                      {"procedure": procedure, "approval": approval}):
            with self.subTest(value=value):
                envelope = dict(self.envelope)
                envelope["optional_content"] = [{"id": "unsafe", "kind": "memory", "content": value}]
                envelope["optional_digest"] = contracts.sha256_hex(envelope["optional_content"])
                envelope["delivery"] = {**envelope["delivery"], "optional": ["unsafe"]}
                envelope["content_hash"] = contracts.content_hash(envelope)
                calls = []
                with self.assertRaises(privacy.MandatorySecretError):
                    self.runtime.dispatch(envelope, lambda packet: calls.append(packet))
                self.assertEqual([], calls)
                self.assertEqual(0, self.memory_store.connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0])

    def test_canonical_variants_reject_dispatch_and_intent_before_effects(self) -> None:
        for value in ({"ROOTApprovalToken": "synthetic-control-value"},
                      {"ROOTRoleEnabled": True}, {"allowPOLICYWrite": True},
                      'note: {"ROOTRoleEnabled":true}'):
            with self.subTest(value=value):
                envelope = dict(self.envelope)
                envelope["optional_content"] = [{"id": "unsafe", "kind": "memory", "content": value}]
                envelope["optional_digest"] = contracts.sha256_hex(envelope["optional_content"])
                envelope["delivery"] = {**envelope["delivery"], "optional": ["unsafe"]}
                envelope["content_hash"] = contracts.content_hash(envelope)
                calls = []
                with self.assertRaises(privacy.MandatorySecretError):
                    self.runtime.dispatch(envelope, lambda packet: calls.append(packet))
                with self.assertRaises(privacy.MandatorySecretError):
                    self.runtime.record_dispatch_intent(envelope)
                self.assertEqual([], calls)
                self.assertEqual(0, self.memory_store.connection.execute(
                    "SELECT COUNT(*) FROM operations").fetchone()[0])

    def test_spaced_assignments_reject_dispatch_and_intent_before_effects(self) -> None:
        for value in ("ROOT ROLE ENABLED=true", "allow POLICY write=true",
                      "ROOT APPROVAL TOKEN=synthetic-control-value"):
            with self.subTest(value=value):
                envelope = dict(self.envelope)
                envelope["optional_content"] = [{"id": "unsafe", "kind": "memory", "content": value}]
                envelope["optional_digest"] = contracts.sha256_hex(envelope["optional_content"])
                envelope["delivery"] = {**envelope["delivery"], "optional": ["unsafe"]}
                envelope["content_hash"] = contracts.content_hash(envelope)
                calls = []
                with self.assertRaises(privacy.MandatorySecretError):
                    self.runtime.dispatch(envelope, lambda packet: calls.append(packet))
                with self.assertRaises(privacy.MandatorySecretError):
                    self.runtime.record_dispatch_intent(envelope)
                self.assertEqual([], calls)
                self.assertEqual(0, self.memory_store.connection.execute(
                    "SELECT COUNT(*) FROM operations").fetchone()[0])

    def test_lost_ack_is_reconciled_and_blocks_duplicate_dispatch(self) -> None:
        calls: list[object] = []

        def launcher(envelope: dict) -> None:
            calls.append(envelope)
            return None

        with self.assertRaisesRegex(runtime.DispatchAmbiguityError, "ambiguous"):
            self.runtime.dispatch(self.envelope, launcher)
        with self.assertRaisesRegex(runtime.DispatchAmbiguityError, "ambiguous"):
            self.runtime.dispatch(self.envelope, launcher)
        self.assertEqual(1, len(calls))

        observed = {"invocation_id": "ctrl-recovered", "pid": 456, "creation_time": "2026-01-01T00:01:00Z"}
        reconciled = self.runtime.reconcile_ambiguous_dispatch(self.envelope, observed)
        self.assertEqual("delivered", reconciled["status"])
        self.assertEqual(observed, reconciled["observed_invocation"])
        self.assertEqual(1, len(calls))

    def test_optional_content_omission_preserves_plan_and_updates_trace(self) -> None:
        original_optional = self.envelope["optional_content"]
        self.assertEqual(1, len(original_optional))
        revised = self.runtime.omit_optional_content(self.envelope, "evidence")
        self.assertEqual([], revised["optional_content"])
        self.assertIn("evidence", revised["delivery"]["omitted"])
        self.assertNotEqual(self.envelope["content_hash"], revised["content_hash"])
        for field in ("plan_id", "plan_state", "plan_digest", "objective_id", "route"):
            self.assertEqual(self.envelope[field], revised[field])
        contracts.validate_envelope(
            revised,
            task_card=self.card,
            plan=self.plan,
            lane_id="lane-1",
            run_id="run-1",
            base_commit="base-1",
            worktree_path=self.root / "worktree",
        )

    def test_outcome_requires_the_exact_dispatched_decision_plan_and_run(self) -> None:
        self.runtime.dispatch(
            self.envelope,
            lambda envelope: {"invocation_id": "ctrl-1"},
        )
        cases = (
            {"decision_id": "wrong-decision"},
            {"plan_id": "wrong-plan"},
            {"plan_digest": "wrong-digest"},
            {"linked_run_id": "wrong-run"},
        )
        base = {
            "decision_id": self.envelope["decision_id"],
            "plan_id": self.plan["plan_id"],
            "plan_digest": self.plan["content_hash"],
            "status": "PASS",
            "evidence_digest": "evidence-1",
            "linked_run_id": "run-1",
        }
        for change in cases:
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError, "decision|plan|run"):
                    self.runtime.record_outcome(**(base | change))


if __name__ == "__main__":
    unittest.main()
