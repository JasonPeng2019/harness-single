"""Synthetic isolated capture and recovery of the accepted SQLite domain state."""

from __future__ import annotations

import json
import hashlib
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, contracts, procedures, store
from memory_harness.snapshot import SnapshotAuthorizationError, SnapshotError, SnapshotService
from tests.local.procedures import test_procedure_contracts as fixture_module
from tests.local.contracts import test_terminal_outcome as outcome_fixture
from tests.local.effects import test_local_effect_state as local_effect_fixture


class SnapshotRestoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = store.MemoryStore(self.root / "source" / "memory.sqlite3")
        self.source.initialize()
        self.target = store.MemoryStore(self.root / "target" / "memory.sqlite3")
        self.artifact = self.root / "capture"
        self.scope = {"application": "harness", "project": "product-a",
                      "namespace": "step03-test", "owner": "root-agent"}
        self.calls: list[tuple[str, Path]] = []

        def authorize(action: str, scope: dict, path: Path, credential: object) -> bool:
            self.calls.append((action, path))
            return credential == "operator" and scope == self.scope

        self.service = SnapshotService(authorizer=authorize)

    def tearDown(self) -> None:
        self.source.close()
        self.target.close()
        self.temporary.cleanup()

    def _state(self) -> None:
        fixture = fixture_module.ProcedureDesignationContractTests("test_approval_never_makes_a_procedure_current")
        fixture.setUp()
        try:
            trusted = procedures.TrustedProcedureService(self.source, trusted_issuers={"ROOT"})
            trusted.designate(procedure=fixture.procedure, approval=fixture.approval,
                              partition=fixture.partition, issuer="ROOT")
            trusted.record_representation(contracts.make_procedure_representation(
                procedure=fixture.procedure, model="synthetic-model", dimensions=2,
                metric="cosine", sanitizer_version="v1", search_text="repair lock",
                vector=[0.2, 0.8],
            ))
            self.partition = fixture.partition
            self.logical_id = fixture.procedure["logical_id"]
            self.revision_id = fixture.procedure["revision_id"]
            self.source.start_native_usage(contracts.make_native_usage_start(
                source="provider:codex", invocation_id="run-1", objective_id="objective-1",
                decision_id=None, maintenance_operation_id=None, stage="execution",
                category="inner_candidate", window_id="run-1",
                binding={"requested": "model-a", "resolved": "model-a", "native": "model-a"},
            ))
            self.source.record_native_usage_receipt(contracts.make_native_usage_receipt(
                source="provider:codex", invocation_id="run-1", receipt_id="receipt-1",
                objective_id="objective-1", decision_id=None, maintenance_operation_id=None,
                window_id="run-1", mode="cumulative", complete=True, stage="execution",
                category="inner_candidate",
                binding={"requested": "model-a", "resolved": "model-a", "native": "model-a"},
                measures={"input": {"value": 7, "unit": "tokens", "included_in_total": True}},
            ))
            pending, _ = self.source.create_external_effect_operation(
                kind="procedure_publication", scope_key="isolated", source_id=self.revision_id,
                source_record=fixture.procedure, payload={"exact": "payload"},
                captured_config=config.MemoryConfig(), current_config=config.MemoryConfig(),
            )
            self.pending_id = pending["operation_id"]
            uncertain, _ = self.source.create_external_effect_operation(
                kind="procedure_publication", scope_key="second", source_id=self.revision_id,
                source_record=fixture.procedure, payload={"exact": "second"},
                captured_config=config.MemoryConfig(), current_config=config.MemoryConfig(),
            )
            claimed = self.source.claim_effect_operation(
                uncertain["operation_id"], current_config=config.MemoryConfig(),
            )
            self.uncertain_id = self.source.mark_effect_uncertain(
                claimed["operation_id"], "lost acknowledgement",
            )["operation_id"]
        finally:
            fixture.tearDown()

    def _export(self, **kwargs: object) -> dict:
        return self.service.export(self.source, self.artifact, scope=self.scope,
                                   credential="operator", **kwargs)

    def _restore(self, **kwargs: object) -> dict:
        return self.service.restore(self.artifact, self.target, scope=self.scope,
                                    credential="operator", **kwargs)

    def _rewrite_manifest(self, manifest: dict) -> None:
        manifest.pop("content_hash", None)
        manifest["content_hash"] = hashlib.sha256(json.dumps(
            manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode("utf-8")).hexdigest()
        (self.artifact / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def test_round_trip_keeps_exact_trust_usage_pending_and_other_root(self) -> None:
        self._state()
        unrelated = self.root / "unrelated.sqlite3"
        unrelated.write_bytes(b"unrelated")
        before = self.source.read_local_current_procedures(self.partition)
        self.assertEqual(1, len(before))
        self.assertFalse(before[0]["revoked"])
        self.assertEqual((), before[0]["defects"])
        self.assertIsNotNone(before[0]["approval"])
        manifest = self._export()
        self.assertEqual("snapshot/v1", manifest["schema"])
        self.assertEqual(str(self.source.path.resolve()), manifest["source"]["path"])
        self.assertEqual(self.scope, manifest["source"]["scope"])
        self.assertEqual("whole_memory_store_file", manifest["source"]["scope_boundary"])
        self.assertEqual("pending", manifest["pending_effects"][self.pending_id])
        self.assertEqual("uncertain", manifest["pending_effects"][self.uncertain_id])
        for operation_id in (self.pending_id, self.uncertain_id):
            self.assertIn({"capability": "atlas", "reference": f"atlas-effect:{operation_id}",
                           "ready": False}, manifest["dependencies"])
        self._restore()
        self.target.initialize()
        self.assertEqual(before, self.target.read_local_current_procedures(self.partition))
        self.assertEqual(self.source.get_effect_operation(self.pending_id),
                         self.target.get_effect_operation(self.pending_id))
        self.assertEqual(self.source.get_effect_operation(self.uncertain_id),
                         self.target.get_effect_operation(self.uncertain_id))
        self.assertEqual(self.source.get_native_usage("provider:codex", "run-1"),
                         self.target.get_native_usage("provider:codex", "run-1"))
        self.assertEqual(7, self.target.get_native_usage("provider:codex", "run-1")
                         ["measures"]["input|tokens|included"]["value"])
        self.assertEqual(b"unrelated", unrelated.read_bytes())

    def test_revocation_tombstone_remains_ineligible_after_restore(self) -> None:
        self._state()
        revision = self.source.get_procedure_revision(self.revision_id)
        self.source.record_procedure_revocation(contracts.make_procedure_revocation(
            procedure=revision, issuer="ROOT", reason="withdraw trust",
        ))
        before = self.source.read_local_current_procedures(self.partition)
        self.assertTrue(before[0]["revoked"])
        self._export()
        self._restore()
        self.target.initialize()
        after = self.target.read_local_current_procedures(self.partition)
        self.assertEqual(before, after)
        self.assertTrue(after[0]["revoked"])

    def test_reviewed_receipt_decision_dispatch_and_outcome_survive(self) -> None:
        case = outcome_fixture.TerminalOutcomeTests("test_exact_observed_native_chain_fixes_pass")
        case.setUp()
        try:
            case.observe()
            outcome = case.runtime.record_terminal_outcome(case.bundle())
            outcome = contracts.make_outcome(
                decision_id=outcome["decision_id"], plan_id=outcome["plan_id"],
                plan_digest=outcome["plan_digest"], status=outcome["status"],
                evidence_digest=outcome["evidence_digest"], linked_run_id=outcome["linked_run_id"],
                task_card_digest=outcome["task_card_digest"], objective_id=outcome["objective_id"],
                observed_at=outcome["observed_at"],
            )
            receipt = contracts.make_review_receipt(
                review_id="review-1", outcome=outcome, decision=case.decision,
                task_card=case.card, plan=case.plan, reviewed_by="ROOT",
                evidence_refs=("review://run-1",),
                protected_source_refs=("protected://run-1",),
                raw_evidence="reviewed exact run",
            )
            case.state.record_review_receipt(receipt)
            manifest = self.service.export(case.state, self.artifact,
                                           scope=self.scope, credential="operator")
            self.assertEqual("incomplete", manifest["completeness"])
            self.assertIn({"capability": "experience", "reference": "protected://run-1",
                           "ready": False}, manifest["dependencies"])
            self._restore()
            self.target.initialize()
            self.assertEqual(case.state.get_review_receipt(receipt["review_receipt_id"]),
                             self.target.get_review_receipt(receipt["review_receipt_id"]))
            self.assertEqual(case.state.get_decision(case.decision["decision_id"]),
                             self.target.get_decision(case.decision["decision_id"]))
            self.assertEqual(case.state.get_outcome(outcome["decision_id"]),
                             self.target.get_outcome(outcome["decision_id"]))
        finally:
            case.tearDown()

    def test_authority_absent_rejected_or_throwing_fails_before_read_or_write(self) -> None:
        with self.assertRaises(SnapshotAuthorizationError):
            SnapshotService()
        for authorizer in (lambda *_: False, lambda *_: 1 / 0):
            service = SnapshotService(authorizer=authorizer)
            with self.subTest(authorizer=authorizer):
                with self.assertRaises(SnapshotAuthorizationError):
                    service.export(self.source, self.artifact, scope=self.scope, credential="operator")
                self.assertFalse(self.artifact.exists())
                with self.assertRaises(SnapshotAuthorizationError):
                    service.restore(self.artifact, self.target, scope=self.scope, credential="operator")
                self.assertFalse(self.target.path.exists())
        class PoisonSource:
            path = self.source.path
            @property
            def connection(self) -> object:
                raise AssertionError("protected source was read")
        with self.assertRaises(SnapshotAuthorizationError):
            SnapshotService(authorizer=lambda *_: False).export(
                PoisonSource(), self.artifact, scope=self.scope, credential="operator",
            )

    def test_authorizer_must_grant_exact_whole_store_path_and_scope_before_read(self) -> None:
        class PoisonSource:
            path = self.source.path
            @property
            def connection(self) -> object:
                raise AssertionError("source bytes were read before authorization")
        expected_path = self.source.path.resolve()
        service = SnapshotService(authorizer=lambda action, scope, path, credential:
                                  path == expected_path.with_name("another.sqlite3") and
                                  scope == self.scope)
        with self.assertRaises(SnapshotAuthorizationError):
            service.export(PoisonSource(), self.artifact, scope=self.scope, credential="operator")
        self.assertFalse(self.artifact.exists())
        with self.assertRaises(SnapshotAuthorizationError):
            service.restore(self.artifact, self.target, scope=self.scope, credential="operator")
        self.assertFalse(self.target.path.exists())

    def test_restore_rejects_tamper_schema_dependency_scope_collision_and_live_handle(self) -> None:
        self._state()
        self._export(dependencies=[{"capability": "atlas", "reference": "atlas://partition/a"}])
        with self.assertRaises(SnapshotError):
            self._restore(required_capabilities=("atlas",))
        self.assertFalse(self.target.path.exists())
        with self.assertRaises(SnapshotError):
            self.service.restore(self.artifact, self.target, scope={**self.scope, "owner": "other"},
                                 credential="operator")
        self.assertFalse(self.target.path.exists())
        with self.assertRaises(SnapshotError):
            SnapshotService(authorizer=lambda *_: True).restore(
                self.artifact, self.target, scope={**self.scope, "owner": "other"},
                credential="operator",
            )
        self.assertFalse(self.target.path.exists())
        db = self.artifact / "state.sqlite3"
        original = db.read_bytes()
        db.write_bytes(original + b"tamper")
        with self.assertRaises(SnapshotError):
            self._restore()
        self.assertFalse(self.target.path.exists())
        db.write_bytes(original)
        manifest_path = self.artifact / "manifest.json"
        original_manifest = manifest_path.read_bytes()
        manifest = json.loads(original_manifest)
        manifest["store_schema_digest"] = "bad"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaises(SnapshotError):
            self._restore()
        self.assertFalse(self.target.path.exists())
        manifest_path.write_bytes(original_manifest)
        self.target.initialize()
        with self.assertRaises(SnapshotError):
            self._restore()
        self.target.close()
        self.target.path.unlink()
        self.target.path.write_bytes(b"collision")
        with self.assertRaises(SnapshotError):
            self._restore()
        self.assertEqual(b"collision", self.target.path.read_bytes())

    def test_verified_dependency_is_required_again_at_restore(self) -> None:
        self._state()
        self.service = SnapshotService(
            authorizer=lambda action, scope, path, credential: credential == "operator",
            dependency_verifier=lambda capability, reference, scope: True,
        )
        manifest = self._export()
        self.assertEqual("complete", manifest["completeness"])
        result = self._restore(required_capabilities=("atlas",))
        self.assertEqual("complete", result["restored"]["completeness"])
        self.assertTrue(result["restored"]["capabilities"]["atlas"])
        self.target.path.unlink()
        self.service = SnapshotService(
            authorizer=lambda action, scope, path, credential: credential == "operator",
        )
        with self.assertRaises(SnapshotError):
            self._restore(required_capabilities=("atlas",))
        self.assertFalse(self.target.path.exists())

    def test_outcome_owned_remote_effects_require_proof_and_preserve_status(self) -> None:
        fixture = local_effect_fixture.LocalEffectStateTests(
            "test_review_evidence_survives_outage_and_payload_readiness")
        fixture.setUp()
        try:
            case = fixture.case
            case.observe()
            outcome = case.runtime.record_terminal_outcome(case.bundle())
            fixture._review(outcome)
            ingestion_id = contracts.effect_operation_id(outcome["outcome_id"], "experience_ingestion")
            skill_id = contracts.effect_operation_id(outcome["outcome_id"], "generated_skill_creation")
            case.state.bind_effect_payload(ingestion_id, {"exact": "adapter payload"})
            manifest = self.service.export(case.state, self.artifact, scope=self.scope,
                                           credential="operator")
            self.assertEqual("incomplete", manifest["completeness"])
            for operation_id in (ingestion_id, skill_id):
                self.assertIn({"capability": "experience", "reference": f"everos-effect:{operation_id}",
                               "ready": False}, manifest["dependencies"])
            self.assertEqual("pending", manifest["pending_effects"][ingestion_id])
            with self.assertRaises(SnapshotError):
                self._restore(required_capabilities=("experience",))
            self.assertFalse(self.target.path.exists())
            result = self._restore()
            self.assertEqual("incomplete", result["restored"]["completeness"])
            self.assertFalse(result["restored"]["capabilities"]["experience"])
            self.target.initialize()
            self.assertEqual(case.state.get_effect_operation(ingestion_id),
                             self.target.get_effect_operation(ingestion_id))
        finally:
            fixture.tearDown()

    def test_empty_reference_remote_request_is_not_vacuously_ready(self) -> None:
        manifest = self._export()
        self.assertEqual([], manifest["dependencies"])
        with self.assertRaises(SnapshotError):
            self._restore(required_capabilities=("experience",))
        with self.assertRaises(SnapshotError):
            self._restore(required_capabilities=("atlas",))
        self.assertFalse(self.target.path.exists())

    def test_restore_separates_capture_proof_from_current_readiness(self) -> None:
        self.service = SnapshotService(authorizer=lambda *_: True,
                                       dependency_verifier=lambda *_: True)
        manifest = self._export(dependencies=[{"capability": "atlas", "reference": "atlas://partition/a"}])
        self.assertEqual("complete", manifest["completeness"])
        capture_bytes = (self.artifact / "manifest.json").read_bytes()
        self.service = SnapshotService(authorizer=lambda *_: True)
        result = self._restore()
        self.assertEqual(manifest, result["capture"])
        self.assertEqual("complete", result["capture"]["completeness"])
        self.assertEqual("incomplete", result["restored"]["completeness"])
        self.assertFalse(result["restored"]["capabilities"]["atlas"])
        self.assertFalse(result["restored"]["dependencies"][0]["ready"])
        self.assertEqual(capture_bytes, (self.artifact / "manifest.json").read_bytes())
        self.assertTrue(self.target.path.exists())

    def test_export_binds_opened_relative_store_after_cwd_change(self) -> None:
        home = self.root / "relative"
        home.mkdir()
        previous = Path.cwd()
        try:
            os.chdir(home)
            relative = store.MemoryStore("relative.sqlite3")
            relative.initialize()
        finally:
            os.chdir(previous)
        try:
            manifest = self.service.export(relative, self.artifact,
                                           scope=self.scope, credential="operator")
            self.assertEqual(str(home / "relative.sqlite3"), manifest["source"]["path"])
        finally:
            relative.close()

    def test_preflight_rejects_missing_reference_corrupt_db_and_incompatible_schema(self) -> None:
        self._state()
        self._export()
        manifest_path = self.artifact / "manifest.json"
        original_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        database = self.artifact / "state.sqlite3"
        original_database = database.read_bytes()

        missing = dict(original_manifest)
        missing["dependencies"] = []
        missing["completeness"] = "complete"
        self._rewrite_manifest(missing)
        with self.assertRaises(SnapshotError):
            self._restore()
        self.assertFalse(self.target.path.exists())

        database.write_bytes(b"not a SQLite database")
        corrupt = dict(original_manifest)
        corrupt["database_sha256"] = hashlib.sha256(database.read_bytes()).hexdigest()
        self._rewrite_manifest(corrupt)
        with self.assertRaises(SnapshotError):
            self._restore()
        self.assertFalse(self.target.path.exists())

        database.write_bytes(original_database)
        connection = sqlite3.connect(database)
        try:
            connection.execute("CREATE TABLE incompatible (value TEXT)")
            connection.commit()
        finally:
            connection.close()
        incompatible = dict(original_manifest)
        incompatible["database_sha256"] = hashlib.sha256(database.read_bytes()).hexdigest()
        self._rewrite_manifest(incompatible)
        with self.assertRaises(SnapshotError):
            self._restore()
        self.assertFalse(self.target.path.exists())

    def test_interrupted_export_never_publishes_complete_artifact(self) -> None:
        self._state()
        def fault(*_: object) -> None:
            raise RuntimeError("interrupted")
        self.service._after_backup = fault
        with self.assertRaises(RuntimeError):
            self._export()
        self.assertFalse(self.artifact.exists())
        self.assertEqual("pending", self.source.get_effect_operation(self.pending_id)["status"])
