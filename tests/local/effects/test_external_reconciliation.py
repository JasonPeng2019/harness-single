"""Shared durable boundary for source-specific external effects."""

from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, contracts, store
from tests.local.contracts import test_terminal_outcome as fixture_module


class ExternalReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.state = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
        self.state.initialize()
        self.source = {"schema": "trusted-source/v1", "source_id": "source-1", "value": "approved"}
        self.source["content_hash"] = contracts.content_hash(self.source)

    def tearDown(self) -> None:
        self.state.close()
        self.directory.cleanup()

    def _create(self, **changes: object) -> dict:
        inputs = {
            "kind": "procedure_publication", "scope_key": "recipient-1",
            "source_id": "source-1", "source_record": self.source,
            "payload": {"publication": "exact"},
            "captured_config": config.MemoryConfig(),
            "current_config": config.MemoryConfig(),
        }
        inputs.update(changes)
        return self.state.create_external_effect_operation(**inputs)[0]

    @staticmethod
    def _evidence(operation: dict, **changes: object) -> dict:
        evidence = {key: operation[key] for key in (
            "operation_id", "kind", "scope_key", "source_digest", "payload_digest",
            "configuration_digest",
        )}
        evidence["adapter_proof"] = {"remote_id": "remote-1", "digest": "exact"}
        evidence.update(changes)
        return evidence

    @staticmethod
    def _claimant(invocation: str, pid: int, created: str) -> dict:
        record = {"schema": "external-effect-claimant/v1", "native_invocation_id": invocation,
                  "pid": pid, "process_created_at": created}
        record["content_hash"] = contracts.content_hash(record)
        return record

    @staticmethod
    def _fence(operation: dict, **changes: object) -> dict:
        record = {"schema": "external-effect-claim-fence/v1",
                  "operation_id": operation["operation_id"],
                  "claim_id": operation["active_claim_id"],
                  "claim_generation": operation["claim_generation"],
                  "claimant_digest": operation["claimant_digest"],
                  "claimant": operation["claimant_record"],
                  "terminated": True}
        record.update(changes)
        record["content_hash"] = contracts.content_hash(record)
        return record

    def test_claim_fence_restart_absence_retry_and_stale_claim(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        claimant_a = self._claimant("native-A", 1101, "2026-09-26T09:00:00Z")
        claimant_b = self._claimant("native-B", 1102, "2026-09-26T09:05:00Z")
        claimed_a = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(), claimant=claimant_a,
        )
        claim_a = claimed_a["active_claim_id"]
        self.assertEqual(1, claimed_a["claim_generation"])
        self.assertEqual(claimant_a, claimed_a["claimant_record"])
        self.assertEqual(contracts.sha256_hex(claimant_a), claimed_a["claimant_digest"])
        proof = self._fence(claimed_a)
        self.state.close()
        self.state = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
        self.state.initialize()
        self.assertEqual("in_flight", self.state.get_effect_operation(identity)["status"])
        with self.assertRaises(store.OperationConflictError):
            self.state.reconcile_external_effect_operation(
                identity, evidence=self._evidence(intent, readback_complete=True), result="absent",
                claim_id=claim_a,
            )
        with self.assertRaises(store.OperationConflictError):
            self.state.isolate_external_effect_claim(identity, claim_a, proof)
        with self.assertRaises(store.OperationConflictError):
            self.state.claim_effect_operation(
                identity, current_config=config.MemoryConfig(), claimant=claimant_b,
            )

        calls = []
        def verifier(stored: dict, evidence: dict) -> bool:
            calls.append((stored, evidence))
            return stored == claimant_a and evidence == proof

        self.state.close()
        self.state = store.MemoryStore(
            Path(self.directory.name) / "effects.sqlite3", external_claim_fence_verifier=verifier,
        )
        self.state.initialize()
        isolated = self.state.isolate_external_effect_claim(identity, claim_a, proof)
        self.assertEqual("uncertain", isolated["status"])
        self.assertEqual(proof, isolated["fence_evidence"])
        self.assertEqual([(claimant_a, proof)], calls)
        self.state.close()
        self.state = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
        self.state.initialize()
        self.assertEqual(isolated, self.state.isolate_external_effect_claim(identity, claim_a, proof))
        absent = self.state.reconcile_external_effect_operation(
            identity, evidence=self._evidence(intent, readback_complete=True), result="absent",
            claim_id=claim_a,
        )
        self.assertEqual("pending", absent["status"])
        self.assertEqual(intent["configuration_digest"], absent["configuration_digest"])
        claimed_b = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(), claimant=claimant_b,
        )
        self.assertEqual(2, claimed_b["claim_generation"])
        self.assertNotEqual(claim_a, claimed_b["active_claim_id"])
        with self.assertRaises(store.OperationConflictError):
            self.state.mark_effect_uncertain(identity, "stale", claim_id=claim_a)
        with self.assertRaises(store.OperationConflictError):
            self.state.confirm_effect_operation(identity, self._evidence(intent), claim_id=claim_a)
        with self.assertRaises(store.OperationConflictError):
            self.state.isolate_external_effect_claim(identity, claim_a, proof)
        confirmed = self.state.confirm_effect_operation(
            identity, self._evidence(intent), claim_id=claimed_b["active_claim_id"],
        )
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(2, confirmed["claim_generation"])

    def test_active_claim_voluntary_uncertainty_and_retry_matrix(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        off = config.resolve_config({"shared_publication": False})
        claimed_a = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(),
            claimant=self._claimant("native-A", 4401, "created-A"),
        )
        claim_a = claimed_a["active_claim_id"]
        for claim_id in (None, "wrong"):
            with self.subTest(stage="active", claim_id=claim_id):
                with self.assertRaises(store.OperationConflictError):
                    self.state.mark_effect_uncertain(identity, "acknowledgement lost", claim_id=claim_id)
                self.assertEqual(claimed_a, self.state.get_effect_operation(identity))

        uncertain = self.state.mark_effect_uncertain(
            identity, "acknowledgement lost", claim_id=claim_a,
        )
        self.assertEqual("uncertain", uncertain["status"])
        self.assertEqual("pending_off", self.state.external_effect_off_state(identity, current_config=off))
        with self.assertRaises(store.OperationConflictError):
            self.state.claim_effect_operation(
                identity, current_config=config.MemoryConfig(),
                claimant=self._claimant("native-B", 4402, "created-B"),
            )
        for claim_id in (None, "wrong"):
            with self.subTest(stage="uncertain", claim_id=claim_id):
                with self.assertRaises(store.OperationConflictError):
                    self.state.reconcile_external_effect_operation(
                        identity, evidence=self._evidence(intent, readback_complete=True),
                        result="absent", claim_id=claim_id,
                    )
                self.assertEqual(uncertain, self.state.get_effect_operation(identity))
        with self.assertRaises(store.OperationConflictError):
            self.state.reconcile_external_effect_operation(
                identity, evidence=self._evidence(intent), result="absent", claim_id=claim_a,
            )
        self.assertEqual(uncertain, self.state.get_effect_operation(identity))
        pending = self.state.reconcile_external_effect_operation(
            identity, evidence=self._evidence(intent, readback_complete=True),
            result="absent", claim_id=claim_a,
        )
        self.assertEqual("pending", pending["status"])
        self.assertEqual("off", self.state.external_effect_off_state(identity, current_config=off))
        with self.assertRaisesRegex(store.OperationConflictError, "off"):
            self.state.claim_effect_operation(
                identity, current_config=off,
                claimant=self._claimant("native-B", 4402, "created-B"),
            )
        claimed_b = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(),
            claimant=self._claimant("native-B", 4402, "created-B"),
        )
        self.assertEqual(claimed_a["claim_generation"] + 1, claimed_b["claim_generation"])
        self.assertNotEqual(claim_a, claimed_b["active_claim_id"])
        for action in (
            lambda: self.state.mark_effect_uncertain(identity, "stale", claim_id=claim_a),
            lambda: self.state.confirm_effect_operation(
                identity, self._evidence(intent), claim_id=claim_a,
            ),
            lambda: self.state.reconcile_external_effect_operation(
                identity, evidence=self._evidence(intent), result="acknowledged", claim_id=claim_a,
            ),
        ):
            with self.subTest(stage="stale", action=action):
                with self.assertRaises(store.OperationConflictError):
                    action()
                self.assertEqual(claimed_b, self.state.get_effect_operation(identity))

    def test_claimant_handle_authority_and_crash_recovery_matrix(self) -> None:
        database = Path(self.directory.name) / "effects.sqlite3"
        for scenario in ("live_peer", "reopened_same", "reopened_fresh"):
            with self.subTest(scenario=scenario):
                intent = self._create(scope_key=scenario)
                identity = intent["operation_id"]
                claimed_a = self.state.claim_effect_operation(
                    identity, current_config=config.MemoryConfig(),
                    claimant=self._claimant(f"native-A-{scenario}", 4401, "created-A"),
                )
                claim_a = claimed_a["active_claim_id"]
                self.assertEqual({identity: claim_a}, self.state._live_effect_claims)
                with self.assertRaises(store.OperationConflictError):
                    self.state.claim_effect_operation(
                        identity, current_config=config.MemoryConfig(),
                        claimant=self._claimant(f"native-B-{scenario}", 4402, "created-B"),
                    )
                self.assertEqual({identity: claim_a}, self.state._live_effect_claims)
                if scenario == "live_peer":
                    peer = store.MemoryStore(database)
                    peer.initialize()
                elif scenario == "reopened_same":
                    self.state.close()
                    self.state.initialize()
                    peer = self.state
                else:
                    self.state.close()
                    self.state = store.MemoryStore(database)
                    self.state.initialize()
                    peer = self.state
                if scenario != "live_peer":
                    self.assertEqual({}, peer._live_effect_claims)
                try:
                    if peer is not self.state:
                        self.assertEqual({}, peer._live_effect_claims)
                    persisted_id = peer.get_effect_operation(identity)["active_claim_id"]
                    self.assertEqual(claim_a, persisted_id)
                    with self.assertRaises(store.OperationConflictError):
                        peer.mark_effect_uncertain(identity, "peer lost acknowledgement", claim_id=persisted_id)
                    self.assertEqual(claimed_a, peer.get_effect_operation(identity))
                    if peer is not self.state:
                        self.assertEqual({}, peer._live_effect_claims)
                    with self.assertRaises(store.OperationConflictError):
                        peer.reconcile_external_effect_operation(
                            identity, evidence=self._evidence(intent, readback_complete=True),
                            result="absent", claim_id=persisted_id,
                        )
                    with self.assertRaises(store.OperationConflictError):
                        peer.claim_effect_operation(
                            identity, current_config=config.MemoryConfig(),
                            claimant=self._claimant(f"native-B-{scenario}", 4402, "created-B"),
                        )
                    self.assertEqual(claimed_a, peer.get_effect_operation(identity))
                finally:
                    if peer is not self.state:
                        peer.close()

                if scenario == "live_peer":
                    uncertain = self.state.mark_effect_uncertain(
                        identity, "acknowledgement lost", claim_id=claim_a,
                    )
                    self.assertEqual("uncertain", uncertain["status"])
                    self.assertEqual({}, self.state._live_effect_claims)
                else:
                    proof = self._fence(claimed_a)
                    self.state.close()
                    self.state = store.MemoryStore(
                        database, external_claim_fence_verifier=lambda stored, evidence:
                        stored == claimed_a["claimant_record"] and evidence == proof,
                    )
                    self.state.initialize()
                    uncertain = self.state.isolate_external_effect_claim(identity, claim_a, proof)
                    self.assertEqual("uncertain", uncertain["status"])
                self.state.reconcile_external_effect_operation(
                    identity, evidence=self._evidence(intent, readback_complete=True),
                    result="absent", claim_id=claim_a,
                )
                claimed_b = self.state.claim_effect_operation(
                    identity, current_config=config.MemoryConfig(),
                    claimant=self._claimant(f"native-B-{scenario}", 4402, "created-B"),
                )
                self.assertEqual(2, claimed_b["claim_generation"])
                self.assertEqual({identity: claimed_b["active_claim_id"]},
                                 self.state._live_effect_claims)
                with self.assertRaises(store.OperationConflictError):
                    self.state.mark_effect_uncertain(identity, "stale", claim_id=claim_a)
                self.state.reconcile_external_effect_operation(
                    identity, evidence=self._evidence(intent), result="acknowledged",
                    claim_id=claimed_b["active_claim_id"],
                )
                self.assertEqual({}, self.state._live_effect_claims)

    def test_active_claim_idempotency_proof_preserves_uncertainty_until_retry(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        claimed_a = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(),
            claimant=self._claimant("native-A", 4501, "created-A"),
        )
        claim_a = claimed_a["active_claim_id"]
        self.state.mark_effect_uncertain(identity, "acknowledgement lost", claim_id=claim_a)
        with self.assertRaises(store.OperationConflictError):
            self.state.reconcile_external_effect_operation(
                identity, evidence=self._evidence(intent, idempotency_key="other"),
                result="idempotent", claim_id=claim_a,
            )
        proved = self.state.reconcile_external_effect_operation(
            identity,
            evidence=self._evidence(intent, idempotency_key=identity),
            result="idempotent", claim_id=claim_a,
        )
        self.assertEqual("uncertain", proved["status"])
        self.assertIsNone(proved["acknowledgement"])
        claimed_b = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(),
            claimant=self._claimant("native-B", 4502, "created-B"),
        )
        self.assertEqual(2, claimed_b["claim_generation"])
        self.assertEqual(proved["reconciliation"], claimed_b["reconciliation"])
        self.assertNotEqual(claim_a, claimed_b["active_claim_id"])

    def test_claim_fence_verifier_identity_and_off_gate(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        claimant = self._claimant("native-A", 2201, "created-A")
        claimed = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(), claimant=claimant,
        )
        proof = self._fence(claimed)
        self.state.close()
        database = Path(self.directory.name) / "effects.sqlite3"
        for verifier in (lambda *_: False, lambda *_: (_ for _ in ()).throw(RuntimeError("query failed"))):
            self.state = store.MemoryStore(database, external_claim_fence_verifier=verifier)
            self.state.initialize()
            before = self.state.get_effect_operation(identity)
            with self.assertRaises(store.OperationConflictError):
                self.state.isolate_external_effect_claim(identity, claimed["active_claim_id"], proof)
            self.assertEqual(before, self.state.get_effect_operation(identity))
            self.state.close()
        seen = []
        self.state = store.MemoryStore(
            database, external_claim_fence_verifier=lambda stored, evidence:
            seen.append((stored, evidence)) or True,
        )
        self.state.initialize()
        wrongs = (
            self._fence(claimed, claimant={**claimant, "pid": 2202}),
            self._fence(claimed, claimant={**claimant, "process_created_at": "reused-PID"}),
            self._fence(claimed, claimant={**claimant, "native_invocation_id": "wrong"}),
            self._fence(claimed, claim_id="wrong"),
            self._fence(claimed, operation_id="wrong"),
            self._fence(claimed, terminated=False),
            {**proof, "content_hash": "wrong"},
        )
        for wrong in wrongs:
            with self.subTest(wrong=wrong):
                before = self.state.get_effect_operation(identity)
                with self.assertRaises(store.OperationConflictError):
                    self.state.isolate_external_effect_claim(identity, claimed["active_claim_id"], wrong)
                self.assertEqual(before, self.state.get_effect_operation(identity))
        self.assertEqual([], seen)
        with self.assertRaises(store.OperationConflictError):
            self.state.isolate_external_effect_claim(identity, "wrong", proof)
        with self.assertRaises(store.OperationConflictError):
            self.state.mark_effect_uncertain(identity, "response lost")
        with self.assertRaises(store.OperationConflictError):
            self.state.mark_effect_uncertain(identity, "response lost", claim_id="wrong")
        with self.assertRaises(store.OperationConflictError):
            self.state.reconcile_external_effect_operation(
                identity, evidence=self._evidence(intent, readback_complete=True),
                result="absent", claim_id=claimed["active_claim_id"],
            )
        with self.assertRaises(store.OperationConflictError):
            self.state.confirm_effect_operation(identity, self._evidence(intent))
        off = config.resolve_config({"shared_publication": False})
        self.assertEqual("pending_off", self.state.external_effect_off_state(identity, current_config=off))
        isolated = self.state.isolate_external_effect_claim(identity, claimed["active_claim_id"], proof)
        self.assertEqual("uncertain", isolated["status"])
        with self.assertRaises(store.OperationConflictError):
            self.state.isolate_external_effect_claim(identity, claimed["active_claim_id"],
                                                    self._fence(claimed, extra="conflict"))
        self.state.reconcile_external_effect_operation(
            identity, evidence=self._evidence(intent, readback_complete=True), result="absent",
            claim_id=claimed["active_claim_id"],
        )
        self.assertEqual("off", self.state.external_effect_off_state(identity, current_config=off))
        with self.assertRaisesRegex(store.OperationConflictError, "off"):
            self.state.claim_effect_operation(
                identity, current_config=off, claimant=self._claimant("native-B", 2202, "created-B"),
            )
        self.assertEqual(1, self.state.get_effect_operation(identity)["claim_generation"])

    def test_claimant_aware_duplicate_and_concurrent_claims_keep_one_owner(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        claimants = (self._claimant("native-A", 3301, "created-A"),
                     self._claimant("native-B", 3302, "created-B"))
        for bad in ({**claimants[0], "pid": 0},
                    {**claimants[0], "process_created_at": ""},
                    {**claimants[0], "content_hash": "wrong"}):
            with self.assertRaises(contracts.ContractError):
                self.state.claim_effect_operation(
                    identity, current_config=config.MemoryConfig(), claimant=bad,
                )
        self.assertEqual("pending", self.state.get_effect_operation(identity)["status"])
        barrier = threading.Barrier(2)

        def claim(claimant: dict) -> str:
            peer = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
            peer.initialize()
            try:
                barrier.wait(timeout=5)
                try:
                    return peer.claim_effect_operation(
                        identity, current_config=config.MemoryConfig(), claimant=claimant,
                    )["active_claim_id"]
                except store.OperationConflictError:
                    return "conflict"
            finally:
                peer.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, claimants))
        self.assertEqual(1, results.count("conflict"))
        claimed = self.state.get_effect_operation(identity)
        self.assertIn(claimed["active_claim_id"], results)
        self.assertEqual(1, claimed["claim_generation"])
        for claimant in claimants:
            with self.assertRaises(store.OperationConflictError):
                self.state.claim_effect_operation(
                    identity, current_config=config.MemoryConfig(), claimant=claimant,
                )
        self.assertEqual(claimed, self.state.get_effect_operation(identity))

    def test_lost_ack_requires_exact_reconciliation_before_retry(self) -> None:
        intent = self._create()
        _, created = self.state.create_external_effect_operation(
            kind="procedure_publication", scope_key="recipient-1", source_id="source-1",
            source_record=self.source, payload={"publication": "exact"},
            captured_config=config.MemoryConfig(), current_config=config.MemoryConfig(),
        )
        self.assertFalse(created)
        self.assertEqual("pending", intent["status"])
        claimed = self.state.claim_effect_operation(intent["operation_id"], current_config=config.MemoryConfig())
        self.assertEqual("in_flight", claimed["status"])
        uncertain = self.state.mark_effect_uncertain(intent["operation_id"], "acknowledgement lost")
        self.assertEqual("uncertain", uncertain["status"])
        with self.assertRaises(store.OperationConflictError):
            self.state.claim_effect_operation(intent["operation_id"], current_config=config.MemoryConfig())
        self.assertEqual("uncertain", self.state.get_effect_operation(intent["operation_id"])["status"])

    def test_exact_replay_and_conflicts_survive_restart(self) -> None:
        intent = self._create()
        self.state.close()
        self.state = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
        self.state.initialize()
        replay = self._create()
        self.assertEqual(intent, replay)
        with self.assertRaisesRegex(store.OperationConflictError, "replay"):
            self._create(payload={"publication": "different"})
        with self.assertRaisesRegex(store.OperationConflictError, "replay"):
            self._create(captured_config=config.resolve_config({"experience_read": False}))
        with self.assertRaisesRegex(contracts.ContractError, "source ID"):
            self._create(source_id="new-identity")
        self.assertNotEqual(intent["operation_id"], self._create(scope_key="recipient-2")["operation_id"])

    def test_same_source_record_cannot_gain_second_identity(self) -> None:
        source = dict(self.source, alternate_id="alternate")
        source["content_hash"] = contracts.content_hash(source)
        original = self._create(source_record=source)
        self.state.claim_effect_operation(original["operation_id"], current_config=config.MemoryConfig())
        self.state.confirm_effect_operation(original["operation_id"], self._evidence(original))
        with self.assertRaisesRegex(store.OperationConflictError, "source already"):
            self._create(source_id="alternate", source_record=source)

    def test_accepted_effect_table_migrates_for_source_owned_rows(self) -> None:
        case = fixture_module.TerminalOutcomeTests(methodName="test_exact_observed_native_chain_fixes_pass")
        case.setUp()
        try:
            case.observe()
            outcome = case.runtime.record_terminal_outcome(case.bundle())
            before = case.state.list_effect_operations(outcome["outcome_id"])
            self.assertEqual(4, len(before))
            connection = case.state.connection
            schema = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='effect_operations'"
            ).fetchone()["sql"]
            old_schema = (schema.replace("effect_operations (", "effect_operations_old (", 1)
                          .replace("outcome_id TEXT REFERENCES", "outcome_id TEXT NOT NULL REFERENCES", 1)
                          .replace("decision_id TEXT,", "decision_id TEXT NOT NULL,", 1))
            columns = ", ".join(row["name"] for row in connection.execute(
                "PRAGMA table_info(effect_operations)"
            ).fetchall() if row["name"] != "reconciliation")
            with connection:
                connection.execute(old_schema.replace("        reconciliation TEXT,\n", ""))
                connection.execute(
                    f"INSERT INTO effect_operations_old ({columns}) "
                    f"SELECT {columns} FROM effect_operations"
                )
                connection.execute("DROP TABLE effect_operations")
                connection.execute("ALTER TABLE effect_operations_old RENAME TO effect_operations")
            case.state.close()
            case.state = store.MemoryStore(case.root / "state.sqlite3")
            case.state.initialize()
            self.assertEqual(before, case.state.list_effect_operations(outcome["outcome_id"]))
        finally:
            case.tearDown()

    def test_fault_readback_and_original_identity(self) -> None:
        intent = self._create()
        calls: list[str] = []

        def submit() -> None:
            self.state.claim_effect_operation(intent["operation_id"], current_config=config.MemoryConfig())
            calls.append(intent["operation_id"])
            self.state.mark_effect_uncertain(intent["operation_id"], "response lost")

        submit()
        with self.assertRaises(store.OperationConflictError):
            submit()
        self.assertEqual([intent["operation_id"]], calls)
        with self.assertRaisesRegex(store.OperationConflictError, "exact"):
            self.state.reconcile_external_effect_operation(
                intent["operation_id"], evidence=self._evidence(intent, payload_digest="wrong"),
                result="acknowledged",
            )
        acknowledged = self.state.reconcile_external_effect_operation(
            intent["operation_id"], evidence=self._evidence(intent), result="acknowledged",
        )
        self.assertEqual("confirmed", acknowledged["status"])
        self.assertEqual(acknowledged, self.state.reconcile_external_effect_operation(
            intent["operation_id"], evidence=self._evidence(intent), result="acknowledged",
        ))
        with self.assertRaises(store.OperationConflictError):
            self.state.claim_effect_operation(intent["operation_id"], current_config=config.MemoryConfig())

    def test_exact_absence_or_original_idempotency_has_distinct_off_state(self) -> None:
        off = config.resolve_config({"shared_publication": False})
        for result, extra in (("absent", {}),
                              ("idempotent", {"idempotency_key": "original"})):
            with self.subTest(result=result):
                intent = self._create(scope_key=result)
                identity = intent["operation_id"]
                self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())
                self.state.mark_effect_uncertain(identity, "response lost")
                evidence = self._evidence(intent, **extra)
                with self.assertRaises(store.OperationConflictError):
                    self.state.reconcile_external_effect_operation(
                        identity, evidence=evidence, result=result,
                    )
                evidence.update(readback_complete=True, idempotency_key=identity)
                pending = self.state.reconcile_external_effect_operation(
                    identity, evidence=evidence, result=result,
                )
                self.assertEqual("pending" if result == "absent" else "uncertain", pending["status"])
                self.assertIsNone(pending["acknowledgement"])
                self.assertEqual(evidence, pending["reconciliation"])
                if result == "absent":
                    self.assertEqual("off", self.state.external_effect_off_state(
                        identity, current_config=off,
                    ))
                    self.assertEqual("confirmed", self.state.confirm_effect_operation(
                        identity, self._evidence(intent),
                    )["status"])
                else:
                    self.assertEqual("pending_off", self.state.external_effect_off_state(
                        identity, current_config=off,
                    ))
                    with self.assertRaisesRegex(store.OperationConflictError, "off"):
                        self.state.claim_effect_operation(identity, current_config=off)
                    self.assertEqual(identity, self.state.claim_effect_operation(
                        identity, current_config=config.MemoryConfig(),
                    )["operation_id"])
                    with self.assertRaises(store.OperationConflictError):
                        self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())
                    self.assertEqual("pending_off", self.state.external_effect_off_state(
                        identity, current_config=off,
                    ))
                    self.assertEqual("confirmed", self.state.confirm_effect_operation(
                        identity, self._evidence(intent),
                    )["status"])
                    self.assertEqual("off", self.state.external_effect_off_state(
                        identity, current_config=off,
                    ))

    def test_idempotency_proof_keeps_unresolved_effect_pending_off(self) -> None:
        off = config.resolve_config({"shared_publication": False})
        intent = self._create()
        identity = intent["operation_id"]
        self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())
        self.state.mark_effect_uncertain(identity, "response lost")
        self.assertEqual("pending_off", self.state.external_effect_off_state(
            identity, current_config=off,
        ))

        proved = self.state.reconcile_external_effect_operation(
            identity,
            evidence=self._evidence(intent, idempotency_key=identity),
            result="idempotent",
        )
        self.assertEqual("uncertain", proved["status"])
        self.assertIsNone(proved["acknowledgement"])
        self.assertEqual("pending_off", self.state.external_effect_off_state(
            identity, current_config=off,
        ))
        self.state.close()
        self.state = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
        self.state.initialize()
        self.assertEqual("uncertain", self.state.get_effect_operation(identity)["status"])
        self.assertEqual("pending_off", self.state.external_effect_off_state(
            identity, current_config=off,
        ))
        retried = self.state.claim_effect_operation(
            identity, current_config=config.MemoryConfig(),
        )
        self.assertEqual(identity, retried["operation_id"])
        self.assertEqual(proved["reconciliation"], retried["reconciliation"])

    def test_late_acknowledgement_after_idempotency_proof_closes_original(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())
        self.state.mark_effect_uncertain(identity, "response lost")
        self.state.reconcile_external_effect_operation(
            identity, evidence=self._evidence(intent, idempotency_key=identity),
            result="idempotent",
        )
        confirmed = self.state.reconcile_external_effect_operation(
            identity, evidence=self._evidence(intent), result="acknowledged",
        )
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual(self._evidence(intent), confirmed["acknowledgement"])
        with self.assertRaises(store.OperationConflictError):
            self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())

    def test_absence_proof_cannot_authorize_another_retry_after_new_lost_ack(self) -> None:
        intent = self._create()
        identity = intent["operation_id"]
        self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())
        self.state.mark_effect_uncertain(identity, "first response lost")
        self.state.reconcile_external_effect_operation(
            identity,
            evidence=self._evidence(intent, readback_complete=True, idempotency_key=identity),
            result="absent",
        )
        self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())
        self.assertIsNone(self.state.get_effect_operation(identity)["reconciliation"])
        self.state.mark_effect_uncertain(identity, "second response lost")
        with self.assertRaises(store.OperationConflictError):
            self.state.claim_effect_operation(identity, current_config=config.MemoryConfig())

    def test_feature_off_blocks_new_and_pending_and_shows_in_flight(self) -> None:
        off = config.resolve_config({"shared_publication": False})
        with self.assertRaisesRegex(store.OperationConflictError, "captured feature is off"):
            self._create(captured_config=off)
        with self.assertRaisesRegex(store.OperationConflictError, "off"):
            self._create(current_config=off)
        pending = self._create()
        self.assertEqual("off", self.state.external_effect_off_state(
            pending["operation_id"], current_config=off,
        ))
        with self.assertRaisesRegex(store.OperationConflictError, "off"):
            self.state.claim_effect_operation(pending["operation_id"], current_config=off)
        self.state.claim_effect_operation(pending["operation_id"], current_config=config.MemoryConfig())
        self.assertEqual("pending_off", self.state.external_effect_off_state(
            pending["operation_id"], current_config=off,
        ))
        self.state.mark_effect_uncertain(pending["operation_id"], "response lost")
        self.assertEqual("pending_off", self.state.external_effect_off_state(
            pending["operation_id"], current_config=off,
        ))
        self.state.reconcile_external_effect_operation(
            pending["operation_id"], evidence=self._evidence(pending), result="acknowledged",
        )
        self.assertEqual("off", self.state.external_effect_off_state(
            pending["operation_id"], current_config=off,
        ))

    def test_two_connections_serialize_one_claim_and_leave_other_scope_free(self) -> None:
        intent = self._create()
        other = self._create(scope_key="recipient-2")
        self.state.claim_effect_operation(intent["operation_id"], current_config=config.MemoryConfig())
        self.state.mark_effect_uncertain(intent["operation_id"], "response lost")
        self.state.reconcile_external_effect_operation(
            intent["operation_id"],
            evidence=self._evidence(intent, idempotency_key=intent["operation_id"]),
            result="idempotent",
        )
        barrier = threading.Barrier(2)

        def claim() -> str:
            peer = store.MemoryStore(Path(self.directory.name) / "effects.sqlite3")
            peer.initialize()
            try:
                barrier.wait(timeout=5)
                try:
                    peer.claim_effect_operation(intent["operation_id"], current_config=config.MemoryConfig())
                    return "claimed"
                except store.OperationConflictError:
                    return "fenced"
            finally:
                peer.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: claim(), range(2)))
        self.assertEqual(["claimed", "fenced"], sorted(results))
        self.assertEqual("in_flight", self.state.claim_effect_operation(
            other["operation_id"], current_config=config.MemoryConfig(),
        )["status"])


if __name__ == "__main__":
    unittest.main()
