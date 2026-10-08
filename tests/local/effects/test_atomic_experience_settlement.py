"""Two SQLite handles settle one source-owned EverOS claim without a split state."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, contracts, experience, store
from tests.local.experience import test_ingestion_and_trust as fixture_module


class AtomicExperienceSettlementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = fixture_module.ReviewedExperienceIngestionTests()
        self.fixture.setUp()
        self.owner = self.fixture.memory_store
        self.trajectory = self.fixture._capture()
        self.peer = store.MemoryStore(self.fixture.root / "memory.sqlite3")
        self.peer.initialize()
        self.session = "session-1"
        self.destination = "everos"
        self.payload = self.make_payload()
        self.operation, _ = self.owner.create_external_effect_operation(
            kind="experience_ingestion",
            scope_key=contracts.sha256_hex({"scope": self.trajectory["scope"],
                                            "destination": self.destination}),
            source_id=self.trajectory["trajectory_id"], source_record=self.trajectory,
            payload=self.payload,
            captured_config=self.owner.get_decision(self.trajectory["decision_id"])["configuration"],
            current_config=config.MemoryConfig(),
        )
        claimant = {"schema": "external-effect-claimant/v1", "native_invocation_id": "native-A",
                    "pid": 1001, "process_created_at": "created-A"}
        claimant["content_hash"] = contracts.content_hash(claimant)
        self.claim = self.owner.claim_effect_operation(
            self.operation["operation_id"], current_config=config.MemoryConfig(), claimant=claimant,
        )
        intent = contracts.make_experience_ingestion(
            trajectory=self.trajectory, destination=self.destination,
            session_id=self.session, payload_digest=contracts.sha256_hex(self.payload),
        )
        self.ingestion, _ = self.owner.create_experience_ingestion(intent)
        self.receipt = contracts.make_case_receipt(
            trajectory=self.trajectory, ingestion=self.ingestion,
            source_case={"id": "case-1", "session_id": self.session},
        )
        self.evidence = {key: self.claim[key] for key in (
            "operation_id", "kind", "scope_key", "source_digest", "payload_digest",
            "configuration_digest",
        )}
        self.evidence["adapter_proof"] = {
            "session_id": self.session, "scope": self.trajectory["scope"],
            "case_ids": [self.receipt["case_id"]],
        }

    def make_payload(self) -> dict:
        return {
            "session_id": self.session,
            "app_id": self.trajectory["scope"]["application"],
            "project_id": self.trajectory["scope"]["project"],
            "messages": [{"sender_id": self.trajectory["scope"]["owner"], "content": "reviewed"}],
        }

    def tearDown(self) -> None:
        self.peer.close()
        self.fixture.tearDown()

    def settle(self, handle: store.MemoryStore, mode: str, **changes: object):
        arguments = {"claim_id": self.claim["active_claim_id"],
                     "claim_generation": self.claim["claim_generation"], "mode": mode}
        arguments.update(changes)
        return handle.settle_external_experience_ingestion(
            self.operation["operation_id"], self.ingestion["ingestion_id"], **arguments,
        )

    def test_lost_response_and_peer_readback_settle_one_pair(self) -> None:
        effect, ingestion, disposition = self.settle(
            self.owner, "uncertain", reason="response lost", ingestion_uncertain=True,
        )
        self.assertEqual(("uncertain", "uncertain", "uncertain"),
                         (effect["status"], ingestion["status"], disposition))
        effect, ingestion, disposition = self.settle(
            self.peer, "acknowledged", evidence=self.evidence, case_receipts=(self.receipt,),
        )
        self.assertEqual(("confirmed", "confirmed", "confirmed"),
                         (effect["status"], ingestion["status"], disposition))
        self.assertEqual(self.receipt, self.peer.get_case_receipt(self.trajectory["scope"], "case-1"))
        joined = self.peer.read_confirmed_case_join(self.trajectory["scope"], "case-1")
        self.assertEqual(self.receipt, joined["case_receipt"])
        versions = effect["version"], ingestion["version"]
        replay = self.settle(self.peer, "acknowledged", evidence=self.evidence,
                             case_receipts=(self.receipt,))
        self.assertEqual((effect, ingestion, "already_confirmed"), replay)
        self.assertEqual(versions, (replay[0]["version"], replay[1]["version"]))
        late = self.settle(self.owner, "uncertain", reason="response lost",
                           ingestion_uncertain=True, evidence=self.evidence,
                           case_receipts=(self.receipt,))
        self.assertEqual((effect, ingestion, "already_confirmed"), late)

    def test_peer_first_and_wrong_handle_uncertainty(self) -> None:
        with self.assertRaises(store.OperationConflictError):
            self.settle(self.peer, "uncertain", reason="copied ID", ingestion_uncertain=True)
        effect, ingestion, _ = self.settle(
            self.peer, "acknowledged", evidence=self.evidence, case_receipts=(self.receipt,),
        )
        self.assertEqual(("confirmed", "confirmed"), (effect["status"], ingestion["status"]))
        self.assertEqual((effect, ingestion, "already_confirmed"), self.settle(
            self.owner, "uncertain", reason="response lost", ingestion_uncertain=True,
            evidence=self.evidence, case_receipts=(self.receipt,),
        ))

    def test_bad_proof_and_stale_claim_leave_every_row_unchanged(self) -> None:
        before = self.owner.get_effect_operation(self.operation["operation_id"]), self.owner.get_experience_ingestion(self.ingestion["ingestion_id"])
        cases = (
            {"claim_id": None, "evidence": self.evidence, "case_receipts": (self.receipt,)},
            {"claim_generation": self.claim["claim_generation"] + 1,
             "evidence": self.evidence, "case_receipts": (self.receipt,)},
            {"evidence": {**self.evidence, "payload_digest": "wrong"},
             "case_receipts": (self.receipt,)},
            {"evidence": self.evidence, "case_receipts": ()},
        )
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(store.OperationConflictError):
                self.settle(self.peer, "acknowledged", **changes)
            self.assertEqual(before, (self.owner.get_effect_operation(self.operation["operation_id"]),
                                      self.owner.get_experience_ingestion(self.ingestion["ingestion_id"])))

    def test_uncertain_effect_can_leave_pending_ingestion_until_exact_readback(self) -> None:
        effect, ingestion, disposition = self.settle(
            self.owner, "uncertain", reason="awaiting readback", ingestion_uncertain=False,
        )
        self.assertEqual(("uncertain", "pending", "uncertain"),
                         (effect["status"], ingestion["status"], disposition))
        self.assertEqual((effect, ingestion, "already_uncertain"), self.settle(
            self.owner, "uncertain", reason="awaiting readback",
        ))
        effect, ingestion, _ = self.settle(
            self.peer, "acknowledged", evidence=self.evidence, case_receipts=(self.receipt,),
        )
        self.assertEqual(("confirmed", "confirmed"), (effect["status"], ingestion["status"]))

    def test_stale_generation_and_reopened_handle_cannot_mutate(self) -> None:
        old_id = self.claim["active_claim_id"]
        self.owner.mark_effect_uncertain(self.operation["operation_id"], "lost", claim_id=old_id)
        absent = dict(self.evidence, readback_complete=True)
        self.owner.reconcile_external_effect_operation(
            self.operation["operation_id"], evidence=absent, result="absent", claim_id=old_id,
        )
        claimant = {"schema": "external-effect-claimant/v1", "native_invocation_id": "native-B",
                    "pid": 1002, "process_created_at": "created-B"}
        claimant["content_hash"] = contracts.content_hash(claimant)
        new_claim = self.peer.claim_effect_operation(
            self.operation["operation_id"], current_config=config.MemoryConfig(), claimant=claimant,
        )
        before = self.peer.get_effect_operation(self.operation["operation_id"])
        with self.assertRaises(store.OperationConflictError):
            self.settle(self.owner, "uncertain", reason="late A", ingestion_uncertain=True)
        with self.assertRaises(store.OperationConflictError):
            self.settle(self.owner, "acknowledged", evidence=self.evidence,
                        case_receipts=(self.receipt,))
        self.assertEqual(before, self.peer.get_effect_operation(self.operation["operation_id"]))
        self.claim = new_claim
        self.peer.close()
        self.peer = store.MemoryStore(self.fixture.root / "memory.sqlite3")
        self.peer.initialize()
        with self.assertRaises(store.OperationConflictError):
            self.settle(self.peer, "uncertain", reason="reopened B", ingestion_uncertain=True)
        self.assertEqual(before, self.peer.get_effect_operation(self.operation["operation_id"]))

    def test_proof_provenance_and_configuration_mismatch_fail_closed(self) -> None:
        wrong_scope = dict(self.evidence, adapter_proof={
            **self.evidence["adapter_proof"], "scope": {**self.trajectory["scope"],
                                                        "owner": "another"},
        })
        wrong_session = dict(self.evidence, adapter_proof={
            **self.evidence["adapter_proof"], "session_id": "another",
        })
        wrong_cases = dict(self.evidence, adapter_proof={
            **self.evidence["adapter_proof"], "case_ids": ["case-1", "missing"],
        })
        bad_receipt = dict(self.receipt, review_receipt_digest="other")
        bad_receipt["content_hash"] = contracts.content_hash(bad_receipt)
        bad_case = dict(self.receipt, source_case={"id": "case-1", "session_id": "other"})
        bad_case["source_case_digest"] = contracts.sha256_hex(bad_case["source_case"])
        bad_case["content_hash"] = contracts.content_hash(bad_case)
        before = (self.owner.get_effect_operation(self.operation["operation_id"]),
                  self.owner.get_experience_ingestion(self.ingestion["ingestion_id"]))
        for evidence, receipts in ((wrong_scope, (self.receipt,)),
                                   (wrong_session, (self.receipt,)),
                                   (wrong_cases, (self.receipt,)),
                                   (self.evidence, (bad_receipt,)),
                                   (self.evidence, (bad_case,))):
            with self.subTest(evidence=evidence, receipts=receipts), self.assertRaises(store.OperationConflictError):
                self.settle(self.peer, "acknowledged", evidence=evidence, case_receipts=receipts)
            self.assertEqual(before, (self.owner.get_effect_operation(self.operation["operation_id"]),
                                      self.owner.get_experience_ingestion(self.ingestion["ingestion_id"])))
        for table, column, value, key, identity in (
            ("effect_operations", "source_id", "wrong", "operation_id", self.operation["operation_id"]),
            ("effect_operations", "source_digest", "wrong", "operation_id", self.operation["operation_id"]),
            ("effect_operations", "source_record", json.dumps({"wrong": "source"}),
             "operation_id", self.operation["operation_id"]),
            ("effect_operations", "scope_key", "wrong", "operation_id", self.operation["operation_id"]),
            ("effect_operations", "configuration_digest", "wrong", "operation_id", self.operation["operation_id"]),
            ("effect_operations", "payload_digest", "wrong", "operation_id", self.operation["operation_id"]),
            ("experience_ingestions", "session_id", "wrong", "ingestion_id", self.ingestion["ingestion_id"]),
            ("experience_ingestions", "scope_digest", "wrong", "ingestion_id", self.ingestion["ingestion_id"]),
        ):
            with self.subTest(table=table, column=column):
                with self.owner.connection:
                    self.owner.connection.execute(
                        f"UPDATE {table} SET {column}=? WHERE {key}=?", (value, identity),
                    )
                with self.assertRaises((store.StoreError, contracts.ContractError)):
                    self.settle(self.peer, "acknowledged", evidence=self.evidence,
                                case_receipts=(self.receipt,))
                with self.owner.connection:
                    original = before[0][column] if table == "effect_operations" else before[1][column]
                    if column == "source_record":
                        original = json.dumps(original)
                    self.owner.connection.execute(
                        f"UPDATE {table} SET {column}=? WHERE {key}=?",
                        (original, identity),
                    )
                self.assertEqual(before, (self.owner.get_effect_operation(self.operation["operation_id"]),
                                          self.owner.get_experience_ingestion(self.ingestion["ingestion_id"])))

    def test_off_state_original_config_unrelated_effect_and_conflicting_replay(self) -> None:
        source = {"schema": "trusted-source/v1", "source_id": "other", "value": "approved"}
        source["content_hash"] = contracts.content_hash(source)
        unrelated, _ = self.owner.create_external_effect_operation(
            kind="procedure_publication", scope_key="other-recipient", source_id="other",
            source_record=source, payload={"publication": "other"},
            captured_config=config.MemoryConfig(), current_config=config.MemoryConfig(),
        )
        off = config.resolve_config({"experience_write": False})
        self.assertEqual("pending_off", self.owner.external_effect_off_state(
            self.operation["operation_id"], current_config=off,
        ))
        effect, ingestion, _ = self.settle(
            self.peer, "acknowledged", evidence=self.evidence, case_receipts=(self.receipt,),
        )
        self.assertEqual("off", self.owner.external_effect_off_state(
            self.operation["operation_id"], current_config=off,
        ))
        self.assertEqual(self.operation["configuration"], effect["configuration"])
        self.assertEqual("pending", self.owner.get_effect_operation(unrelated["operation_id"])["status"])
        wrong = dict(self.evidence, adapter_proof={**self.evidence["adapter_proof"],
                                                    "case_ids": ["other"]})
        with self.assertRaises(store.OperationConflictError):
            self.settle(self.peer, "acknowledged", evidence=wrong,
                        case_receipts=(self.receipt,))
        self.assertEqual(effect, self.owner.get_effect_operation(self.operation["operation_id"]))
        self.assertEqual(ingestion, self.owner.get_experience_ingestion(self.ingestion["ingestion_id"]))


class PublicAdapterExperienceSettlementTests(AtomicExperienceSettlementTests):
    def make_payload(self) -> dict:
        surface = fixture_module._FakeEverOS()
        scope = self.fixture.scope
        base_root = self.fixture.root / "everos"
        adapter = experience.EverOSAdapter(
            scope=scope,
            base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                surface,
                memory_root=experience.EverOSAdapter.memory_root_for_scope(base_root, scope),
                resolve_memory_root=lambda: experience.EverOSAdapter.memory_root_for_scope(
                    base_root, scope
                ),
            ),
            privacy_policy=self.fixture.policy,
        )
        self.destination = adapter.destination
        self.session = adapter.session_id_for(self.trajectory["trajectory_id"])
        payload = adapter.add_payload(self.trajectory, session_id=self.session)
        mutator = getattr(self, "payload_mutator", None)
        return mutator(payload) if mutator is not None else payload

    def test_public_adapter_payload_settles_exact_durable_pair(self) -> None:
        scope = self.trajectory["scope"]
        self.assertEqual(
            "mh-app-" + contracts.sha256_hex({"value": scope["application"]})[:32],
            self.payload["app_id"],
        )
        self.assertEqual(
            "mh-project-" + contracts.sha256_hex({"value": scope["project"]})[:32],
            self.payload["project_id"],
        )
        self.assertEqual(
            "mh-owner-" + contracts.sha256_hex({"value": scope["owner"]})[:32],
            self.payload["messages"][0]["sender_id"],
        )
        self.assertEqual(self.operation["payload_digest"], self.ingestion["payload_digest"])
        effect, ingestion, disposition = self.settle(
            self.owner, "uncertain", reason="response lost", ingestion_uncertain=True,
        )
        self.assertEqual(("uncertain", "uncertain", "uncertain"),
                         (effect["status"], ingestion["status"], disposition))
        effect, ingestion, disposition = self.settle(
            self.peer, "acknowledged", evidence=self.evidence, case_receipts=(self.receipt,),
        )
        self.assertEqual(("confirmed", "confirmed", "confirmed"),
                         (effect["status"], ingestion["status"], disposition))
        self.assertEqual(contracts.sha256_hex(self.payload), effect["payload_digest"])
        self.assertEqual(self.receipt, self.peer.get_case_receipt(scope, "case-1"))

    def test_wrong_or_mixed_payload_identities_leave_claim_and_ingestion_unchanged(self) -> None:
        scope = self.trajectory["scope"]

        def altered(**fields: str):
            def mutate(payload: dict) -> dict:
                payload.update(fields)
                return payload
            return mutate

        def sender(value: str):
            def mutate(payload: dict) -> dict:
                payload["messages"][0]["sender_id"] = value
                return payload
            return mutate

        def mixed_messages(payload: dict) -> dict:
            payload["messages"].append({**payload["messages"][0], "sender_id": scope["owner"]})
            return payload

        def wrong_raw_owner(payload: dict) -> dict:
            payload.update(app_id=scope["application"], project_id=scope["project"])
            payload["messages"][0]["sender_id"] = "wrong"
            return payload

        other_hash = contracts.sha256_hex({"value": "other"})[:32]
        cases = (
            ("wrong raw app", altered(app_id="wrong", project_id=scope["project"])),
            ("wrong raw project", altered(app_id=scope["application"], project_id="wrong")),
            ("wrong raw owner", wrong_raw_owner),
            ("wrong transport app", altered(app_id="mh-app-" + other_hash)),
            ("wrong transport project", altered(project_id="mh-project-" + other_hash)),
            ("wrong transport owner", sender("mh-owner-" + other_hash)),
            ("mixed app", altered(app_id=scope["application"])),
            ("mixed project", altered(project_id=scope["project"])),
            ("mixed owner", sender(scope["owner"])),
            ("mixed messages", mixed_messages),
            ("wrong session", altered(session_id="other")),
        )
        for name, mutator in cases:
            with self.subTest(name=name):
                attempt = type(self)()
                attempt.payload_mutator = mutator
                attempt.setUp()
                try:
                    before = (attempt.owner.get_effect_operation(attempt.operation["operation_id"]),
                              attempt.owner.get_experience_ingestion(attempt.ingestion["ingestion_id"]))
                    with self.assertRaisesRegex(store.OperationConflictError,
                                                "experience settlement identity differs"):
                        attempt.settle(attempt.owner, "uncertain", reason="response lost",
                                       ingestion_uncertain=True)
                    self.assertEqual(before, (
                        attempt.owner.get_effect_operation(attempt.operation["operation_id"]),
                        attempt.owner.get_experience_ingestion(attempt.ingestion["ingestion_id"]),
                    ))
                finally:
                    attempt.tearDown()


if __name__ == "__main__":
    unittest.main()
