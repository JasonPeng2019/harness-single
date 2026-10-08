"""Deterministic exact-read and lost-acknowledgement procedure publication faults."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import atlas, config, contracts, privacy, procedures, store


class _Collection:
    def __init__(self) -> None:
        self.documents: dict[str, dict] = {}
        self.events: list[tuple[str, str]] = []
        self.fail_next_read_for: str | None = None
        self.fail_read_after_replace_for: str | None = None
        self.lose_insert_ack_for: str | None = None

    def find_one(self, query: dict) -> dict | None:
        identifier = query["_id"]
        self.events.append(("read", identifier))
        if self.fail_next_read_for == identifier:
            self.fail_next_read_for = None
            raise RuntimeError("synthetic readback outage")
        document = self.documents.get(identifier)
        return dict(document) if document is not None else None

    def insert_one(self, document: dict) -> object:
        identifier = document["_id"]
        self.events.append(("insert", identifier))
        if identifier in self.documents:
            raise RuntimeError("duplicate id")
        self.documents[identifier] = dict(document)
        if self.lose_insert_ack_for == identifier:
            self.lose_insert_ack_for = None
            raise RuntimeError("synthetic insert acknowledgement loss")
        return object()

    def replace_one(self, query: dict, document: dict, *, upsert: bool) -> object:
        identifier = query["_id"]
        self.events.append(("replace", identifier))
        existing = self.documents.get(identifier)
        if existing is None or any(existing.get(key) != value for key, value in query.items()):
            return SimpleNamespace(matched_count=0)
        self.documents[identifier] = dict(document)
        if self.fail_read_after_replace_for == identifier:
            self.fail_read_after_replace_for = None
            self.fail_next_read_for = identifier
        return SimpleNamespace(matched_count=1)


class _FaultAdapter(atlas.AtlasProcedureAdapter):
    def __init__(self, *, collection: _Collection) -> None:
        super().__init__(
            collection=collection, vector_store=object(),
            location={"database": "procedures_test", "collection": "trusted",
                      "index": "vector_index"},
        )
        self.publication_writes = 0
        self.lose_ack = False
        self.lose_before_commit = False
        self.fail_before_commit = False
        self.fail_exact_read = False
        self.miss_exact_read = False
        self.miss_state_read = False

    def write_publication(self, publication: dict) -> dict:
        self.publication_writes += 1
        if self.fail_before_commit:
            raise atlas.AtlasProcedureError("synthetic definite pre-commit rejection")
        if self.lose_before_commit:
            self.lose_before_commit = False
            raise atlas.AtlasProcedureAmbiguityError("synthetic lost response before commit")
        document = super().write_publication(publication)
        if self.lose_ack:
            self.lose_ack = False
            raise atlas.AtlasProcedureAmbiguityError("synthetic lost response after commit")
        return document

    def exact_read(self, publication_id: str) -> atlas.AtlasExactProcedureSnapshot | None:
        if self.fail_exact_read:
            self.fail_exact_read = False
            raise atlas.AtlasProcedureError("synthetic exact-read outage")
        if self.miss_exact_read:
            self.miss_exact_read = False
            return None
        snapshot = super().exact_read(publication_id)
        if self.miss_state_read and snapshot is not None:
            self.miss_state_read = False
            return atlas.AtlasExactProcedureSnapshot(
                document=snapshot.document,
                current=snapshot.current,
                publication_state=None,
                revocation=snapshot.revocation,
            )
        return snapshot


class AtlasExternalReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.terminated_claimants: set[tuple[str, int, str]] = set()
        self.memory_store = store.MemoryStore(Path(self.temporary.name) / "memory.sqlite3",
            external_claim_fence_verifier=self._verified_fence)
        self.memory_store.initialize()
        self.scope = {
            "application": "harness",
            "project": "product-a",
            "namespace": "exact-reconciliation",
            "owner": "root-agent",
        }
        self.partition = {
            "scope": "project",
            "application": "harness",
            "project": "product-a",
            "namespace": "exact-reconciliation",
            "recipients": [dict(self.scope)],
        }
        self.procedure = contracts.make_procedure_revision(
            logical_name="bounded-recovery",
            origin="curated",
            origin_scope=self.scope,
            body="Inspect the lock before recovery.",
            references=[{"id": "guide://lock", "content": "Preserve lock state."}],
            predicates={
                "applicability": {},
                "conflicts": {},
                "capabilities": {},
                "routes": {},
            },
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/1"},
            created_at="2026-09-21T00:00:00Z",
        )
        self.approval = contracts.make_procedure_approval(
            approval_id="approval-exact",
            procedure=self.procedure,
            issuer="ROOT",
            recipients=[self.scope],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:01Z",
        )
        self.representation = contracts.make_procedure_representation(
            procedure=self.procedure,
            model="deterministic-test-embedding/v1",
            dimensions=3,
            metric="cosine",
            sanitizer_version="known-secret/v1",
            search_text="lock recovery",
            vector=[0.1, 0.2, 0.3],
            created_at="2026-09-21T00:00:02Z",
        )
        self.service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}
        )
        self.service.record_approved_revision(self.procedure, self.approval)
        self.service.record_representation(self.representation)
        self.designation = self.service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            issuer="ROOT",
        )
        self.collection = _Collection()
        self.adapter = _FaultAdapter(collection=self.collection)
        self.service.publish_designation(self.designation, self.adapter)

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def _publish(self) -> dict:
        return self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )

    def test_restricted_entry_blocks_new_publication_but_reads_submitted_uncertainty(self) -> None:
        restricted = config.resolve_network_mode("restricted_local")
        before = list(self.collection.events)
        with self.assertRaises(procedures.ProcedureNetworkDeniedError):
            self.service.publish(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation,
                adapter=self.adapter, network_resolution=restricted,
            )
        self.assertEqual(before, self.collection.events)
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending = self.memory_store.get_procedure_publication(
            contracts.make_procedure_publication(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation,
            )["publication_id"]
        )
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("acknowledged", self.service.reconcile_publication(
            pending["publication_id"], self.adapter, network_resolution=restricted,
            shared_publication_enabled=False)["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_legacy_ambiguous_publish_replay_is_exact_only_while_off(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        self.assertEqual(1, self.adapter.publication_writes)
        self.assertEqual("acknowledged", self.service.publish(
            procedure=self.procedure, approval=self.approval,
            representation=self.representation, designation=self.designation,
            adapter=self.adapter,
            network_resolution=config.resolve_network_mode("restricted_local"),
            shared_publication_enabled=False,
        )["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_requested_atlas_only_downgrade_and_verified_effective_are_distinct(self) -> None:
        downgraded = config.resolve_network_mode("atlas_memory_only")
        self.assertEqual("soft_guardrail_network", downgraded.effective_mode)
        self.assertEqual("acknowledged", self.service.publish(
            procedure=self.procedure, approval=self.approval,
            representation=self.representation, designation=self.designation,
            adapter=self.adapter, network_resolution=downgraded,
        )["status"])

    def test_restricted_safety_administration_fences_locally_and_remains_pending(self) -> None:
        restricted = config.resolve_network_mode("restricted_local")
        before = list(self.collection.events)
        result = self.service.withdraw_partition(
            logical_id=self.designation["logical_id"], partition=self.partition,
            issuer="ROOT", adapter=self.adapter, network_resolution=restricted,
        )
        self.assertFalse(result["managed_complete"])
        self.assertEqual(before, self.collection.events)
        self.assertEqual("withdrawn", self.memory_store.get_current_procedure_designation(
            self.designation["logical_id"], self.partition)["state"])
        before = list(self.collection.events)
        retry = self.service.reconcile_withdrawal(
            logical_id=self.designation["logical_id"], partition=self.partition,
            adapter=self.adapter, network_resolution=restricted,
        )
        self.assertFalse(retry["managed_complete"])
        self.assertEqual(before, self.collection.events)
        self.assertTrue(self.service.reconcile_withdrawal(
            logical_id=self.designation["logical_id"], partition=self.partition,
            adapter=self.adapter)["managed_complete"])

    def _claimant(self, invocation: str = "native-test") -> dict:
        record = {"schema": "external-effect-claimant/v1", "native_invocation_id": invocation,
                  "pid": 4142 if invocation == "native-test" else 5153,
                  "process_created_at": ("2026-09-26T09:00:00Z" if invocation == "native-test"
                                         else "2026-09-26T09:05:00Z")}
        record["content_hash"] = contracts.content_hash(record)
        return record

    def _verified_fence(self, claimant: dict, fence: dict) -> bool:
        identity = (claimant["native_invocation_id"], claimant["pid"],
                    claimant["process_created_at"])
        return (identity in self.terminated_claimants
                and fence.get("verifier_proof") == "terminated-by-test-root"
                and fence.get("claimant") == claimant)

    def _fence(self, effect: dict) -> dict:
        record = {"schema": "external-effect-claim-fence/v1",
                  "operation_id": effect["operation_id"], "claim_id": effect["active_claim_id"],
                  "claim_generation": effect["claim_generation"],
                  "claimant_digest": effect["claimant_digest"],
                  "claimant": effect["claimant_record"], "terminated": True,
                  "verifier_proof": "terminated-by-test-root"}
        record["content_hash"] = contracts.content_hash(record)
        return record

    def _governed_publish(self, *, current_on: bool = True,
                          network_resolution=None,
                          claim_fence: dict | None = None,
                          invocation: str = "native-test",
                          shared_publication_enabled: bool = True) -> dict:
        return self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
            claimant=self._claimant(invocation), claim_fence=claim_fence,
            captured_config={"shared_publication": True},
            current_config={"shared_publication": current_on},
            network_resolution=network_resolution,
            shared_publication_enabled=shared_publication_enabled,
            atlas_scope={
                "database": "procedures_test",
                "collection": "trusted",
                "index": "vector_index",
                "namespace": self.partition["namespace"],
                "recipients": self.partition["recipients"],
            },
        )

    def test_governed_restricted_retry_confirms_original_without_another_write(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        effect = [row for row in self.memory_store.list_effect_operations()
                  if row["kind"] == "procedure_publication"][0]
        self.assertEqual("uncertain", effect["status"])
        before_writes = self.adapter.publication_writes
        restricted = config.resolve_network_mode("restricted_local")
        self.assertEqual("acknowledged", self._governed_publish(
            current_on=False, network_resolution=restricted,
            shared_publication_enabled=False,
        )["status"])
        self.assertEqual(before_writes, self.adapter.publication_writes)
        retained = self.memory_store.get_effect_operation(effect["operation_id"])
        self.assertEqual("confirmed", retained["status"])
        self.assertEqual({"shared_publication": True}, retained["configuration"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_feature_off_and_outage_are_separate_from_network_denial(self) -> None:
        before = list(self.collection.events)
        with self.assertRaises(procedures.ProcedureIneligibleError) as off:
            self.service.publish(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation,
                adapter=self.adapter, shared_publication_enabled=False,
            )
        self.assertNotIsInstance(off.exception, procedures.ProcedureNetworkDeniedError)
        self.assertEqual(before, self.collection.events)
        self.adapter.fail_before_commit = True
        with self.assertRaises(procedures.ProcedureError) as outage:
            self._publish()
        self.assertNotIsInstance(outage.exception, procedures.ProcedureNetworkDeniedError)
        self.assertGreater(len(self.collection.events), len(before))

    def test_restricted_revocation_fences_locally_without_remote_call(self) -> None:
        before = list(self.collection.events)
        result = self.service.revoke(
            procedure=self.procedure, issuer="ROOT", reason="superseded",
            adapter=self.adapter,
            network_resolution=config.resolve_network_mode("restricted_local"),
        )
        self.assertFalse(result["managed_complete"])
        self.assertEqual(before, self.collection.events)
        self.assertTrue(self.memory_store.is_procedure_revoked(self.procedure["revision_id"]))
        self.assertTrue(self.service.revoke(
            procedure=self.procedure, issuer="ROOT", reason="superseded",
            adapter=self.adapter,
        )["managed_complete"])

    def test_governed_publication_persists_exact_intent_before_one_write(self) -> None:
        original_write = self.adapter.write_publication

        def inspect_before_write(publication: dict) -> dict:
            effects = [row for row in self.memory_store.list_effect_operations()
                       if row["kind"] == "procedure_publication"]
            self.assertEqual(1, len(effects))
            effect = effects[0]
            self.assertEqual("in_flight", effect["status"])
            self.assertEqual(publication, effect["source_record"])
            self.assertEqual(atlas.make_atlas_procedure_document(publication), effect["payload_record"]["document"])
            self.assertEqual("procedures_test", effect["payload_record"]["atlas_scope"]["database"])
            self.assertEqual({"shared_publication": True}, effect["configuration"])
            return original_write(publication)

        self.adapter.write_publication = inspect_before_write  # type: ignore[method-assign]
        publication = self._governed_publish()
        self.assertEqual("acknowledged", publication["status"])
        self.assertEqual("confirmed", [row for row in self.memory_store.list_effect_operations()
                                       if row["kind"] == "procedure_publication"][0]["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_governed_lost_ack_reconciles_while_off_without_another_write(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        effect = [row for row in self.memory_store.list_effect_operations()
                  if row["kind"] == "procedure_publication"][0]
        self.assertEqual("uncertain", effect["status"])
        self.assertEqual("pending_off", self.memory_store.external_effect_off_state(
            effect["operation_id"], current_config={"shared_publication": False}))
        recovered = self._governed_publish(current_on=False, shared_publication_enabled=False)
        self.assertEqual("acknowledged", recovered["status"])
        confirmed = self.memory_store.get_effect_operation(effect["operation_id"])
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual({"shared_publication": True}, confirmed["configuration"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_confirmed_effect_with_local_ack_outage_recovers_while_restricted(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        original = self.memory_store.list_effect_operations()[0]
        original_read = self.adapter.exact_read
        reads = 0

        def fail_second_read(publication_id: str):
            nonlocal reads
            reads += 1
            if reads == 2:
                raise atlas.AtlasProcedureError("synthetic post-confirm exact-read outage")
            return original_read(publication_id)

        self.adapter.exact_read = fail_second_read  # type: ignore[method-assign]
        restricted = config.resolve_network_mode("restricted_local")
        with self.assertRaises(atlas.AtlasProcedureError):
            self._governed_publish(current_on=False, network_resolution=restricted,
                                   shared_publication_enabled=False)
        self.assertEqual(2, reads)
        confirmed = self.memory_store.get_effect_operation(original["operation_id"])
        self.assertEqual("confirmed", confirmed["status"])
        self.assertEqual("ambiguous", self.memory_store.list_procedure_publications()[0]["status"])
        self.assertEqual("ambiguous", self.memory_store.list_procedure_remote_operations(
            payload_id=original["source_id"])[0]["status"])
        self.assertEqual(1, self.adapter.publication_writes)

        before_retry = len(self.collection.events)
        recovered = self._governed_publish(current_on=False, network_resolution=restricted,
                                           shared_publication_enabled=False)
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual("acknowledged", self.memory_store.list_procedure_remote_operations(
            payload_id=original["source_id"])[0]["status"])
        retained = self.memory_store.get_effect_operation(original["operation_id"])
        self.assertEqual(original["operation_id"], retained["operation_id"])
        self.assertEqual(original["active_claim_id"], retained["active_claim_id"])
        self.assertEqual(original["claim_generation"], retained["claim_generation"])
        self.assertEqual({"shared_publication": True}, retained["configuration"])
        self.assertEqual(1, self.adapter.publication_writes)
        self.assertEqual(3, reads)
        self.assertTrue(self.collection.events[before_retry:])
        self.assertTrue(all(event == "read" for event, _ in self.collection.events[before_retry:]))

    def test_confirmed_complete_publication_denies_restricted_retry_without_adapter_calls(self) -> None:
        publication = self._governed_publish()
        before = list(self.collection.events)
        restricted = config.resolve_network_mode("restricted_local")
        with self.assertRaises(procedures.ProcedureNetworkDeniedError):
            self._governed_publish(current_on=False, network_resolution=restricted,
                                   shared_publication_enabled=False)
        with self.assertRaises(procedures.ProcedureNetworkDeniedError):
            self.service.reconcile_publication(
                publication["publication_id"], self.adapter,
                network_resolution=restricted, shared_publication_enabled=False)
        self.assertEqual(before, self.collection.events)
        self.assertEqual(1, self.adapter.publication_writes)

    def test_off_reconcile_exactly_confirms_submitted_uncertainty(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        publication = self.memory_store.list_procedure_publications()[0]
        before_writes = self.adapter.publication_writes
        recovered = self.service.reconcile_publication(
            publication["publication_id"], self.adapter,
            network_resolution=config.resolve_network_mode("restricted_local"),
            shared_publication_enabled=False,
        )
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual(before_writes, self.adapter.publication_writes)
        self.assertEqual("confirmed", self.memory_store.list_effect_operations()[0]["status"])

    def test_restricted_absent_uncertainty_cannot_retry_or_claim(self) -> None:
        self.adapter.lose_before_commit = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        original = self.memory_store.list_effect_operations()[0]
        with self.assertRaises(procedures.ProcedureIneligibleError):
            self._governed_publish(
                current_on=False,
                network_resolution=config.resolve_network_mode("restricted_local"),
                shared_publication_enabled=False,
            )
        retained = self.memory_store.get_effect_operation(original["operation_id"])
        self.assertEqual("pending", retained["status"])
        self.assertEqual(original["claim_generation"], retained["claim_generation"])
        self.assertEqual({"shared_publication": True}, retained["configuration"])
        self.assertEqual(1, self.adapter.publication_writes)
        before = list(self.collection.events)
        with self.assertRaises(procedures.ProcedureNetworkDeniedError):
            self._governed_publish(
                current_on=False,
                network_resolution=config.resolve_network_mode("restricted_local"),
                shared_publication_enabled=False,
            )
        with self.assertRaises(procedures.ProcedureNetworkDeniedError):
            self.service.reconcile_publication(
                original["source_id"], self.adapter,
                network_resolution=config.resolve_network_mode("restricted_local"),
                shared_publication_enabled=False,
            )
        self.assertEqual(before, self.collection.events)

    def test_feature_off_fresh_governed_publication_has_zero_adapter_calls(self) -> None:
        before = list(self.collection.events)
        with self.assertRaises(procedures.ProcedureIneligibleError):
            self._governed_publish(current_on=False, shared_publication_enabled=False)
        self.assertEqual(before, self.collection.events)
        self.assertEqual([], self.memory_store.list_effect_operations())

    def test_restricted_in_flight_recovery_requires_verified_fence(self) -> None:
        original_write = self.adapter.write_publication

        def interrupted_write(publication: dict) -> dict:
            original_write(publication)
            raise RuntimeError("synthetic interruption after remote commit")

        self.adapter.write_publication = interrupted_write  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            self._governed_publish()
        effect = self.memory_store.list_effect_operations()[0]
        self.assertEqual("in_flight", effect["status"])
        before = list(self.collection.events)
        restricted = config.resolve_network_mode("restricted_local")
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish(current_on=False, network_resolution=restricted,
                                   shared_publication_enabled=False)
        self.assertEqual(before, self.collection.events)
        wrong = self._fence(effect)
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish(current_on=False, network_resolution=restricted,
                                   shared_publication_enabled=False, claim_fence=wrong)
        self.assertEqual(before, self.collection.events)
        self.terminated_claimants.add(("native-test", 4142, "2026-09-26T09:00:00Z"))
        self.assertEqual("acknowledged", self._governed_publish(
            current_on=False, network_resolution=restricted,
            shared_publication_enabled=False, claim_fence=self._fence(effect),
        )["status"])
        self.assertEqual("confirmed", self.memory_store.get_effect_operation(
            effect["operation_id"])["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_governed_complete_absence_allows_one_same_id_retry(self) -> None:
        self.adapter.lose_before_commit = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        effect = [row for row in self.memory_store.list_effect_operations()
                  if row["kind"] == "procedure_publication"][0]
        with self.assertRaises(procedures.ProcedureIneligibleError):
            self._governed_publish(current_on=False)
        pending = self.memory_store.get_effect_operation(effect["operation_id"])
        self.assertEqual("pending", pending["status"])
        self.assertEqual(1, self.adapter.publication_writes)
        recovered = self._governed_publish()
        self.assertEqual("acknowledged", recovered["status"])
        retained = self.memory_store.get_effect_operation(effect["operation_id"])
        self.assertEqual(effect["operation_id"], retained["operation_id"])
        self.assertEqual(retained["payload_record"]["document"],
                         self.collection.documents[recovered["publication_id"]])
        self.assertEqual(2, self.adapter.publication_writes)

    def test_governed_incomplete_readback_keeps_pending_off_without_retry(self) -> None:
        self.adapter.lose_before_commit = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        effect = [row for row in self.memory_store.list_effect_operations()
                  if row["kind"] == "procedure_publication"][0]
        self.adapter.fail_exact_read = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish(
                current_on=False,
                network_resolution=config.resolve_network_mode("restricted_local"),
                shared_publication_enabled=False,
            )
        retained = self.memory_store.get_effect_operation(effect["operation_id"])
        self.assertEqual("uncertain", retained["status"])
        self.assertEqual(effect["claim_generation"], retained["claim_generation"])
        self.assertEqual({"shared_publication": True}, retained["configuration"])
        self.assertEqual("pending_off", self.memory_store.external_effect_off_state(
            effect["operation_id"], current_config={"shared_publication": False}))
        self.assertEqual(1, self.adapter.publication_writes)

    def test_governed_uncertainty_requires_exact_readback_on_legacy_entry_points(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        publication = self.memory_store.list_procedure_publications()[0]
        with self.assertRaises(procedures.ProcedureError):
            self._publish()
        recovered = self.service.reconcile_publication(publication["publication_id"], self.adapter)
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual("confirmed", [row for row in self.memory_store.list_effect_operations()
                                       if row["kind"] == "procedure_publication"][0]["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_governed_lost_ack_then_withdrawal_preserves_original_uncertainty(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        publication = self.memory_store.list_procedure_publications()[0]
        self.service.withdraw_partition(
            logical_id=self.procedure["logical_id"], partition=self.partition,
            issuer="ROOT", adapter=self.adapter,
        )
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.reconcile_publication(publication["publication_id"], self.adapter)
        self.assertEqual("uncertain", [row for row in self.memory_store.list_effect_operations()
                                       if row["kind"] == "procedure_publication"][0]["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_governed_lost_ack_then_revocation_preserves_original_uncertainty(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        publication = self.memory_store.list_procedure_publications()[0]
        self.service.revoke(
            procedure=self.procedure, issuer="ROOT", reason="superseded",
            adapter=self.adapter,
        )
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.reconcile_publication(publication["publication_id"], self.adapter)
        self.assertEqual("uncertain", [row for row in self.memory_store.list_effect_operations()
                                       if row["kind"] == "procedure_publication"][0]["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_governed_recovery_rejects_changed_atlas_scope(self) -> None:
        self.adapter.lose_before_commit = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        with self.assertRaises(procedures.ProcedureError):
            self.service.publish(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation,
                adapter=self.adapter, captured_config={"shared_publication": True},
                current_config={"shared_publication": True},
                atlas_scope={"database": "other_database", "collection": "trusted",
                             "index": "vector_index", "namespace": self.partition["namespace"],
                             "recipients": self.partition["recipients"]},
            )
        self.assertEqual(1, self.adapter.publication_writes)
        self.assertEqual(1, len([row for row in self.memory_store.list_effect_operations()
                                 if row["kind"] == "procedure_publication"]))

    def test_governed_direct_recovery_rejects_wrong_adapter_location(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        publication = self.memory_store.list_procedure_publications()[0]
        wrong_adapter = _FaultAdapter(collection=self.collection)
        wrong_adapter.location = {"database": "other_database", "collection": "trusted",
                                  "index": "vector_index"}
        with self.assertRaises(procedures.ProcedureError):
            self.service.reconcile_publication(publication["publication_id"], wrong_adapter)
        self.assertEqual("uncertain", [row for row in self.memory_store.list_effect_operations()
                                       if row["kind"] == "procedure_publication"][0]["status"])
        self.service.reconcile_publication(publication["publication_id"], self.adapter)
        with self.assertRaises(procedures.ProcedureError):
            self.service.reconcile_publication(publication["publication_id"], wrong_adapter)

    def test_governed_off_rejects_new_effect_before_remote_publication(self) -> None:
        with self.assertRaises(procedures.ProcedureIneligibleError):
            self._governed_publish(current_on=False)
        self.assertEqual([], [row for row in self.memory_store.list_effect_operations()
                              if row["kind"] == "procedure_publication"])
        self.assertEqual([], self.memory_store.list_procedure_publications())
        self.assertEqual(0, self.adapter.publication_writes)
        self.assertEqual("acknowledged", self._governed_publish()["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_acknowledged_governed_replay_rejects_wrong_scope_config_and_adapter(self) -> None:
        self.assertEqual("acknowledged", self._governed_publish()["status"])
        self.assertEqual("acknowledged", self._governed_publish()["status"])
        for overrides in (
            {"captured_config": {"shared_publication": False}},
            {"atlas_scope": {"database": "other", "collection": "trusted",
                             "index": "vector_index", "namespace": self.partition["namespace"],
                             "recipients": self.partition["recipients"]}},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(procedures.ProcedureError):
                self.service.publish(
                    procedure=self.procedure, approval=self.approval,
                    representation=self.representation, designation=self.designation,
                    adapter=self.adapter, claimant=self._claimant(),
                    captured_config=overrides.get("captured_config", {"shared_publication": True}),
                    current_config={"shared_publication": True},
                    atlas_scope=overrides.get("atlas_scope", self._atlas_scope()),
                )
        wrong = _FaultAdapter(collection=self.collection)
        wrong.location = {**wrong.location, "database": "other"}
        with self.assertRaises(procedures.ProcedureError):
            self.service.publish(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation,
                adapter=wrong, claimant=self._claimant(),
                captured_config={"shared_publication": True},
                current_config={"shared_publication": True}, atlas_scope=self._atlas_scope())
        self.assertEqual(1, self.adapter.publication_writes)

    def _atlas_scope(self) -> dict:
        return {"database": "procedures_test", "collection": "trusted",
                "index": "vector_index", "namespace": self.partition["namespace"],
                "recipients": self.partition["recipients"]}

    def test_legacy_acknowledgement_cannot_be_adopted_as_governed(self) -> None:
        self.assertEqual("acknowledged", self._publish()["status"])
        with self.assertRaises(procedures.ProcedureError):
            self._governed_publish()
        self.assertEqual([], self.memory_store.list_effect_operations())

    def test_acknowledged_governed_replay_still_requires_content_bound_claimant(self) -> None:
        self._governed_publish()
        invalid = self._claimant()
        invalid["pid"] = 0
        invalid["content_hash"] = contracts.content_hash(invalid)
        with self.assertRaises(procedures.ProcedureError):
            self.service.publish(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation,
                adapter=self.adapter, claimant=invalid,
                captured_config={"shared_publication": True},
                current_config={"shared_publication": True}, atlas_scope=self._atlas_scope())

    def test_partial_publication_then_safety_transition_does_not_confirm_original(self) -> None:
        self.collection.fail_next_read_for = atlas.publication_state_document_id(
            contracts.make_procedure_publication(
                procedure=self.procedure, approval=self.approval,
                representation=self.representation, designation=self.designation)["publication_id"])
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        publication = self.memory_store.list_procedure_publications()[0]
        self.service.withdraw_partition(logical_id=self.procedure["logical_id"],
            partition=self.partition, issuer="ROOT", adapter=self.adapter)
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.reconcile_publication(publication["publication_id"], self.adapter)
        effect = self.memory_store.list_effect_operations()[0]
        self.assertNotEqual("confirmed", effect["status"])
        self.assertEqual("withdrawn", self.collection.documents[
            atlas.publication_state_document_id(publication["publication_id"])]["state"])

    def test_confirmed_publication_retains_original_proof_after_safety_revocation(self) -> None:
        publication = self._governed_publish()
        effect = self.memory_store.list_effect_operations()[0]
        original_proof = effect["acknowledgement"]
        safety = self.service.revoke(procedure=self.procedure, issuer="ROOT",
            reason="unsafe", adapter=self.adapter)
        self.assertTrue(safety["managed_complete"])
        self.assertEqual("confirmed", self.memory_store.get_effect_operation(
            effect["operation_id"])["status"])
        self.assertEqual(original_proof, self.memory_store.get_effect_operation(
            effect["operation_id"])["acknowledgement"])
        self.assertEqual("revoked", self.collection.documents[
            atlas.publication_state_document_id(publication["publication_id"])]["state"])

    def test_abandoned_in_flight_claim_needs_provider_fence_before_absent_retry(self) -> None:
        def interrupted_write(_publication: dict) -> dict:
            raise RuntimeError("synthetic process interruption after durable claim")

        self.adapter.write_publication = interrupted_write  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            self._governed_publish()
        effect = [row for row in self.memory_store.list_effect_operations()
                  if row["kind"] == "procedure_publication"][0]
        self.assertEqual("in_flight", effect["status"])
        self.assertTrue(self.adapter.publication_effect_absent(effect["source_id"]))
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish()
        self.assertEqual("in_flight", self.memory_store.get_effect_operation(
            effect["operation_id"])["status"])

    def test_reopened_claim_needs_exact_verified_fence_before_new_generation(self) -> None:
        original_write = self.adapter.write_publication

        def interrupted_write(_publication: dict) -> dict:
            raise RuntimeError("synthetic process interruption after claim")

        self.adapter.write_publication = interrupted_write  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            self._governed_publish()
        first = self.memory_store.list_effect_operations()[0]
        self.memory_store.close()
        self.terminated_claimants.add(("native-test", 4142, "2026-09-26T09:00:00Z"))
        self.memory_store = store.MemoryStore(Path(self.temporary.name) / "memory.sqlite3",
            external_claim_fence_verifier=self._verified_fence)
        self.memory_store.initialize()
        self.service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"})
        self.adapter.write_publication = original_write  # type: ignore[method-assign]
        wrong = self._fence(first)
        wrong["claim_id"] = "wrong-claim"
        wrong["content_hash"] = contracts.content_hash(wrong)
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish(claim_fence=wrong)
        self.assertEqual("in_flight", self.memory_store.get_effect_operation(
            first["operation_id"])["status"])
        with self.assertRaises(procedures.ProcedureIneligibleError):
            self._governed_publish(current_on=False, claim_fence=self._fence(first))
        pending = self.memory_store.get_effect_operation(first["operation_id"])
        self.assertEqual("pending", pending["status"])
        self.assertEqual(1, pending["claim_generation"])
        recovered = self._governed_publish(invocation="native-recovery")
        self.assertEqual("acknowledged", recovered["status"])
        confirmed = self.memory_store.get_effect_operation(first["operation_id"])
        self.assertEqual(2, confirmed["claim_generation"])
        self.assertNotEqual(first["active_claim_id"], confirmed["active_claim_id"])
        self.assertEqual(1, self.adapter.publication_writes)
        with self.assertRaises(store.OperationConflictError):
            self.memory_store.confirm_effect_operation(
                first["operation_id"], confirmed["acknowledgement"],
                claim_id=first["active_claim_id"])

    def test_reopened_claim_without_constructor_verifier_stays_in_flight(self) -> None:
        def interrupted_write(_publication: dict) -> dict:
            raise RuntimeError("synthetic process interruption after claim")

        self.adapter.write_publication = interrupted_write  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            self._governed_publish()
        first = self.memory_store.list_effect_operations()[0]
        self.memory_store.close()
        self.memory_store = store.MemoryStore(Path(self.temporary.name) / "memory.sqlite3")
        self.memory_store.initialize()
        self.service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"})
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._governed_publish(claim_fence=self._fence(first))
        self.assertEqual("in_flight", self.memory_store.get_effect_operation(
            first["operation_id"])["status"])

    def test_interruption_between_governed_intent_and_local_publication_recovers(self) -> None:
        original_create = self.memory_store.create_procedure_publication

        def interrupted_create(_publication: dict) -> tuple[dict, bool]:
            raise RuntimeError("synthetic interruption before local publication intent")

        self.memory_store.create_procedure_publication = interrupted_create  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            self._governed_publish()
        self.assertEqual("pending", self.memory_store.list_effect_operations()[0]["status"])
        self.assertEqual([], self.memory_store.list_procedure_publications())
        self.memory_store.create_procedure_publication = original_create  # type: ignore[method-assign]
        self.assertEqual("acknowledged", self._governed_publish()["status"])

    def _publication_and_operation(self) -> tuple[dict, dict]:
        publication = self.memory_store.list_procedure_publications()[0]
        operations = [
            operation
            for operation in self.memory_store.list_procedure_remote_operations(
                payload_id=publication["publication_id"]
            )
            if operation["kind"] == "publication"
        ]
        self.assertEqual(1, len(operations))
        return publication, operations[0]

    def _operation(self, *, kind: str, payload_id: str) -> dict:
        operations = [
            operation
            for operation in self.memory_store.list_procedure_remote_operations(payload_id=payload_id)
            if operation["kind"] == kind
        ]
        self.assertEqual(1, len(operations))
        return operations[0]

    def _successor_designation(self) -> dict:
        successor = contracts.make_procedure_revision(
            logical_name=self.procedure["logical_name"],
            origin="curated",
            origin_scope=self.scope,
            body="Inspect the lock and its owner before recovery.",
            references=self.procedure["behavior"]["references"],
            predicates=self.procedure["behavior"]["predicates"],
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/2"},
            created_at="2026-09-21T00:00:03Z",
        )
        approval = contracts.make_procedure_approval(
            approval_id="approval-successor",
            procedure=successor,
            issuer="ROOT",
            recipients=[self.scope],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:04Z",
        )
        return self.service.designate(
            procedure=successor,
            approval=approval,
            partition=self.partition,
            issuer="ROOT",
        )

    def test_success_uses_stable_remote_ids_and_exact_readback(self) -> None:
        publication = self._publish()
        operation = self._publication_and_operation()[1]
        publication_id = publication["publication_id"]
        state_id = atlas.publication_state_document_id(publication_id)
        self.assertEqual(publication_id, self.collection.documents[publication_id]["_id"])
        self.assertEqual(state_id, self.collection.documents[state_id]["_id"])
        self.assertEqual(
            contracts.make_procedure_remote_operation(
                kind="publication",
                logical_id=publication["logical_id"],
                revision_id=publication["revision_id"],
                payload_id=publication_id,
                payload_digest=publication["payload_digest"],
                partition_id=publication["partition_id"],
            )["operation_id"],
            operation["operation_id"],
        )
        self.assertEqual("acknowledged", publication["status"])
        self.assertEqual("acknowledged", operation["status"])
        self.assertEqual(
            {
                "remote_id": publication_id,
                "remote_digest": self.collection.documents[publication_id]["content_hash"],
            },
            operation["remote_receipt"],
        )
        for identifier in (publication_id, state_id):
            insert_at = self.collection.events.index(("insert", identifier))
            self.assertIn(("read", identifier), self.collection.events[insert_at + 1 :])

    def test_worker_bound_search_text_is_rejected_before_any_publication_effect(self) -> None:
        representation = contracts.make_procedure_representation(
            procedure=self.procedure,
            model=self.representation["model"],
            dimensions=3,
            metric="cosine",
            sanitizer_version=self.representation["sanitizer_version"],
            search_text='lock recovery {"publish_authority":true}',
            vector=[0.1, 0.2, 0.3],
            created_at="2026-09-21T00:00:03Z",
        )
        self.service.record_representation(representation)
        before_events = list(self.collection.events)
        before_documents = dict(self.collection.documents)

        with self.assertRaisesRegex(procedures.ProcedureError, "privacy scan"):
            self.service.publish(
                procedure=self.procedure,
                approval=self.approval,
                representation=representation,
                designation=self.designation,
                adapter=self.adapter,
            )

        self.assertEqual([], self.memory_store.list_procedure_publications())
        self.assertEqual(
            [], [op for op in self.memory_store.list_procedure_remote_operations()
                 if op["kind"] == "publication"],
        )
        self.assertEqual(before_events, self.collection.events)
        self.assertEqual(before_documents, self.collection.documents)
        self.assertEqual(0, self.adapter.publication_writes)

    def test_worker_bound_body_is_rejected_before_any_publication_effect(self) -> None:
        procedure = contracts.make_procedure_revision(
            logical_name="unsafe-recovery",
            origin="curated",
            origin_scope=self.scope,
            body='Inspect lock {"publish_authority":true}',
            references=[],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
            source={"kind": "curated_authoring", "provenance_ref": "curation://unsafe/1"},
            created_at="2026-09-21T00:00:03Z",
        )
        approval = contracts.make_procedure_approval(
            approval_id="approval-unsafe",
            procedure=procedure,
            issuer="ROOT",
            recipients=[self.scope],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:04Z",
        )
        representation = contracts.make_procedure_representation(
            procedure=procedure,
            model="deterministic-test-embedding/v1",
            dimensions=3,
            metric="cosine",
            sanitizer_version="known-secret/v1",
            search_text="safe lock recovery",
            vector=[0.1, 0.2, 0.3],
            created_at="2026-09-21T00:00:05Z",
        )
        self.service.record_approved_revision(procedure, approval)
        self.service.record_representation(representation)
        designation = self.service.designate(
            procedure=procedure, approval=approval, partition=self.partition, issuer="ROOT"
        )
        before_events = list(self.collection.events)
        before_documents = dict(self.collection.documents)

        with self.assertRaisesRegex(procedures.ProcedureError, "privacy scan"):
            self.service.publish(
                procedure=procedure,
                approval=approval,
                representation=representation,
                designation=designation,
                adapter=self.adapter,
            )

        self.assertEqual([], self.memory_store.list_procedure_publications())
        self.assertEqual(
            [], [op for op in self.memory_store.list_procedure_remote_operations()
                 if op["kind"] == "publication"],
        )
        self.assertEqual(before_events, self.collection.events)
        self.assertEqual(before_documents, self.collection.documents)
        self.assertEqual(0, self.adapter.publication_writes)

    def test_whole_publication_guard_rejects_reference_secret_without_mutation_or_effect(self) -> None:
        guarded = procedures.TrustedProcedureService(
            self.memory_store,
            trusted_issuers={"ROOT"},
            privacy_policy=privacy.PrivacyPolicy(known_secrets=("guide://lock",)),
        )
        before_events = list(self.collection.events)
        before_documents = dict(self.collection.documents)

        with self.assertRaisesRegex(procedures.ProcedureError, "privacy scan"):
            guarded.publish(
                procedure=self.procedure,
                approval=self.approval,
                representation=self.representation,
                designation=self.designation,
                adapter=self.adapter,
            )

        self.assertEqual("guide://lock", self.procedure["behavior"]["references"][0]["id"])
        self.assertEqual([], self.memory_store.list_procedure_publications())
        self.assertEqual(
            [], [op for op in self.memory_store.list_procedure_remote_operations()
                 if op["kind"] == "publication"],
        )
        self.assertEqual(before_events, self.collection.events)
        self.assertEqual(before_documents, self.collection.documents)
        self.assertEqual(0, self.adapter.publication_writes)

    def test_lost_ack_after_commit_reconciles_without_a_second_write(self) -> None:
        self.adapter.lose_ack = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertIsNone(operation["remote_receipt"])

        recovered = self._publish()
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual(1, self.adapter.publication_writes)
        self.assertEqual(
            operation["operation_id"], self._publication_and_operation()[1]["operation_id"]
        )

    def test_collection_lost_insert_ack_uses_exact_id_readback(self) -> None:
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        publication_id = publication["publication_id"]
        state_id = atlas.publication_state_document_id(publication_id)
        self.collection.lose_insert_ack_for = publication_id

        completed = self._publish()
        self.assertEqual("acknowledged", completed["status"])
        self.assertEqual("acknowledged", self._publication_and_operation()[1]["status"])
        self.assertEqual(1, self.collection.events.count(("insert", publication_id)))
        self.assertEqual(1, self.collection.events.count(("insert", state_id)))
        insert_at = self.collection.events.index(("insert", publication_id))
        self.assertIn(("read", publication_id), self.collection.events[insert_at + 1 :])

    def test_lost_ack_without_exact_evidence_stays_ambiguous_and_does_not_resubmit(self) -> None:
        self.adapter.lose_before_commit = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        self.assertEqual("ambiguous", self._publication_and_operation()[0]["status"])
        self.assertEqual(
            operation["operation_id"], self._publication_and_operation()[1]["operation_id"]
        )
        self.assertEqual(1, self.adapter.publication_writes)
        self.assertNotIn(pending["publication_id"], self.collection.documents)

    def test_definite_pre_commit_rejection_is_blocked(self) -> None:
        self.adapter.fail_before_commit = True
        with self.assertRaises(procedures.ProcedureError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("blocked", pending["status"])
        self.assertEqual("blocked", operation["status"])
        self.assertIsNone(operation["remote_receipt"])
        self.assertNotIn(pending["publication_id"], self.collection.documents)
        with self.assertRaises(procedures.ProcedureIneligibleError):
            self._publish()
        self.assertEqual(1, self.adapter.publication_writes)

    def test_pre_write_exact_read_outage_is_blocked_without_submission(self) -> None:
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        self.collection.fail_next_read_for = publication["publication_id"]
        with self.assertRaises(procedures.ProcedureError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("blocked", pending["status"])
        self.assertEqual("blocked", operation["status"])
        self.assertNotIn(publication["publication_id"], self.collection.documents)
        self.assertEqual(1, self.adapter.publication_writes)

    def test_state_id_read_outage_after_publication_insert_is_ambiguous(self) -> None:
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        publication_id = publication["publication_id"]
        state_id = atlas.publication_state_document_id(publication_id)
        self.collection.fail_next_read_for = state_id

        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertIsNone(operation["remote_receipt"])
        self.assertIn(publication_id, self.collection.documents)
        self.assertNotIn(state_id, self.collection.documents)
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        self.assertEqual(1, self.adapter.publication_writes)

    def test_exact_read_error_after_write_stays_ambiguous_until_reconciled(self) -> None:
        self.adapter.fail_exact_read = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertIsNone(operation["remote_receipt"])
        self.assertEqual("acknowledged", self._publish()["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_missing_exact_read_after_write_stays_ambiguous_until_reconciled(self) -> None:
        self.adapter.miss_exact_read = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertEqual("acknowledged", self._publish()["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_missing_lifecycle_readback_after_write_stays_ambiguous(self) -> None:
        self.adapter.miss_state_read = True
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertEqual("acknowledged", self._publish()["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_collection_readback_outage_after_insert_is_ambiguous(self) -> None:
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        publication_id = publication["publication_id"]
        original_insert = self.collection.insert_one

        def insert_then_fail_read(document: dict) -> object:
            result = original_insert(document)
            if document["_id"] == publication_id:
                self.collection.fail_next_read_for = publication_id
            return result

        self.collection.insert_one = insert_then_fail_read  # type: ignore[method-assign]
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertEqual(1, self.adapter.publication_writes)
        self.assertEqual(publication_id, pending["publication_id"])
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        self.assertEqual("ambiguous", self._publication_and_operation()[0]["status"])
        self.assertEqual(1, self.adapter.publication_writes)

    def test_designation_post_write_exact_current_outage_is_ambiguous(self) -> None:
        successor = self._successor_designation()
        current_id = atlas.current_document_id(successor["logical_id"], successor["partition_id"])
        original_write = self.adapter.write_designation

        def write_then_lose_read(designation: dict) -> dict:
            remote = original_write(designation)
            self.collection.fail_next_read_for = current_id
            return remote

        self.adapter.write_designation = write_then_lose_read  # type: ignore[method-assign]
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.publish_designation(successor, self.adapter)
        operation = self._operation(kind="designation", payload_id=successor["designation_id"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertEqual(successor["designation_id"], self.collection.documents[current_id]["designation_id"])
        self.assertEqual(1, self.collection.events.count(("replace", current_id)))

        self.service.publish_designation(successor, self.adapter)
        recovered = self._operation(kind="designation", payload_id=successor["designation_id"])
        self.assertEqual(operation["operation_id"], recovered["operation_id"])
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual(1, self.collection.events.count(("replace", current_id)))

    def test_designation_pre_write_exact_current_outage_is_blocked(self) -> None:
        successor = self._successor_designation()
        current_id = atlas.current_document_id(successor["logical_id"], successor["partition_id"])
        self.collection.fail_next_read_for = current_id

        with self.assertRaises(procedures.ProcedureError):
            self.service.publish_designation(successor, self.adapter)
        operation = self._operation(kind="designation", payload_id=successor["designation_id"])
        self.assertEqual("blocked", operation["status"])
        self.assertEqual(0, self.collection.events.count(("replace", current_id)))
        self.assertEqual(self.designation["designation_id"], self.collection.documents[current_id]["designation_id"])

    def test_withdrawal_post_replace_read_outage_is_ambiguous(self) -> None:
        current_id = atlas.current_document_id(self.designation["logical_id"], self.designation["partition_id"])
        self.collection.fail_read_after_replace_for = current_id

        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.withdraw_partition(
                logical_id=self.designation["logical_id"],
                partition=self.partition,
                issuer="ROOT",
                adapter=self.adapter,
            )
        withdrawal = self.memory_store.get_current_procedure_designation(
            self.designation["logical_id"], self.partition
        )["record"]
        operation = self._operation(kind="withdrawal", payload_id=withdrawal["withdrawal_id"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertEqual("withdrawn", self.collection.documents[current_id]["state"])
        self.assertEqual(1, self.collection.events.count(("replace", current_id)))

        self.service.reconcile_withdrawal(
            logical_id=self.designation["logical_id"],
            partition=self.partition,
            adapter=self.adapter,
        )
        recovered = self._operation(kind="withdrawal", payload_id=withdrawal["withdrawal_id"])
        self.assertEqual(operation["operation_id"], recovered["operation_id"])
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual(1, self.collection.events.count(("replace", current_id)))

    def test_collection_local_insert_typeerror_is_definite_and_creates_no_document(self) -> None:
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        def reject_before_submission(document: dict, required_driver_option: object) -> object:
            raise AssertionError("collection body must not run")

        # Python rejects the collection call before entering its body.
        self.collection.insert_one = reject_before_submission  # type: ignore[method-assign]

        with self.assertRaises(procedures.ProcedureError) as caught:
            self._publish()
        self.assertIs(type(caught.exception), procedures.ProcedureError)
        pending, operation = self._publication_and_operation()
        self.assertEqual("blocked", pending["status"])
        self.assertEqual("blocked", operation["status"])
        self.assertNotIn(publication["publication_id"], self.collection.documents)
        self.assertNotIn(("insert", publication["publication_id"]), self.collection.events)
        self.assertIsNone(operation["remote_receipt"])

    def test_collection_typeerror_after_insert_and_read_outage_stays_ambiguous(self) -> None:
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        publication_id = publication["publication_id"]
        original_insert = self.collection.insert_one

        def insert_then_fail_response(document: dict) -> object:
            result = original_insert(document)
            if document["_id"] == publication_id:
                self.collection.fail_next_read_for = publication_id
                raise TypeError("synthetic post-submit response failure")
            return result

        self.collection.insert_one = insert_then_fail_response  # type: ignore[method-assign]
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self._publish()
        pending, operation = self._publication_and_operation()
        self.assertEqual("ambiguous", pending["status"])
        self.assertEqual("ambiguous", operation["status"])
        self.assertIn(publication_id, self.collection.documents)

    def test_revocation_ambiguity_survives_later_exact_read_outage(self) -> None:
        original_write = self.adapter.write_revocation
        lose_ack = True

        def write_then_lose_ack(revocation: dict) -> dict:
            nonlocal lose_ack
            remote = original_write(revocation)
            if lose_ack:
                lose_ack = False
                raise atlas.AtlasProcedureAmbiguityError("synthetic revocation acknowledgement loss")
            return remote

        self.adapter.write_revocation = write_then_lose_ack  # type: ignore[method-assign]
        first = self.service.revoke(
            procedure=self.procedure, issuer="ROOT", reason="unsafe", adapter=self.adapter
        )
        revocation = first["revocation"]
        operation = self._operation(kind="revocation", payload_id=revocation["revocation_id"])
        self.assertEqual("ambiguous", operation["status"])

        remote_id = atlas.revocation_document_id(revocation["revision_id"])
        self.collection.fail_next_read_for = remote_id
        second = self.service.revoke(
            procedure=self.procedure, issuer="ROOT", reason="unsafe", adapter=self.adapter
        )
        retained = self._operation(kind="revocation", payload_id=revocation["revocation_id"])
        self.assertFalse(second["managed_complete"])
        self.assertEqual(operation["operation_id"], retained["operation_id"])
        self.assertEqual("ambiguous", retained["status"])
        self.assertEqual(1, self.collection.events.count(("insert", remote_id)))

        recovered = self.service.revoke(
            procedure=self.procedure, issuer="ROOT", reason="unsafe", adapter=self.adapter
        )
        acknowledged = self._operation(kind="revocation", payload_id=revocation["revocation_id"])
        self.assertTrue(recovered["managed_complete"])
        self.assertEqual(operation["operation_id"], acknowledged["operation_id"])
        self.assertEqual("acknowledged", acknowledged["status"])
        self.assertEqual(1, self.collection.events.count(("insert", remote_id)))

    def test_publication_state_ambiguity_survives_read_and_definite_attempt_faults(self) -> None:
        publication = self._publish()
        original_write = self.adapter.write_publication_state
        attempts = 0

        def fail_state_attempt(pending: dict, *, state: str) -> dict:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise atlas.AtlasProcedureAmbiguityError("synthetic state acknowledgement loss")
            if attempts == 2:
                raise atlas.AtlasProcedureError("synthetic definite state attempt failure")
            return original_write(pending, state=state)

        self.adapter.write_publication_state = fail_state_attempt  # type: ignore[method-assign]
        first = self.service.withdraw_partition(
            logical_id=self.designation["logical_id"],
            partition=self.partition,
            issuer="ROOT",
            adapter=self.adapter,
        )
        operation = self._operation(kind="withdraw_publication", payload_id=publication["publication_id"])
        self.assertFalse(first["managed_complete"])
        self.assertEqual("ambiguous", operation["status"])

        self.adapter.fail_exact_read = True
        self.service.reconcile_withdrawal(
            logical_id=self.designation["logical_id"], partition=self.partition, adapter=self.adapter
        )
        self.assertEqual(
            "ambiguous",
            self._operation(kind="withdraw_publication", payload_id=publication["publication_id"])["status"],
        )
        self.service.reconcile_withdrawal(
            logical_id=self.designation["logical_id"], partition=self.partition, adapter=self.adapter
        )
        retained = self._operation(kind="withdraw_publication", payload_id=publication["publication_id"])
        self.assertEqual(operation["operation_id"], retained["operation_id"])
        self.assertEqual("ambiguous", retained["status"])
        self.assertEqual(2, attempts)

        original_write(publication, state="withdrawn")
        completed = self.service.reconcile_withdrawal(
            logical_id=self.designation["logical_id"], partition=self.partition, adapter=self.adapter
        )
        acknowledged = self._operation(kind="withdraw_publication", payload_id=publication["publication_id"])
        self.assertTrue(completed["managed_complete"])
        self.assertEqual(operation["operation_id"], acknowledged["operation_id"])
        self.assertEqual("acknowledged", acknowledged["status"])
        self.assertEqual(2, attempts)


if __name__ == "__main__":
    unittest.main()
