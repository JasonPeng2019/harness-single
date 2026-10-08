from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import atlas, config, contracts, privacy, procedures, store


class _WriteResult:
    def __init__(self, matched_count: int) -> None:
        self.matched_count = matched_count


class _Collection:
    def __init__(self) -> None:
        self.documents: dict[str, dict] = {}
        self.exact_reads: list[str] = []

    def find_one(self, filter: dict) -> dict | None:
        identifier = filter.get("_id")
        if isinstance(identifier, str):
            self.exact_reads.append(identifier)
            document = self.documents.get(identifier)
            return dict(document) if document is not None else None
        return None

    def insert_one(self, document: dict) -> object:
        identifier = document["_id"]
        if identifier in self.documents:
            raise RuntimeError("duplicate key")
        self.documents[identifier] = dict(document)
        return object()

    def replace_one(self, filter: dict, replacement: dict, upsert: bool = False) -> _WriteResult:
        identifier = filter.get("_id")
        current = self.documents.get(identifier)
        if current is None:
            return _WriteResult(0)
        if any(current.get(key) != value for key, value in filter.items()):
            return _WriteResult(0)
        self.documents[identifier] = dict(replacement)
        return _WriteResult(1)


class _Document:
    def __init__(self, publication_id: str) -> None:
        self.id = publication_id
        self.metadata = {"_id": publication_id}


class _VectorStore:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.publication_ids: list[str] = []

    def similarity_search_with_score(self, query: str, **kwargs: object) -> list[tuple[_Document, float]]:
        self.calls.append({"query": query, **kwargs})
        return [(_Document(publication_id), 0.99) for publication_id in self.publication_ids]


class _LostAcknowledgementAdapter(atlas.AtlasProcedureAdapter):
    """The remote write committed but its response was lost to the caller."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.lose_next_publication_ack = True

    def write_publication(self, publication: dict) -> dict:
        document = super().write_publication(publication)
        if self.lose_next_publication_ack:
            self.lose_next_publication_ack = False
            raise atlas.AtlasProcedureAmbiguityError("synthetic lost acknowledgement")
        return document


class _LostWithdrawalAcknowledgementAdapter(atlas.AtlasProcedureAdapter):
    """The remote withdrawal committed but its response was lost to the caller."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.lose_next_withdrawal_ack = True

    def write_withdrawal(self, withdrawal: dict) -> dict:
        document = super().write_withdrawal(withdrawal)
        if self.lose_next_withdrawal_ack:
            self.lose_next_withdrawal_ack = False
            raise atlas.AtlasProcedureAmbiguityError("synthetic lost acknowledgement")
        return document


class _BlockedThenSuccessPublicationStateAdapter(atlas.AtlasProcedureAdapter):
    """One stable lifecycle operation fails once, then can be reconciled."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.fail_next_state_write = True
        self.state_write_calls = 0

    def write_publication_state(self, publication: dict, *, state: str) -> dict:
        self.state_write_calls += 1
        if self.fail_next_state_write:
            self.fail_next_state_write = False
            raise atlas.AtlasProcedureError("synthetic publication-state outage")
        return super().write_publication_state(publication, state=state)


class _LostPublicationStateAcknowledgementAdapter(atlas.AtlasProcedureAdapter):
    """The remote lifecycle state committed, but the response was lost."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.lose_next_state_ack = True
        self.state_write_calls = 0

    def write_publication_state(self, publication: dict, *, state: str) -> dict:
        self.state_write_calls += 1
        document = super().write_publication_state(publication, state=state)
        if self.lose_next_state_ack:
            self.lose_next_state_ack = False
            raise atlas.AtlasProcedureAmbiguityError(
                "synthetic publication-state acknowledgement loss"
            )
        return document


class AtlasProcedureBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.memory_store = store.MemoryStore(Path(self.temporary.name) / "memory.sqlite3")
        self.memory_store.initialize()
        self.scope = {
            "application": "harness",
            "project": "product-a",
            "namespace": "atlas-test",
            "owner": "root-agent",
        }
        self.partition = {
            "scope": "project",
            "application": "harness",
            "project": "product-a",
            "namespace": "atlas-test",
            "recipients": [dict(self.scope)],
        }
        self.procedure = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.scope,
            body="Inspect the local lock before network recovery.",
            references=[{"id": "guide://lock", "content": "Preserve the lock invariant."}],
            predicates={
                "applicability": {"all": [{"field": "language", "operator": "equals", "value": "python"}]},
                "conflicts": {},
                "capabilities": {},
                "routes": {"all": [{"field": "route", "operator": "equals", "value": "ordinary"}]},
            },
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/1"},
            created_at="2026-09-21T00:00:00Z",
        )
        self.approval = contracts.make_procedure_approval(
            approval_id="approval-1",
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
            search_text="parser lock recovery",
            vector=[0.1, 0.2, 0.3],
            created_at="2026-09-21T00:00:02Z",
        )
        self.query_representation = {
            "model": self.representation["model"],
            "dimensions": self.representation["dimensions"],
            "metric": self.representation["metric"],
            "sanitizer_version": self.representation["sanitizer_version"],
        }
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
        self.vector_store = _VectorStore()
        self.adapter = atlas.AtlasProcedureAdapter(
            collection=self.collection, vector_store=self.vector_store
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def test_service_entry_blocks_direct_lookup_and_designation_while_restricted(self) -> None:
        restricted = config.resolve_network_mode("restricted_local")
        before_reads = list(self.collection.exact_reads)
        before_searches = list(self.vector_store.calls)
        self.assertEqual([], self.service.resolve_atlas(
            "parser lock recovery", receiver=self.scope, facts={"language": "python"},
            route="ordinary", adapter=self.adapter,
            representation=self.query_representation, network_resolution=restricted,
        ))
        self.assertEqual([], self.service.resolve_atlas(
            "parser lock recovery", receiver=self.scope, facts={"language": "python"},
            route="ordinary", adapter=self.adapter,
            representation=self.query_representation,
            atlas_shared_retrieval_enabled=False,
        ))
        with self.assertRaises(procedures.ProcedureNetworkDeniedError):
            self.service.publish_designation(
                self.designation, self.adapter, network_resolution=restricted)
        self.assertEqual(before_reads, self.collection.exact_reads)
        self.assertEqual(before_searches, self.vector_store.calls)

    def test_search_only_discovers_then_exact_read_revalidates_before_delivery(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        self.vector_store.publication_ids = [publication["publication_id"]]

        delivered = self.service.resolve_atlas(
            "parser lock recovery",
            receiver=self.scope,
            facts={"language": "python"},
            route="ordinary",
            adapter=self.adapter,
            representation=self.query_representation,
        )
        self.assertEqual([publication["publication_id"]], [item["publication_id"] for item in delivered])
        self.assertTrue(self.vector_store.calls)
        self.assertIn(publication["publication_id"], self.collection.exact_reads)
        self.assertEqual(
            [],
            self.service.resolve_atlas(
                "parser lock recovery",
                receiver={**self.scope, "owner": "other-agent"},
                facts={"language": "python"},
                route="ordinary",
                adapter=self.adapter,
                representation=self.query_representation,
            ),
        )
        for wrong_scope in (
            {**self.scope, "application": "other-application"},
            {**self.scope, "project": "other-project"},
            {**self.scope, "namespace": "other-namespace"},
        ):
            self.assertEqual(
                [],
                self.service.resolve_atlas(
                    "parser lock recovery",
                    receiver=wrong_scope,
                    facts={"language": "python"},
                    route="ordinary",
                    adapter=self.adapter,
                    representation=self.query_representation,
                ),
            )
        self.assertEqual(
            [],
            self.service.resolve_atlas(
                "parser lock recovery",
                receiver=self.scope,
                facts={"language": "python"},
                route="ordinary",
                adapter=self.adapter,
                representation={
                    "model": "different-embedding/v2",
                    "dimensions": 3,
                    "metric": "cosine",
                    "sanitizer_version": "known-secret/v1",
                },
            ),
        )
        untrusted_receiver = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"someone-else"}
        )
        self.assertEqual(
            [],
            untrusted_receiver.resolve_atlas(
                "parser lock recovery",
                receiver=self.scope,
                facts={"language": "python"},
                route="ordinary",
                adapter=self.adapter,
                representation=self.query_representation,
            ),
        )

        result = self.service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="synthetic revocation",
            adapter=self.adapter,
        )
        self.assertTrue(result["managed_complete"])
        self.assertEqual(
            [],
            self.service.resolve_atlas(
                "parser lock recovery",
                receiver=self.scope,
                facts={"language": "python"},
                route="ordinary",
                adapter=self.adapter,
                representation=self.query_representation,
            ),
        )

    def test_delivery_requires_the_exact_query_representation_identity(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        self.vector_store.publication_ids = [publication["publication_id"]]

        calls_before = len(self.vector_store.calls)
        self.assertEqual(
            [],
            self.service.resolve_atlas(
                "parser lock recovery",
                receiver=self.scope,
                facts={"language": "python"},
                route="ordinary",
                adapter=self.adapter,
            ),
        )
        self.assertEqual(calls_before, len(self.vector_store.calls))

        for field, incompatible_value in {
            "model": "different-embedding/v2",
            "dimensions": 4,
            "metric": "dot_product",
            "sanitizer_version": "known-secret/v2",
        }.items():
            with self.subTest(field=field):
                incompatible = dict(self.query_representation)
                incompatible[field] = incompatible_value
                self.assertEqual(
                    [],
                    self.service.resolve_atlas(
                        "parser lock recovery",
                        receiver=self.scope,
                        facts={"language": "python"},
                        route="ordinary",
                        adapter=self.adapter,
                        representation=incompatible,
                    ),
                )

    def test_resolution_delivers_one_most_specific_partition_per_logical_procedure(self) -> None:
        """A shared replica cannot add a second delivery or outrank project scope."""

        shared_partition = {
            "scope": "shared",
            "application": self.scope["application"],
            "project": "shared-library",
            "namespace": self.scope["namespace"],
            "recipients": [self.scope],
        }
        shared_designation = self.service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=shared_partition,
            issuer="ROOT",
        )
        self.service.publish_designation(self.designation, self.adapter)
        self.service.publish_designation(shared_designation, self.adapter)
        project_publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        shared_publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=shared_designation,
            adapter=self.adapter,
        )
        # Search ordering is untrusted discovery data; put shared first.
        self.vector_store.publication_ids = [
            shared_publication["publication_id"],
            project_publication["publication_id"],
        ]

        delivered = self.service.resolve_atlas(
            "parser lock recovery",
            receiver=self.scope,
            facts={"language": "python"},
            route="ordinary",
            adapter=self.adapter,
            representation=self.query_representation,
        )
        self.assertEqual([project_publication["publication_id"]], [
            item["publication_id"] for item in delivered
        ])

    def test_ambiguous_publication_is_exact_read_reconciled_without_a_new_identity(self) -> None:
        adapter = _LostAcknowledgementAdapter(
            collection=self.collection, vector_store=self.vector_store
        )
        self.service.publish_designation(self.designation, adapter)
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.publish(
                procedure=self.procedure,
                approval=self.approval,
                representation=self.representation,
                designation=self.designation,
                adapter=adapter,
            )
        publication = self.memory_store.list_procedure_publications()[0]
        self.assertEqual("ambiguous", publication["status"])
        operation = self.memory_store.list_procedure_remote_operations(
            payload_id=publication["publication_id"]
        )[0]
        self.assertEqual("ambiguous", operation["status"])

        reconciled = self.service.reconcile_publication(publication["publication_id"], adapter)
        self.assertEqual("acknowledged", reconciled["status"])
        operations = self.memory_store.list_procedure_remote_operations(
            payload_id=publication["publication_id"]
        )
        self.assertEqual("acknowledged", operations[0]["status"])
        self.assertEqual(1, sum(
            document.get("document_kind") == "trusted_procedure_publication"
            for document in self.collection.documents.values()
        ))

    def test_remote_committed_state_exact_reads_to_acknowledged_after_restart(self) -> None:
        """A crash after durable remote commit is not permission to resend it."""

        shared_partition = {
            "scope": "shared",
            "application": self.scope["application"],
            "project": "shared-library",
            "namespace": self.scope["namespace"],
            "recipients": [self.scope],
        }
        shared_designation = self.service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=shared_partition,
            issuer="ROOT",
        )
        remote_current = self.adapter.write_designation(shared_designation)
        designation_operation = contracts.make_procedure_remote_operation(
            kind="designation",
            logical_id=shared_designation["logical_id"],
            revision_id=shared_designation["revision_id"],
            payload_id=shared_designation["designation_id"],
            payload_digest=shared_designation["content_hash"],
            partition_id=shared_designation["partition_id"],
        )
        designation_operation, _ = self.memory_store.create_procedure_remote_operation(
            designation_operation
        )
        self.memory_store.update_procedure_remote_operation(
            designation_operation["operation_id"],
            status="remote_committed",
            expected_version=designation_operation["version"],
            remote_receipt={
                "remote_id": remote_current["_id"],
                "remote_digest": remote_current["content_hash"],
            },
        )
        self.service.publish_designation(shared_designation, self.adapter)
        self.assertEqual(
            "acknowledged",
            self.memory_store.get_procedure_remote_operation(
                designation_operation["operation_id"]
            )["status"],
        )

        self.service.publish_designation(self.designation, self.adapter)
        publication = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
        )
        publication, _ = self.memory_store.create_procedure_publication(publication)
        remote_publication = self.adapter.write_publication(publication)
        publication_operation = contracts.make_procedure_remote_operation(
            kind="publication",
            logical_id=publication["logical_id"],
            revision_id=publication["revision_id"],
            payload_id=publication["publication_id"],
            payload_digest=publication["payload_digest"],
            partition_id=publication["partition_id"],
        )
        publication_operation, _ = self.memory_store.create_procedure_remote_operation(
            publication_operation
        )
        receipt = {
            "remote_id": remote_publication["_id"],
            "remote_digest": remote_publication["content_hash"],
        }
        self.memory_store.update_procedure_remote_operation(
            publication_operation["operation_id"],
            status="remote_committed",
            expected_version=publication_operation["version"],
            remote_receipt=receipt,
        )
        self.memory_store.update_procedure_publication(
            publication["publication_id"],
            status="remote_committed",
            expected_version=publication["version"],
            remote_receipt=receipt,
        )

        recovered = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        self.assertEqual("acknowledged", recovered["status"])
        self.assertEqual(
            "acknowledged",
            self.memory_store.get_procedure_remote_operation(
                publication_operation["operation_id"]
            )["status"],
        )

    def test_ambiguous_withdrawal_reconciles_by_exact_current_without_replay(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        adapter = _LostWithdrawalAcknowledgementAdapter(
            collection=self.collection, vector_store=self.vector_store
        )
        with self.assertRaises(procedures.ProcedureRemoteAmbiguityError):
            self.service.withdraw_partition(
                logical_id=self.procedure["logical_id"],
                partition=self.partition,
                issuer="ROOT",
                adapter=adapter,
            )

        reconciled = self.service.reconcile_withdrawal(
            logical_id=self.procedure["logical_id"],
            partition=self.partition,
            adapter=adapter,
        )
        self.assertTrue(reconciled["managed_complete"])
        self.assertEqual(
            "withdrawn",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )
        withdrawal_operation = next(
            operation
            for operation in self.memory_store.list_procedure_remote_operations()
            if operation["kind"] == "withdrawal"
        )
        self.assertEqual("acknowledged", withdrawal_operation["status"])

    def test_withdrawal_waits_for_the_same_blocked_publication_state_operation(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        retry_adapter = _BlockedThenSuccessPublicationStateAdapter(
            collection=self.collection, vector_store=self.vector_store
        )

        first = self.service.withdraw_partition(
            logical_id=self.procedure["logical_id"],
            partition=self.partition,
            issuer="ROOT",
            adapter=retry_adapter,
        )
        self.assertFalse(first["managed_complete"])
        self.assertEqual(
            "fenced",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )
        publication_operations = [
            operation
            for operation in self.memory_store.list_procedure_remote_operations(
                payload_id=publication["publication_id"]
            )
            if operation["kind"] == "withdraw_publication"
        ]
        self.assertEqual(1, len(publication_operations))
        operation_id = publication_operations[0]["operation_id"]
        self.assertEqual("blocked", publication_operations[0]["status"])

        second = self.service.reconcile_withdrawal(
            logical_id=self.procedure["logical_id"],
            partition=self.partition,
            adapter=retry_adapter,
        )
        self.assertTrue(second["managed_complete"])
        self.assertEqual(2, retry_adapter.state_write_calls)
        self.assertEqual(
            "acknowledged",
            self.memory_store.get_procedure_remote_operation(operation_id)["status"],
        )
        self.assertEqual(
            "withdrawn",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )

    def test_withdrawal_exactly_reconciles_an_ambiguous_publication_state_operation(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        retry_adapter = _LostPublicationStateAcknowledgementAdapter(
            collection=self.collection, vector_store=self.vector_store
        )

        first = self.service.withdraw_partition(
            logical_id=self.procedure["logical_id"],
            partition=self.partition,
            issuer="ROOT",
            adapter=retry_adapter,
        )
        self.assertFalse(first["managed_complete"])
        self.assertEqual(
            "fenced",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )
        publication_operation = next(
            operation
            for operation in self.memory_store.list_procedure_remote_operations(
                payload_id=publication["publication_id"]
            )
            if operation["kind"] == "withdraw_publication"
        )
        self.assertEqual("ambiguous", publication_operation["status"])

        second = self.service.reconcile_withdrawal(
            logical_id=self.procedure["logical_id"],
            partition=self.partition,
            adapter=retry_adapter,
        )
        self.assertTrue(second["managed_complete"])
        self.assertEqual(1, retry_adapter.state_write_calls)
        self.assertEqual(
            "acknowledged",
            self.memory_store.get_procedure_remote_operation(
                publication_operation["operation_id"]
            )["status"],
        )
        self.assertEqual(
            "withdrawn",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )

    def test_revocation_waits_for_the_same_blocked_publication_state_operation(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        retry_adapter = _BlockedThenSuccessPublicationStateAdapter(
            collection=self.collection, vector_store=self.vector_store
        )

        first = self.service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="synthetic blocked publication state",
            adapter=retry_adapter,
        )
        self.assertFalse(first["managed_complete"])
        self.assertEqual(
            "revocation_pending",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )
        publication_operations = [
            operation
            for operation in self.memory_store.list_procedure_remote_operations(
                payload_id=publication["publication_id"]
            )
            if operation["kind"] == "revoke_publication"
        ]
        self.assertEqual(1, len(publication_operations))
        operation_id = publication_operations[0]["operation_id"]
        self.assertEqual("blocked", publication_operations[0]["status"])

        second = self.service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="synthetic blocked publication state",
            adapter=retry_adapter,
        )
        self.assertTrue(second["managed_complete"])
        self.assertEqual(2, retry_adapter.state_write_calls)
        self.assertEqual(
            "acknowledged",
            self.memory_store.get_procedure_remote_operation(operation_id)["status"],
        )
        self.assertEqual(
            "revoked",
            self.memory_store.get_procedure_publication(publication["publication_id"])["status"],
        )

    def test_repeat_revocation_reuses_the_durable_tombstone_and_operation(self) -> None:
        first = self.service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="repeatable safety tombstone",
            adapter=self.adapter,
        )
        second = self.service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="repeatable safety tombstone",
            adapter=self.adapter,
        )
        self.assertEqual(
            first["revocation"]["revocation_id"], second["revocation"]["revocation_id"]
        )
        operations = [
            operation
            for operation in self.memory_store.list_procedure_remote_operations(
                revision_id=self.procedure["revision_id"]
            )
            if operation["kind"] == "revocation"
        ]
        self.assertEqual(1, len(operations))
        self.assertEqual("acknowledged", operations[0]["status"])

    def test_stale_publish_is_fenced_by_a_newer_remote_designation(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        stale = contracts.make_procedure_publication(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            created_at="2026-09-21T00:00:03Z",
        )
        stale, created = self.memory_store.create_procedure_publication(stale)
        self.assertTrue(created)

        successor = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.scope,
            body="Inspect the local lock and owner before network recovery.",
            references=self.procedure["behavior"]["references"],
            predicates=self.procedure["behavior"]["predicates"],
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/next"},
            created_at="2026-09-21T00:00:04Z",
        )
        successor_approval = contracts.make_procedure_approval(
            approval_id="approval-2",
            procedure=successor,
            issuer="ROOT",
            recipients=[self.scope],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:05Z",
        )
        successor_designation = self.service.designate(
            procedure=successor,
            approval=successor_approval,
            partition=self.partition,
            issuer="ROOT",
        )
        self.service.publish_designation(successor_designation, self.adapter)
        with self.assertRaises(atlas.AtlasProcedureFencedError):
            self.adapter.write_publication(stale)
        self.assertIsNone(self.adapter.exact_read(stale["publication_id"]))

    def test_withdrawal_is_narrow_and_revocation_fences_every_tracked_copy(self) -> None:
        peer = {
            "application": "harness",
            "project": "product-a",
            "namespace": "atlas-test",
            "owner": "peer-agent",
        }
        peer_partition = {
            "scope": "project",
            "application": "harness",
            "project": "product-a",
            "namespace": "atlas-test",
            "recipients": [peer],
        }
        broad_approval = contracts.make_procedure_approval(
            approval_id="approval-multi",
            procedure=self.procedure,
            issuer="ROOT",
            recipients=[self.scope, peer],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:06Z",
        )
        first = self.service.designate(
            procedure=self.procedure,
            approval=broad_approval,
            partition=self.partition,
            issuer="ROOT",
        )
        second = self.service.designate(
            procedure=self.procedure,
            approval=broad_approval,
            partition=peer_partition,
            issuer="ROOT",
        )
        self.service.publish_designation(first, self.adapter)
        self.service.publish_designation(second, self.adapter)
        first_publication = self.service.publish(
            procedure=self.procedure,
            approval=broad_approval,
            representation=self.representation,
            designation=first,
            adapter=self.adapter,
        )
        second_publication = self.service.publish(
            procedure=self.procedure,
            approval=broad_approval,
            representation=self.representation,
            designation=second,
            adapter=self.adapter,
        )
        self.assertNotEqual(first_publication["publication_id"], second_publication["publication_id"])
        exposure = self.service.record_exposure(
            publication_id=first_publication["publication_id"],
            recipient=self.scope,
            delivery_id="delivery-before-revocation",
        )

        withdrawn = self.service.withdraw_partition(
            logical_id=self.procedure["logical_id"],
            partition=self.partition,
            issuer="ROOT",
            adapter=self.adapter,
        )
        self.assertTrue(withdrawn["managed_complete"])
        self.vector_store.publication_ids = [
            first_publication["publication_id"], second_publication["publication_id"]
        ]
        self.assertEqual(
            [second_publication["publication_id"]],
            [
                item["publication_id"]
                for item in self.service.resolve_atlas(
                    "parser lock recovery",
                    receiver=peer,
                    facts={"language": "python"},
                    route="ordinary",
                    adapter=self.adapter,
                    representation=self.query_representation,
                )
            ],
        )

        revoked = self.service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="synthetic global revocation",
            adapter=self.adapter,
        )
        self.assertTrue(revoked["managed_complete"])
        self.assertEqual([exposure["exposure_id"]], [item["exposure_id"] for item in revoked["exposures"]])
        self.assertTrue(all(
            publication["status"] == "revoked"
            for publication in self.memory_store.list_procedure_publications(
                revision_id=self.procedure["revision_id"]
            )
        ))

    def test_publication_scans_full_behavior_and_representation_without_mutating_it(self) -> None:
        secret = "synthetic-step03-secret"
        procedure = contracts.make_procedure_revision(
            logical_name="sensitive-procedure",
            origin="curated",
            origin_scope=self.scope,
            body=f"Never publish {secret}.",
            references=[{"id": "guide://sensitive", "content": f"Reference {secret}."}],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
            source={"kind": "curated_authoring", "provenance_ref": "curation://sensitive"},
            created_at="2026-09-21T00:00:12Z",
        )
        approval = contracts.make_procedure_approval(
            approval_id="sensitive-approval",
            procedure=procedure,
            issuer="ROOT",
            recipients=[self.scope],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:13Z",
        )
        representation = contracts.make_procedure_representation(
            procedure=procedure,
            model="deterministic-test-embedding/v1",
            dimensions=3,
            metric="cosine",
            sanitizer_version="known-secret/v1",
            search_text=f"sensitive {secret}",
            vector=[0.4, 0.5, 0.6],
            created_at="2026-09-21T00:00:14Z",
        )
        guarded = procedures.TrustedProcedureService(
            self.memory_store,
            trusted_issuers={"ROOT"},
            privacy_policy=privacy.PrivacyPolicy(known_secrets=(secret,)),
        )
        guarded.record_approved_revision(procedure, approval)
        guarded.record_representation(representation)
        designation = guarded.designate(
            procedure=procedure,
            approval=approval,
            partition=self.partition,
            issuer="ROOT",
        )
        with self.assertRaisesRegex(procedures.ProcedureError, "privacy scan"):
            guarded.publish(
                procedure=procedure,
                approval=approval,
                representation=representation,
                designation=designation,
                adapter=self.adapter,
            )
        self.assertIn(secret, procedure["behavior"]["body"])
        self.assertEqual({}, self.collection.documents)
        self.assertEqual(
            [], self.memory_store.list_procedure_publications(revision_id=procedure["revision_id"])
        )
        self.assertEqual(
            [], [op for op in self.memory_store.list_procedure_remote_operations()
                 if op["kind"] == "publication"]
        )

    def test_service_policy_sanitizes_discovery_query_before_atlas(self) -> None:
        self.service.publish_designation(self.designation, self.adapter)
        publication = self.service.publish(
            procedure=self.procedure,
            approval=self.approval,
            representation=self.representation,
            designation=self.designation,
            adapter=self.adapter,
        )
        self.vector_store.publication_ids = [publication["publication_id"]]
        secret = "synthetic-query-secret"
        guarded = procedures.TrustedProcedureService(
            self.memory_store,
            trusted_issuers={"ROOT"},
            privacy_policy=privacy.PrivacyPolicy(known_secrets=(secret,)),
        )
        delivered = guarded.resolve_atlas(
            f"parser {secret} recovery",
            receiver=self.scope,
            facts={"language": "python"},
            route="ordinary",
            adapter=self.adapter,
            representation=self.query_representation,
        )
        self.assertEqual([publication["publication_id"]], [
            item["publication_id"] for item in delivered
        ])
        self.assertNotIn(secret, self.vector_store.calls[-1]["query"])

    def test_private_partitions_and_metric_mismatches_never_reach_atlas(self) -> None:
        """The remote boundary cannot widen local scope or change vector meaning."""

        private_partition = {
            "scope": "private",
            "application": self.scope["application"],
            "project": self.scope["project"],
            "namespace": self.scope["namespace"],
            "owner": self.scope["owner"],
            "recipients": [self.scope],
        }
        private_designation = self.service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=private_partition,
            issuer="ROOT",
        )
        with self.assertRaisesRegex(procedures.ProcedureIneligibleError, "private"):
            self.service.publish_designation(private_designation, self.adapter)

        with self.assertRaisesRegex(procedures.ProcedureError, "durable publication intent"):
            self.service.publish(
                procedure=self.procedure,
                approval=self.approval,
                representation={},
                designation=self.designation,
                adapter=self.adapter,
            )
        self.assertEqual({}, self.collection.documents)

        dot_representation = contracts.make_procedure_representation(
            procedure=self.procedure,
            model="deterministic-test-embedding/v1",
            dimensions=3,
            metric="dot_product",
            sanitizer_version="known-secret/v1",
            search_text="parser lock recovery",
            vector=[0.1, 0.2, 0.3],
            created_at="2026-09-21T00:00:15Z",
        )
        self.service.record_representation(dot_representation)
        with self.assertRaisesRegex(procedures.ProcedureIneligibleError, "metric"):
            self.service.publish(
                procedure=self.procedure,
                approval=self.approval,
                representation=dot_representation,
                designation=self.designation,
                adapter=self.adapter,
            )
        self.assertEqual({}, self.collection.documents)


if __name__ == "__main__":
    unittest.main()
