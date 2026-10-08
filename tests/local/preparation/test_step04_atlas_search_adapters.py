"""STEP-04: the Atlas SearchStore adapter over the accepted procedure service.

These tests pin the narrow integration slice that turns the already accepted
``procedures.TrustedProcedureService.resolve_atlas`` discovery/exact-read/
delivery path into one real ``search.SearchStore`` input for
``preparation.PreparationService``:

- only a delivery the accepted service revalidated (exact live publication,
  recipient, predicates, current designation, no revocation, exact
  representation identity) becomes one integrity-bound procedure candidate
  that keeps its exact logical/revision/publication identity, approval and
  current-designation evidence, recipient scope, predicate result, live
  freshness, and provenance;
- a raw vector hit, a stale publication, an incompatible representation, a
  revoked revision, and an unauthorized receiver never become guidance;
- the minimal source-specific preparation gate suppresses the *actual* Atlas
  task-path call when ``atlas_shared_retrieval`` is disabled (even when
  ``generated_skill_use`` is true) or when the network mode is
  ``restricted_local``, while an enabled local store still delivers;
- the store consumes only the already sanitized bounded-search query and its
  exact representation identity.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    atlas,
    atlas_adapters,
    config,
    contracts,
    experience,
    local_adapters,
    preparation,
    privacy,
    procedures,
    store,
    templates,
)


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value


class _WriteResult:
    def __init__(self, matched_count: int) -> None:
        self.matched_count = matched_count


class _Collection:
    """Deterministic exact-read collection double (no live Atlas service)."""

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
    """Deterministic Vector Search double; records every discovery call."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.publication_ids: list[str] = []

    def similarity_search_with_score(
        self, query: str, **kwargs: object
    ) -> list[tuple[_Document, float]]:
        self.calls.append({"query": query, **kwargs})
        return [(_Document(publication_id), 0.99) for publication_id in self.publication_ids]


class Step04AtlasSearchAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "store_seconds": 8.0,
                "minimum_optional_slice_seconds": 1.0,
            }
        )
        self.policy = privacy.PrivacyPolicy()
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        self.card = contracts.make_task_card(
            task="Fix the regression failure in the parser lock test",
            base_commit="base-1",
        )
        self.candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="parser-lock-repair",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )
        self.receiver = {
            "application": "harness",
            "project": "product-a",
            "namespace": "atlas-search-adapter",
            "owner": "root-agent",
        }
        self.partition = {
            "scope": "project",
            "application": self.receiver["application"],
            "project": self.receiver["project"],
            "namespace": self.receiver["namespace"],
            "recipients": [dict(self.receiver)],
        }
        self.identity = templates.representation_identity(limits=self.limits)
        self.procedure = self._procedure("parser-lock-repair")
        self.approval = contracts.make_procedure_approval(
            approval_id="approval-parser-lock",
            procedure=self.procedure,
            issuer="ROOT",
            recipients=[dict(self.receiver)],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:01Z",
        )
        self.representation = self._representation(
            self.procedure, search_text="parser lock recovery regression failure"
        )
        self.procedure_service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}
        )
        self.procedure_service.record_approved_revision(self.procedure, self.approval)
        self.procedure_service.record_representation(self.representation)
        self.designation = self.procedure_service.designate(
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

    # -- helpers -----------------------------------------------------------

    def _procedure(self, logical_name: str) -> dict:
        return contracts.make_procedure_revision(
            logical_name=logical_name,
            origin="curated",
            origin_scope=self.receiver,
            body="Inspect the local lock before network recovery.",
            references=[{"id": "guide://lock", "content": "Preserve the lock invariant."}],
            predicates={
                "applicability": {
                    "all": [{"field": "language", "operator": "equals", "value": "python"}]
                },
                "conflicts": {},
                "capabilities": {},
                "routes": {
                    "all": [{"field": "route", "operator": "equals", "value": "ordinary"}]
                },
            },
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/1"},
            created_at="2026-09-21T00:00:00Z",
        )

    def _representation(
        self,
        procedure: dict,
        *,
        search_text: str,
        identity: dict | None = None,
    ) -> dict:
        selected = identity or self.identity
        return contracts.make_procedure_representation(
            procedure=procedure,
            model=selected["model"],
            dimensions=selected["dimensions"],
            metric=selected["metric"],
            sanitizer_version=selected["sanitizer_version"],
            search_text=search_text,
            vector=[0.01] * int(selected["dimensions"]),
            created_at="2026-09-21T00:00:02Z",
        )

    def _publish(
        self,
        *,
        procedure=None,
        approval=None,
        representation=None,
        designation=None,
        partition=None,
    ):
        procedure = procedure or self.procedure
        approval = approval or self.approval
        representation = representation or self.representation
        designation = designation or self.designation
        partition = partition or self.partition
        designation = self.procedure_service.designate(
            procedure=procedure,
            approval=approval,
            partition=partition,
            issuer="ROOT",
        )
        self.procedure_service.publish_designation(designation, self.adapter)
        publication = self.procedure_service.publish(
            procedure=procedure,
            approval=approval,
            representation=representation,
            designation=designation,
            adapter=self.adapter,
        )
        self.vector_store.publication_ids.append(publication["publication_id"])
        return publication

    def _payload(
        self, text: str, *, route: str = "ordinary", identity: dict | None = None
    ) -> dict:
        objective = templates.objective_representation(text, route=route, limits=self.limits)
        representation = {
            key: objective[key]
            for key in ("model", "dimensions", "metric", "sanitizer_version")
        }
        if identity is not None:
            representation = dict(identity)
        return privacy.safe_query_payload(
            {
                "representation": representation,
                "tokens": list(objective["tokens"])[:32],
                "route": route,
            },
            self.policy,
        )

    def _store(self, **overrides):
        arguments = {
            "procedure_service": self.procedure_service,
            "adapter": self.adapter,
            "receiver": self.receiver,
            "facts": {"language": "python"},
            "route": "ordinary",
            "limits": self.limits,
        }
        arguments.update(overrides)
        return atlas_adapters.make_atlas_search_store(**arguments)

    def test_direct_store_query_respects_captured_restricted_and_off_gates(self) -> None:
        self._publish()
        payload = self._payload("parser lock recovery")
        baseline_reads = list(self.collection.exact_reads)
        baseline_searches = list(self.vector_store.calls)
        restricted = self._store(network_resolution=config.resolve_network_mode(
            "restricted_local"))
        self.assertEqual([], list(restricted.query(payload)))
        disabled = self._store(atlas_shared_retrieval_enabled=False)
        self.assertEqual([], list(disabled.query(payload)))
        self.assertEqual(baseline_reads, self.collection.exact_reads)
        self.assertEqual(baseline_searches, self.vector_store.calls)
        self.assertEqual(1, len(list(self._store().query(payload))))

    def test_provider_downgrade_and_verified_effective_store_queries(self) -> None:
        self._publish()
        payload = self._payload("parser lock recovery")
        downgraded = config.resolve_network_mode("atlas_memory_only")
        self.assertEqual("soft_guardrail_network", downgraded.effective_mode)
        self.assertEqual(1, len(list(self._store(
            network_resolution=contracts.network_resolution_record(downgraded)
        ).query(payload))))
        evidence = {
            "launched_payload": {"verified": True, "evidence_id": "launch-proof"},
            "unrelated_destination_block": {
                "blocked": True, "independent": True,
                "source_kind": "independent_network_boundary", "evidence_id": "egress-proof",
            },
        }
        context = {key: "bound-test" for key in config.NETWORK_CONTEXT_FIELDS}
        verified = config.NetworkResolver(lambda _evidence, _context: True).resolve(
            "atlas_memory_only", evidence=evidence, context=context)
        self.assertEqual("atlas_memory_only", verified.effective_mode)
        self.assertEqual(1, len(list(self._store(
            network_resolution=verified
        ).query(payload))))
        legacy = {
            "requested_mode": "atlas_memory_only", "effective_mode": "atlas_memory_only",
            "enforcement_sources": ["legacy_captured_mode"],
            "disclosed_limits": ["legacy row has no captured network enforcement proof"],
            "input_evidence": {},
        }
        with self.assertRaises(atlas.AtlasProcedureError):
            list(self._store(network_resolution=legacy).query(payload))
        unverified = dict(legacy, enforcement_sources=["requested_soft_guardrail_policy"])
        with self.assertRaises(atlas.AtlasProcedureError):
            list(self._store(network_resolution=unverified).query(payload))

    def _prepare(self, stores, **overrides):
        arguments = {
            "task_card": self.card,
            "plan": self.candidate,
            "objective_id": "parser-lock-repair",
            "route": "ordinary",
            "stores": list(stores),
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    def _attempts(self, outcome):
        return {entry["store_id"]: entry for entry in outcome.trace["attempts"]}

    def _selected_procedures(self, outcome):
        return [
            item
            for item in outcome.trace["candidates"]
            if item["kind"] == "procedure" and item["disposition"] == "selected"
        ]

    # -- the accepted delivery becomes one procedure candidate -------------

    def test_real_resolution_becomes_one_integrity_bound_candidate(self) -> None:
        publication = self._publish()
        store = self._store()
        self.assertTrue(store.requires_network)
        self.assertEqual("procedure", store.kind)
        self.assertEqual(self.receiver, dict(store.scope))

        payload = self._payload("parser lock recovery regression failure")
        candidates = list(store.query(payload))
        self.assertEqual(1, len(candidates))
        candidate = candidates[0]

        # Exact logical/revision/publication identity.
        self.assertEqual(self.procedure["logical_id"], candidate["logical_id"])
        self.assertEqual(self.procedure["revision_id"], candidate["revision_id"])
        body = candidate["payload"]
        self.assertEqual(publication["publication_id"], body["publication_id"])
        self.assertEqual(self.procedure["logical_id"], body["logical_id"])
        self.assertEqual(self.procedure["revision_id"], body["revision_id"])

        # Approval/current-designation evidence and the predicate result.
        self.assertEqual(self.approval, body["approval"])
        self.assertEqual(self.designation, body["designation"])
        self.assertEqual(self.partition, body["partition"])
        self.assertEqual(self.receiver, body["recipient"])
        self.assertTrue(body["predicates_ok"])
        self.assertEqual("live", body["freshness"])
        self.assertEqual("approved", candidate["approval_status"])
        self.assertEqual("current", candidate["designation"])
        self.assertTrue(candidate["predicates_ok"])

        # Exact recipient scope and integrity binding.
        self.assertEqual(self.receiver, dict(candidate["scope"]))
        self.assertEqual(
            contracts.sha256_hex(body), candidate["payload_digest"]
        )
        self.assertEqual("project", candidate["specificity"])

        # The comparable representation is the exact central identity.
        representation = dict(candidate["representation"])
        for key, value in self.identity.items():
            self.assertEqual(value, representation[key], key)
        self.assertTrue(representation["declared"])
        self.assertIn("lock", representation["tokens"])
        self.assertGreater(candidate["score"], 0.0)

        # Provenance stays exact: the publication it came from is recorded.
        provenance = body["provenance"]
        self.assertEqual(publication["publication_id"], provenance["publication_id"])
        self.assertEqual(self.designation["designation_id"], provenance["designation_id"])
        self.assertEqual(self.representation["representation_id"], provenance["representation_id"])
        self.assertEqual("atlas_trusted_procedure", provenance["authority"])

        # Only the already sanitized bounded query was used, and the accepted
        # service performed a real discovery plus an exact read.
        self.assertEqual(1, len(self.vector_store.calls))
        self.assertEqual(" ".join(payload["tokens"]), self.vector_store.calls[0]["query"])
        self.assertEqual(
            self.receiver["application"],
            self.vector_store.calls[0]["pre_filter"]["application"],
        )
        self.assertIn(publication["publication_id"], self.collection.exact_reads)

        # A second identical query returns the same candidate without mutating
        # the accepted service records.
        again = list(store.query(payload))
        self.assertEqual(candidates, again)

    def test_preparation_service_consumes_the_real_service_delivery(self) -> None:
        publication = self._publish()
        outcome = self._prepare([self._store()])

        attempts = self._attempts(outcome)
        self.assertEqual("completed", attempts["atlas-shared-procedures"]["status"])
        selected = self._selected_procedures(outcome)
        self.assertEqual(1, len(selected))
        record = selected[0]
        self.assertEqual(self.procedure["logical_id"], record["logical_id"])
        self.assertEqual(self.procedure["revision_id"], record["revision_id"])
        self.assertEqual(publication["publication_id"], record["payload"]["publication_id"])
        self.assertEqual(self.receiver, dict(record["scope"]))
        self.assertEqual("live", record["freshness"])
        self.assertEqual(
            {"atlas-shared-procedures"},
            {entry["store_id"] for entry in record["provenance"]},
        )
        # One bounded standard pass: exactly one discovery call, one exact read.
        self.assertEqual(1, len(self.vector_store.calls))
        self.assertEqual("optional_memory", outcome.trace["outcome"])
        # The candidate never becomes the plan: a pending candidate keeps its
        # exact identity and state until ROOT decides.
        self.assertEqual("candidate_review", outcome.disposition["branch"])
        self.assertEqual("candidate", outcome.plan["state"])

    # -- fail-closed conversion -------------------------------------------

    def test_raw_vector_hit_without_exact_read_evidence_is_not_guidance(self) -> None:
        self.vector_store.publication_ids = ["no-such-publication"]
        store = self._store()
        self.assertEqual([], list(store.query(self._payload("parser lock recovery"))))
        outcome = self._prepare([self._store()])
        self.assertEqual([], self._selected_procedures(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])

    def test_stale_publication_state_is_not_guidance(self) -> None:
        publication = self._publish()
        self.adapter.write_publication_state(publication, state="withdrawn")
        store = self._store()
        self.assertEqual([], list(store.query(self._payload("parser lock recovery"))))
        outcome = self._prepare([self._store()])
        self.assertEqual([], self._selected_procedures(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])

    def test_revoked_revision_is_not_guidance(self) -> None:
        self._publish()
        result = self.procedure_service.revoke(
            procedure=self.procedure,
            issuer="ROOT",
            reason="synthetic revocation",
            adapter=self.adapter,
        )
        self.assertTrue(result["managed_complete"])
        store = self._store()
        self.assertEqual([], list(store.query(self._payload("parser lock recovery"))))
        outcome = self._prepare([self._store()])
        self.assertEqual([], self._selected_procedures(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])

    def test_unauthorized_receiver_is_not_guidance(self) -> None:
        self._publish()
        other = dict(self.receiver, owner="other-agent")
        store = self._store(receiver=other)
        self.assertEqual([], list(store.query(self._payload("parser lock recovery"))))
        outcome = self._prepare([self._store(receiver=other)])
        self.assertEqual([], self._selected_procedures(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])

    def test_incompatible_representation_is_not_guidance(self) -> None:
        self._publish()
        other_identity = {
            "model": "other-embedding/v2",
            "dimensions": self.identity["dimensions"],
            "metric": self.identity["metric"],
            "sanitizer_version": self.identity["sanitizer_version"],
        }
        # The store's central identity and the bounded query disagree: no
        # discovery call may even begin.
        calls_before = len(self.vector_store.calls)
        store = self._store()
        self.assertEqual(
            [], list(store.query(self._payload("parser lock recovery", identity=other_identity)))
        )
        self.assertEqual(calls_before, len(self.vector_store.calls))

        # A separate delivery with another representation identity is never a
        # candidate even when discovery returns it next to a matching one.
        other_procedure = self._procedure("other-lock-repair")
        other_approval = contracts.make_procedure_approval(
            approval_id="approval-other-lock",
            procedure=other_procedure,
            issuer="ROOT",
            recipients=[dict(self.receiver)],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:03Z",
        )
        other_representation = self._representation(
            other_procedure,
            search_text="other lock recovery",
            identity=other_identity,
        )
        self.procedure_service.record_approved_revision(other_procedure, other_approval)
        self.procedure_service.record_representation(other_representation)
        self._publish(
            procedure=other_procedure,
            approval=other_approval,
            representation=other_representation,
        )
        candidates = list(store.query(self._payload("parser lock recovery")))
        self.assertEqual(
            [self.procedure["logical_id"]], [item["logical_id"] for item in candidates]
        )

    # -- the source-specific preparation gate ------------------------------

    def test_disabling_atlas_retrieval_suppresses_the_actual_call(self) -> None:
        self._publish()
        store = self._store()
        calls_before = len(self.vector_store.calls)
        reads_before = len(self.collection.exact_reads)
        outcome = self._prepare(
            [store],
            request={"atlas_shared_retrieval": False, "generated_skill_use": True},
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts["atlas-shared-procedures"]["status"])
        self.assertIn("configuration", attempts["atlas-shared-procedures"]["reason"])
        self.assertEqual(calls_before, len(self.vector_store.calls))
        self.assertEqual(reads_before, len(self.collection.exact_reads))
        self.assertEqual([], self._selected_procedures(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])

        # A different repository base owns an independent decision, so its
        # enabled policy can exercise the Atlas gate.
        enabled = self._prepare(
            [self._store()], task_card=contracts.make_task_card(
                task=self.card["task"], base_commit="base-enabled"
            ),
        )
        self.assertEqual(
            "completed", self._attempts(enabled)["atlas-shared-procedures"]["status"]
        )
        self.assertEqual(1, len(self.vector_store.calls))

    def test_restricted_local_never_begins_the_atlas_call(self) -> None:
        self._publish()
        calls_before = len(self.vector_store.calls)
        reads_before = len(self.collection.exact_reads)
        outcome = self._prepare(
            [self._store()],
            network_mode="restricted_local",
            request={"atlas_shared_retrieval": True, "generated_skill_use": True},
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts["atlas-shared-procedures"]["status"])
        self.assertIn("restricted-local", attempts["atlas-shared-procedures"]["reason"])
        self.assertEqual(calls_before, len(self.vector_store.calls))
        self.assertEqual(reads_before, len(self.collection.exact_reads))
        self.assertEqual([], self._selected_procedures(outcome))

    def test_local_candidate_delivery_is_unaffected_when_atlas_is_disabled(self) -> None:
        self._publish()
        trajectory = self._reviewed_trajectory()
        local_stores = local_adapters.make_local_search_stores(
            experience_service=experience.ReviewedExperienceService(
                self.memory_store, privacy_policy=self.policy
            ),
            scope=self._experience_scope(),
            registry=(),
            limits=self.limits,
        )
        outcome = self._prepare(
            [self._store()] + list(local_stores),
            request={"atlas_shared_retrieval": False, "generated_skill_use": True},
            network_mode="restricted_local",
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts["atlas-shared-procedures"]["status"])
        self.assertEqual("completed", attempts["local-reviewed-experience"]["status"])
        self.assertEqual([], self.vector_store.calls)
        evidence = [
            item
            for item in outcome.trace["candidates"]
            if item["kind"] == "historical_evidence" and item["disposition"] == "selected"
        ]
        self.assertEqual([trajectory["trajectory_id"]], [item["logical_id"] for item in evidence])

    # -- the factory surface stays explicit --------------------------------

    def test_factory_requires_explicit_service_adapter_receiver_and_route(self) -> None:
        with self.assertRaises(atlas_adapters.AtlasAdapterError):
            self._store(procedure_service=object())
        with self.assertRaises(atlas_adapters.AtlasAdapterError):
            self._store(adapter=object())
        with self.assertRaises(atlas_adapters.AtlasAdapterError):
            self._store(receiver={"application": "harness"})
        with self.assertRaises(atlas_adapters.AtlasAdapterError):
            self._store(facts="not-an-object")
        with self.assertRaises(atlas_adapters.AtlasAdapterError):
            self._store(route="not-a-route")
        with self.assertRaises(atlas_adapters.AtlasAdapterError):
            self._store(limits=object())
        store = self._store()
        self.assertEqual("atlas-shared-procedures", store.store_id)
        self.assertEqual("procedure", store.kind)
        self.assertTrue(store.requires_network)
        self.assertEqual("live", store.freshness)

    # -- local reviewed evidence used by the unaffected-delivery test -------

    def _experience_scope(self):
        return experience.ExperienceScope(
            application=self.receiver["application"],
            project=self.receiver["project"],
            namespace=self.receiver["namespace"],
            owner=self.receiver["owner"],
        )

    def _reviewed_trajectory(self) -> dict:
        scope = self._experience_scope()
        service = experience.ReviewedExperienceService(
            self.memory_store, privacy_policy=self.policy
        )
        plan = contracts.make_plan(
            plan_id="accepted-plan-local",
            objective_id="objective-local",
            route="ordinary",
            state="accepted",
            content={"steps": ["inspect", "repair", "verify"]},
            accepted_by="ROOT",
        )
        card = contracts.make_task_card(
            task="Fix the parser lock regression",
            base_commit="base-1",
            memory_handoff=contracts.make_memory_handoff(
                objective_id=plan["objective_id"], route="ordinary", plan=plan
            ),
        )
        decision = contracts.make_decision(card, plan)
        self.memory_store.record_decision(decision)
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"],
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status="PASS",
            evidence_digest="terminal-evidence-local",
            linked_run_id="retired-run-local",
            task_card_digest=card["content_hash"],
            objective_id=plan["objective_id"],
        )
        self.memory_store.record_outcome(outcome)
        review = contracts.make_review_receipt(
            review_id="review-local",
            outcome=outcome,
            decision=decision,
            task_card=card,
            plan=plan,
            reviewed_by="ROOT",
            evidence_refs=("review://run/retired-run-local", "check://parser"),
            protected_source_refs=("evidence://protected/parser-log",),
            raw_evidence="The parser lock repair passed and the focused check is green.",
            failed_hypotheses=("the network adapter caused the regression",),
        )
        return service.capture(
            task_card=card,
            plan=plan,
            decision=decision,
            outcome=outcome,
            review_receipt=review,
            scope=scope,
        )


if __name__ == "__main__":
    unittest.main()
