from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts, procedures, store


class ProcedureDesignationContractTests(unittest.TestCase):
    """T08: approval and current designation are independent facts."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.memory_store = store.MemoryStore(Path(self.temporary.name) / "memory.sqlite3")
        self.memory_store.initialize()
        self.origin_scope = {
            "application": "harness",
            "project": "product-a",
            "namespace": "step03-test",
            "owner": "root-agent",
        }
        self.receiver = dict(self.origin_scope)
        self.partition = {
            "scope": "project",
            "application": "harness",
            "project": "product-a",
            "namespace": "step03-test",
            "recipients": [self.receiver],
        }
        self.procedure = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.origin_scope,
            body="Inspect the local lock before attempting network recovery.",
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
            recipients=[self.receiver],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root-operator"},
            approved_at="2026-09-21T00:00:01Z",
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    def test_approval_never_makes_a_procedure_current(self) -> None:
        service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}
        )

        service.record_approved_revision(self.procedure, self.approval)
        self.assertIsNone(
            self.memory_store.get_current_procedure_designation(
                self.procedure["logical_id"], self.partition
            )
        )

        designation = service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            issuer="ROOT",
        )
        current = self.memory_store.get_current_procedure_designation(
            self.procedure["logical_id"], self.partition
        )
        self.assertEqual(designation["designation_id"], current["designation_id"])
        self.assertEqual(1, current["generation"])

    def test_approval_requires_retained_authority_evidence(self) -> None:
        with self.assertRaisesRegex(contracts.ContractError, "authority_evidence"):
            contracts.make_procedure_approval(
                approval_id="unsupported-approval",
                procedure=self.procedure,
                issuer="ROOT",
                recipients=[self.receiver],
                authority_evidence={},
                approved_at="2026-09-21T00:00:01Z",
            )

    def test_meaning_changes_require_a_new_revision_and_new_approval(self) -> None:
        changed = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.origin_scope,
            body="Inspect the local lock, capture owner state, then attempt recovery.",
            references=[{"id": "guide://lock", "content": "Preserve the lock invariant."}],
            predicates={
                "applicability": {"all": [{"field": "language", "operator": "equals", "value": "python"}]},
                "conflicts": {},
                "capabilities": {},
                "routes": {"all": [{"field": "route", "operator": "equals", "value": "ordinary"}]},
            },
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/2"},
            created_at="2026-09-21T00:00:02Z",
        )
        self.assertEqual(self.procedure["logical_id"], changed["logical_id"])
        self.assertNotEqual(self.procedure["revision_id"], changed["revision_id"])
        with self.assertRaisesRegex(contracts.ContractError, "exact procedure"):
            contracts.validate_procedure_approval(self.approval, procedure=changed)

        changed_reference = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.origin_scope,
            body=self.procedure["behavior"]["body"],
            references=[{"id": "guide://lock", "content": "A changed prerequisite is behavior."}],
            predicates=self.procedure["behavior"]["predicates"],
            source=self.procedure["source"],
            created_at="2026-09-21T00:00:03Z",
        )
        changed_predicate = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.origin_scope,
            body=self.procedure["behavior"]["body"],
            references=self.procedure["behavior"]["references"],
            predicates={
                **self.procedure["behavior"]["predicates"],
                "capabilities": {"all": [{"field": "capabilities.lock", "operator": "equals", "value": True}]},
            },
            source=self.procedure["source"],
            created_at="2026-09-21T00:00:04Z",
        )
        self.assertNotEqual(self.procedure["revision_id"], changed_reference["revision_id"])
        self.assertNotEqual(self.procedure["revision_id"], changed_predicate["revision_id"])

        other_origin = contracts.make_procedure_revision(
            logical_name=self.procedure["logical_name"],
            origin="curated",
            origin_scope={**self.origin_scope, "owner": "other-author"},
            body=self.procedure["behavior"]["body"],
            references=self.procedure["behavior"]["references"],
            predicates=self.procedure["behavior"]["predicates"],
            source={"kind": "curated_authoring", "provenance_ref": "curation://other/1"},
            created_at="2026-09-21T00:00:04Z",
        )
        self.assertNotEqual(self.procedure["logical_id"], other_origin["logical_id"])

    def test_delayed_designation_loses_and_rollback_is_a_new_operation(self) -> None:
        service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}
        )
        service.record_approved_revision(self.procedure, self.approval)
        first = service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            issuer="ROOT",
        )
        successor = contracts.make_procedure_revision(
            logical_name="parser-lock-repair",
            origin="curated",
            origin_scope=self.origin_scope,
            body="Inspect the lock and its owner before network recovery.",
            references=self.procedure["behavior"]["references"],
            predicates=self.procedure["behavior"]["predicates"],
            source={"kind": "curated_authoring", "provenance_ref": "curation://root/next"},
            created_at="2026-09-21T00:00:05Z",
        )
        successor_approval = contracts.make_procedure_approval(
            approval_id="approval-2",
            procedure=successor,
            issuer="ROOT",
            recipients=[self.receiver],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root-operator"},
            approved_at="2026-09-21T00:00:06Z",
        )
        second = service.designate(
            procedure=successor,
            approval=successor_approval,
            partition=self.partition,
            issuer="ROOT",
        )
        self.assertEqual(2, second["generation"])

        delayed = contracts.make_procedure_designation(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            generation=2,
            predecessor_generation=1,
            issuer="ROOT",
            created_at="2026-09-21T00:00:07Z",
        )
        with self.assertRaisesRegex(store.ProcedureDesignationConflictError, "newer"):
            self.memory_store.record_procedure_designation(delayed)

        rollback = service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            issuer="ROOT",
        )
        self.assertEqual(3, rollback["generation"])
        self.assertEqual(self.procedure["revision_id"], rollback["revision_id"])
        self.assertNotEqual(first["designation_id"], rollback["designation_id"])

    def test_retrying_the_exact_current_designation_is_idempotent(self) -> None:
        service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}
        )
        first = service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            issuer="ROOT",
        )
        retried = service.designate(
            procedure=self.procedure,
            approval=self.approval,
            partition=self.partition,
            issuer="ROOT",
        )
        self.assertEqual(first["designation_id"], retried["designation_id"])
        self.assertEqual(1, retried["generation"])

    def test_predicate_unknowns_and_conflicts_fail_closed(self) -> None:
        predicates = {
            "applicability": {"all": [{"field": "language", "operator": "equals", "value": "python"}]},
            "conflicts": {"all": [{"field": "unsafe", "operator": "equals", "value": True}]},
            "capabilities": {"all": [{"field": "capabilities.lock", "operator": "present"}]},
            "routes": {"all": [{"field": "route", "operator": "equals", "value": "ordinary"}]},
        }
        self.assertFalse(
            contracts.procedure_predicates_match(
                predicates, {"language": "python", "capabilities": {"lock": True}}, route="ordinary"
            )
        )
        self.assertFalse(
            contracts.procedure_predicates_match(
                predicates,
                {"language": "python", "unsafe": True, "capabilities": {"lock": True}},
                route="ordinary",
            )
        )

    def test_generated_revision_rejoins_exact_step02_candidate_and_approval(self) -> None:
        candidate = contracts.make_generated_skill_candidate(
            scope=self.origin_scope,
            skill_id="generated-parser-lock",
            content="Inspect the generated parser lock evidence before recovery.",
            source_cases=[
                {
                    "case_id": "case-1",
                    "case_receipt_id": "case-receipt-1",
                    "trajectory_id": "trajectory-1",
                    "review_receipt_id": "review-1",
                    "review_receipt_digest": "review-digest-1",
                }
            ],
            metadata={"source": "synthetic"},
            created_at="2026-09-21T00:00:08Z",
        )
        self.memory_store.record_generated_skill_candidate(candidate)
        source_approval = contracts.make_skill_approval(
            approval_id="generated-source-approval",
            candidate=candidate,
            issuer="ROOT",
            recipients=(self.origin_scope["owner"],),
            approved_at="2026-09-21T00:00:09Z",
            authority_evidence={"policy_id": "step02-test/v1", "subject": "root"},
        )
        self.memory_store.record_skill_approval(source_approval)
        service = procedures.TrustedProcedureService(
            self.memory_store, trusted_issuers={"ROOT"}
        )
        generated = service.procedure_from_generated_skill(
            candidate_id=candidate["candidate_id"],
            skill_approval_id=source_approval["approval_id"],
            logical_name="generated-parser-lock",
            references=[{"id": "guide://generated", "content": "Generated source is historical."}],
            predicates={
                "applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}
            },
        )
        approval = contracts.make_procedure_approval(
            approval_id="procedure-approval",
            procedure=generated,
            issuer="ROOT",
            recipients=[self.receiver],
            authority_evidence={"policy_id": "trusted-root/v1", "subject": "root"},
            approved_at="2026-09-21T00:00:10Z",
        )
        service.record_approved_revision(generated, approval)

        forged_source = dict(generated["source"])
        forged_source["candidate_digest"] = "not-the-durable-candidate"
        forged = contracts.make_procedure_revision(
            logical_name=generated["logical_name"],
            origin="generated",
            origin_scope=self.origin_scope,
            body=generated["behavior"]["body"],
            references=generated["behavior"]["references"],
            predicates=generated["behavior"]["predicates"],
            source=forged_source,
            created_at="2026-09-21T00:00:11Z",
        )
        with self.assertRaisesRegex(store.ProcedureConflictError, "provenance"):
            self.memory_store.record_procedure_revision(forged)


if __name__ == "__main__":
    unittest.main()
