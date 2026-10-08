"""STEP-04 additive migration: reopen accepted STEP-01..STEP-03 state.

The accepted baseline already owns decisions, dispatches, outcomes, reviews,
reviewed experience, and the trusted-procedure lifecycle. STEP-04 adds
preparation, search-trace, plan-disposition, APC child-operation, and final
context tables. Reopening an accepted database must preserve every existing
record and identity while adding the new tables additively.
"""

from __future__ import annotations

import sys
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, contracts, preparation, procedures, search, store


_STEP04_TABLES = frozenset(
    {
        "preparations",
        "search_traces",
        "search_candidates",
        "plan_dispositions",
        "apc_child_operations",
        "final_contexts",
    }
)
_E2BD6BD_SCHEMA = Path(__file__).with_name("fixtures") / "e2bd6bd_schema.sql"


def _insert(connection: sqlite3.Connection, table: str, fields: dict) -> None:
    columns = tuple(fields)
    connection.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        tuple(fields.values()),
    )


class Step04MigrationCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary.name) / "accepted-step-03.sqlite3"
        self.card = contracts.make_task_card(
            task="Preserve the accepted STEP-03 record",
            base_commit="accepted-base",
        )
        self.plan = contracts.make_plan(
            plan_id="accepted-plan",
            objective_id="objective-1",
            route="ordinary",
            state="accepted",
            accepted_by="ROOT",
            content={"steps": ["inspect", "verify"]},
        )
        self.decision = contracts.make_decision(
            self.card, self.plan, strategy="standard", configuration={"strategy": "standard"}
        )
        self.envelope = contracts.make_envelope(
            task_card=self.card,
            plan=self.plan,
            decision_id=self.decision["decision_id"],
            lane_id="lane-1",
            run_id="run-1",
            worktree_path="C:/synthetic/accepted",
            base_commit="accepted-base",
            mandatory_content=[{"id": "task", "kind": "task", "content": self.card["task"]}],
        )
        self.operation = contracts.make_operation(kind="dispatch", envelope=self.envelope)
        self.outcome = contracts.make_outcome(
            decision_id=self.decision["decision_id"],
            plan_id=self.plan["plan_id"],
            plan_digest=self.plan["content_hash"],
            status="PASS",
            evidence_digest="accepted-evidence",
            linked_run_id="run-1",
            task_card_digest=self.card["content_hash"],
            objective_id=self.plan["objective_id"],
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_accepted_records(self, memory_store: store.MemoryStore) -> None:
        memory_store.record_decision(self.decision)
        memory_store.create_operation(self.operation)
        memory_store.record_outcome(self.outcome)

    def test_reopening_an_accepted_database_preserves_records_and_adds_step04(self) -> None:
        accepted = store.MemoryStore(self.database_path)
        accepted.initialize()
        try:
            self._write_accepted_records(accepted)
        finally:
            accepted.close()

        # Emulate the accepted baseline: STEP-04 tables do not exist yet.
        reopened = store.MemoryStore(self.database_path)
        reopened.initialize()
        try:
            assert reopened.connection is not None
            for table in _STEP04_TABLES:
                reopened.connection.execute(f"DROP TABLE {table}")
            reopened.connection.commit()
        finally:
            reopened.close()

        upgraded = store.MemoryStore(self.database_path)
        upgraded.initialize()
        try:
            tables = {
                row["name"]
                for row in upgraded.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertTrue(_STEP04_TABLES.issubset(tables))

            self.assertEqual(
                self.decision["content_hash"],
                upgraded.get_decision(self.decision["decision_id"])["content_hash"],
            )
            operations = upgraded.list_operations(self.decision["decision_id"])
            self.assertEqual([self.operation["operation_id"]], [item["operation_id"] for item in operations])
            self.assertEqual(
                self.outcome["outcome_id"],
                upgraded.get_outcome(self.decision["decision_id"])["outcome_id"],
            )

            # The reopened tables accept fresh STEP-04 records unchanged.
            limits = config.resolve_limits({"default_deadline_seconds": 300.0})
            preparation = contracts.make_preparation(
                task_card=self.card,
                decision_id=self.decision["decision_id"],
                objective_id="objective-1",
                route="ordinary",
                plan_id=self.plan["plan_id"],
                plan_digest=self.plan["content_hash"],
                plan_state=self.plan["state"],
                current_plan_state="execution_accepted",
                strategy="standard",
                requested_strategy="standard",
                configuration={"strategy": "standard"},
                network_mode="normal",
                budget_source="trusted_deadline",
                remaining_seconds=300.0,
                execution_reserve_seconds=limits.execution_reserve_seconds,
                stage_allowance_seconds=limits.standard_stage_seconds,
                deadline_monotonic=1300.0,
                spent_seconds=0.0,
            )
            upgraded.record_preparation(preparation)
            self.assertEqual(
                preparation["preparation_id"],
                upgraded.get_preparation(preparation["preparation_id"])["preparation_id"],
            )
        finally:
            upgraded.close()

    def test_legacy_database_without_step04_columns_still_opens(self) -> None:
        accepted = store.MemoryStore(self.database_path)
        accepted.initialize()
        try:
            self._write_accepted_records(accepted)
            # The accepted baseline predates the additive envelope/run columns
            # on operations and the configuration columns on decisions.
            assert accepted.connection is not None
            accepted.connection.execute("ALTER TABLE operations RENAME TO operations_old")
            accepted.connection.execute(
                """
                CREATE TABLE operations (
                    operation_id TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    observed_invocation TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            accepted.connection.execute(
                """
                INSERT INTO operations (
                    operation_id, decision_id, kind, status, observed_invocation,
                    created_at, updated_at
                )
                SELECT operation_id, decision_id, kind, status, observed_invocation,
                       created_at, updated_at
                FROM operations_old
                """
            )
            accepted.connection.execute("DROP TABLE operations_old")
            accepted.connection.commit()
        finally:
            accepted.close()

        upgraded = store.MemoryStore(self.database_path)
        upgraded.initialize()
        try:
            assert upgraded.connection is not None
            columns = {
                row["name"]
                for row in upgraded.connection.execute("PRAGMA table_info(operations)")
            }
            self.assertIn("envelope_digest", columns)
            self.assertIn("run_id", columns)
        finally:
            upgraded.close()


class AcceptedStep03SchemaTests(unittest.TestCase):
    """Reopen records created in the complete e2bd6bd STEP-03 table layout."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary.name) / "accepted-e2bd6bd.sqlite3"
        self.plan = contracts.make_plan(
            plan_id="legacy-plan", objective_id="legacy-objective", route="ordinary",
            state="accepted", accepted_by="ROOT", content={"steps": ["inspect", "repair"]},
        )
        self.card = contracts.make_task_card(task="Keep legacy evidence", base_commit="legacy-base")
        self.decision = contracts.make_decision(
            self.card, self.plan, strategy="standard", configuration={"strategy": "standard"}
        )
        envelope = contracts.make_envelope(
            task_card=self.card, plan=self.plan, decision_id=self.decision["decision_id"],
            lane_id="legacy-lane", run_id="legacy-run", worktree_path="C:/legacy/worktree",
            base_commit="legacy-base",
        )
        self.operation = contracts.make_operation(kind="dispatch", envelope=envelope)
        self.outcome = contracts.make_outcome(
            decision_id=self.decision["decision_id"], plan_id=self.plan["plan_id"],
            plan_digest=self.plan["content_hash"], status="PASS",
            evidence_digest="legacy-evidence", linked_run_id="legacy-run",
            task_card_digest=self.card["content_hash"], objective_id=self.plan["objective_id"],
        )
        self.receipt = contracts.make_review_receipt(
            review_id="legacy-root-review", outcome=self.outcome, decision=self.decision,
            task_card=self.card, plan=self.plan, reviewed_by="ROOT",
            evidence_refs=("review://legacy-run",), raw_evidence="Verified legacy repair.",
            failed_hypotheses=("network outage",),
        )
        scope = {"application": "harness", "project": "product", "namespace": "legacy",
                 "owner": "root-agent"}
        self.trajectory = contracts.make_reviewed_trajectory(
            task_card=self.card, plan=self.plan, decision=self.decision,
            outcome=self.outcome, review_receipt=self.receipt, scope=scope,
        )
        self.procedure = contracts.make_procedure_revision(
            logical_name="legacy-lock-repair", origin="curated", origin_scope=scope,
            body="Inspect the lock before retrying.",
            references=[{"id": "guide://legacy", "content": "Keep the lock invariant."}],
            predicates={"applicability": {}, "conflicts": {}, "capabilities": {}, "routes": {}},
            source={"kind": "curated_authoring", "provenance_ref": "curation://legacy/1"},
            created_at="2026-09-21T00:00:00Z",
        )
        self.approval = contracts.make_procedure_approval(
            approval_id="legacy-approval", procedure=self.procedure, issuer="ROOT",
            recipients=[scope], authority_evidence={"policy_id": "trusted-root/v1"},
            approved_at="2026-09-21T00:00:01Z",
        )
        self.partition = {
            "scope": "project", "application": "harness", "project": "product",
            "namespace": "legacy", "recipients": [scope],
        }
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(_E2BD6BD_SCHEMA.read_text(encoding="utf-8"))
        _insert(connection, "decisions", {
            key: json.dumps(self.decision[key], sort_keys=True) if key == "configuration"
            else self.decision[key]
            for key in ("decision_id", "task_card_digest", "objective_id", "route", "plan_id",
                        "plan_state", "plan_digest", "strategy", "configuration",
                        "configuration_digest", "state", "created_at")
        } | {"updated_at": self.decision["created_at"]})
        _insert(connection, "operations", {
            key: self.operation[key] for key in
            ("operation_id", "decision_id", "envelope_digest", "run_id", "kind", "status",
             "created_at")
        } | {"observed_invocation": None, "updated_at": self.operation["created_at"]})
        _insert(connection, "outcomes", {
            key: self.outcome[key] for key in
            ("outcome_id", "decision_id", "task_card_digest", "objective_id", "plan_id",
             "plan_digest", "status", "evidence_digest", "linked_run_id", "observed_at")
        } | {"created_at": self.outcome["observed_at"]})
        connection.commit()
        # These accepted persistence APIs write STEP-03 records without running
        # today's initializer, so the schema remains exactly the captured one.
        legacy = store.MemoryStore(self.database_path)
        legacy.connection = connection
        legacy.record_review_receipt(self.receipt)
        legacy.record_reviewed_trajectory(self.trajectory)
        trusted = procedures.TrustedProcedureService(legacy, trusted_issuers={"ROOT"})
        trusted.record_approved_revision(self.procedure, self.approval)
        self.designation = trusted.designate(
            procedure=self.procedure, approval=self.approval,
            partition=self.partition, issuer="ROOT",
        )
        legacy.close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _snapshot(self) -> dict[str, tuple[str, tuple[tuple, ...], list[tuple]]]:
        with closing(sqlite3.connect(self.database_path)) as connection:
            tables = connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            return {
                name: (
                    ddl,
                    tuple(tuple(row) for row in connection.execute(f"PRAGMA table_info({name})")),
                    [tuple(row) for row in connection.execute(f"SELECT * FROM {name} ORDER BY rowid")],
                )
                for name, ddl in tables
            }

    def _assert_original_state(self, original: dict, upgraded: store.MemoryStore) -> None:
        assert upgraded.connection is not None
        for table, (ddl, column_info, rows) in original.items():
            with self.subTest(table=table):
                current_ddl = upgraded.connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()[0]
                current_info = tuple(tuple(row) for row in upgraded.connection.execute(
                    f"PRAGMA table_info({table})"
                ))
                self.assertEqual(column_info, current_info[:len(column_info)])
                columns = tuple(row[1] for row in column_info)
                self.assertEqual(rows, [tuple(row) for row in upgraded.connection.execute(
                    f"SELECT {', '.join(columns)} FROM {table} ORDER BY rowid"
                )])
                if table != "decisions":
                    self.assertEqual(ddl, current_ddl)
        self.assertEqual(self.decision["content_hash"], upgraded.get_decision(
            self.decision["decision_id"]
        )["content_hash"])
        self.assertEqual(self.operation["operation_id"], upgraded.list_operations(
            self.decision["decision_id"]
        )[0]["operation_id"])
        self.assertEqual(self.outcome["outcome_id"], upgraded.get_outcome(
            self.decision["decision_id"]
        )["outcome_id"])
        self.assertEqual(self.receipt, upgraded.get_review_receipt(self.receipt["review_receipt_id"]))
        self.assertEqual(self.trajectory, upgraded.get_reviewed_trajectory(
            self.trajectory["trajectory_id"]
        ))
        self.assertEqual(self.procedure, upgraded.get_procedure_revision(
            self.procedure["revision_id"]
        ))
        self.assertEqual(self.approval, upgraded.get_procedure_approval(
            self.approval["approval_id"]
        ))
        current = upgraded.get_current_procedure_designation(
            self.procedure["logical_id"], self.partition
        )
        self.assertEqual(self.designation, current["record"])
        self.assertEqual(self.designation["designation_id"], current["designation_id"])
        self.assertEqual(1, current["generation"])

    def test_accepted_step03_schema_reopens_additively_with_original_meaning(self) -> None:
        original = self._snapshot()
        self.assertEqual(19, len(original))
        self.assertFalse(_STEP04_TABLES.intersection(original))
        upgraded = store.MemoryStore(self.database_path)
        upgraded.initialize()
        try:
            self._assert_original_state(original, upgraded)
            assert upgraded.connection is not None
            tables = {row["name"] for row in upgraded.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            self.assertTrue(_STEP04_TABLES.issubset(tables))
            self.assertIn("content_hash", {
                row["name"] for row in upgraded.connection.execute("PRAGMA table_info(decisions)")
            })
        finally:
            upgraded.close()

    def test_incompatible_preparations_blocks_only_its_owner_without_changes(self) -> None:
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                "CREATE TABLE preparations (preparation_id TEXT PRIMARY KEY, legacy_payload BLOB NOT NULL)"
            )
            connection.execute(
                "INSERT INTO preparations VALUES (?, ?)", ("legacy-preparation", b"\x00legacy\xff")
            )
            connection.commit()
        original = self._snapshot()
        upgraded = store.MemoryStore(self.database_path)
        upgraded.initialize()
        try:
            self._assert_original_state(original, upgraded)
            calls: list[object] = []
            assert upgraded.connection is not None
            writes_before = upgraded.connection.total_changes
            service = preparation.PreparationService(
                store=upgraded, config=config.resolve_config({"strategy": "standard"}),
                limits=config.resolve_limits(), clock=lambda: 1000.0,
            )
            with self.assertRaisesRegex(
                preparation.MandatoryStateFailure, "incompatible preparations schema"
            ):
                service.prepare(
                    task_card=self.card, plan=self.plan, objective_id=self.plan["objective_id"],
                    deadline=1300.0, stores=[search.SearchStore(
                        store_id="optional", kind="historical_evidence",
                        query=lambda query: calls.append(query) or [],
                    )],
                )
            self.assertEqual([], calls)
            self.assertEqual(writes_before, upgraded.connection.total_changes)
            self._assert_original_state(original, upgraded)
            self.assertEqual(0, upgraded.connection.execute(
                "SELECT COUNT(*) FROM plan_dispositions"
            ).fetchone()[0])
        finally:
            upgraded.close()


if __name__ == "__main__":
    unittest.main()
