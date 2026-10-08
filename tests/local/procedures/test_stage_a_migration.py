from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memory_harness import contracts, store


_ACCEPTED_STAGE_A_CORE_SCHEMA = """
CREATE TABLE decisions (
    decision_id TEXT PRIMARY KEY,
    task_card_digest TEXT NOT NULL,
    objective_id TEXT NOT NULL,
    route TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    plan_state TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    strategy TEXT NOT NULL,
    configuration TEXT NOT NULL,
    configuration_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
    envelope_digest TEXT NOT NULL,
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    observed_invocation TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE outcomes (
    outcome_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE REFERENCES decisions(decision_id),
    task_card_digest TEXT NOT NULL,
    objective_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    evidence_digest TEXT NOT NULL,
    linked_run_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class StageAToStep03MigrationTests(unittest.TestCase):
    def test_initialize_adds_procedure_schema_without_losing_stage_a_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database_path = Path(temporary) / "accepted-stage-a.sqlite3"
            plan = contracts.make_plan(
                plan_id="stage-a-plan",
                objective_id="stage-a-objective",
                route="ordinary",
                state="accepted",
                content={"steps": ["inspect", "verify"]},
                accepted_by="ROOT",
            )
            task_card = contracts.make_task_card(
                task="Preserve the accepted Stage-A record",
                base_commit="stage-a-base",
                memory_handoff=contracts.make_memory_handoff(
                    objective_id=plan["objective_id"], route="ordinary", plan=plan
                ),
            )
            decision = contracts.make_decision(task_card, plan)
            envelope = contracts.make_envelope(
                task_card=task_card,
                plan=plan,
                decision_id=decision["decision_id"],
                lane_id="stage-a-lane",
                run_id="stage-a-run",
                worktree_path="C:/synthetic/stage-a",
                base_commit=task_card["base_commit"],
            )
            operation = contracts.make_operation(kind="dispatch", envelope=envelope)
            outcome = contracts.make_outcome(
                decision_id=decision["decision_id"],
                plan_id=plan["plan_id"],
                plan_digest=plan["content_hash"],
                status="PASS",
                evidence_digest="stage-a-evidence",
                linked_run_id="stage-a-run",
                task_card_digest=task_card["content_hash"],
                objective_id=plan["objective_id"],
            )

            connection = sqlite3.connect(database_path)
            try:
                connection.executescript(_ACCEPTED_STAGE_A_CORE_SCHEMA)
                connection.execute(
                    """
                    INSERT INTO decisions (
                        decision_id, task_card_digest, objective_id, route, plan_id,
                        plan_state, plan_digest, strategy, configuration,
                        configuration_digest, state, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision["decision_id"],
                        decision["task_card_digest"],
                        decision["objective_id"],
                        decision["route"],
                        decision["plan_id"],
                        decision["plan_state"],
                        decision["plan_digest"],
                        decision["strategy"],
                        json.dumps(decision["configuration"], sort_keys=True),
                        decision["configuration_digest"],
                        decision["state"],
                        decision["created_at"],
                        decision["created_at"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO operations (
                        operation_id, decision_id, envelope_digest, run_id, kind, status,
                        observed_invocation, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        operation["operation_id"],
                        operation["decision_id"],
                        operation["envelope_digest"],
                        operation["run_id"],
                        operation["kind"],
                        operation["status"],
                        None,
                        operation["created_at"],
                        operation["created_at"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO outcomes (
                        outcome_id, decision_id, task_card_digest, objective_id, plan_id,
                        plan_digest, status, evidence_digest, linked_run_id, observed_at,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        outcome["outcome_id"],
                        outcome["decision_id"],
                        outcome["task_card_digest"],
                        outcome["objective_id"],
                        outcome["plan_id"],
                        outcome["plan_digest"],
                        outcome["status"],
                        outcome["evidence_digest"],
                        outcome["linked_run_id"],
                        outcome["observed_at"],
                        outcome["observed_at"],
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            upgraded = store.MemoryStore(database_path)
            upgraded.initialize()
            try:
                self.assertEqual(
                    decision["configuration"],
                    upgraded.get_decision(decision["decision_id"])["configuration"],
                )
                self.assertEqual(
                    operation["operation_id"],
                    upgraded.list_operations(decision["decision_id"])[0]["operation_id"],
                )
                self.assertEqual(
                    outcome["outcome_id"],
                    upgraded.get_outcome(decision["decision_id"])["outcome_id"],
                )
                assert upgraded.connection is not None
                tables = {
                    row["name"]
                    for row in upgraded.connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                self.assertTrue(
                    {
                        "procedure_revisions",
                        "procedure_approvals",
                        "procedure_representations",
                        "procedure_designations",
                        "procedure_withdrawals",
                        "procedure_current_designations",
                        "procedure_publications",
                        "procedure_revocations",
                        "procedure_remote_operations",
                        "procedure_exposures",
                    }.issubset(tables)
                )
            finally:
                upgraded.close()


if __name__ == "__main__":
    unittest.main()
