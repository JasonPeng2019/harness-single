"""Small standard-library SQLite store for Stage-A decisions and operations."""

from __future__ import annotations

import json
import math
import os
import sqlite3
from dataclasses import asdict
from itertools import combinations
from pathlib import Path
from typing import Any, Callable, Mapping

from . import contracts
from .config import MemoryConfig, effect_submission_enabled, resolve_config


class StoreError(RuntimeError):
    """The durable store could not be initialized or updated."""


class OutcomeConflictError(StoreError):
    """A different terminal outcome already exists for this decision."""


class OperationConflictError(StoreError):
    """An operation replay conflicts with its durable state."""


class TrajectoryConflictError(StoreError):
    """A reviewed trajectory replay conflicts with immutable local evidence."""


class ExperienceConflictError(StoreError):
    """An extraction receipt, case, candidate, or approval conflicts with evidence."""


class ProcedureConflictError(StoreError):
    """Trusted procedure evidence or its immutable source binding conflicts."""


class ProcedureDesignationConflictError(ProcedureConflictError):
    """A delayed designation or withdrawal lost its conditional generation race."""


class PreparationConflictError(StoreError):
    """A preparation attempt lost the durable decision-budget race."""


class NativeUsageConflictError(StoreError):
    """Native invocation identity, ownership, or receipt evidence conflicts."""


_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS decisions (
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
        content_hash TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS operations (
        operation_id TEXT PRIMARY KEY,
        decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
        envelope_digest TEXT NOT NULL,
        run_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        observed_invocation TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dispatch_bindings (
        operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),
        lane_id TEXT NOT NULL,
        envelope_record TEXT NOT NULL,
        native_receipt_required INTEGER NOT NULL,
        supersedes_rejected_attempt_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outcomes (
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
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS native_outcomes (
        outcome_id TEXT PRIMARY KEY REFERENCES outcomes(outcome_id),
        terminal_evidence TEXT NOT NULL,
        acceptance_status TEXT NOT NULL,
        exceptional_acceptance INTEGER NOT NULL,
        supersedes_outcome_id TEXT REFERENCES outcomes(outcome_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS effect_operations (
        operation_id TEXT PRIMARY KEY,
        outcome_id TEXT REFERENCES outcomes(outcome_id),
        decision_id TEXT,
        kind TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        status TEXT NOT NULL,
        source_id TEXT,
        source_digest TEXT,
        source_record TEXT,
        payload_digest TEXT,
        payload_record TEXT,
        adapter_operation_id TEXT,
        adapter_payload_digest TEXT,
        configuration TEXT NOT NULL,
        configuration_digest TEXT NOT NULL,
        version INTEGER NOT NULL,
        acknowledgement TEXT,
        reconciliation TEXT,
        uncertainty TEXT,
        claim_generation INTEGER NOT NULL DEFAULT 0,
        active_claim_id TEXT,
        claimant_record TEXT,
        claimant_digest TEXT,
        fence_evidence TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(outcome_id, kind, scope_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rejected_native_attempts (
        rejected_attempt_id TEXT PRIMARY KEY,
        decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
        operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
        run_id TEXT NOT NULL,
        result_digest TEXT NOT NULL,
        review_digest TEXT NOT NULL,
        acceptance_digest TEXT NOT NULL,
        evidence_digest TEXT NOT NULL,
        record TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rejected_authorizations (
        rejected_attempt_id TEXT PRIMARY KEY REFERENCES rejected_native_attempts(rejected_attempt_id),
        operation_id TEXT NOT NULL UNIQUE REFERENCES operations(operation_id),
        state TEXT NOT NULL CHECK(state IN ('reserved', 'spent'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS review_receipts (
        review_receipt_id TEXT PRIMARY KEY,
        outcome_id TEXT NOT NULL UNIQUE REFERENCES outcomes(outcome_id),
        review_id TEXT NOT NULL,
        decision_id TEXT NOT NULL,
        task_card_digest TEXT NOT NULL,
        task_text TEXT NOT NULL,
        objective_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        plan_id TEXT NOT NULL,
        plan_digest TEXT NOT NULL,
        route TEXT NOT NULL,
        reviewed_by TEXT NOT NULL,
        state TEXT NOT NULL,
        evidence_refs TEXT NOT NULL,
        protected_source_refs TEXT NOT NULL,
        raw_evidence TEXT NOT NULL,
        failed_hypotheses TEXT NOT NULL,
        reviewed_at TEXT NOT NULL,
        content_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reviewed_trajectories (
        trajectory_id TEXT PRIMARY KEY,
        outcome_id TEXT NOT NULL UNIQUE REFERENCES outcomes(outcome_id),
        scope_digest TEXT NOT NULL,
        scope TEXT NOT NULL,
        task_card_digest TEXT NOT NULL,
        task_text TEXT NOT NULL,
        objective_id TEXT NOT NULL,
        decision_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        accepted_plan_id TEXT NOT NULL,
        accepted_plan_digest TEXT NOT NULL,
        route TEXT NOT NULL,
        status TEXT NOT NULL,
        review_receipt_id TEXT NOT NULL,
        review_id TEXT NOT NULL,
        review_receipt_digest TEXT NOT NULL,
        review_state TEXT NOT NULL,
        reviewed_by TEXT NOT NULL,
        reviewed_at TEXT NOT NULL,
        evidence_refs TEXT NOT NULL,
        protected_source_refs TEXT NOT NULL,
        failed_hypotheses TEXT NOT NULL,
        raw_evidence TEXT NOT NULL,
        evidence_digest TEXT NOT NULL,
        recorded_at TEXT NOT NULL,
        content_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS experience_ingestions (
        ingestion_id TEXT PRIMARY KEY,
        trajectory_id TEXT NOT NULL UNIQUE REFERENCES reviewed_trajectories(trajectory_id),
        scope_digest TEXT NOT NULL,
        scope TEXT NOT NULL,
        destination TEXT NOT NULL,
        session_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        status TEXT NOT NULL,
        case_ids TEXT NOT NULL,
        error TEXT,
        version INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS experience_case_receipts (
        case_receipt_id TEXT PRIMARY KEY,
        case_id TEXT NOT NULL,
        trajectory_id TEXT NOT NULL REFERENCES reviewed_trajectories(trajectory_id),
        ingestion_id TEXT NOT NULL REFERENCES experience_ingestions(ingestion_id),
        scope_digest TEXT NOT NULL,
        scope TEXT NOT NULL,
        review_receipt_id TEXT NOT NULL,
        review_receipt_digest TEXT NOT NULL,
        source_case TEXT NOT NULL,
        source_case_digest TEXT NOT NULL,
        created_at TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        UNIQUE(case_id, scope_digest)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS generated_skill_candidates (
        candidate_id TEXT PRIMARY KEY,
        skill_id TEXT NOT NULL,
        origin TEXT NOT NULL,
        state TEXT NOT NULL,
        scope_digest TEXT NOT NULL,
        scope TEXT NOT NULL,
        content TEXT NOT NULL,
        content_digest TEXT NOT NULL,
        source_cases TEXT NOT NULL,
        metadata TEXT NOT NULL,
        created_at TEXT NOT NULL,
        content_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS generated_skill_approvals (
        approval_id TEXT PRIMARY KEY,
        candidate_id TEXT NOT NULL REFERENCES generated_skill_candidates(candidate_id),
        skill_id TEXT NOT NULL,
        origin TEXT NOT NULL,
        scope_digest TEXT NOT NULL,
        scope TEXT NOT NULL,
        content_digest TEXT NOT NULL,
        issuer TEXT NOT NULL,
        recipients TEXT NOT NULL,
        source_cases TEXT NOT NULL,
        approved_at TEXT NOT NULL,
        authority_evidence TEXT NOT NULL,
        content_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_revisions (
        revision_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        origin_scope_digest TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_approvals (
        approval_id TEXT PRIMARY KEY,
        revision_id TEXT NOT NULL REFERENCES procedure_revisions(revision_id),
        logical_id TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_representations (
        representation_id TEXT PRIMARY KEY,
        revision_id TEXT NOT NULL REFERENCES procedure_revisions(revision_id),
        logical_id TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_designations (
        designation_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL REFERENCES procedure_revisions(revision_id),
        partition_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(logical_id, partition_id, generation)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_withdrawals (
        withdrawal_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL,
        partition_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(logical_id, partition_id, generation)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_current_designations (
        logical_id TEXT NOT NULL,
        partition_id TEXT NOT NULL,
        current_id TEXT NOT NULL,
        revision_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        state TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY(logical_id, partition_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_publications (
        publication_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL REFERENCES procedure_revisions(revision_id),
        partition_id TEXT NOT NULL,
        designation_id TEXT NOT NULL,
        status TEXT NOT NULL,
        version INTEGER NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_revocations (
        revocation_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL UNIQUE REFERENCES procedure_revisions(revision_id),
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_remote_operations (
        operation_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL,
        partition_id TEXT,
        payload_id TEXT NOT NULL,
        status TEXT NOT NULL,
        version INTEGER NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS procedure_exposures (
        exposure_id TEXT PRIMARY KEY,
        publication_id TEXT NOT NULL REFERENCES procedure_publications(publication_id),
        revision_id TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        delivered_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS preparations (
        preparation_id TEXT PRIMARY KEY,
        decision_id TEXT NOT NULL,
        task_card_digest TEXT NOT NULL,
        objective_id TEXT NOT NULL,
        route TEXT NOT NULL,
        strategy TEXT NOT NULL,
        current_plan_state TEXT NOT NULL,
        status TEXT NOT NULL,
        supersedes TEXT,
        superseded_by TEXT,
        record TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS search_traces (
        preparation_id TEXT PRIMARY KEY,
        outcome TEXT NOT NULL,
        record TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS search_candidates (
        candidate_id TEXT NOT NULL,
        preparation_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL,
        disposition TEXT NOT NULL,
        score REAL NOT NULL,
        record TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (candidate_id, preparation_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS plan_dispositions (
        disposition_id TEXT PRIMARY KEY,
        decision_id TEXT NOT NULL,
        objective_id TEXT NOT NULL,
        branch TEXT NOT NULL,
        record TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS apc_child_operations (
        child_operation_id TEXT PRIMARY KEY,
        parent_decision_id TEXT NOT NULL,
        parent_objective_id TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        status TEXT NOT NULL,
        observed_invocation TEXT,
        result_digest TEXT,
        record TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS final_contexts (
        context_id TEXT PRIMARY KEY,
        decision_id TEXT NOT NULL,
        plan_id TEXT NOT NULL,
        plan_digest TEXT NOT NULL,
        envelope_digest TEXT NOT NULL,
        record TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS native_usage_starts (
        source TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        objective_id TEXT,
        maintenance_operation_id TEXT,
        record TEXT NOT NULL,
        PRIMARY KEY (source, invocation_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS native_usage_receipts (
        source TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        receipt_id TEXT NOT NULL,
        record TEXT NOT NULL,
        PRIMARY KEY (source, invocation_id, receipt_id),
        FOREIGN KEY (source, invocation_id)
            REFERENCES native_usage_starts (source, invocation_id)
    )
    """,
]

class MemoryStore:
    """A minimal durable store with explicit outcome conflict semantics."""

    def __init__(
        self, path: str | Path, *,
        external_claim_fence_verifier: Callable[[Mapping[str, Any], Mapping[str, Any]], bool] | None = None,
    ) -> None:
        if external_claim_fence_verifier is not None and not callable(external_claim_fence_verifier):
            raise TypeError("external claim fence verifier must be callable")
        self.path = Path(path)
        self._external_claim_fence_verifier = external_claim_fence_verifier
        self.connection: sqlite3.Connection | None = None
        self._opened_path: Path | None = None
        self._live_effect_claims: dict[str, str] = {}

    def __enter__(self) -> "MemoryStore":
        self.initialize()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def initialize(self) -> None:
        if self.connection is not None:
            return
        self._live_effect_claims.clear()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.connection = sqlite3.connect(str(self.path), timeout=30.0)
            # Remember the database this connection actually opened so a
            # relative path keeps working for later reads after the process
            # cwd moves; ``self.path`` keeps the caller's form.
            self._opened_path = Path(os.path.abspath(self.path))
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA journal_mode=WAL")
            with self.connection:
                self.connection.execute("BEGIN IMMEDIATE")
                authorization_table_existed = self.connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='rejected_authorizations'"
                ).fetchone() is not None
                for statement in _SCHEMA:
                    self.connection.execute(statement)
                self._ensure_column("operations", "envelope_digest", "TEXT")
                self._ensure_column("operations", "run_id", "TEXT")
                self._ensure_column("dispatch_bindings", "supersedes_rejected_attempt_id", "TEXT")
                self._migrate_dispatch_authorizations(authorization_table_existed)
                self._ensure_column("outcomes", "task_card_digest", "TEXT")
                self._ensure_column("outcomes", "objective_id", "TEXT")
                self._ensure_column("decisions", "configuration", "TEXT")
                self._ensure_column("decisions", "configuration_digest", "TEXT")
                self._ensure_column("decisions", "content_hash", "TEXT")
                self._ensure_column(
                    "generated_skill_candidates",
                    "state",
                    "TEXT NOT NULL DEFAULT 'proposed'",
                )
                self._ensure_column(
                    "experience_ingestions",
                    "version",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                self._ensure_column(
                    "generated_skill_approvals",
                    "authority_evidence",
                    "TEXT",
                )
                self._ensure_column("review_receipts", "task_text", "TEXT")
                self._ensure_column("review_receipts", "route", "TEXT")
                self._ensure_column("effect_operations", "adapter_operation_id", "TEXT")
                self._ensure_column("effect_operations", "adapter_payload_digest", "TEXT")
                self._ensure_column("effect_operations", "reconciliation", "TEXT")
                self._ensure_column("effect_operations", "claim_generation", "INTEGER NOT NULL DEFAULT 0")
                self._ensure_column("effect_operations", "active_claim_id", "TEXT")
                self._ensure_column("effect_operations", "claimant_record", "TEXT")
                self._ensure_column("effect_operations", "claimant_digest", "TEXT")
                self._ensure_column("effect_operations", "fence_evidence", "TEXT")
                self._migrate_external_effect_columns()
                self._migrate_local_effect_intents()
        except sqlite3.Error as exc:
            if self.connection is not None:
                self.connection.close()
                self.connection = None
            self._opened_path = None
            raise StoreError(f"cannot initialize memory store {self.path}: {exc}") from exc

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        connection = self._require_connection()
        columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def _migrate_external_effect_columns(self) -> None:
        """Allow source-owned effects without changing accepted outcome-owned rows."""
        connection = self._require_connection()
        columns = connection.execute("PRAGMA table_info(effect_operations)").fetchall()
        if not any(row["name"] == "outcome_id" and row["notnull"] for row in columns):
            return
        schema = next(statement for statement in _SCHEMA
                      if "CREATE TABLE IF NOT EXISTS effect_operations (" in statement)
        connection.execute(schema.replace("effect_operations (", "effect_operations_new (", 1))
        names = ", ".join(row["name"] for row in columns)
        connection.execute(f"INSERT INTO effect_operations_new ({names}) SELECT {names} FROM effect_operations")
        connection.execute("DROP TABLE effect_operations")
        connection.execute("ALTER TABLE effect_operations_new RENAME TO effect_operations")

    def _migrate_local_effect_intents(self) -> None:
        """Recover pre-STEP-09 native outcomes and already retained local evidence."""
        connection = self._require_connection()
        rows = connection.execute(
            "SELECT outcomes.* FROM outcomes JOIN native_outcomes USING(outcome_id)"
        ).fetchall()
        for row in rows:
            outcome = dict(row)
            self._create_local_effect_intents(outcome, self.get_decision(outcome["decision_id"]))
            receipt = connection.execute(
                "SELECT review_receipt_id FROM review_receipts WHERE outcome_id=?",
                (outcome["outcome_id"],),
            ).fetchone()
            if receipt is not None:
                record = self.get_review_receipt(receipt["review_receipt_id"])
                self._bridge_local_source_in_transaction(
                    outcome["outcome_id"], "review_receipt", record["review_receipt_id"], record,
                )
            trajectory = connection.execute(
                "SELECT trajectory_id FROM reviewed_trajectories WHERE outcome_id=?",
                (outcome["outcome_id"],),
            ).fetchone()
            if trajectory is not None:
                record = self.get_reviewed_trajectory(trajectory["trajectory_id"])
                for kind in ("recent_evidence", "experience_ingestion", "generated_skill_creation"):
                    self._bridge_local_source_in_transaction(
                        outcome["outcome_id"], kind, record["trajectory_id"], record,
                    )
                ingestion = self.get_experience_ingestion_for_trajectory(record["trajectory_id"])
                if ingestion is not None:
                    self._bridge_experience_ingestion_in_transaction(ingestion)

    def _migrate_dispatch_authorizations(self, authorization_table_existed: bool) -> None:
        connection = self._require_connection()
        definition = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='dispatch_bindings'"
        ).fetchone()["sql"]
        old_index = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='dispatch_rejected_authorization'"
        ).fetchone()
        if old_index is not None and "CREATE UNIQUE INDEX" in old_index["sql"].upper():
            connection.execute("DROP INDEX dispatch_rejected_authorization")
        if "SUPERSEDES_REJECTED_ATTEMPT_ID TEXT UNIQUE" in definition.upper():
            connection.execute(
                "CREATE TABLE dispatch_bindings_migrated ("
                "operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id), "
                "lane_id TEXT NOT NULL, envelope_record TEXT NOT NULL, "
                "native_receipt_required INTEGER NOT NULL, "
                "supersedes_rejected_attempt_id TEXT)"
            )
            connection.execute(
                "INSERT INTO dispatch_bindings_migrated SELECT operation_id, lane_id, "
                "envelope_record, native_receipt_required, supersedes_rejected_attempt_id "
                "FROM dispatch_bindings"
            )
            connection.execute("DROP TABLE dispatch_bindings")
            connection.execute("ALTER TABLE dispatch_bindings_migrated RENAME TO dispatch_bindings")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS dispatch_rejected_authorization "
            "ON dispatch_bindings(supersedes_rejected_attempt_id)"
        )
        if not authorization_table_existed:
            connection.execute(
                "INSERT INTO rejected_authorizations (rejected_attempt_id, operation_id, state) "
                "SELECT b.supersedes_rejected_attempt_id, b.operation_id, "
                "CASE WHEN o.status='delivered' THEN 'spent' ELSE 'reserved' END "
                "FROM dispatch_bindings b JOIN operations o USING(operation_id) "
                "WHERE b.supersedes_rejected_attempt_id IS NOT NULL "
                "AND o.status!='failed_pre_spawn'"
            )

    def close(self) -> None:
        self._live_effect_claims.clear()
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        self._opened_path = None

    def _require_connection(self) -> sqlite3.Connection:
        if self.connection is None:
            raise StoreError("memory store is not initialized")
        return self.connection

    def _open_read_only_connection(self) -> sqlite3.Connection:
        """Open one read-only connection owned by the calling thread.

        The shared store connection is thread-affine, so it cannot be used from
        another thread.  The accepted bounded search queries every store on its
        own bounded worker thread, so this seam lets the local reviewed-evidence
        read run there: the connection is created by the caller and closed by
        the caller, and it never writes, migrates, or changes shared state.
        Its URI is built from the absolute database identity captured by
        ``initialize``, so relative store paths keep working after the
        process cwd moves.
        """

        opened_path = self._opened_path
        if self.connection is None or opened_path is None:
            raise StoreError("memory store is not initialized")
        try:
            connection = sqlite3.connect(
                f"{opened_path.as_uri()}?mode=ro", uri=True, timeout=30.0
            )
        except (sqlite3.Error, ValueError) as exc:
            raise StoreError(
                f"cannot open read-only memory store {opened_path}: {exc}"
            ) from exc
        connection.row_factory = sqlite3.Row
        return connection

    def record_decision(self, decision: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_decision(decision)
        connection = self._require_connection()
        values = (
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
            decision["content_hash"],
            decision["created_at"],
            decision["created_at"],
        )
        with connection:
            connection.execute(
                """
                INSERT INTO decisions (
                    decision_id, task_card_digest, objective_id, route, plan_id,
                    plan_state, plan_digest, strategy, configuration,
                    configuration_digest, state, content_hash, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(decision_id) DO UPDATE SET
                    task_card_digest=excluded.task_card_digest,
                    objective_id=excluded.objective_id,
                    route=excluded.route,
                    plan_id=excluded.plan_id,
                    plan_state=excluded.plan_state,
                    plan_digest=excluded.plan_digest,
                    strategy=excluded.strategy,
                    configuration=excluded.configuration,
                    configuration_digest=excluded.configuration_digest,
                    state=excluded.state,
                    content_hash=excluded.content_hash,
                    updated_at=excluded.updated_at
                """,
                values,
            )
        return self.get_decision(str(decision["decision_id"]))

    def get_decision(self, decision_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"decision not found: {decision_id}")
        result = dict(row)
        if result.get("configuration"):
            result["configuration"] = json.loads(result["configuration"])
        if not result.get("content_hash"):
            # An accepted-baseline row predates the additive content-hash
            # column. Rebuild the canonical decision, excluding SQLite-only
            # updated_at and including the contract schema discriminator.
            canonical = {
                "schema": contracts.DECISION_SCHEMA,
                **{key: result[key] for key in (
                    "decision_id", "task_card_digest", "objective_id", "route",
                    "plan_id", "plan_state", "plan_digest", "strategy",
                    "configuration", "configuration_digest", "state", "created_at",
                )},
            }
            result["content_hash"] = contracts.content_hash(canonical)
        return result

    def find_logical_decision(
        self, identity: Mapping[str, str]
    ) -> dict[str, Any] | None:
        """Read one exact preparation owner; only an empty result means absence."""

        keys = (
            "task_card_digest", "objective_id", "route", "plan_id",
            "plan_state", "plan_digest",
        )
        if any(not isinstance(identity.get(key), str) or not identity[key] for key in keys):
            raise StoreError("logical decision identity is incomplete")
        try:
            connection = self._require_connection()

            def validated_decision(stored: sqlite3.Row) -> dict[str, Any]:
                row = dict(stored)
                record = {
                    "schema": contracts.DECISION_SCHEMA,
                    **{key: row[key] for key in (
                        "decision_id", *keys, "strategy", "configuration_digest",
                        "state", "created_at",
                    )},
                    "configuration": json.loads(row["configuration"]),
                }
                # The accepted nullable column has no bearing on the captured
                # identity. Reconstruct only that accepted hash representation.
                record["content_hash"] = row["content_hash"] or contracts.content_hash(record)
                contracts.validate_decision(record)
                expected_state = (
                    "prepared" if record["plan_state"] == "accepted"
                    else record["plan_state"]
                )
                if record["state"] != expected_state:
                    raise StoreError("logical decision state conflicts with its plan state")
                return record

            # Preparations retain the exact plan identity independently of the
            # decision columns. Consult this durable owner before deciding that
            # a damaged decision row is absent.
            indexed_fields = (
                "preparation_id", "decision_id", "task_card_digest", "objective_id",
                "route", "strategy", "current_plan_state", "status", "supersedes",
                "superseded_by",
            )
            try:
                prepared_rows = connection.execute(
                    "SELECT " + ", ".join((*indexed_fields, "record", "created_at", "updated_at"))
                    + " FROM preparations ORDER BY preparation_id"
                ).fetchall()
            except sqlite3.OperationalError as exc:
                if "no such column" in str(exc) or "no such table" in str(exc):
                    raise StoreError(f"incompatible preparations schema: {exc}") from exc
                raise
            prepared_owners = set()
            for stored in prepared_rows:
                if any(stored[key] != identity[key] for key in (
                    "task_card_digest", "objective_id", "route"
                )):
                    # The serialized exact identity can reveal an owner hidden
                    # by an indexed-column error. Do not recover via a partial
                    # column match or validate unrelated malformed records.
                    try:
                        serialized = json.loads(str(stored["record"]))
                    except (ValueError, TypeError):
                        continue
                    if not isinstance(serialized, Mapping) or any(
                        serialized.get(key) != identity[key] for key in keys
                    ):
                        continue
                try:
                    packet = self._stored_record(stored, contracts.validate_preparation)
                except StoreError as exc:
                    # A malformed preparation cannot hide an exact owner. A
                    # valid distinct decision can, however, prove that this
                    # row belongs to another plan under the same task/route.
                    other = connection.execute(
                        "SELECT * FROM decisions WHERE decision_id=?",
                        (stored["decision_id"],),
                    ).fetchone()
                    if other is not None:
                        try:
                            distinct = validated_decision(other)
                        except (StoreError, ValueError, TypeError, KeyError,
                                contracts.ContractError):
                            pass
                        else:
                            if any(distinct[key] != identity[key] for key in keys):
                                continue
                    raise StoreError(f"related logical preparation is unreadable: {exc}") from exc
                inconsistent = [key for key in indexed_fields if packet[key] != stored[key]]
                if any(packet[key] != identity[key] for key in keys):
                    if not inconsistent:
                        continue
                    if packet["decision_id"] != stored["decision_id"]:
                        raise StoreError("distinct logical preparation owner is inconsistent")
                    other = connection.execute(
                        "SELECT * FROM decisions WHERE decision_id=?",
                        (packet["decision_id"],),
                    ).fetchone()
                    if other is None:
                        raise StoreError("distinct logical preparation decision is missing")
                    distinct = validated_decision(other)
                    if any(distinct[key] != packet[key] for key in keys):
                        raise StoreError("distinct logical preparation identity is inconsistent")
                    continue
                if inconsistent:
                    raise StoreError(
                        f"related logical preparation {inconsistent[0]} is inconsistent"
                    )
                prepared_owners.add(packet["decision_id"])
            if len(prepared_owners) > 1:
                raise StoreError("ambiguous logical decision: multiple prepared owners")

            exact_predicate = " AND ".join(f"{key}=?" for key in keys)
            exact_rows = connection.execute(
                f"SELECT * FROM decisions WHERE {exact_predicate} ORDER BY decision_id",
                tuple(identity[key] for key in keys),
            ).fetchall()
            matches = [validated_decision(stored) for stored in exact_rows]
            if len(matches) > 1:
                raise StoreError("ambiguous logical decision: multiple durable owners")
            if prepared_owners:
                prepared_id = next(iter(prepared_owners))
                if matches and matches[0]["decision_id"] != prepared_id:
                    raise StoreError("prepared logical decision conflicts with a second owner")
                if matches:
                    return matches[0]
                stored = connection.execute(
                    "SELECT * FROM decisions WHERE decision_id=?", (prepared_id,)
                ).fetchone()
                if stored is None:
                    raise StoreError("prepared logical decision row is missing")
                record = validated_decision(stored)
                if any(record[key] != identity[key] for key in keys):
                    raise StoreError("prepared logical decision identity is inconsistent")
                return record
            if matches:
                return matches[0]

            # Only when no exact owner exists, inspect nearby standalone rows.
            # One or two damaged identity columns cannot silently mint a new
            # decision, while a known exact owner stays independent of them.
            related = tuple(combinations(keys, len(keys) - 2))
            predicate = " OR ".join(
                "(" + " AND ".join(f"{key}=?" for key in group) + ")"
                for group in related
            )
            rows = connection.execute(
                f"SELECT * FROM decisions WHERE {predicate} ORDER BY decision_id",
                tuple(identity[key] for group in related for key in group),
            ).fetchall()
            for stored in rows:
                validated_decision(stored)
            return None
        except StoreError:
            raise
        except (sqlite3.Error, ValueError, TypeError, KeyError, contracts.ContractError) as exc:
            raise StoreError(f"logical decision is unreadable: {exc}") from exc

    def decision_id_exists(self, decision_id: str) -> bool:
        """Test an accepted identifier before allocating a distinct plan owner."""

        try:
            return self._require_connection().execute(
                "SELECT 1 FROM decisions WHERE decision_id=?", (decision_id,)
            ).fetchone() is not None
        except sqlite3.Error as exc:
            raise StoreError(f"cannot check decision identifier: {exc}") from exc

    def record_operation(self, operation: Mapping[str, Any]) -> dict[str, Any]:
        """Advance an existing intent; observation can never create one."""
        contracts.validate_operation(operation)
        connection = self._require_connection()
        operation_id = str(operation["operation_id"])
        existing_row = connection.execute(
            "SELECT * FROM operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if existing_row is None:
            raise OperationConflictError("dispatch observation requires a matching prior intent")
        existing = self._operation_from_row(existing_row)
        binding = connection.execute(
            "SELECT * FROM dispatch_bindings WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if binding is not None:
            bound_envelope = json.loads(binding["envelope_record"])
            if operation.get("envelope_record") != bound_envelope:
                raise OperationConflictError("dispatch transition envelope differs from its exact intent")
            self._validate_final_dispatch_binding(bound_envelope, current=False)
        elif operation.get("envelope_record") is not None:
            raise OperationConflictError("finalized dispatch has no exact intent binding")
        if operation["status"] == "delivered" and binding is not None and binding["native_receipt_required"]:
            self._validate_native_receipt(operation.get("observed_invocation"))
        for field in ("decision_id", "kind", "envelope_digest", "run_id"):
            if existing[field] != operation[field]:
                raise OperationConflictError(
                    f"operation identity conflict for {operation_id}: {field} differs"
                )
        if (
            existing["status"] == operation["status"]
            and existing["observed_invocation"] == operation.get("observed_invocation")
        ):
            return self.get_operation(operation_id)
        allowed = {
            ("pending", "ambiguous"),
            ("pending", "delivered"),
            ("ambiguous", "delivered"),
            ("pending", "failed_pre_spawn"),
            ("pending", "abandoned"),
        }
        if (existing["status"], operation["status"]) not in allowed:
            raise OperationConflictError(
                f"operation {operation_id} cannot transition from "
                f"{existing['status']!r} to {operation['status']!r}"
            )
        observed = operation.get("observed_invocation")
        observed_json = json.dumps(observed, sort_keys=True) if observed is not None else None
        with connection:
            cursor = connection.execute(
                "UPDATE operations SET status=?, observed_invocation=?, updated_at=? "
                "WHERE operation_id=? AND status=?",
                (operation["status"], observed_json, operation["created_at"],
                 operation_id, existing["status"]),
            )
            if cursor.rowcount != 1:
                raise OperationConflictError("dispatch operation changed during transition")
            if binding is not None and binding["supersedes_rejected_attempt_id"] is not None:
                if operation["status"] == "failed_pre_spawn":
                    authorization = connection.execute(
                        "DELETE FROM rejected_authorizations WHERE rejected_attempt_id=? "
                        "AND operation_id=? AND state='reserved'",
                        (binding["supersedes_rejected_attempt_id"], operation_id),
                    )
                elif operation["status"] == "delivered":
                    authorization = connection.execute(
                        "UPDATE rejected_authorizations SET state='spent' "
                        "WHERE rejected_attempt_id=? AND operation_id=? AND state='reserved'",
                        (binding["supersedes_rejected_attempt_id"], operation_id),
                    )
                else:
                    authorization = None
                if authorization is not None and authorization.rowcount != 1:
                    raise OperationConflictError("rejected authorization owner changed")
        return self.get_operation(str(operation["operation_id"]))

    @staticmethod
    def _validate_native_receipt(receipt: Any) -> None:
        if not isinstance(receipt, Mapping):
            raise ValueError("native receipt must be an object")
        pid = receipt.get("pid")
        creation = receipt.get("creation_time")
        if (not isinstance(pid, int) or isinstance(pid, bool) or pid < 1
                or not isinstance(creation, str) or not creation
                or receipt.get("invocation_id") != f"controller:{pid}:{creation}"):
            raise ValueError("native receipt must identify the exact controller process")

    def _validate_final_dispatch_binding(self, envelope: Mapping[str, Any], *, current: bool = True) -> None:
        """Check the durable accepted decision and latest exact finalized packet."""
        contracts.validate_record(envelope, contracts.FINAL_ENVELOPE_SCHEMA)
        context = envelope.get("final_context")
        contracts.validate_finalized_context(context)
        if envelope.get("plan_state") != "accepted" or context.get("accepted_by") != "ROOT":
            raise ValueError("dispatch requires a ROOT-accepted plan")
        decision = self.get_decision(str(envelope["decision_id"]))
        for field in ("task_card_digest", "objective_id", "route", "plan_id", "plan_digest",
                      "strategy", "configuration_digest"):
            if envelope.get(field) != decision.get(field):
                raise ValueError(f"dispatch {field} does not match its durable decision")
        if (decision["plan_state"] != "accepted" or decision["state"] == "abandoned"
                or envelope.get("configuration") != decision["configuration"]):
            raise ValueError("dispatch decision is not accepted and active")
        if current:
            latest = self.get_final_context_for_decision(str(envelope["decision_id"]))
            if latest is None or latest != context:
                raise ValueError("dispatch final context is missing or stale")
        row = self._require_connection().execute(
            "SELECT envelope_digest FROM final_contexts WHERE context_id=?",
            (context["context_id"],),
        ).fetchone()
        if row is None or row["envelope_digest"] != envelope["content_hash"]:
            raise ValueError("dispatch envelope does not match its durable final context")
        if (envelope.get("final_context_id") != context["context_id"]
                or envelope.get("final_context_integrity") != context["integrity"]
                or envelope.get("final_context") != context):
            raise ValueError("dispatch context identity mismatch")
        for field in ("lane_id", "run_id", "decision_id", "task", "task_card_digest",
                      "objective_id", "route", "plan_id", "plan_digest", "plan_revision",
                      "accepted_by", "base_commit", "worktree_path", "strategy",
                      "configuration", "configuration_digest", "mandatory_content",
                      "optional_content", "mandatory_digest", "optional_digest", "checkpoint",
                      "execution_role", "invocation_target", "recipient", "delivery_trace"):
            if envelope.get(field) != context.get(field):
                raise ValueError(f"dispatch {field} does not match final context")
        if (envelope.get("delivery", {}).get("omitted") != context.get("omitted")
                or context.get("execution_role") != "worker"):
            raise ValueError("dispatch delivery or role does not match final context")

    def create_dispatch_intent(
        self, envelope: Mapping[str, Any], *, native_receipt_required: bool = True,
        supersedes_rejected_attempt_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically claim an exact final envelope before native spawn."""
        operation = contracts.make_operation(kind="dispatch", envelope=envelope)
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            self._validate_final_dispatch_binding(envelope)
            if supersedes_rejected_attempt_id is not None and (
                not isinstance(supersedes_rejected_attempt_id, str) or not supersedes_rejected_attempt_id
            ):
                raise OperationConflictError("rejected attempt ID must be nonempty")
            existing_binding = connection.execute(
                "SELECT * FROM dispatch_bindings WHERE operation_id=?",
                (operation["operation_id"],),
            ).fetchone()
            if existing_binding is not None:
                if (json.loads(existing_binding["envelope_record"]) != dict(envelope)
                        or existing_binding["supersedes_rejected_attempt_id"] != supersedes_rejected_attempt_id):
                    raise OperationConflictError("dispatch intent identity or authorization conflict")
                return self.get_operation(operation["operation_id"]), False
            conflicts = connection.execute(
                "SELECT operations.*, operations.rowid AS sequence, dispatch_bindings.lane_id FROM operations "
                "LEFT JOIN dispatch_bindings USING(operation_id) "
                "WHERE decision_id=? OR dispatch_bindings.lane_id=?",
                (envelope["decision_id"], envelope["lane_id"]),
            ).fetchall()
            prior_delivered = [row for row in conflicts if row["decision_id"] == envelope["decision_id"]
                               and row["kind"] == "dispatch" and row["status"] == "delivered"]
            if supersedes_rejected_attempt_id is not None:
                attempt_row = connection.execute(
                    "SELECT record FROM rejected_native_attempts WHERE rejected_attempt_id=?",
                    (supersedes_rejected_attempt_id,),
                ).fetchone()
                if attempt_row is None or not prior_delivered:
                    raise OperationConflictError("rejected attempt authorization is missing")
                attempt = json.loads(attempt_row["record"])
                latest = max(prior_delivered, key=lambda row: row["sequence"])
                old_envelope = attempt["terminal_evidence"]["dispatch"]["envelope"]
                same = ("decision_id", "task_card_digest", "objective_id", "plan_id",
                        "plan_digest", "base_commit", "recipient", "route", "lane_id")
                if (attempt["decision_id"] != envelope["decision_id"]
                        or attempt["operation_id"] != latest["operation_id"]
                        or any(row["run_id"] == envelope["run_id"] for row in conflicts
                               if row["decision_id"] == envelope["decision_id"])
                        or old_envelope["final_context_id"] == envelope["final_context_id"]
                        or any(old_envelope[key] != envelope[key] for key in same)
                        or connection.execute(
                            "SELECT 1 FROM rejected_authorizations WHERE rejected_attempt_id=?",
                            (supersedes_rejected_attempt_id,),
                        ).fetchone() is not None
                        or connection.execute(
                            "SELECT 1 FROM outcomes WHERE decision_id=?", (envelope["decision_id"],)
                        ).fetchone() is not None):
                    raise OperationConflictError("rejected attempt does not authorize this corrected run")
                for prior in prior_delivered:
                    if connection.execute(
                        "SELECT 1 FROM rejected_native_attempts WHERE operation_id=?",
                        (prior["operation_id"],),
                    ).fetchone() is None:
                        raise OperationConflictError("prior delivered run has no ROOT-rejected attempt")
            elif prior_delivered:
                raise OperationConflictError("delivered dispatch requires an explicit rejected attempt")
            for row in conflicts:
                if row["kind"] != "dispatch":
                    continue
                if row["operation_id"] == operation["operation_id"]:
                    continue
                if (row["status"] in ("pending", "ambiguous", "abandoned") or
                        (row["decision_id"] == envelope["decision_id"] and
                         row["status"] != "failed_pre_spawn" and
                         supersedes_rejected_attempt_id is None)):
                    raise OperationConflictError("conflicting dispatch owns the decision or lane")
            if supersedes_rejected_attempt_id is not None and any(
                row["decision_id"] == envelope["decision_id"] and
                row["status"] not in ("failed_pre_spawn", "delivered") for row in conflicts
            ):
                raise OperationConflictError("prior dispatch remains unresolved")
            cursor = connection.execute(
                """INSERT OR IGNORE INTO operations
                (operation_id, decision_id, envelope_digest, run_id, kind, status,
                 observed_invocation, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'dispatch', 'pending', NULL, ?, ?)""",
                (operation["operation_id"], operation["decision_id"], operation["envelope_digest"],
                 operation["run_id"], operation["created_at"], operation["created_at"]),
            )
            if cursor.rowcount == 1:
                connection.execute(
                    "INSERT INTO dispatch_bindings "
                    "(operation_id, lane_id, envelope_record, native_receipt_required, "
                    "supersedes_rejected_attempt_id) VALUES (?, ?, ?, ?, ?)",
                    (operation["operation_id"], envelope["lane_id"],
                     self._serialize_record(envelope), int(native_receipt_required),
                     supersedes_rejected_attempt_id),
                )
                if supersedes_rejected_attempt_id is not None:
                    connection.execute(
                        "INSERT INTO rejected_authorizations "
                        "(rejected_attempt_id, operation_id, state) VALUES (?, ?, 'reserved')",
                        (supersedes_rejected_attempt_id, operation["operation_id"]),
                    )
        existing = self.get_operation(operation["operation_id"])
        if existing.get("envelope_record") != dict(envelope):
            raise OperationConflictError("dispatch intent identity conflict")
        return existing, cursor.rowcount == 1

    def create_operation(self, operation: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        """Create an intent exactly once and report whether this call created it."""

        contracts.validate_operation(operation)
        if operation["status"] != "pending" or operation.get("observed_invocation") is not None:
            raise OperationConflictError("only a pending intent may be created")
        if operation.get("envelope_record") is not None:
            raise OperationConflictError("finalized dispatch requires exact final intent creation")
        connection = self._require_connection()
        observed = operation.get("observed_invocation")
        observed_json = json.dumps(observed, sort_keys=True) if observed is not None else None
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM final_contexts WHERE decision_id=? LIMIT 1",
                (operation["decision_id"],),
            ).fetchone() is not None:
                raise OperationConflictError("finalized decision requires exact final dispatch intent")
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO operations (
                    operation_id, decision_id, envelope_digest, run_id,
                    kind, status, observed_invocation, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation["operation_id"], operation["decision_id"],
                    operation["envelope_digest"], operation["run_id"],
                    operation["kind"], operation["status"], observed_json,
                    operation["created_at"], operation["created_at"],
                ),
            )
        return self.get_operation(str(operation["operation_id"])), cursor.rowcount == 1

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"operation not found: {operation_id}")
        result = self._operation_from_row(row)
        binding = connection.execute(
            "SELECT * FROM dispatch_bindings WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if binding is not None:
            result.update({
                "lane_id": binding["lane_id"],
                "envelope_record": json.loads(binding["envelope_record"]),
                "native_receipt_required": binding["native_receipt_required"],
                "supersedes_rejected_attempt_id": binding["supersedes_rejected_attempt_id"],
            })
        return result

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        if result["observed_invocation"] is not None:
            result["observed_invocation"] = json.loads(result["observed_invocation"])
        return result

    def list_operations(self, decision_id: str) -> list[dict[str, Any]]:
        connection = self._require_connection()
        rows = connection.execute(
            "SELECT * FROM operations WHERE decision_id = ? ORDER BY created_at, operation_id",
            (decision_id,),
        ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            results.append(self._operation_from_row(row))
        return results

    def record_outcome(self, outcome: Mapping[str, Any]) -> dict[str, Any]:
        """Legacy/generic fixation; finalized enhanced decisions need native evidence."""
        contracts.validate_outcome(outcome)
        connection = self._require_connection()
        decision_id = str(outcome["decision_id"])
        if self.get_final_context_for_decision(decision_id) is not None:
            raise OperationConflictError("finalized outcome requires exact native terminal evidence")
        existing_row = connection.execute(
            "SELECT * FROM outcomes WHERE decision_id = ?", (decision_id,)
        ).fetchone()
        if existing_row is not None:
            existing = dict(existing_row)
            if existing["outcome_id"] == outcome["outcome_id"]:
                return existing
            raise OutcomeConflictError(
                "terminal outcome conflict for decision "
                f"{decision_id}: existing {existing['outcome_id']}, incoming {outcome['outcome_id']}"
            )
        values = (
            outcome["outcome_id"],
            decision_id,
            outcome["task_card_digest"],
            outcome["objective_id"],
            outcome["plan_id"],
            outcome["plan_digest"],
            outcome["status"],
            outcome["evidence_digest"],
            outcome["linked_run_id"],
            outcome["observed_at"],
            outcome["observed_at"],
        )
        with connection:
            connection.execute(
                """
                INSERT INTO outcomes (
                    outcome_id, decision_id, task_card_digest, objective_id,
                    plan_id, plan_digest, status,
                    evidence_digest, linked_run_id, observed_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
        return self.get_outcome(decision_id)

    def _validate_native_terminal_join(self, evidence: Mapping[str, Any]) -> None:
        """Compare a validated bundle to immutable decision, context and observation."""
        connection = self._require_connection()
        decision_id = str(evidence["decision_id"])
        decision = self.get_decision(decision_id)
        if any(decision.get(key) != value for key, value in evidence["decision"].items() if key != "schema"):
            raise OutcomeConflictError("terminal decision differs from durable decision")
        final_context = self.get_final_context_for_decision(decision_id)
        if final_context != evidence["final_context"]:
            raise OutcomeConflictError("terminal final context differs from durable context")
        dispatch = evidence["dispatch"]
        context_row = connection.execute(
            "SELECT envelope_digest FROM final_contexts WHERE context_id=?",
            (final_context["context_id"],),
        ).fetchone()
        if context_row is None or context_row["envelope_digest"] != dispatch["envelope_digest"]:
            raise OutcomeConflictError("terminal dispatch does not own the durable final context")
        incoming_operation = dispatch["operation"]
        row = connection.execute(
            "SELECT * FROM operations WHERE operation_id=?",
            (incoming_operation["operation_id"],),
        ).fetchone()
        if row is None:
            raise OperationConflictError("terminal outcome has no observed native dispatch")
        durable_operation = self._operation_from_row(row)
        if any(durable_operation.get(key) != value for key, value in incoming_operation.items()):
            raise OperationConflictError("terminal native dispatch differs from durable observation")
        binding = connection.execute(
            "SELECT * FROM dispatch_bindings WHERE operation_id=?",
            (incoming_operation["operation_id"],),
        ).fetchone()
        if binding is not None:
            if json.loads(binding["envelope_record"]) != dispatch["envelope"] or not binding["native_receipt_required"]:
                raise OperationConflictError("terminal dispatch lacks exact native intent binding")
        elif dispatch["envelope"].get("schema") != contracts.ENVELOPE_SCHEMA:
            raise OperationConflictError("terminal finalized dispatch lacks exact native intent binding")

    def record_rejected_native_attempt(self, evidence: Mapping[str, Any]) -> dict[str, Any]:
        """Retain an exact ROOT-rejected run without consuming its quality slot."""
        attempt = contracts.make_rejected_native_attempt(evidence)
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT record FROM rejected_native_attempts WHERE operation_id=?",
                (attempt["operation_id"],),
            ).fetchone()
            if existing is not None:
                if json.loads(existing["record"]) == attempt:
                    return attempt
                raise OutcomeConflictError("rejected native attempt contradicts retained operation evidence")
            self._validate_native_terminal_join(evidence)
            if connection.execute(
                "SELECT 1 FROM outcomes WHERE decision_id=?", (attempt["decision_id"],)
            ).fetchone() is not None:
                raise OutcomeConflictError("fixed quality outcome already owns decision")
            connection.execute(
                "INSERT INTO rejected_native_attempts (rejected_attempt_id, decision_id, "
                "operation_id, run_id, result_digest, review_digest, acceptance_digest, "
                "evidence_digest, record) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (attempt["rejected_attempt_id"], attempt["decision_id"],
                 attempt["operation_id"], attempt["run_id"], attempt["result_digest"],
                 attempt["review_digest"], attempt["acceptance_digest"],
                 attempt["evidence_digest"], self._serialize_record(attempt)),
            )
        return attempt

    def get_rejected_native_attempt(self, rejected_attempt_id: str) -> dict[str, Any]:
        row = self._require_connection().execute(
            "SELECT record FROM rejected_native_attempts WHERE rejected_attempt_id=?",
            (rejected_attempt_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"rejected native attempt not found: {rejected_attempt_id}")
        return json.loads(row["record"])

    def record_terminal_outcome(
        self, outcome: Mapping[str, Any], evidence: Mapping[str, Any], *,
        supersedes_outcome_id: str | None = None,
    ) -> dict[str, Any]:
        """Atomically join an observed ROOT-accepted dispatch and fix quality."""
        contracts.validate_outcome(outcome)
        contracts.validate_native_terminal_evidence(evidence)
        if evidence["acceptance"]["approval"] != "ACCEPTED":
            raise OutcomeConflictError("terminal outcome requires ROOT ACCEPTED")
        connection = self._require_connection()
        decision_id = str(outcome["decision_id"])
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            self._validate_native_terminal_join(evidence)
            if connection.execute(
                "SELECT 1 FROM rejected_native_attempts WHERE operation_id=?",
                (evidence["dispatch"]["operation"]["operation_id"],),
            ).fetchone() is not None:
                raise OutcomeConflictError("ROOT-rejected run cannot fix quality")
            if (outcome["evidence_digest"] != evidence["content_hash"] or
                    outcome["status"] != evidence["review"]["review_outcome"] or
                    outcome["linked_run_id"] != evidence["run_id"]):
                raise OutcomeConflictError("terminal outcome differs from its evidence")
            existing = connection.execute(
                "SELECT * FROM outcomes WHERE decision_id=?", (decision_id,),
            ).fetchone()
            if existing is not None:
                native = connection.execute(
                    "SELECT * FROM native_outcomes WHERE outcome_id=?",
                    (existing["outcome_id"],),
                ).fetchone()
                if (existing["outcome_id"] == outcome["outcome_id"] and native is not None
                        and json.loads(native["terminal_evidence"]) == dict(evidence)
                        and native["supersedes_outcome_id"] == supersedes_outcome_id):
                    return self.get_outcome(decision_id)
                raise OutcomeConflictError("terminal outcome conflict for decision " + decision_id)
            if supersedes_outcome_id is not None:
                predecessor = connection.execute(
                    "SELECT * FROM outcomes WHERE outcome_id=?", (supersedes_outcome_id,),
                ).fetchone()
                if predecessor is None or predecessor["decision_id"] == decision_id:
                    raise OutcomeConflictError("supersession requires a distinct fixed predecessor")
                old_native = connection.execute(
                    "SELECT terminal_evidence FROM native_outcomes WHERE outcome_id=?",
                    (supersedes_outcome_id,),
                ).fetchone()
                if old_native is None:
                    raise OutcomeConflictError("supersession requires a native predecessor")
                old_evidence = json.loads(old_native["terminal_evidence"])
                if (predecessor["objective_id"] != outcome["objective_id"] or
                        evidence["accepted_plan"].get("supersedes") != predecessor["plan_id"] or
                        evidence["task_card"].get("task") != old_evidence["task_card"].get("task") or
                        evidence["task_card"].get("base_commit") != old_evidence["task_card"].get("base_commit")):
                    raise OutcomeConflictError("supersession requires an explicit linked accepted plan for the same task")
            connection.execute(
                "INSERT INTO outcomes (outcome_id, decision_id, task_card_digest, objective_id, "
                "plan_id, plan_digest, status, evidence_digest, linked_run_id, observed_at, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (outcome["outcome_id"], decision_id, outcome["task_card_digest"],
                 outcome["objective_id"], outcome["plan_id"], outcome["plan_digest"],
                 outcome["status"], outcome["evidence_digest"], outcome["linked_run_id"],
                 outcome["observed_at"], outcome["observed_at"]),
            )
            acceptance = evidence["acceptance"]
            exceptional = bool(acceptance.get("force_accept_reason"))
            connection.execute(
                "INSERT INTO native_outcomes (outcome_id, terminal_evidence, acceptance_status, "
                "exceptional_acceptance, supersedes_outcome_id) VALUES (?, ?, ?, ?, ?)",
                (outcome["outcome_id"], self._serialize_record(evidence),
                 acceptance["approval"], int(exceptional), supersedes_outcome_id),
            )
            self._create_local_effect_intents(outcome, self.get_decision(decision_id))
        return self.get_outcome(decision_id)

    def get_outcome(self, decision_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM outcomes WHERE decision_id = ?", (decision_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"outcome not found for decision: {decision_id}")
        outcome = dict(row)
        native = connection.execute(
            "SELECT * FROM native_outcomes WHERE outcome_id=?", (outcome["outcome_id"],),
        ).fetchone()
        if native is not None:
            outcome.update({
                "terminal_evidence": json.loads(native["terminal_evidence"]),
                "acceptance_status": native["acceptance_status"],
                "exceptional_acceptance": bool(native["exceptional_acceptance"]),
                "supersedes_outcome_id": native["supersedes_outcome_id"],
            })
        return outcome

    def _create_local_effect_intents(
        self, outcome: Mapping[str, Any], decision: Mapping[str, Any]
    ) -> None:
        """Called inside the same write transaction as native quality fixation."""
        connection = self._require_connection()
        captured = decision["configuration"]
        for kind in contracts.LOCAL_EFFECT_KINDS:
            if not effect_submission_enabled(captured, kind):
                continue
            operation_id = contracts.effect_operation_id(outcome["outcome_id"], kind)
            connection.execute(
                "INSERT OR IGNORE INTO effect_operations (operation_id, outcome_id, decision_id, "
                "kind, scope_key, status, configuration, configuration_digest, version, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, 'outcome', 'waiting_source', ?, ?, 0, ?, ?)",
                (operation_id, outcome["outcome_id"], decision["decision_id"], kind,
                 self._serialize_record(captured), decision["configuration_digest"],
                 outcome["observed_at"], outcome["observed_at"]),
            )

    @staticmethod
    def _effect_from_row(row: sqlite3.Row) -> dict[str, Any]:
        record = dict(row)
        for field in ("source_record", "payload_record", "configuration", "acknowledgement",
                      "reconciliation", "claimant_record", "fence_evidence"):
            if record[field] is not None:
                record[field] = json.loads(record[field])
        record["schema"] = contracts.EFFECT_OPERATION_SCHEMA
        return record

    def get_effect_operation(self, operation_id: str) -> dict[str, Any]:
        row = self._require_connection().execute(
            "SELECT * FROM effect_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"effect operation not found: {operation_id}")
        return self._effect_from_row(row)

    def list_effect_operations(
        self, outcome_id: str | None = None, *, actionable_only: bool = False
    ) -> list[dict[str, Any]]:
        """Return history or only work needing source, submission, or reconciliation."""
        clauses, parameters = [], []
        if outcome_id is not None:
            clauses.append("outcome_id=?")
            parameters.append(outcome_id)
        if actionable_only:
            clauses.append("status!='confirmed'")
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._require_connection().execute(
            "SELECT * FROM effect_operations" + where + " ORDER BY outcome_id, kind, scope_key",
            parameters,
        ).fetchall()
        return [self._effect_from_row(row) for row in rows]

    def create_external_effect_operation(
        self, *, kind: str, scope_key: str, source_id: str,
        source_record: Mapping[str, Any], payload: Mapping[str, Any],
        captured_config: Mapping[str, Any] | MemoryConfig,
        current_config: Mapping[str, Any] | MemoryConfig,
    ) -> tuple[dict[str, Any], bool]:
        """Persist an immutable source-specific intent before a remote submission.

        The caller owns source authorization, recipient/privacy checks, and remote
        readback. This store binds their exact records and serializes claims.
        """
        operation_id = contracts.external_effect_operation_id(source_id, kind, scope_key)
        if kind not in ("experience_ingestion", "generated_skill_creation", "procedure_publication"):
            raise contracts.ContractError("unknown governed external effect kind")
        if not isinstance(source_record, Mapping) or not isinstance(source_record.get("schema"), str):
            raise contracts.ContractError("external effect needs a content-bound source record")
        contracts.validate_record(source_record, source_record["schema"])
        if source_id not in source_record.values():
            raise contracts.ContractError("external source ID is not bound to its record")
        if not isinstance(payload, Mapping) or not payload:
            raise contracts.ContractError("external effect payload must be nonempty")
        captured = (asdict(captured_config) if isinstance(captured_config, MemoryConfig)
                    else dict(captured_config))
        resolve_config(captured)  # Validate, but retain the caller's exact capture.
        if not effect_submission_enabled(captured, kind):
            raise OperationConflictError("captured feature is off")
        source_digest = contracts.sha256_hex(source_record)
        payload_digest = contracts.sha256_hex(payload)
        configuration_digest = contracts.sha256_hex(captured)
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM effect_operations WHERE operation_id=?", (operation_id,),
            ).fetchone()
            if row is not None:
                existing = self._effect_from_row(row)
                expected = (kind, scope_key, source_id, source_digest, payload_digest,
                            configuration_digest)
                actual = tuple(existing[field] for field in (
                    "kind", "scope_key", "source_id", "source_digest", "payload_digest",
                    "configuration_digest",
                ))
                if existing["outcome_id"] is not None or actual != expected:
                    raise OperationConflictError("external effect exact replay conflicts")
                return existing, False
            duplicate_source = connection.execute(
                "SELECT operation_id FROM effect_operations WHERE outcome_id IS NULL "
                "AND kind=? AND scope_key=? AND source_digest=?",
                (kind, scope_key, source_digest),
            ).fetchone()
            if duplicate_source is not None:
                raise OperationConflictError("external source already owns this effect scope")
            if not effect_submission_enabled(current_config, kind):
                raise OperationConflictError("current effective feature is off")
            now = contracts.utc_now()
            connection.execute(
                "INSERT INTO effect_operations (operation_id, outcome_id, decision_id, kind, "
                "scope_key, status, source_id, source_digest, source_record, payload_digest, "
                "payload_record, configuration, configuration_digest, version, created_at, updated_at) "
                "VALUES (?, NULL, NULL, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                (operation_id, kind, scope_key, source_id, source_digest,
                 self._serialize_record(source_record), payload_digest,
                 self._serialize_record(payload), self._serialize_record(captured),
                 configuration_digest, now, now),
            )
        return self.get_effect_operation(operation_id), True

    def external_effect_off_state(
        self, operation_id: str, *, current_config: Mapping[str, Any] | MemoryConfig,
    ) -> str:
        """Report pending_off while an exact source-owned mutation is unresolved."""
        operation = self.get_effect_operation(operation_id)
        if operation["outcome_id"] is not None:
            raise OperationConflictError("operation is not source-owned")
        if effect_submission_enabled(current_config, operation["kind"]):
            return "on"
        return "pending_off" if operation["status"] in ("in_flight", "uncertain") else "off"

    @staticmethod
    def _require_effect_claim(operation: Mapping[str, Any], claim_id: str | None) -> None:
        if operation["outcome_id"] is None and claim_id != operation["active_claim_id"]:
            raise OperationConflictError("external effect requires its exact active claim ID")

    def _require_live_effect_claim(self, operation: Mapping[str, Any], claim_id: str | None) -> None:
        self._require_effect_claim(operation, claim_id)
        if self._live_effect_claims.get(operation["operation_id"]) != claim_id:
            raise OperationConflictError("external effect requires its issuing claimant handle")

    @staticmethod
    def _validate_external_claimant(claimant: Mapping[str, Any]) -> None:
        if (not isinstance(claimant, Mapping) or
                set(claimant) != {"schema", "native_invocation_id", "pid",
                                  "process_created_at", "content_hash"} or
                not isinstance(claimant.get("native_invocation_id"), str) or
                not claimant["native_invocation_id"].strip() or
                type(claimant.get("pid")) is not int or claimant["pid"] <= 0 or
                not isinstance(claimant.get("process_created_at"), str) or
                not claimant["process_created_at"].strip()):
            raise contracts.ContractError("external claimant needs exact native invocation and PID creation identity")
        contracts.validate_record(claimant, "external-effect-claimant/v1")

    def isolate_external_effect_claim(
        self, operation_id: str, claim_id: str, fence_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Isolate a stopped source-owned claimant using a constructor-bound verifier.

        This only makes the effect uncertain. The adapter must separately read the
        remote exact state and reconcile absence or same-ID idempotency before retry.
        The verifier receives the stored claimant and exact fence evidence and must
        return ``True`` after checking native PID plus process-creation termination.
        Evidence is a content-hashed ``external-effect-claim-fence/v1`` record with
        operation_id, claim_id, claim_generation, claimant_digest, the exact
        claimant record, and ``terminated: true``; it may carry verifier proof.
        """
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            claimant = operation["claimant_record"]
            if (operation["outcome_id"] is not None or not isinstance(claim_id, str) or
                    not claim_id or claim_id != operation["active_claim_id"] or
                    claimant is None):
                raise OperationConflictError("external claim isolation requires its exact active claimant")
            expected = {
                "operation_id": operation_id, "claim_id": claim_id,
                "claim_generation": operation["claim_generation"],
                "claimant_digest": operation["claimant_digest"],
            }
            if (not isinstance(fence_evidence, Mapping) or
                    any(fence_evidence.get(key) != value for key, value in expected.items()) or
                    type(fence_evidence.get("claim_generation")) is not int or
                    fence_evidence.get("terminated") is not True or
                    fence_evidence.get("claimant_digest") != contracts.sha256_hex(claimant) or
                    contracts.canonical_json(fence_evidence.get("claimant")) !=
                    contracts.canonical_json(claimant)):
                raise OperationConflictError("external claim fence identity conflicts")
            try:
                contracts.validate_record(fence_evidence, "external-effect-claim-fence/v1")
            except contracts.ContractError as exc:
                raise OperationConflictError("external claim fence record is invalid") from exc
            if operation["fence_evidence"] is not None:
                if operation["fence_evidence"] != dict(fence_evidence):
                    raise OperationConflictError("external claim fence replay conflicts")
                return operation
            if operation["status"] != "in_flight":
                raise OperationConflictError("only the current in-flight claim can be isolated")
            verifier = self._external_claim_fence_verifier
            if verifier is None:
                raise OperationConflictError("trusted external claim fence verifier is unavailable")
            try:
                verified = verifier(claimant, fence_evidence)
            except Exception as exc:
                raise OperationConflictError("trusted external claim fence verification failed") from exc
            if verified is not True:
                raise OperationConflictError("trusted external claim fence was not proven")
            connection.execute(
                "UPDATE effect_operations SET status='uncertain', uncertainty=?, fence_evidence=?, "
                "version=version+1, updated_at=? WHERE operation_id=?",
                ("claimant isolated; remote effect unresolved",
                 self._serialize_record(fence_evidence), contracts.utc_now(), operation_id),
            )
        self._live_effect_claims.pop(operation_id, None)
        return self.get_effect_operation(operation_id)

    def reconcile_external_effect_operation(
        self, operation_id: str, *, evidence: Mapping[str, Any], result: str,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        """Record exact readback or demonstrated idempotency under the original ID.

        The trusted adapter supplies evidence bound to this operation. `absent`
        means its readback can prove no mutation exists; `idempotent` means a
        retry with this operation ID is demonstrably deduplicated by the remote.
        Idempotency leaves the remote outcome uncertain until exact resolution.
        """
        if result not in ("acknowledged", "absent", "idempotent"):
            raise contracts.ContractError("unknown external reconciliation result")
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            if operation["outcome_id"] is not None:
                raise OperationConflictError("operation is not source-owned")
            self._require_effect_claim(operation, claim_id)
            expected = {key: operation[key] for key in (
                "operation_id", "kind", "scope_key", "source_digest", "payload_digest",
                "configuration_digest",
            )}
            if (not isinstance(evidence, Mapping) or
                    any(evidence.get(key) != value for key, value in expected.items()) or
                    not isinstance(evidence.get("adapter_proof"), Mapping) or
                    not evidence["adapter_proof"]):
                raise OperationConflictError("external readback is not bound to exact operation")
            if result == "idempotent" and evidence.get("idempotency_key") != operation_id:
                raise OperationConflictError("adapter idempotency does not use original identity")
            if result == "absent" and evidence.get("readback_complete") is not True:
                raise OperationConflictError("exact absence requires complete readback")
            if result == "acknowledged":
                if operation["status"] == "confirmed":
                    if operation["acknowledgement"] != dict(evidence):
                        raise OperationConflictError("external acknowledgement conflicts")
                    return operation
                if operation["status"] not in ("in_flight", "uncertain") and not (
                    operation["status"] == "pending" and operation["reconciliation"] is not None
                ):
                    raise OperationConflictError("effect has no submission to acknowledge")
                status = "confirmed"
            else:
                if operation["status"] != "uncertain":
                    raise OperationConflictError("only an uncertain effect can be retry-authorized")
                status = "pending" if result == "absent" else "uncertain"
            connection.execute(
                "UPDATE effect_operations SET status=?, acknowledgement=?, reconciliation=?, uncertainty=?, "
                "version=version+1, updated_at=? WHERE operation_id=?",
                (status, self._serialize_record(evidence) if status == "confirmed" else None,
                 self._serialize_record(evidence),
                 operation["uncertainty"] if status == "uncertain" else None,
                 contracts.utc_now(), operation_id),
            )
        if status in ("pending", "confirmed"):
            self._live_effect_claims.pop(operation_id, None)
        return self.get_effect_operation(operation_id)

    def settle_external_experience_ingestion(
        self, operation_id: str, ingestion_id: str, *, claim_id: str,
        claim_generation: int, mode: str, reason: str | None = None,
        ingestion_uncertain: bool = False, evidence: Mapping[str, Any] | None = None,
        case_receipts: tuple[Mapping[str, Any], ...] = (),
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        """Settle one claimed EverOS effect and its ingestion in one transaction.

        ``uncertain`` is a voluntary issuing-handle transition; ``acknowledged``
        accepts an exact readback from either handle. Dispositions are
        ``uncertain``, ``already_uncertain``, ``confirmed``, and
        ``already_confirmed``. A confirmed peer wins over a late uncertainty.
        """
        if mode not in ("uncertain", "acknowledged"):
            raise contracts.ContractError("unknown experience settlement mode")
        if (not isinstance(claim_id, str) or not claim_id or
                type(claim_generation) is not int or claim_generation <= 0):
            raise OperationConflictError("settlement requires an exact claim and generation")
        if mode == "uncertain" and (not isinstance(reason, str) or not reason):
            raise contracts.ContractError("uncertainty needs a reason")
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            ingestion = self.get_experience_ingestion(ingestion_id)
            if (operation["outcome_id"] is not None or
                    operation["kind"] != "experience_ingestion" or
                    operation["source_id"] != ingestion["trajectory_id"] or
                    operation["active_claim_id"] != claim_id or
                    operation["claim_generation"] != claim_generation or
                    operation["claimant_record"] is None or
                    operation["claimant_digest"] != contracts.sha256_hex(operation["claimant_record"]) or
                    claim_id != contracts.sha256_hex({
                        "operation_id": operation_id, "claim_generation": claim_generation,
                        "claimant_digest": operation["claimant_digest"],
                    })):
                raise OperationConflictError("experience settlement claim or source differs")
            trajectory = self.get_reviewed_trajectory(ingestion["trajectory_id"])
            self._durable_review_receipt_for_trajectory(trajectory)
            expected_scope_key = contracts.sha256_hex({
                "scope": trajectory["scope"], "destination": ingestion["destination"],
            })
            decision = self.get_decision(trajectory["decision_id"])
            payload = operation["payload_record"]
            scope = trajectory["scope"]
            transport_ids = {
                field: f"mh-{prefix}-{contracts.sha256_hex({'value': scope[field]})[:32]}"
                for field, prefix in (("application", "app"), ("project", "project"),
                                      ("owner", "owner"))
            }
            expected_owner = None
            if isinstance(payload, Mapping):
                if (payload.get("app_id"), payload.get("project_id")) == (
                    scope["application"], scope["project"]
                ):
                    expected_owner = scope["owner"]
                elif (payload.get("app_id"), payload.get("project_id")) == (
                    transport_ids["application"], transport_ids["project"]
                ):
                    expected_owner = transport_ids["owner"]
            if (operation["operation_id"] != contracts.external_effect_operation_id(
                    trajectory["trajectory_id"], "experience_ingestion", expected_scope_key) or
                    operation["source_digest"] != contracts.sha256_hex(trajectory) or
                    operation["source_record"] != trajectory or
                    operation["scope_key"] != expected_scope_key or
                    ingestion["scope"] != trajectory["scope"] or
                    ingestion["scope_digest"] != trajectory["scope_digest"] or
                    not isinstance(payload, Mapping) or
                    operation["payload_digest"] != contracts.sha256_hex(payload) or
                    ingestion["payload_digest"] != operation["payload_digest"] or
                    payload.get("session_id") != ingestion["session_id"] or
                    expected_owner is None or
                    not isinstance(payload.get("messages"), list) or
                    not payload["messages"] or
                    any(not isinstance(message, Mapping) or
                            message.get("sender_id") != expected_owner
                            for message in payload["messages"]) or
                    operation["configuration"] != decision["configuration"] or
                    operation["configuration_digest"] != contracts.sha256_hex(decision["configuration"]) or
                    not effect_submission_enabled(operation["configuration"], "experience_ingestion") or
                    operation["adapter_operation_id"] not in (None, ingestion_id) or
                    operation["adapter_payload_digest"] not in (None, ingestion["payload_digest"])):
                raise OperationConflictError("experience settlement identity differs")

            normalized = [dict(receipt) for receipt in case_receipts]
            if mode == "acknowledged" or evidence is not None or normalized:
                expected_evidence = {key: operation[key] for key in (
                    "operation_id", "kind", "scope_key", "source_digest",
                    "payload_digest", "configuration_digest",
                )}
                proof = evidence.get("adapter_proof") if isinstance(evidence, Mapping) else None
                if (not isinstance(evidence, Mapping) or
                        any(evidence.get(key) != value for key, value in expected_evidence.items()) or
                        not isinstance(proof, Mapping) or
                        proof.get("session_id") != ingestion["session_id"] or
                        proof.get("scope") != trajectory["scope"] or
                        not normalized):
                    raise OperationConflictError("experience readback is not exact")
                case_ids = []
                for receipt in normalized:
                    try:
                        contracts.validate_case_receipt(receipt)
                    except contracts.ContractError as exc:
                        raise OperationConflictError("invalid experience case receipt") from exc
                    if (receipt["ingestion_id"] != ingestion_id or
                            receipt["trajectory_id"] != trajectory["trajectory_id"] or
                            receipt["scope"] != trajectory["scope"] or
                            receipt["scope_digest"] != trajectory["scope_digest"] or
                            receipt["review_receipt_id"] != trajectory["review_receipt_id"] or
                            receipt["review_receipt_digest"] != trajectory["review_receipt_digest"] or
                            receipt["source_case"].get("session_id") != ingestion["session_id"]):
                        raise OperationConflictError("experience case receipt provenance differs")
                    case_ids.append(receipt["case_id"])
                if (len(case_ids) != len(set(case_ids)) or
                        proof.get("case_ids") != sorted(case_ids)):
                    raise OperationConflictError("experience readback case proof is incomplete")
            if operation["status"] == "confirmed":
                if (ingestion["status"] != "confirmed" or
                        operation["adapter_operation_id"] != ingestion_id or
                        operation["acknowledgement"] != {
                            "ingestion_id": ingestion_id,
                            "payload_digest": ingestion["payload_digest"],
                            "case_receipts": [self.get_case_receipt(trajectory["scope"], case_id)
                                              for case_id in ingestion["case_ids"]],
                        } or
                        (evidence is not None and operation["reconciliation"] != dict(evidence)) or
                        (normalized and sorted(normalized, key=lambda item: item["case_id"]) !=
                         operation["acknowledgement"]["case_receipts"])):
                    raise OperationConflictError("confirmed experience settlement differs")
                return operation, ingestion, "already_confirmed"
            if operation["status"] not in ("in_flight", "uncertain") or ingestion["status"] not in ("pending", "uncertain"):
                raise OperationConflictError("experience settlement has no active submission")
            if mode == "uncertain":
                self._require_live_effect_claim(operation, claim_id)
                if operation["status"] == "uncertain" and operation["uncertainty"] != reason:
                    raise OperationConflictError("uncertainty reason conflicts")
                if ingestion["status"] == "uncertain" and ingestion["error"] != reason:
                    raise OperationConflictError("ingestion uncertainty reason conflicts")
                if ingestion_uncertain and ingestion["status"] == "pending":
                    updated = dict(ingestion, status="uncertain", error=reason,
                                   version=ingestion["version"] + 1)
                    updated["content_hash"] = contracts.content_hash(updated)
                    contracts.validate_experience_ingestion(updated)
                    connection.execute(
                        "UPDATE experience_ingestions SET status='uncertain', error=?, version=?, "
                        "content_hash=?, updated_at=? WHERE ingestion_id=?",
                        (reason, updated["version"], updated["content_hash"],
                         contracts.utc_now(), ingestion_id),
                    )
                changed = operation["status"] != "uncertain" or operation["adapter_operation_id"] is None
                if changed:
                    connection.execute(
                        "UPDATE effect_operations SET status='uncertain', uncertainty=?, "
                        "adapter_operation_id=?, adapter_payload_digest=?, version=version+1, "
                        "updated_at=? WHERE operation_id=?",
                        (reason, ingestion_id, ingestion["payload_digest"],
                         contracts.utc_now(), operation_id),
                    )
                return (self.get_effect_operation(operation_id),
                        self.get_experience_ingestion(ingestion_id),
                        "uncertain" if changed or (ingestion_uncertain and ingestion["status"] == "pending")
                        else "already_uncertain")

            for receipt in normalized:
                prior = connection.execute(
                    "SELECT * FROM experience_case_receipts WHERE case_id=? AND scope_digest=?",
                    (receipt["case_id"], receipt["scope_digest"]),
                ).fetchone()
                if prior is not None:
                    if self._case_receipt_from_row(prior) != receipt:
                        raise OperationConflictError("experience case receipt conflicts")
                    continue
                connection.execute(
                    "INSERT INTO experience_case_receipts (case_receipt_id, case_id, trajectory_id, "
                    "ingestion_id, scope_digest, scope, review_receipt_id, review_receipt_digest, "
                    "source_case, source_case_digest, created_at, content_hash) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (receipt["case_receipt_id"], receipt["case_id"], receipt["trajectory_id"],
                     receipt["ingestion_id"], receipt["scope_digest"],
                     json.dumps(receipt["scope"], sort_keys=True), receipt["review_receipt_id"],
                     receipt["review_receipt_digest"],
                     json.dumps(receipt["source_case"], sort_keys=True),
                     receipt["source_case_digest"], receipt["created_at"], receipt["content_hash"]),
                )
            updated = dict(ingestion, status="confirmed", case_ids=sorted(case_ids),
                           error=None, version=ingestion["version"] + 1)
            updated["content_hash"] = contracts.content_hash(updated)
            contracts.validate_experience_ingestion(updated)
            connection.execute(
                "UPDATE experience_ingestions SET status='confirmed', case_ids=?, error=NULL, "
                "version=?, content_hash=?, updated_at=? WHERE ingestion_id=?",
                (json.dumps(updated["case_ids"], sort_keys=True), updated["version"],
                 updated["content_hash"], contracts.utc_now(), ingestion_id),
            )
            acknowledgement = {"ingestion_id": ingestion_id,
                               "payload_digest": ingestion["payload_digest"],
                               "case_receipts": sorted(normalized, key=lambda item: item["case_id"])}
            connection.execute(
                "UPDATE effect_operations SET status='confirmed', adapter_operation_id=?, "
                "adapter_payload_digest=?, acknowledgement=?, reconciliation=?, uncertainty=NULL, "
                "version=version+1, updated_at=? WHERE operation_id=?",
                (ingestion_id, ingestion["payload_digest"], self._serialize_record(acknowledgement),
                 self._serialize_record(dict(evidence)), contracts.utc_now(), operation_id),
            )
            self._bridge_experience_ingestion_in_transaction(updated)
            return (self.get_effect_operation(operation_id),
                    self.get_experience_ingestion(ingestion_id), "confirmed")

    def bind_effect_source(
        self, operation_id: str, source_id: str, source_record: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Bind an exact durable local source; a different source cannot replace it."""
        if not isinstance(source_id, str) or not source_id or not isinstance(source_record, Mapping):
            raise contracts.ContractError("effect source requires an ID and record")
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            self._bind_effect_source_in_transaction(operation_id, source_id, source_record)
        return self.get_effect_operation(operation_id)

    def _bind_effect_source_in_transaction(
        self, operation_id: str, source_id: str, source_record: Mapping[str, Any]
    ) -> None:
        operation = self.get_effect_operation(operation_id)
        self._validate_effect_source(operation, source_id, source_record)
        digest = contracts.sha256_hex(source_record)
        if operation["source_digest"] is not None:
            if (operation["source_id"], operation["source_digest"]) != (source_id, digest):
                raise OperationConflictError("effect operation has a different source")
            return
        status = "waiting_payload" if operation["kind"] in (
            "experience_ingestion", "generated_skill_creation"
        ) else "confirmed"
        acknowledgement = ({"local_source_id": source_id, "source_digest": digest}
                           if status == "confirmed" else None)
        self._require_connection().execute(
            "UPDATE effect_operations SET source_id=?, source_digest=?, source_record=?, "
            "status=?, acknowledgement=?, version=version+1, updated_at=? WHERE operation_id=?",
            (source_id, digest, self._serialize_record(source_record), status,
             self._serialize_record(acknowledgement) if acknowledgement else None,
            contracts.utc_now(), operation_id),
        )

    def _bridge_local_source_in_transaction(
        self, outcome_id: str, kind: str, source_id: str, source_record: Mapping[str, Any]
    ) -> None:
        connection = self._require_connection()
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='effect_operations'"
        ).fetchone() is None:
            return  # Accepted pre-migration stores retain their review methods.
        operation_id = contracts.effect_operation_id(outcome_id, kind)
        if connection.execute(
            "SELECT 1 FROM effect_operations WHERE operation_id=?", (operation_id,)
        ).fetchone() is not None:
            self._bind_effect_source_in_transaction(operation_id, source_id, source_record)

    def _validate_effect_source(
        self, operation: Mapping[str, Any], source_id: str, source_record: Mapping[str, Any]
    ) -> None:
        table, key = {
            "review_receipt": ("review_receipts", "review_receipt_id"),
            "recent_evidence": ("reviewed_trajectories", "trajectory_id"),
            "experience_ingestion": ("reviewed_trajectories", "trajectory_id"),
            "generated_skill_creation": ("reviewed_trajectories", "trajectory_id"),
        }.get(operation["kind"], (None, None))
        if table is None:
            return  # STEP-10 source validation belongs to its source-specific adapter.
        if source_record.get(key) != source_id or source_record.get("outcome_id") != operation["outcome_id"]:
            raise OperationConflictError("effect source does not match fixed outcome")
        row = self._require_connection().execute(
            f"SELECT 1 FROM {table} WHERE {key}=? AND outcome_id=?",
            (source_id, operation["outcome_id"]),
        ).fetchone()
        if row is None:
            raise OperationConflictError("effect source is not durable for fixed outcome")
        durable = (self.get_review_receipt(source_id) if table == "review_receipts"
                   else self.get_reviewed_trajectory(source_id))
        if durable != dict(source_record):
            raise OperationConflictError("effect source differs from exact durable record")

    def bind_effect_payload(
        self, operation_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Retain exact adapter payload before any optional submission."""
        if not isinstance(payload, Mapping) or not payload:
            raise contracts.ContractError("effect payload must be a nonempty record")
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            if operation["source_record"] is None:
                raise OperationConflictError("effect source must be durable before payload")
            if operation["kind"] in ("review_receipt", "recent_evidence"):
                raise OperationConflictError("local evidence has no separate submission payload")
            digest = contracts.sha256_hex(payload)
            if (operation["adapter_payload_digest"] is not None and
                    operation["adapter_payload_digest"] != digest):
                raise OperationConflictError("effect payload differs from existing ingestion identity")
            if operation["payload_digest"] is not None:
                if operation["payload_digest"] != digest:
                    raise OperationConflictError("effect operation has a different payload")
                return operation
            if operation["status"] != "waiting_payload":
                raise OperationConflictError("effect operation cannot accept a payload in current state")
            connection.execute(
                "UPDATE effect_operations SET payload_digest=?, payload_record=?, status='pending', "
                "version=version+1, updated_at=? WHERE operation_id=?",
                (digest, self._serialize_record(payload), contracts.utc_now(), operation_id),
            )
        return self.get_effect_operation(operation_id)

    def claim_effect_operation(
        self, operation_id: str, *, current_config: Mapping[str, Any] | MemoryConfig,
        claimant: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Claim pending work or a proved same-ID retry; off blocks either claim.

        A new submission consumes an absence proof, so a later lost acknowledgement
        cannot reuse that proof as permission for another retry.
        Source-owned callers may supply a content-bound native claimant identity;
        the returned active_claim_id must accompany later mutation/reconciliation.
        The ``external-effect-claimant/v1`` record contains native_invocation_id,
        positive pid, process_created_at, and content_hash. The creation value is
        an opaque exact identity supplied by the trusted native composition.
        """
        if claimant is not None:
            self._validate_external_claimant(claimant)
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            if claimant is not None and operation["outcome_id"] is not None:
                raise OperationConflictError("claimant-aware claims require a source-owned operation")
            if (claimant is None and operation["outcome_id"] is None and
                    operation["claim_generation"] > 0):
                raise OperationConflictError("source-owned retry requires a new exact claimant")
            if not effect_submission_enabled(current_config, operation["kind"]):
                raise OperationConflictError("current effective feature is off")
            if operation["outcome_id"] is not None and operation["kind"] == "experience_ingestion" and (
                operation["adapter_operation_id"] is not None or
                self.get_experience_ingestion_for_trajectory(operation["source_id"]) is not None
            ):
                raise OperationConflictError("existing experience ingestion requires exact reconciliation")
            idempotent_retry = (
                operation["outcome_id"] is None and operation["status"] == "uncertain" and
                operation["reconciliation"] is not None and
                operation["reconciliation"].get("idempotency_key") == operation_id
            )
            if operation["status"] != "pending" and not idempotent_retry:
                raise OperationConflictError("effect is not pending; reconcile existing claim")
            generation = operation["claim_generation"] + 1 if claimant is not None else 0
            claimant_digest = contracts.sha256_hex(claimant) if claimant is not None else None
            claim_id = (contracts.sha256_hex({"operation_id": operation_id,
                                              "claim_generation": generation,
                                              "claimant_digest": claimant_digest})
                        if claimant is not None else None)
            connection.execute(
                "UPDATE effect_operations SET status='in_flight', "
                "reconciliation=CASE WHEN outcome_id IS NULL AND status='pending' "
                "THEN NULL ELSE reconciliation END, claim_generation=?, active_claim_id=?, "
                "claimant_record=?, claimant_digest=?, fence_evidence=NULL, "
                "version=version+1, updated_at=? WHERE operation_id=?",
                (generation, claim_id,
                 self._serialize_record(claimant) if claimant is not None else None,
                 claimant_digest, contracts.utc_now(), operation_id),
            )
        self._live_effect_claims.pop(operation_id, None)
        if claim_id is not None:
            self._live_effect_claims[operation_id] = claim_id
        return self.get_effect_operation(operation_id)

    def mark_effect_uncertain(
        self, operation_id: str, reason: str, *, claim_id: str | None = None,
    ) -> dict[str, Any]:
        """Let the exact active claimant declare its submission ambiguous."""
        if not isinstance(reason, str) or not reason:
            raise contracts.ContractError("uncertainty needs a reason")
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            self._require_effect_claim(operation, claim_id)
            if operation["status"] not in ("in_flight", "uncertain"):
                raise OperationConflictError("only an in-flight effect can become uncertain")
            if operation["status"] == "in_flight" and operation["claimant_record"] is not None:
                self._require_live_effect_claim(operation, claim_id)
            if operation["status"] == "uncertain" and operation["uncertainty"] != reason:
                raise OperationConflictError("uncertainty reason conflicts")
            connection.execute(
                "UPDATE effect_operations SET status='uncertain', uncertainty=?, version=version+1, "
                "updated_at=? WHERE operation_id=? AND status='in_flight'",
                (reason, contracts.utc_now(), operation_id),
            )
        self._live_effect_claims.pop(operation_id, None)
        return self.get_effect_operation(operation_id)

    def confirm_effect_operation(
        self, operation_id: str, acknowledgement: Mapping[str, Any], *,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(acknowledgement, Mapping) or not acknowledgement:
            raise contracts.ContractError("confirmation requires an exact acknowledgement")
        if self.get_effect_operation(operation_id)["outcome_id"] is None:
            return self.reconcile_external_effect_operation(
                operation_id, evidence=acknowledgement, result="acknowledged",
                claim_id=claim_id,
            )
        connection = self._require_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            operation = self.get_effect_operation(operation_id)
            if operation["status"] == "confirmed":
                if operation["acknowledgement"] != dict(acknowledgement):
                    raise OperationConflictError("confirmed effect acknowledgement conflicts")
                return operation
            if operation["status"] not in ("in_flight", "uncertain"):
                raise OperationConflictError("effect has no submission to confirm")
            connection.execute(
                "UPDATE effect_operations SET status='confirmed', acknowledgement=?, uncertainty=NULL, "
                "version=version+1, updated_at=? WHERE operation_id=?",
                (self._serialize_record(acknowledgement), contracts.utc_now(), operation_id),
            )
        return self.get_effect_operation(operation_id)

    def record_review_receipt(self, review_receipt: Mapping[str, Any]) -> dict[str, Any]:
        """Persist the exact ROOT receipt that authorizes a later trajectory.

        This is durable retention of the existing review receipt, not a second
        review workflow.  A trajectory may only copy its protected narrative
        and failed hypotheses from this immutable local source.
        """

        contracts.validate_review_receipt(review_receipt)
        connection = self._require_connection()
        durable_outcome = connection.execute(
            "SELECT * FROM outcomes WHERE outcome_id = ?",
            (review_receipt["outcome_id"],),
        ).fetchone()
        if durable_outcome is None:
            raise StoreError(
                "review receipt outcome is not durable: "
                f"{review_receipt['outcome_id']}"
            )
        outcome = dict(durable_outcome)
        expected_outcome_fields = {
            "decision_id": review_receipt["decision_id"],
            "task_card_digest": review_receipt["task_card_digest"],
            "objective_id": review_receipt["objective_id"],
            "plan_id": review_receipt["plan_id"],
            "plan_digest": review_receipt["plan_digest"],
            "linked_run_id": review_receipt["run_id"],
            "observed_at": review_receipt["reviewed_at"],
        }
        for field, expected in expected_outcome_fields.items():
            if outcome[field] != expected:
                raise TrajectoryConflictError(
                    "review receipt does not match durable outcome "
                    f"for {field}"
                )
        receipt_id = str(review_receipt["review_receipt_id"])
        existing = connection.execute(
            "SELECT * FROM review_receipts WHERE review_receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        if existing is not None:
            restored = self._review_receipt_from_row(existing)
            if restored["content_hash"] != review_receipt["content_hash"]:
                raise TrajectoryConflictError(
                    f"review receipt conflict for {receipt_id}"
                )
            return restored
        linked = connection.execute(
            "SELECT review_receipt_id FROM review_receipts WHERE outcome_id = ?",
            (review_receipt["outcome_id"],),
        ).fetchone()
        if linked is not None:
            raise TrajectoryConflictError(
                "terminal outcome already has a different durable review receipt: "
                f"{review_receipt['outcome_id']}"
            )
        with connection:
            connection.execute(
                """
                INSERT INTO review_receipts (
                    review_receipt_id, outcome_id, review_id, decision_id,
                    task_card_digest, task_text, objective_id, run_id, plan_id, plan_digest,
                    route,
                    reviewed_by, state, evidence_refs, protected_source_refs,
                    raw_evidence, failed_hypotheses, reviewed_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_receipt["review_receipt_id"],
                    review_receipt["outcome_id"],
                    review_receipt["review_id"],
                    review_receipt["decision_id"],
                    review_receipt["task_card_digest"],
                    review_receipt["task_text"],
                    review_receipt["objective_id"],
                    review_receipt["run_id"],
                    review_receipt["plan_id"],
                    review_receipt["plan_digest"],
                    review_receipt["route"],
                    review_receipt["reviewed_by"],
                    review_receipt["state"],
                    json.dumps(review_receipt["evidence_refs"], sort_keys=True),
                    json.dumps(review_receipt["protected_source_refs"], sort_keys=True),
                    review_receipt["raw_evidence"],
                    json.dumps(review_receipt["failed_hypotheses"], sort_keys=False),
                    review_receipt["reviewed_at"],
                    review_receipt["content_hash"],
                ),
            )
            self._bridge_local_source_in_transaction(
                review_receipt["outcome_id"], "review_receipt", receipt_id,
                self.get_review_receipt(receipt_id),
            )
        return self.get_review_receipt(receipt_id)

    def get_review_receipt(self, review_receipt_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM review_receipts WHERE review_receipt_id = ?",
            (review_receipt_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"review receipt not found: {review_receipt_id}")
        return self._review_receipt_from_row(row)

    @staticmethod
    def _review_receipt_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for field in ("evidence_refs", "protected_source_refs", "failed_hypotheses"):
            result[field] = json.loads(result[field])
        result["schema"] = contracts.REVIEW_RECEIPT_SCHEMA
        contracts.validate_review_receipt(result)
        return result

    def _durable_review_receipt_for_trajectory(
        self, trajectory: Mapping[str, Any], *, connection: sqlite3.Connection | None = None
    ) -> dict[str, Any]:
        """Return the full receipt and reject flattened narrative substitution.

        ``connection`` lets a caller-owned read connection (for example the
        read-only one used by the recent-evidence search) perform the identical
        validation.  Every check stays exactly the same; other callers keep
        using the shared store connection.
        """

        if connection is None:
            connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM review_receipts WHERE review_receipt_id = ?",
            (trajectory["review_receipt_id"],),
        ).fetchone()
        if row is None:
            raise TrajectoryConflictError(
                "reviewed trajectory requires a durable review receipt"
            )
        receipt = self._review_receipt_from_row(row)
        expected_fields = {
            "outcome_id": trajectory["outcome_id"],
            "decision_id": trajectory["decision_id"],
            "task_card_digest": trajectory["task_card_digest"],
            "task_text": trajectory["task_text"],
            "objective_id": trajectory["objective_id"],
            "run_id": trajectory["run_id"],
            "plan_id": trajectory["accepted_plan_id"],
            "plan_digest": trajectory["accepted_plan_digest"],
            "route": trajectory["route"],
            "review_id": trajectory["review_id"],
            "content_hash": trajectory["review_receipt_digest"],
            "state": trajectory["review_state"],
            "reviewed_by": trajectory["reviewed_by"],
            "reviewed_at": trajectory["reviewed_at"],
            "evidence_refs": trajectory["evidence_refs"],
            "protected_source_refs": trajectory["protected_source_refs"],
            "raw_evidence": trajectory["raw_evidence"],
            "failed_hypotheses": trajectory["failed_hypotheses"],
        }
        for field, expected in expected_fields.items():
            if receipt[field] != expected:
                raise TrajectoryConflictError(
                    "reviewed trajectory does not match durable review receipt "
                    f"for {field}"
                )
        return receipt

    def record_reviewed_trajectory(
        self, trajectory: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Persist immutable reviewed evidence before any optional extraction."""

        contracts.validate_reviewed_trajectory(trajectory)
        connection = self._require_connection()
        persisted_outcome = connection.execute(
            "SELECT * FROM outcomes WHERE outcome_id = ?",
            (trajectory["outcome_id"],),
        ).fetchone()
        if persisted_outcome is None:
            raise StoreError(
                "reviewed trajectory outcome is not durable: "
                f"{trajectory['outcome_id']}"
            )
        outcome = dict(persisted_outcome)
        expected_outcome_fields = {
            "decision_id": trajectory["decision_id"],
            "task_card_digest": trajectory["task_card_digest"],
            "objective_id": trajectory["objective_id"],
            "plan_id": trajectory["accepted_plan_id"],
            "plan_digest": trajectory["accepted_plan_digest"],
            "evidence_digest": trajectory["evidence_digest"],
            "linked_run_id": trajectory["run_id"],
            "observed_at": trajectory["recorded_at"],
        }
        for field, expected in expected_outcome_fields.items():
            if outcome[field] != expected:
                raise TrajectoryConflictError(
                    "reviewed trajectory does not match durable outcome "
                    f"for {field}"
                )
        expected_status = {
            "PASS": "reviewed_success",
            "FAIL": "reviewed_failure",
            "BLOCKED": "reviewed_failure",
        }.get(outcome["status"])
        if expected_status != trajectory["status"]:
            raise TrajectoryConflictError(
                "reviewed trajectory status does not match durable outcome"
            )
        self._durable_review_receipt_for_trajectory(trajectory)
        trajectory_id = str(trajectory["trajectory_id"])
        existing = connection.execute(
            "SELECT * FROM reviewed_trajectories WHERE trajectory_id = ?",
            (trajectory_id,),
        ).fetchone()
        if existing is not None:
            restored = self._trajectory_from_row(existing)
            if restored["content_hash"] != trajectory["content_hash"]:
                raise TrajectoryConflictError(
                    f"reviewed trajectory conflict for {trajectory_id}"
                )
            return restored
        linked = connection.execute(
            "SELECT trajectory_id FROM reviewed_trajectories WHERE outcome_id = ?",
            (trajectory["outcome_id"],),
        ).fetchone()
        if linked is not None:
            raise TrajectoryConflictError(
                "terminal outcome already has a different reviewed trajectory: "
                f"{trajectory['outcome_id']}"
            )
        values = (
            trajectory["trajectory_id"],
            trajectory["outcome_id"],
            trajectory["scope_digest"],
            json.dumps(trajectory["scope"], sort_keys=True),
            trajectory["task_card_digest"],
            trajectory["task_text"],
            trajectory["objective_id"],
            trajectory["decision_id"],
            trajectory["run_id"],
            trajectory["accepted_plan_id"],
            trajectory["accepted_plan_digest"],
            trajectory["route"],
            trajectory["status"],
            trajectory["review_receipt_id"],
            trajectory["review_id"],
            trajectory["review_receipt_digest"],
            trajectory["review_state"],
            trajectory["reviewed_by"],
            trajectory["reviewed_at"],
            json.dumps(trajectory["evidence_refs"], sort_keys=True),
            json.dumps(trajectory["protected_source_refs"], sort_keys=True),
            json.dumps(trajectory["failed_hypotheses"], sort_keys=False),
            trajectory["raw_evidence"],
            trajectory["evidence_digest"],
            trajectory["recorded_at"],
            trajectory["content_hash"],
        )
        with connection:
            connection.execute(
                """
                INSERT INTO reviewed_trajectories (
                    trajectory_id, outcome_id, scope_digest, scope,
                    task_card_digest, task_text, objective_id, decision_id, run_id,
                    accepted_plan_id, accepted_plan_digest, route, status,
                    review_receipt_id, review_id, review_receipt_digest, review_state,
                    reviewed_by, reviewed_at, evidence_refs,
                    protected_source_refs, failed_hypotheses, raw_evidence,
                    evidence_digest, recorded_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            durable_trajectory = self.get_reviewed_trajectory(trajectory_id)
            for kind in ("recent_evidence", "experience_ingestion", "generated_skill_creation"):
                self._bridge_local_source_in_transaction(
                    trajectory["outcome_id"], kind, trajectory_id, durable_trajectory,
                )
        return self.get_reviewed_trajectory(trajectory_id)

    def get_reviewed_trajectory(self, trajectory_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM reviewed_trajectories WHERE trajectory_id = ?",
            (trajectory_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"reviewed trajectory not found: {trajectory_id}")
        trajectory = self._trajectory_from_row(row)
        self._durable_review_receipt_for_trajectory(trajectory)
        return trajectory

    def list_reviewed_trajectories(
        self, scope: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        connection = self._require_connection()
        expected_scope = contracts.normalize_experience_scope(scope)
        scope_digest = contracts.sha256_hex(expected_scope)
        rows = connection.execute(
            """
            SELECT * FROM reviewed_trajectories
            WHERE scope_digest = ?
            ORDER BY recorded_at, trajectory_id
            """,
            (scope_digest,),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            trajectory = self._trajectory_from_row(row)
            self._durable_review_receipt_for_trajectory(trajectory)
            if trajectory["scope"] == expected_scope:
                result.append(trajectory)
        return result

    @staticmethod
    def _trajectory_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for field in (
            "scope",
            "evidence_refs",
            "protected_source_refs",
            "failed_hypotheses",
        ):
            result[field] = json.loads(result[field])
        result["schema"] = contracts.REVIEWED_TRAJECTORY_SCHEMA
        contracts.validate_reviewed_trajectory(result)
        return result

    def list_recent_reviewed_trajectories(
        self, scope: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        """Return only local evidence whose EverOS representation is unconfirmed.

        The lookup and its durable receipt validation run on a read-only
        connection created and closed in the calling thread, so the accepted
        bounded search can query this local store from its own worker thread.
        The query, the scope proof, and the receipt checks are unchanged.
        """

        connection = self._open_read_only_connection()
        try:
            expected_scope = contracts.normalize_experience_scope(scope)
            scope_digest = contracts.sha256_hex(expected_scope)
            rows = connection.execute(
                """
                SELECT trajectory.* FROM reviewed_trajectories AS trajectory
                WHERE trajectory.scope_digest = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM experience_ingestions AS ingestion
                      WHERE ingestion.trajectory_id = trajectory.trajectory_id
                        AND ingestion.status = 'confirmed'
                  )
                ORDER BY trajectory.recorded_at, trajectory.trajectory_id
                """,
                (scope_digest,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                trajectory = self._trajectory_from_row(row)
                self._durable_review_receipt_for_trajectory(
                    trajectory, connection=connection
                )
                if trajectory["scope"] == expected_scope:
                    result.append(trajectory)
            return result
        finally:
            connection.close()

    def create_experience_ingestion(
        self, ingestion: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """Persist a representation-write intent exactly once before calling EverOS."""

        contracts.validate_experience_ingestion(ingestion)
        connection = self._require_connection()
        with connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO experience_ingestions (
                    ingestion_id, trajectory_id, scope_digest, scope, destination,
                    session_id, payload_digest, status, case_ids, error,
                    version, created_at, content_hash, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingestion["ingestion_id"],
                    ingestion["trajectory_id"],
                    ingestion["scope_digest"],
                    json.dumps(ingestion["scope"], sort_keys=True),
                    ingestion["destination"],
                    ingestion["session_id"],
                    ingestion["payload_digest"],
                    ingestion["status"],
                    json.dumps(ingestion["case_ids"], sort_keys=True),
                    ingestion.get("error"),
                    ingestion["version"],
                    ingestion["created_at"],
                    ingestion["content_hash"],
                    ingestion["created_at"],
                ),
            )
            self._bridge_experience_ingestion_in_transaction(
                self.get_experience_ingestion(str(ingestion["ingestion_id"]))
            )
        persisted = self.get_experience_ingestion(str(ingestion["ingestion_id"]))
        if cursor.rowcount == 0:
            for field in (
                "trajectory_id",
                "scope_digest",
                "destination",
                "session_id",
                "payload_digest",
            ):
                if persisted[field] != ingestion[field]:
                    raise ExperienceConflictError(
                        "experience ingestion identity conflict for "
                        f"{ingestion['ingestion_id']}: {field} differs"
                    )
        return persisted, cursor.rowcount == 1

    def _bridge_experience_ingestion_in_transaction(self, ingestion: Mapping[str, Any]) -> None:
        """Project exact adapter progress into the local effect in the same transaction."""
        connection = self._require_connection()
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='effect_operations'"
        ).fetchone() is None:
            return
        trajectory = connection.execute(
            "SELECT outcome_id FROM reviewed_trajectories WHERE trajectory_id=?",
            (ingestion["trajectory_id"],),
        ).fetchone()
        if trajectory is None:
            return
        operation_id = contracts.effect_operation_id(trajectory["outcome_id"], "experience_ingestion")
        if connection.execute(
            "SELECT 1 FROM effect_operations WHERE operation_id=?", (operation_id,)
        ).fetchone() is None:
            return  # Generic legacy outcomes have no new effect operation.
        operation = self.get_effect_operation(operation_id)
        if operation["source_id"] != ingestion["trajectory_id"]:
            raise ExperienceConflictError("ingestion does not match effect source")
        if operation["adapter_operation_id"] is not None:
            if (operation["adapter_operation_id"] != ingestion["ingestion_id"] or
                    operation["adapter_payload_digest"] != ingestion["payload_digest"]):
                raise ExperienceConflictError("effect already has a different ingestion identity")
        if (operation["payload_digest"] is not None and
                operation["payload_digest"] != ingestion["payload_digest"]):
            raise ExperienceConflictError("ingestion payload differs from retained effect payload")
        if ingestion["status"] == "confirmed":
            receipts = []
            for case_id in ingestion["case_ids"]:
                row = connection.execute(
                    "SELECT * FROM experience_case_receipts WHERE case_id=? AND scope_digest=?",
                    (case_id, ingestion["scope_digest"]),
                ).fetchone()
                if row is None:
                    raise ExperienceConflictError("confirmed ingestion has no exact case receipt")
                receipt = self._case_receipt_from_row(row)
                if (receipt["ingestion_id"] != ingestion["ingestion_id"] or
                        receipt["trajectory_id"] != ingestion["trajectory_id"] or
                        receipt["scope"] != ingestion["scope"]):
                    raise ExperienceConflictError("confirmed ingestion case receipt differs")
                receipts.append(receipt)
            status = "confirmed"
            acknowledgement = {
                "ingestion_id": ingestion["ingestion_id"],
                "payload_digest": ingestion["payload_digest"],
                "case_receipts": receipts,
            }
            uncertainty = None
        elif ingestion["status"] == "pending":
            # The durable intent precedes the remote call. Its outcome may be unknown.
            status = "in_flight" if operation["status"] != "uncertain" else "uncertain"
            acknowledgement = None
            uncertainty = operation["uncertainty"] if status == "uncertain" else None
        else:
            status = "uncertain"
            acknowledgement = None
            uncertainty = ingestion["error"] or "adapter ingestion requires reconciliation"
        if operation["status"] == "confirmed" and (
            status != "confirmed" or operation["acknowledgement"] != acknowledgement
        ):
            raise ExperienceConflictError("confirmed effect conflicts with ingestion evidence")
        if (operation["adapter_operation_id"] == ingestion["ingestion_id"] and
                operation["adapter_payload_digest"] == ingestion["payload_digest"] and
                operation["status"] == status and
                operation["acknowledgement"] == acknowledgement and
                operation["uncertainty"] == uncertainty):
            return
        connection.execute(
            "UPDATE effect_operations SET adapter_operation_id=?, adapter_payload_digest=?, "
            "status=?, acknowledgement=?, uncertainty=?, version=version+1, updated_at=? "
            "WHERE operation_id=?",
            (ingestion["ingestion_id"], ingestion["payload_digest"], status,
             self._serialize_record(acknowledgement) if acknowledgement is not None else None,
             uncertainty, contracts.utc_now(), operation_id),
        )

    def get_experience_ingestion(self, ingestion_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM experience_ingestions WHERE ingestion_id = ?",
            (ingestion_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"experience ingestion not found: {ingestion_id}")
        return self._ingestion_from_row(row)

    def get_experience_ingestion_for_trajectory(
        self, trajectory_id: str
    ) -> dict[str, Any] | None:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM experience_ingestions WHERE trajectory_id = ?",
            (trajectory_id,),
        ).fetchone()
        return self._ingestion_from_row(row) if row is not None else None

    def update_experience_ingestion(
        self,
        ingestion_id: str,
        *,
        status: str,
        expected_version: int,
        case_ids: list[str] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Advance a local operation without converting uncertainty into success."""

        existing = self.get_experience_ingestion(ingestion_id)
        if (
            not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
            or expected_version < 0
        ):
            raise ExperienceConflictError("experience ingestion version must be nonnegative")
        if existing["version"] != expected_version:
            raise ExperienceConflictError(
                f"stale experience ingestion update for {ingestion_id}"
            )
        allowed = {
            "pending": {"pending", "uncertain", "confirmed", "blocked"},
            "uncertain": {"uncertain", "confirmed", "blocked"},
            "blocked": {"blocked"},
            "confirmed": {"confirmed"},
        }
        if status not in allowed.get(existing["status"], set()):
            raise ExperienceConflictError(
                f"experience ingestion {ingestion_id} cannot transition from "
                f"{existing['status']!r} to {status!r}"
            )
        updated = dict(existing)
        updated["status"] = status
        if case_ids is not None:
            updated["case_ids"] = list(case_ids)
        updated["error"] = error
        updated["version"] = expected_version + 1
        updated["content_hash"] = contracts.content_hash(updated)
        contracts.validate_experience_ingestion(updated)
        connection = self._require_connection()
        with connection:
            cursor = connection.execute(
                """
                UPDATE experience_ingestions
                SET status = ?, case_ids = ?, error = ?, version = ?, content_hash = ?, updated_at = ?
                WHERE ingestion_id = ? AND version = ?
                """,
                (
                    updated["status"],
                    json.dumps(updated["case_ids"], sort_keys=True),
                    updated["error"],
                    updated["version"],
                    updated["content_hash"],
                    contracts.utc_now(),
                    ingestion_id,
                    expected_version,
                ),
            )
            if cursor.rowcount != 1:
                raise ExperienceConflictError(
                    f"stale experience ingestion update for {ingestion_id}"
                )
            self._bridge_experience_ingestion_in_transaction(updated)
        return self.get_experience_ingestion(ingestion_id)

    @staticmethod
    def _ingestion_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["scope"] = json.loads(result["scope"])
        result["case_ids"] = json.loads(result["case_ids"])
        result["schema"] = contracts.EXPERIENCE_INGESTION_SCHEMA
        result.pop("updated_at", None)
        contracts.validate_experience_ingestion(result)
        return result

    def confirm_experience_ingestion(
        self,
        ingestion_id: str,
        case_receipts: list[Mapping[str, Any]],
        *,
        expected_version: int,
    ) -> dict[str, Any]:
        """Atomically retain exact case receipts and mark their representation confirmed."""

        existing = self.get_experience_ingestion(ingestion_id)
        if (
            not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
            or expected_version < 0
        ):
            raise ExperienceConflictError("experience ingestion version must be nonnegative")
        if existing["version"] != expected_version:
            raise ExperienceConflictError(
                f"stale experience ingestion confirmation for {ingestion_id}"
            )
        if existing["status"] not in {"pending", "uncertain", "confirmed"}:
            raise ExperienceConflictError(
                f"blocked experience ingestion {ingestion_id} cannot be confirmed"
            )
        if not case_receipts:
            raise ExperienceConflictError("confirmed experience ingestion requires case receipts")
        normalized = [dict(receipt) for receipt in case_receipts]
        case_ids: list[str] = []
        for receipt in normalized:
            contracts.validate_case_receipt(receipt)
            if receipt["ingestion_id"] != ingestion_id:
                raise ExperienceConflictError("case receipt has a different ingestion id")
            if receipt["trajectory_id"] != existing["trajectory_id"]:
                raise ExperienceConflictError("case receipt has a different trajectory")
            if receipt["scope_digest"] != existing["scope_digest"]:
                raise ExperienceConflictError("case receipt has a different scope")
            if receipt["scope"] != existing["scope"]:
                raise ExperienceConflictError("case receipt has a different exact scope")
            case_ids.append(str(receipt["case_id"]))
        if len(case_ids) != len(set(case_ids)):
            raise ExperienceConflictError("case receipt ids must be unique")

        connection = self._require_connection()
        if existing["status"] == "confirmed":
            if sorted(case_ids) != existing["case_ids"]:
                raise ExperienceConflictError(
                    "confirmed experience ingestion cannot gain or lose case receipts"
                )
            for receipt in normalized:
                prior = connection.execute(
                    """
                    SELECT * FROM experience_case_receipts
                    WHERE case_id = ? AND scope_digest = ?
                    """,
                    (receipt["case_id"], receipt["scope_digest"]),
                ).fetchone()
                if (
                    prior is None
                    or self._case_receipt_from_row(prior)["content_hash"]
                    != receipt["content_hash"]
                ):
                    raise ExperienceConflictError(
                        "confirmed experience ingestion receipt changed for "
                        f"{receipt['case_id']}"
                    )
            return existing
        with connection:
            for receipt in normalized:
                prior = connection.execute(
                    """
                    SELECT * FROM experience_case_receipts
                    WHERE case_id = ? AND scope_digest = ?
                    """,
                    (receipt["case_id"], receipt["scope_digest"]),
                ).fetchone()
                if prior is not None:
                    restored = self._case_receipt_from_row(prior)
                    if restored["content_hash"] != receipt["content_hash"]:
                        raise ExperienceConflictError(
                            "case receipt conflict for case " f"{receipt['case_id']}"
                        )
                    continue
                connection.execute(
                    """
                    INSERT INTO experience_case_receipts (
                        case_receipt_id, case_id, trajectory_id, ingestion_id,
                        scope_digest, scope, review_receipt_id, review_receipt_digest,
                        source_case,
                        source_case_digest, created_at, content_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        receipt["case_receipt_id"],
                        receipt["case_id"],
                        receipt["trajectory_id"],
                        receipt["ingestion_id"],
                        receipt["scope_digest"],
                        json.dumps(receipt["scope"], sort_keys=True),
                        receipt["review_receipt_id"],
                        receipt["review_receipt_digest"],
                        json.dumps(receipt["source_case"], sort_keys=True),
                        receipt["source_case_digest"],
                        receipt["created_at"],
                        receipt["content_hash"],
                    ),
                )
            updated = dict(existing)
            updated["status"] = "confirmed"
            updated["case_ids"] = sorted(case_ids)
            updated["error"] = None
            updated["version"] = expected_version + 1
            updated["content_hash"] = contracts.content_hash(updated)
            contracts.validate_experience_ingestion(updated)
            cursor = connection.execute(
                """
                UPDATE experience_ingestions
                SET status = ?, case_ids = ?, error = NULL, version = ?, content_hash = ?, updated_at = ?
                WHERE ingestion_id = ? AND version = ?
                """,
                (
                    updated["status"],
                    json.dumps(updated["case_ids"], sort_keys=True),
                    updated["version"],
                    updated["content_hash"],
                    contracts.utc_now(),
                    ingestion_id,
                    expected_version,
                ),
            )
            if cursor.rowcount != 1:
                raise ExperienceConflictError(
                    f"stale experience ingestion confirmation for {ingestion_id}"
                )
            self._bridge_experience_ingestion_in_transaction(updated)
        return self.get_experience_ingestion(ingestion_id)

    def read_confirmed_case_join(
        self, scope: Mapping[str, Any], case_id: str
    ) -> dict[str, Any]:
        """Return the exact durable confirmed-receipt join for one case.

        The accepted reconcile path confirms one EverOS case receipt only from
        an exact scoped search, so a later read that wants to reuse that case
        re-establishes the whole chain instead of trusting a remote hit: the
        case receipt, its confirmed ingestion, the reviewed trajectory, and the
        durable review receipt all have to agree on the same case id, ingestion,
        trajectory, review receipt, and exact four-part scope, and the retained
        source case's session must equal its confirmed ingestion session exactly
        -- the same equality the accepted reconcile path required before any
        receipt could be confirmed.

        The lookup runs on one read-only connection created and closed by the
        calling thread, because the accepted bounded search queries every store
        on its own bounded worker thread and the shared store connection is
        thread-affine.  It never writes, migrates, or changes shared state, and
        every check fails closed: a missing, unconfirmed, altered, unreviewed,
        or out-of-scope chain raises instead of returning a partial record.
        """

        if not isinstance(case_id, str) or not case_id.strip():
            raise StoreError("case id must be a nonempty string")
        connection = self._open_read_only_connection()
        try:
            expected_scope = contracts.normalize_experience_scope(scope)
            scope_digest = contracts.sha256_hex(expected_scope)
            row = connection.execute(
                """
                SELECT * FROM experience_case_receipts
                WHERE case_id = ? AND scope_digest = ?
                """,
                (case_id, scope_digest),
            ).fetchone()
            if row is None:
                raise StoreError(f"case receipt not found for {case_id}")
            case_receipt = self._case_receipt_from_row(row)
            if case_receipt["scope"] != expected_scope:
                raise StoreError(
                    f"case receipt is outside the requested scope: {case_id}"
                )
            ingestion_row = connection.execute(
                "SELECT * FROM experience_ingestions WHERE ingestion_id = ?",
                (case_receipt["ingestion_id"],),
            ).fetchone()
            if ingestion_row is None:
                raise StoreError(f"case receipt has no durable ingestion: {case_id}")
            ingestion = self._ingestion_from_row(ingestion_row)
            if ingestion["status"] != "confirmed":
                raise StoreError(
                    "case receipt ingestion is not confirmed: "
                    f"{ingestion['status']}"
                )
            if (
                ingestion["scope"] != expected_scope
                or ingestion["scope_digest"] != scope_digest
                or ingestion["trajectory_id"] != case_receipt["trajectory_id"]
            ):
                raise StoreError(
                    f"confirmed ingestion is outside the case receipt scope: {case_id}"
                )
            if case_id not in ingestion["case_ids"]:
                raise StoreError(
                    f"confirmed ingestion does not claim the case receipt: {case_id}"
                )
            if (
                case_receipt["source_case"].get("session_id")
                != ingestion["session_id"]
            ):
                # A direct public store confirmation can disagree with the
                # accepted reconcile path, which required this exact equality
                # (``validate_case``) before any receipt could be confirmed.
                raise StoreError(
                    f"case receipt source session does not match its ingestion: {case_id}"
                )
            trajectory_row = connection.execute(
                "SELECT * FROM reviewed_trajectories WHERE trajectory_id = ?",
                (case_receipt["trajectory_id"],),
            ).fetchone()
            if trajectory_row is None:
                raise StoreError(
                    f"case receipt has no durable reviewed trajectory: {case_id}"
                )
            trajectory = self._trajectory_from_row(trajectory_row)
            if trajectory["scope"] != expected_scope:
                raise StoreError(
                    f"reviewed trajectory is outside the requested scope: {case_id}"
                )
            if trajectory["status"] not in contracts.REVIEWED_TRAJECTORY_STATUSES:
                raise StoreError(
                    f"reviewed trajectory is not a reviewed status: {case_id}"
                )
            if trajectory["review_state"] not in contracts.REVIEW_STATES:
                raise StoreError(
                    f"reviewed trajectory is not in a review state: {case_id}"
                )
            if (
                case_receipt["review_receipt_id"] != trajectory["review_receipt_id"]
                or case_receipt["review_receipt_digest"]
                != trajectory["review_receipt_digest"]
            ):
                raise StoreError(f"case receipt review binding changed: {case_id}")
            review_receipt = self._durable_review_receipt_for_trajectory(
                trajectory, connection=connection
            )
            return {
                "case_receipt": case_receipt,
                "ingestion": ingestion,
                "trajectory": trajectory,
                "review_receipt": review_receipt,
            }
        finally:
            connection.close()

    def get_case_receipt(
        self, scope: Mapping[str, Any], case_id: str
    ) -> dict[str, Any]:
        connection = self._require_connection()
        expected_scope = contracts.normalize_experience_scope(scope)
        row = connection.execute(
            """
            SELECT * FROM experience_case_receipts
            WHERE case_id = ? AND scope_digest = ?
            """,
            (case_id, contracts.sha256_hex(expected_scope)),
        ).fetchone()
        if row is None:
            raise StoreError(f"case receipt not found for {case_id}")
        result = self._case_receipt_from_row(row)
        if result["scope"] != expected_scope:
            raise StoreError(f"case receipt is outside the requested scope: {case_id}")
        return result

    @staticmethod
    def _case_receipt_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["scope"] = json.loads(result["scope"])
        result["source_case"] = json.loads(result["source_case"])
        result["schema"] = contracts.CASE_RECEIPT_SCHEMA
        contracts.validate_case_receipt(result)
        return result

    def record_generated_skill_candidate(
        self, candidate: Mapping[str, Any]
    ) -> dict[str, Any]:
        contracts.validate_generated_skill_candidate(candidate)
        connection = self._require_connection()
        candidate_id = str(candidate["candidate_id"])
        existing = connection.execute(
            "SELECT * FROM generated_skill_candidates WHERE candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if existing is not None:
            restored = self._generated_skill_from_row(existing)
            identity_fields = (
                "skill_id",
                "origin",
                "state",
                "scope_digest",
                "scope",
                "content",
                "content_digest",
                "source_cases",
                "metadata",
            )
            if any(restored[field] != candidate[field] for field in identity_fields):
                raise ExperienceConflictError(
                    f"generated skill candidate conflict for {candidate_id}"
                )
            return restored
        with connection:
            connection.execute(
                """
                INSERT INTO generated_skill_candidates (
                    candidate_id, skill_id, origin, state, scope_digest, scope, content,
                    content_digest, source_cases, metadata, created_at, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate["candidate_id"],
                    candidate["skill_id"],
                    candidate["origin"],
                    candidate["state"],
                    candidate["scope_digest"],
                    json.dumps(candidate["scope"], sort_keys=True),
                    candidate["content"],
                    candidate["content_digest"],
                    json.dumps(candidate["source_cases"], sort_keys=True),
                    json.dumps(candidate["metadata"], sort_keys=True),
                    candidate["created_at"],
                    candidate["content_hash"],
                ),
            )
        return self.get_generated_skill_candidate(candidate_id)

    def get_generated_skill_candidate(self, candidate_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM generated_skill_candidates WHERE candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"generated skill candidate not found: {candidate_id}")
        return self._generated_skill_from_row(row)

    @staticmethod
    def _generated_skill_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for field in ("scope", "source_cases", "metadata"):
            result[field] = json.loads(result[field])
        result["schema"] = contracts.GENERATED_SKILL_SCHEMA
        contracts.validate_generated_skill_candidate(result)
        return result

    def read_generated_skill_candidates_for_scope(
        self, skill_id: str, scope: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        """Read the durable Step-02 generated candidates of one exact skill.

        The exact four-part scope and the exact stable skill id are the
        smallest read-only lookup a later remote-skill rejoin needs: a
        discovered EverOS hit is compared only against the durable generated
        candidates this store already holds for that same skill and scope, so
        EverOS presence and its query-dependent ranking score never stand in
        for a stable identity.  The lookup runs on one read-only connection
        created and closed by the calling thread, because the accepted bounded
        search queries every store on its own bounded worker thread while the
        shared store connection is thread-affine.  It never writes, migrates,
        or promotes anything: every returned record remains the
        non-authoritative, proposed Step-02 candidate it was stored as, and no
        approval, designation, or guidance is derived from this read.

        Each row is validated on its own: the stored scope has to equal the
        requested scope exactly, and the accepted column reader revalidates
        the contract identity and the row content hash of that candidate.  A
        foreign, altered, or unreadable row is omitted by itself, so one
        corrupt row can never suppress an unrelated intact candidate of the
        same skill and scope.
        """

        if not isinstance(skill_id, str) or not skill_id.strip():
            raise StoreError("skill id must be a nonempty string")
        connection = self._open_read_only_connection()
        try:
            expected_scope = contracts.normalize_experience_scope(scope)
            rows = connection.execute(
                """
                SELECT * FROM generated_skill_candidates
                WHERE skill_id = ? AND scope_digest = ?
                ORDER BY candidate_id
                """,
                (skill_id, contracts.sha256_hex(expected_scope)),
            ).fetchall()
            candidates: list[dict[str, Any]] = []
            for row in rows:
                try:
                    candidate = self._read_only_skill_candidate(
                        connection, str(row["candidate_id"])
                    )
                except Exception:
                    # One altered or unreadable row is isolated: it is never
                    # returned, and it never suppresses an intact candidate
                    # of the same skill and scope.
                    continue
                if candidate["scope"] != expected_scope:
                    continue
                candidates.append(candidate)
            return candidates
        finally:
            connection.close()

    def record_skill_approval(self, approval: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_skill_approval(approval)
        connection = self._require_connection()
        self._durable_candidate_for_approval(approval)
        approval_id = str(approval["approval_id"])
        existing = connection.execute(
            "SELECT * FROM generated_skill_approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
        if existing is not None:
            restored = self._skill_approval_from_row(existing)
            if restored["content_hash"] != approval["content_hash"]:
                raise ExperienceConflictError(f"skill approval conflict for {approval_id}")
            return restored
        with connection:
            connection.execute(
                """
                INSERT INTO generated_skill_approvals (
                    approval_id, candidate_id, skill_id, origin, scope_digest, scope,
                    content_digest, issuer, recipients, source_cases, approved_at,
                    authority_evidence, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    approval["approval_id"],
                    approval["candidate_id"],
                    approval["skill_id"],
                    approval["origin"],
                    approval["scope_digest"],
                    json.dumps(approval["scope"], sort_keys=True),
                    approval["content_digest"],
                    approval["issuer"],
                    json.dumps(approval["recipients"], sort_keys=True),
                    json.dumps(approval["source_cases"], sort_keys=True),
                    approval["approved_at"],
                    json.dumps(approval["authority_evidence"], sort_keys=True),
                    approval["content_hash"],
                ),
            )
        return self.get_skill_approval(approval_id)

    def get_skill_approval(self, approval_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM generated_skill_approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"skill approval not found: {approval_id}")
        approval = self._skill_approval_from_row(row)
        self._durable_candidate_for_approval(approval)
        return approval

    def _durable_candidate_for_approval(
        self, approval: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Rejoin an approval to its exact candidate before accepting it."""

        try:
            candidate = self.get_generated_skill_candidate(str(approval["candidate_id"]))
        except StoreError as exc:
            raise ExperienceConflictError(
                "skill approval cannot resolve its durable generated candidate"
            ) from exc
        expected_fields = {
            "skill_id": approval["skill_id"],
            "origin": approval["origin"],
            "scope_digest": approval["scope_digest"],
            "scope": approval["scope"],
            "content_digest": approval["content_digest"],
            "source_cases": approval["source_cases"],
        }
        for field, expected in expected_fields.items():
            if candidate[field] != expected:
                raise ExperienceConflictError(
                    "skill approval does not rejoin its exact durable candidate "
                    f"for {field}"
                )
        return candidate

    @staticmethod
    def _skill_approval_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for field in ("scope", "recipients", "source_cases"):
            result[field] = json.loads(result[field])
        authority_evidence = result.get("authority_evidence")
        if not isinstance(authority_evidence, str):
            raise ExperienceConflictError(
                "skill approval has no retained authority evidence"
            )
        result["authority_evidence"] = json.loads(authority_evidence)
        result["schema"] = contracts.SKILL_APPROVAL_SCHEMA
        contracts.validate_skill_approval(result)
        return result

    # Trusted-procedure state lives in separate tables so Stage-A decisions,
    # dispatch operations, and outcomes remain readable without migration loss.

    @staticmethod
    def _stored_record(row: sqlite3.Row, validator: Any) -> dict[str, Any]:
        try:
            record = json.loads(str(row["record"]))
        except (TypeError, ValueError) as exc:
            raise ProcedureConflictError("stored procedure record is not valid JSON") from exc
        if not isinstance(record, dict):
            raise ProcedureConflictError("stored procedure record is not an object")
        try:
            validator(record)
        except contracts.ContractError as exc:
            raise ProcedureConflictError("stored procedure record violates its contract") from exc
        return record

    @staticmethod
    def _serialize_record(record: Mapping[str, Any]) -> str:
        return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def _validate_durable_procedure_source(
        self, procedure: Mapping[str, Any]
    ) -> None:
        """Rejoin generated provenance to the accepted Step-02 evidence.

        Curated/builtin origins retain their explicit provenance reference in
        the procedure record.  Generated material must additionally resolve
        the durable candidate and its already-verified source approval.
        """

        source = procedure["source"]
        if source.get("kind") != "generated_skill":
            return
        required = (
            "candidate_id",
            "candidate_digest",
            "skill_approval_id",
            "skill_approval_digest",
            "source_cases",
        )
        if any(not source.get(field) for field in required):
            raise ProcedureConflictError("generated procedure source lacks exact approval provenance")
        try:
            candidate = self.get_generated_skill_candidate(str(source["candidate_id"]))
            skill_approval = self.get_skill_approval(str(source["skill_approval_id"]))
        except StoreError as exc:
            raise ProcedureConflictError(
                "generated procedure source cannot resolve durable Step-02 evidence"
            ) from exc
        expected = {
            "candidate_digest": candidate["content_hash"],
            "skill_approval_digest": skill_approval["content_hash"],
            "source_cases": candidate["source_cases"],
        }
        if any(source.get(field) != value for field, value in expected.items()):
            raise ProcedureConflictError("generated procedure source provenance changed or is ambiguous")
        if skill_approval["candidate_id"] != candidate["candidate_id"]:
            raise ProcedureConflictError("generated procedure source approval names another candidate")
        if candidate["content"] != procedure["behavior"]["body"]:
            raise ProcedureConflictError("generated procedure body does not match its approved source candidate")

    def record_procedure_revision(self, procedure: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_procedure_revision(procedure)
        self._validate_durable_procedure_source(procedure)
        connection = self._require_connection()
        revision_id = str(procedure["revision_id"])
        existing = connection.execute(
            "SELECT * FROM procedure_revisions WHERE revision_id = ?", (revision_id,)
        ).fetchone()
        if existing is not None:
            restored = self._stored_record(existing, contracts.validate_procedure_revision)
            if restored["content_hash"] != procedure["content_hash"]:
                raise ProcedureConflictError(f"procedure revision conflict for {revision_id}")
            return restored
        with connection:
            connection.execute(
                """
                INSERT INTO procedure_revisions (
                    revision_id, logical_id, origin_scope_digest, record, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    procedure["revision_id"],
                    procedure["logical_id"],
                    procedure["origin_scope_digest"],
                    self._serialize_record(procedure),
                    procedure["content_hash"],
                    procedure["created_at"],
                ),
            )
        return self.get_procedure_revision(revision_id)

    def get_procedure_revision(self, revision_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_revisions WHERE revision_id = ?", (revision_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure revision not found: {revision_id}")
        procedure = self._stored_record(row, contracts.validate_procedure_revision)
        self._validate_durable_procedure_source(procedure)
        return procedure

    def record_procedure_approval(self, approval: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_procedure_approval(approval)
        procedure = self.get_procedure_revision(str(approval["revision_id"]))
        try:
            contracts.validate_procedure_approval(approval, procedure=procedure)
        except contracts.ContractError as exc:
            raise ProcedureConflictError("procedure approval does not bind durable procedure") from exc
        connection = self._require_connection()
        approval_id = str(approval["approval_id"])
        existing = connection.execute(
            "SELECT * FROM procedure_approvals WHERE approval_id = ?", (approval_id,)
        ).fetchone()
        if existing is not None:
            restored = self._stored_record(existing, contracts.validate_procedure_approval)
            if restored["content_hash"] != approval["content_hash"]:
                raise ProcedureConflictError(f"procedure approval conflict for {approval_id}")
            return restored
        with connection:
            connection.execute(
                """
                INSERT INTO procedure_approvals (
                    approval_id, revision_id, logical_id, record, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    approval["approval_id"], approval["revision_id"], approval["logical_id"],
                    self._serialize_record(approval), approval["content_hash"], approval["approved_at"],
                ),
            )
        return self.get_procedure_approval(approval_id)

    def get_procedure_approval(self, approval_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_approvals WHERE approval_id = ?", (approval_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure approval not found: {approval_id}")
        approval = self._stored_record(row, contracts.validate_procedure_approval)
        procedure = self.get_procedure_revision(str(approval["revision_id"]))
        try:
            contracts.validate_procedure_approval(approval, procedure=procedure)
        except contracts.ContractError as exc:
            raise ProcedureConflictError("stored approval does not bind durable procedure") from exc
        return approval

    def record_procedure_representation(
        self, representation: Mapping[str, Any]
    ) -> dict[str, Any]:
        contracts.validate_procedure_representation(representation)
        procedure = self.get_procedure_revision(str(representation["revision_id"]))
        try:
            contracts.validate_procedure_representation(representation, procedure=procedure)
        except contracts.ContractError as exc:
            raise ProcedureConflictError("representation does not bind durable procedure") from exc
        connection = self._require_connection()
        representation_id = str(representation["representation_id"])
        existing = connection.execute(
            "SELECT * FROM procedure_representations WHERE representation_id = ?",
            (representation_id,),
        ).fetchone()
        if existing is not None:
            restored = self._stored_record(existing, contracts.validate_procedure_representation)
            if restored["content_hash"] != representation["content_hash"]:
                raise ProcedureConflictError(
                    f"procedure representation conflict for {representation_id}"
                )
            return restored
        with connection:
            connection.execute(
                """
                INSERT INTO procedure_representations (
                    representation_id, revision_id, logical_id, record, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    representation["representation_id"], representation["revision_id"],
                    representation["logical_id"], self._serialize_record(representation),
                    representation["content_hash"], representation["created_at"],
                ),
            )
        return self.get_procedure_representation(representation_id)

    def get_procedure_representation(self, representation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_representations WHERE representation_id = ?",
            (representation_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure representation not found: {representation_id}")
        representation = self._stored_record(row, contracts.validate_procedure_representation)
        procedure = self.get_procedure_revision(str(representation["revision_id"]))
        try:
            contracts.validate_procedure_representation(representation, procedure=procedure)
        except contracts.ContractError as exc:
            raise ProcedureConflictError("stored representation does not bind durable procedure") from exc
        return representation

    def _current_procedure_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            source_record = json.loads(str(row["record"]))
        except (TypeError, ValueError) as exc:
            raise ProcedureConflictError("stored current procedure state is invalid") from exc
        if not isinstance(source_record, dict):
            raise ProcedureConflictError("stored current procedure state is not an object")
        return {
            "logical_id": row["logical_id"],
            "partition_id": row["partition_id"],
            "designation_id": row["current_id"],
            "revision_id": row["revision_id"],
            "generation": row["generation"],
            "state": row["state"],
            "record": source_record,
            "content_hash": row["content_hash"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def read_local_current_procedures(
        self,
        partition: Mapping[str, Any],
        *,
        receiver: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Read the durable local current procedures of one exact partition.

        The exact partition is the smallest read-only lookup the bounded local
        procedure store needs; the optional explicit ``receiver`` additionally
        discovers durable current rows whose stored project/shared partition
        names that exact recipient even when other recipients make the stored
        partition identity differ from the constructed lookup key.  A
        discovered row keeps its exact stored partition, and the caller still
        re-checks authorization against that durable partition.  The lookup
        runs on a read-only connection created and closed by the calling
        thread, because the accepted bounded search queries every store on its
        own worker thread while the shared store connection is thread-affine;
        it never writes, migrates, or changes shared state.

        Every returned entry is one durable current row joined to its exact
        designation, procedure revision, approval, search representations, and
        revocation tombstone -- plus, for a generated-origin revision, its
        rechecked Step-02 candidate, skill approval, source case, and review
        receipts.  One missing, altered, or unreadable link keeps that row's
        exact identity and an explicit defect instead of raising, so a single
        bad local record can never suppress an unrelated eligible procedure.
        """

        normalized = contracts.normalize_procedure_partition(partition)
        partition_id = contracts.procedure_partition_id(normalized)
        receiver_scope = (
            contracts.normalize_experience_scope(receiver)
            if receiver is not None
            else None
        )
        connection = self._open_read_only_connection()
        try:
            rows = connection.execute(
                "SELECT * FROM procedure_current_designations ORDER BY logical_id"
            ).fetchall()
            entries: list[dict[str, Any]] = []
            seen: set[tuple[str, str]] = set()
            for row in rows:
                entry = self._local_procedure_entry(
                    row,
                    normalized,
                    connection=connection,
                    receiver=receiver_scope,
                )
                if entry is None:
                    continue
                key = (entry["logical_id"], entry["partition_id"])
                if key in seen:
                    continue
                if (
                    entry["partition_id"] != partition_id
                    and entry.get("receiver_authorized") is not True
                ):
                    continue
                seen.add(key)
                entries.append(entry)
            return entries
        finally:
            connection.close()

    def _local_procedure_entry(
        self,
        row: sqlite3.Row,
        expected_partition: Mapping[str, Any],
        *,
        connection: sqlite3.Connection,
        receiver: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Join one durable current-designation row to its exact evidence.

        A row of the requested exact partition is always returned (even with an
        explicit defect).  With an explicit receiver, a durable project/shared
        designation of the receiver's application and namespace is returned
        only when its recipients name that exact receiver; the returned entry
        keeps the exact stored partition, so the caller's authorization check
        still evaluates the durable partition rather than the lookup key.
        """

        entry: dict[str, Any] = {
            "logical_id": str(row["logical_id"]),
            "partition": None,
            "partition_id": str(row["partition_id"]),
            "designation_id": str(row["current_id"]),
            "revision_id": str(row["revision_id"]),
            "generation": None,
            "state": str(row["state"]),
            "designation": None,
            "procedure": None,
            "approval": None,
            "representations": (),
            "revocation": None,
            "revoked": False,
            "source_cases": (),
            "defects": (),
            "receiver_authorized": False,
        }
        exact = entry["partition_id"] == contracts.procedure_partition_id(
            dict(expected_partition)
        )
        row_generation = row["generation"]
        if isinstance(row_generation, bool) or not isinstance(row_generation, int):
            # Only an exact durable integer is a current generation: text such
            # as "malformed" is unreadable, and a float such as 1.5 or the
            # boolean True would be truncated to an integer the row never
            # wrote.  Both are one unreadable link of this row only: the row
            # keeps its exact identity with an explicit defect instead of
            # raising, so a single bad row can never suppress an unrelated
            # eligible current row of the same read.
            if not exact:
                return None
            entry["partition"] = dict(expected_partition)
            entry["defects"] = (
                "the local current designation generation is not an exact integer",
            )
            return entry
        entry["generation"] = row_generation
        if entry["state"] != "active":
            if not exact:
                return None
            # A withdrawn partition keeps its exact durable state; the caller
            # decides that it is not a deliverable current designation.
            entry["partition"] = dict(expected_partition)
            entry["defects"] = (
                f"the local current designation is {entry['state']!r}, not active",
            )
            return entry
        try:
            designation = self._read_only_record(
                connection,
                "procedure_designations",
                "designation_id",
                entry["designation_id"],
                contracts.validate_procedure_designation,
            )
        except Exception as exc:
            if not exact:
                return None
            entry["partition"] = dict(expected_partition)
            entry["defects"] = (
                f"the local current designation is not durable: {exc}",
            )
            return entry
        try:
            stored_partition = contracts.normalize_procedure_partition(
                designation["partition"]
            )
        except contracts.ContractError as exc:
            if not exact:
                return None
            entry["partition"] = dict(expected_partition)
            entry["defects"] = (
                f"the local current designation partition is not exact: {exc}",
            )
            return entry
        # Every active current row -- exact-partition or receiver-discovered --
        # must describe its exact joined designation: the row's logical,
        # revision, generation, and current designation identities, its stored
        # designation record, and its content hash.  A row that disagrees with
        # the durable designation is never deliverable, so a receiver-discovered
        # multi-recipient row cannot promote an altered generation or hash.
        if not self._current_row_matches_designation(row, designation):
            if not exact:
                return None
            entry["partition"] = dict(expected_partition)
            entry["defects"] = (
                "the durable designation does not match its current row",
            )
            return entry
        if exact:
            if stored_partition != dict(expected_partition):
                entry["partition"] = dict(expected_partition)
                entry["defects"] = (
                    "the durable designation does not match its current row",
                )
                return entry
        elif not self._receiver_names_the_designation(receiver, stored_partition):
            return None
        else:
            entry["receiver_authorized"] = True
        entry["partition"] = stored_partition
        entry["designation"] = designation
        try:
            procedure = self._read_only_record(
                connection,
                "procedure_revisions",
                "revision_id",
                entry["revision_id"],
                contracts.validate_procedure_revision,
            )
            approval = self._read_only_record(
                connection,
                "procedure_approvals",
                "approval_id",
                str(designation["approval_id"]),
                contracts.validate_procedure_approval,
            )
            contracts.validate_procedure_approval(approval, procedure=procedure)
            contracts.validate_procedure_designation(
                designation, procedure=procedure, approval=approval
            )
        except Exception as exc:
            entry["defects"] = (
                f"the local designation lacks durable procedure evidence: {exc}",
            )
            return entry
        entry["procedure"] = procedure
        entry["approval"] = approval
        defects: list[str] = []
        representations: list[dict[str, Any]] = []
        representation_rows = connection.execute(
            """
            SELECT * FROM procedure_representations
            WHERE revision_id = ?
            ORDER BY representation_id
            """,
            (entry["revision_id"],),
        ).fetchall()
        for representation_row in representation_rows:
            try:
                representation = self._stored_record(
                    representation_row, contracts.validate_procedure_representation
                )
                if representation.get("content_hash") != representation_row["content_hash"]:
                    raise ProcedureConflictError(
                        "the stored representation content hash changed"
                    )
                contracts.validate_procedure_representation(
                    representation, procedure=procedure
                )
            except Exception as exc:
                defects.append(f"one local representation is unusable: {exc}")
                continue
            representations.append(representation)
        # One deterministic order keeps an equally valid set of projections
        # stable no matter how they were recorded.
        representations.sort(
            key=lambda item: (
                str(item.get("model")),
                int(item.get("dimensions") or 0),
                str(item.get("metric")),
                str(item.get("sanitizer_version")),
                str(item.get("representation_id")),
            )
        )
        entry["representations"] = tuple(representations)
        revocation_row = connection.execute(
            "SELECT * FROM procedure_revocations WHERE revision_id = ?",
            (entry["revision_id"],),
        ).fetchone()
        if revocation_row is not None:
            # A visible tombstone is authoritative even when its own record is
            # unreadable, so the revision stays revoked either way.
            entry["revoked"] = True
            try:
                revocation = self._stored_record(
                    revocation_row, contracts.validate_procedure_revocation
                )
                contracts.validate_procedure_revocation(revocation, procedure=procedure)
            except Exception as exc:
                defects.append(f"the local revocation tombstone is unusable: {exc}")
            else:
                entry["revocation"] = revocation
        source = procedure.get("source")
        if procedure.get("origin") == "generated" or (
            isinstance(source, Mapping) and source.get("kind") == "generated_skill"
        ):
            # A generated-origin revision is never delivered on the strength
            # of its own approval alone: its retained Step-02 provenance has
            # to be present, exact, and re-established from the durable
            # candidate, skill approval, and source-case receipts -- and its
            # retained source kind has to be exactly the accepted generated
            # Step-02 kind, so no other source can claim this trust path.
            source_cases, source_defects = self._local_procedure_source(
                source, procedure, connection=connection
            )
            defects.extend(source_defects)
            entry["source_cases"] = source_cases
        entry["defects"] = tuple(defects)
        return entry

    @staticmethod
    def _current_row_matches_designation(
        row: sqlite3.Row, designation: Mapping[str, Any]
    ) -> bool:
        """Return whether one current row still describes its joined designation.

        The current-designation row is the durable pointer a local delivery
        resolves, so it must keep the exact identity, stored record, and
        content hash of the designation it names.  A row whose identity,
        record, or hash was altered is not the current designation of any
        deliverable revision and fails closed before candidate emission.
        """

        row_generation = row["generation"]
        designation_generation = designation.get("generation")
        if (
            isinstance(row_generation, bool)
            or not isinstance(row_generation, int)
            or isinstance(designation_generation, bool)
            or not isinstance(designation_generation, int)
        ):
            # Only an exact durable integer generation can describe the
            # current designation: 1.5, "1", and True all denote an integer
            # neither durable record wrote, so they fail closed for this row
            # only instead of being truncated into a match.
            return False
        if (
            str(row["logical_id"]) != str(designation.get("logical_id"))
            or str(row["partition_id"]) != str(designation.get("partition_id"))
            or str(row["current_id"]) != str(designation.get("designation_id"))
            or str(row["revision_id"]) != str(designation.get("revision_id"))
            or row_generation != designation_generation
        ):
            return False
        if str(row["content_hash"]) != str(designation.get("content_hash")):
            return False
        try:
            stored_record = json.loads(str(row["record"]))
        except (TypeError, ValueError):
            return False
        if not isinstance(stored_record, dict):
            return False
        return stored_record == dict(designation)

    @staticmethod
    def _receiver_names_the_designation(
        receiver: Mapping[str, Any] | None, partition: Mapping[str, Any]
    ) -> bool:
        """Return whether a durable partition names this exact receiver.

        Only a project/shared partition of the receiver's own application and
        namespace can authorize it; a private partition is never discovered
        this way, and a project partition stays inside the receiver's project
        while a shared partition may name cross-project recipients.
        """

        if receiver is None:
            return False
        if partition.get("scope") not in ("project", "shared"):
            return False
        if (
            partition.get("application") != receiver.get("application")
            or partition.get("namespace") != receiver.get("namespace")
        ):
            return False
        if partition.get("scope") == "project" and partition.get("project") != receiver.get("project"):
            return False
        key = contracts.canonical_json(dict(receiver)).decode("utf-8")
        return any(
            contracts.canonical_json(dict(item)).decode("utf-8") == key
            for item in partition.get("recipients", ())
        )

    def _local_procedure_source(
        self,
        source: Any,
        procedure: Mapping[str, Any],
        *,
        connection: sqlite3.Connection,
    ) -> tuple[tuple[dict[str, Any], ...], list[str]]:
        """Recheck one generated-origin revision's durable Step-02 provenance.

        A generated-origin revision earns this trust path only with the exact
        accepted Step-02 source kind: any other retained source keeps the
        revision outside the generated provenance rejoin, so it is omitted
        instead of being delivered on a trusted approval alone.
        """

        if not isinstance(source, Mapping) or source.get("kind") != "generated_skill":
            return (), ["the generated procedure source is not the accepted Step-02 kind"]
        try:
            # The two Step-02 tables keep their records as explicit columns,
            # so they use the accepted column-shaped readers instead of the
            # JSON-record reader the procedure tables use.
            candidate = self._read_only_skill_candidate(
                connection, str(source.get("candidate_id") or "")
            )
            skill_approval = self._read_only_skill_approval(
                connection, str(source.get("skill_approval_id") or "")
            )
        except Exception as exc:
            return (), [f"the generated procedure source is not durable: {exc}"]
        source_cases = [dict(item) for item in candidate["source_cases"]]
        if (
            source.get("candidate_digest") != candidate["content_hash"]
            or source.get("skill_approval_digest") != skill_approval["content_hash"]
            or source.get("source_cases") != source_cases
            or candidate["content"] != procedure["behavior"]["body"]
            or candidate["scope"] != procedure["origin_scope"]
        ):
            # The retained Step-02 candidate also has to name exactly the
            # revision's own origin scope: a candidate from another scope was
            # never the reviewed source of this origin, so it is ambiguous
            # provenance instead of delivery approval.
            return (), ["the generated procedure source provenance changed or is ambiguous"]
        if any(
            skill_approval[field] != candidate[field]
            for field in (
                "candidate_id",
                "skill_id",
                "origin",
                "scope_digest",
                "scope",
                "content_digest",
                "source_cases",
            )
        ):
            # The accepted store rejoins an approval to its exact candidate
            # before accepting it; this read repeats that exact rejoin.
            return (), ["the generated procedure source approval does not rejoin its candidate"]
        try:
            for source_case in source_cases:
                self._rejoin_local_source_case(source_case, candidate["scope"])
        except Exception as exc:
            return (), [f"the generated procedure source case is not durable: {exc}"]
        return tuple(source_cases), []

    def _rejoin_local_source_case(
        self, source_case: Mapping[str, Any], scope: Mapping[str, Any]
    ) -> None:
        """Re-establish one retained Step-02 source case from its receipts.

        The accepted ``read_confirmed_case_join`` already revalidates the
        durable case receipt, its confirmed ingestion, the reviewed
        trajectory, and the durable review receipt inside the exact scope; this
        read additionally requires the retained provenance to name exactly
        those receipts, so a missing or changed source receipt omits that
        guidance instead of delivering it on partial trust.
        """

        required = (
            "case_id",
            "case_receipt_id",
            "trajectory_id",
            "review_receipt_id",
            "review_receipt_digest",
        )
        if any(not source_case.get(field) for field in required):
            raise ProcedureConflictError("a generated source case lacks exact receipts")
        join = self.read_confirmed_case_join(scope, str(source_case["case_id"]))
        case_receipt = join["case_receipt"]
        trajectory = join["trajectory"]
        if (
            case_receipt["case_receipt_id"] != source_case["case_receipt_id"]
            or case_receipt["trajectory_id"] != source_case["trajectory_id"]
            or case_receipt["review_receipt_id"] != source_case["review_receipt_id"]
            or case_receipt["review_receipt_digest"] != source_case["review_receipt_digest"]
            or trajectory["review_receipt_id"] != source_case["review_receipt_id"]
            or trajectory["review_receipt_digest"] != source_case["review_receipt_digest"]
        ):
            raise ProcedureConflictError("the generated source case receipt changed")

    def _read_only_skill_candidate(
        self, connection: sqlite3.Connection, candidate_id: str
    ) -> dict[str, Any]:
        """Read one durable Step-02 candidate on the calling thread's read.

        The candidate table keeps its record as explicit columns, so the
        accepted column-shaped reader applies; a row whose stored content hash
        no longer matches its own retained record is never accepted.
        """

        row = connection.execute(
            "SELECT * FROM generated_skill_candidates WHERE candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"generated skill candidate not found: {candidate_id}")
        candidate = MemoryStore._generated_skill_from_row(row)
        if candidate.get("content_hash") != row["content_hash"]:
            raise ExperienceConflictError(
                "generated skill candidate content hash no longer matches its row"
            )
        return candidate

    def _read_only_skill_approval(
        self, connection: sqlite3.Connection, approval_id: str
    ) -> dict[str, Any]:
        """Read one durable Step-02 skill approval on the calling thread's read."""

        row = connection.execute(
            "SELECT * FROM generated_skill_approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"skill approval not found: {approval_id}")
        approval = MemoryStore._skill_approval_from_row(row)
        if approval.get("content_hash") != row["content_hash"]:
            raise ExperienceConflictError(
                "skill approval content hash no longer matches its row"
            )
        return approval

    @staticmethod
    def _read_only_record(
        connection: sqlite3.Connection,
        table: str,
        column: str,
        value: Any,
        validator: Any,
    ) -> dict[str, Any]:
        """Return one validated durable record of the calling thread's read.

        ``table``, ``column``, and ``validator`` are literals owned by this
        module, and every check fails closed: a missing row, unreadable JSON, an
        invalid contract record, or a content hash that no longer matches its
        row raises instead of returning a partial record.
        """

        row = connection.execute(
            f"SELECT * FROM {table} WHERE {column} = ?", (value,)
        ).fetchone()
        if row is None:
            raise StoreError(f"{table} record not found for {column}={value!r}")
        record = MemoryStore._stored_record(row, validator)
        if record.get("content_hash") != row["content_hash"]:
            raise ProcedureConflictError(
                f"{table} record content hash no longer matches its row"
            )
        return record

    def get_current_procedure_designation(
        self, logical_id: str, partition: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        normalized = contracts.normalize_procedure_partition(partition)
        partition_id = contracts.procedure_partition_id(normalized)
        connection = self._require_connection()
        row = connection.execute(
            """
            SELECT * FROM procedure_current_designations
            WHERE logical_id = ? AND partition_id = ?
            """,
            (logical_id, partition_id),
        ).fetchone()
        return self._current_procedure_from_row(row) if row is not None else None

    def get_procedure_designation(self, designation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_designations WHERE designation_id = ?", (designation_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure designation not found: {designation_id}")
        designation = self._stored_record(row, contracts.validate_procedure_designation)
        procedure = self.get_procedure_revision(str(designation["revision_id"]))
        approval = self.get_procedure_approval(str(designation["approval_id"]))
        try:
            contracts.validate_procedure_designation(
                designation, procedure=procedure, approval=approval
            )
        except contracts.ContractError as exc:
            raise ProcedureConflictError("stored designation lacks durable procedure evidence") from exc
        return designation

    def record_procedure_designation(
        self, designation: Mapping[str, Any]
    ) -> dict[str, Any]:
        contracts.validate_procedure_designation(designation)
        procedure = self.get_procedure_revision(str(designation["revision_id"]))
        approval = self.get_procedure_approval(str(designation["approval_id"]))
        try:
            contracts.validate_procedure_designation(
                designation, procedure=procedure, approval=approval
            )
        except contracts.ContractError as exc:
            raise ProcedureConflictError("designation does not bind durable evidence") from exc
        if self.is_procedure_revoked(str(designation["revision_id"])):
            raise ProcedureConflictError("a revoked procedure revision cannot become current")
        connection = self._require_connection()
        designation_id = str(designation["designation_id"])
        with connection:
            existing = connection.execute(
                "SELECT * FROM procedure_designations WHERE designation_id = ?",
                (designation_id,),
            ).fetchone()
            if existing is not None:
                restored = self._stored_record(existing, contracts.validate_procedure_designation)
                if restored["content_hash"] != designation["content_hash"]:
                    raise ProcedureDesignationConflictError(
                        f"procedure designation conflict for {designation_id}"
                    )
                return restored
            current_row = connection.execute(
                """
                SELECT * FROM procedure_current_designations
                WHERE logical_id = ? AND partition_id = ?
                """,
                (designation["logical_id"], designation["partition_id"]),
            ).fetchone()
            generation = int(designation["generation"])
            if current_row is None:
                if generation != 1 or designation["predecessor_generation"] is not None:
                    raise ProcedureDesignationConflictError(
                        "initial designation must begin at generation one"
                    )
                connection.execute(
                    """
                    INSERT INTO procedure_current_designations (
                        logical_id, partition_id, current_id, revision_id, generation, state,
                        record, content_hash, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                    """,
                    (
                        designation["logical_id"], designation["partition_id"],
                        designation["designation_id"], designation["revision_id"], generation,
                        self._serialize_record(designation), designation["content_hash"],
                        designation["created_at"], designation["created_at"],
                    ),
                )
            else:
                current = self._current_procedure_from_row(current_row)
                if generation <= int(current["generation"]):
                    raise ProcedureDesignationConflictError(
                        "delayed procedure designation cannot replace newer current state"
                    )
                if designation["predecessor_generation"] != current["generation"]:
                    raise ProcedureDesignationConflictError(
                        "procedure designation predecessor is not current"
                    )
                cursor = connection.execute(
                    """
                    UPDATE procedure_current_designations
                    SET current_id = ?, revision_id = ?, generation = ?, state = 'active',
                        record = ?, content_hash = ?, updated_at = ?
                    WHERE logical_id = ? AND partition_id = ? AND generation = ?
                    """,
                    (
                        designation["designation_id"], designation["revision_id"], generation,
                        self._serialize_record(designation), designation["content_hash"],
                        designation["created_at"], designation["logical_id"],
                        designation["partition_id"], current["generation"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProcedureDesignationConflictError("stale procedure designation update")
            connection.execute(
                """
                INSERT INTO procedure_designations (
                    designation_id, logical_id, revision_id, partition_id, generation,
                    record, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    designation["designation_id"], designation["logical_id"], designation["revision_id"],
                    designation["partition_id"], designation["generation"],
                    self._serialize_record(designation), designation["content_hash"], designation["created_at"],
                ),
            )
        return self.get_procedure_designation(designation_id)

    def get_procedure_withdrawal(self, withdrawal_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_withdrawals WHERE withdrawal_id = ?", (withdrawal_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure withdrawal not found: {withdrawal_id}")
        return self._stored_record(row, contracts.validate_procedure_withdrawal)

    def record_procedure_withdrawal(
        self, withdrawal: Mapping[str, Any]
    ) -> dict[str, Any]:
        contracts.validate_procedure_withdrawal(withdrawal)
        connection = self._require_connection()
        withdrawal_id = str(withdrawal["withdrawal_id"])
        with connection:
            existing = connection.execute(
                "SELECT * FROM procedure_withdrawals WHERE withdrawal_id = ?",
                (withdrawal_id,),
            ).fetchone()
            if existing is not None:
                restored = self._stored_record(existing, contracts.validate_procedure_withdrawal)
                if restored["content_hash"] != withdrawal["content_hash"]:
                    raise ProcedureDesignationConflictError(
                        f"procedure withdrawal conflict for {withdrawal_id}"
                    )
                return restored
            current_row = connection.execute(
                """
                SELECT * FROM procedure_current_designations
                WHERE logical_id = ? AND partition_id = ?
                """,
                (withdrawal["logical_id"], withdrawal["partition_id"]),
            ).fetchone()
            if current_row is None:
                raise ProcedureDesignationConflictError("withdrawal has no current designation")
            current = self._current_procedure_from_row(current_row)
            if current["state"] != "active":
                raise ProcedureDesignationConflictError("procedure partition is already withdrawn")
            if (
                current["designation_id"] != withdrawal["predecessor_designation_id"]
                or current["generation"] != withdrawal["predecessor_generation"]
                or current["revision_id"] != withdrawal["revision_id"]
            ):
                raise ProcedureDesignationConflictError(
                    "withdrawal predecessor is no longer the current designation"
                )
            cursor = connection.execute(
                """
                UPDATE procedure_current_designations
                SET current_id = ?, generation = ?, state = 'withdrawn', record = ?,
                    content_hash = ?, updated_at = ?
                WHERE logical_id = ? AND partition_id = ? AND generation = ? AND state = 'active'
                """,
                (
                    withdrawal["withdrawal_id"], withdrawal["generation"],
                    self._serialize_record(withdrawal), withdrawal["content_hash"],
                    withdrawal["created_at"], withdrawal["logical_id"], withdrawal["partition_id"],
                    withdrawal["predecessor_generation"],
                ),
            )
            if cursor.rowcount != 1:
                raise ProcedureDesignationConflictError("stale procedure withdrawal update")
            connection.execute(
                """
                INSERT INTO procedure_withdrawals (
                    withdrawal_id, logical_id, revision_id, partition_id, generation,
                    record, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    withdrawal["withdrawal_id"], withdrawal["logical_id"], withdrawal["revision_id"],
                    withdrawal["partition_id"], withdrawal["generation"],
                    self._serialize_record(withdrawal), withdrawal["content_hash"], withdrawal["created_at"],
                ),
            )
            self._fence_publications_in_transaction(
                connection,
                logical_id=str(withdrawal["logical_id"]),
                partition_id=str(withdrawal["partition_id"]),
                status="fenced",
            )
        return self.get_procedure_withdrawal(withdrawal_id)

    def _fence_publications_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        logical_id: str,
        partition_id: str | None = None,
        revision_id: str | None = None,
        status: str,
    ) -> None:
        clauses = ["logical_id = ?"]
        values: list[Any] = [logical_id]
        if partition_id is not None:
            clauses.append("partition_id = ?")
            values.append(partition_id)
        if revision_id is not None:
            clauses.append("revision_id = ?")
            values.append(revision_id)
        rows = connection.execute(
            "SELECT * FROM procedure_publications WHERE " + " AND ".join(clauses), values
        ).fetchall()
        for row in rows:
            existing = self._stored_record(row, contracts.validate_procedure_publication)
            if existing["status"] in {"revoked", "withdrawn", "blocked", "revocation_pending"}:
                continue
            updated = contracts.revise_procedure_publication(existing, status=status)
            connection.execute(
                """
                UPDATE procedure_publications
                SET status = ?, version = ?, record = ?, content_hash = ?, updated_at = ?
                WHERE publication_id = ? AND version = ?
                """,
                (
                    updated["status"], updated["version"], self._serialize_record(updated),
                    updated["content_hash"], contracts.utc_now(), updated["publication_id"],
                    existing["version"],
                ),
            )

    def get_procedure_publication(self, publication_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_publications WHERE publication_id = ?", (publication_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure publication not found: {publication_id}")
        return self._stored_record(row, contracts.validate_procedure_publication)

    def list_procedure_publications(
        self,
        *,
        logical_id: str | None = None,
        revision_id: str | None = None,
        partition: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        connection = self._require_connection()
        clauses: list[str] = []
        values: list[Any] = []
        if logical_id is not None:
            clauses.append("logical_id = ?")
            values.append(logical_id)
        if revision_id is not None:
            clauses.append("revision_id = ?")
            values.append(revision_id)
        if partition is not None:
            clauses.append("partition_id = ?")
            values.append(contracts.procedure_partition_id(partition))
        query = "SELECT * FROM procedure_publications"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, publication_id"
        return [
            self._stored_record(row, contracts.validate_procedure_publication)
            for row in connection.execute(query, values).fetchall()
        ]

    def create_procedure_publication(
        self, publication: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """Durably create a publication intent before any remote side effect."""

        contracts.validate_procedure_publication(publication)
        procedure = self.get_procedure_revision(str(publication["revision_id"]))
        approval = self.get_procedure_approval(str(publication["approval"]["approval_id"]))
        representation = self.get_procedure_representation(
            str(publication["representation"]["representation_id"])
        )
        designation = self.get_procedure_designation(
            str(publication["designation"]["designation_id"])
        )
        expected = contracts.make_procedure_publication(
            procedure=procedure,
            approval=approval,
            representation=representation,
            designation=designation,
            created_at=publication["created_at"],
        )
        if expected["payload_digest"] != publication["payload_digest"]:
            raise ProcedureConflictError("publication does not match durable procedure evidence")
        current = self.get_current_procedure_designation(
            str(publication["logical_id"]), publication["partition"]
        )
        if current is None or current["state"] != "active":
            raise ProcedureConflictError("publication partition has no active current designation")
        if (
            current["designation_id"] != publication["designation"]["designation_id"]
            or current["generation"] != publication["designation"]["generation"]
            or current["revision_id"] != publication["revision_id"]
        ):
            raise ProcedureDesignationConflictError(
                "publication designation is no longer current"
            )
        if self.is_procedure_revoked(str(publication["revision_id"])):
            raise ProcedureConflictError("revoked procedure revision cannot be published")
        connection = self._require_connection()
        publication_id = str(publication["publication_id"])
        with connection:
            existing = connection.execute(
                "SELECT * FROM procedure_publications WHERE publication_id = ?",
                (publication_id,),
            ).fetchone()
            if existing is not None:
                restored = self._stored_record(existing, contracts.validate_procedure_publication)
                if restored["payload_digest"] != publication["payload_digest"]:
                    raise ProcedureConflictError(
                        f"procedure publication conflict for {publication_id}"
                    )
                return restored, False
            connection.execute(
                """
                INSERT INTO procedure_publications (
                    publication_id, logical_id, revision_id, partition_id, designation_id,
                    status, version, record, content_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    publication["publication_id"], publication["logical_id"], publication["revision_id"],
                    publication["partition_id"], publication["designation"]["designation_id"],
                    publication["status"], publication["version"], self._serialize_record(publication),
                    publication["content_hash"], publication["created_at"], publication["created_at"],
                ),
            )
        return self.get_procedure_publication(publication_id), True

    def update_procedure_publication(
        self,
        publication_id: str,
        *,
        status: str,
        expected_version: int,
        remote_receipt: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        existing = self.get_procedure_publication(publication_id)
        if existing["version"] != expected_version:
            raise ProcedureConflictError(f"stale procedure publication update for {publication_id}")
        allowed = {
            "intent": {"ambiguous", "remote_committed", "fenced", "withdrawn", "revoked", "blocked", "revocation_pending"},
            "ambiguous": {"remote_committed", "fenced", "withdrawn", "revoked", "blocked", "revocation_pending"},
            "remote_committed": {"acknowledged", "fenced", "withdrawn", "revoked", "revocation_pending"},
            "acknowledged": {"fenced", "withdrawn", "revoked", "revocation_pending"},
            "fenced": {"withdrawn", "revoked", "revocation_pending"},
            "revocation_pending": {"revoked", "withdrawn"},
            "withdrawn": {"revoked"},
            "revoked": set(),
            "blocked": set(),
        }
        if status == existing["status"] and (
            remote_receipt == existing.get("remote_receipt") and error == existing.get("error")
        ):
            return existing
        if status not in allowed.get(existing["status"], set()):
            raise ProcedureConflictError(
                f"procedure publication {publication_id} cannot transition from "
                f"{existing['status']!r} to {status!r}"
            )
        updated = contracts.revise_procedure_publication(
            existing, status=status, remote_receipt=remote_receipt, error=error
        )
        connection = self._require_connection()
        with connection:
            cursor = connection.execute(
                """
                UPDATE procedure_publications
                SET status = ?, version = ?, record = ?, content_hash = ?, updated_at = ?
                WHERE publication_id = ? AND version = ?
                """,
                (
                    updated["status"], updated["version"], self._serialize_record(updated),
                    updated["content_hash"], contracts.utc_now(), publication_id, expected_version,
                ),
            )
            if cursor.rowcount != 1:
                raise ProcedureConflictError(
                    f"stale procedure publication update for {publication_id}"
                )
        return self.get_procedure_publication(publication_id)

    def get_procedure_revocation(self, revision_id: str) -> dict[str, Any] | None:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_revocations WHERE revision_id = ?", (revision_id,)
        ).fetchone()
        return self._stored_record(row, contracts.validate_procedure_revocation) if row is not None else None

    def is_procedure_revoked(self, revision_id: str) -> bool:
        return self.get_procedure_revocation(revision_id) is not None

    def record_procedure_revocation(
        self, revocation: Mapping[str, Any]
    ) -> dict[str, Any]:
        contracts.validate_procedure_revocation(revocation)
        procedure = self.get_procedure_revision(str(revocation["revision_id"]))
        try:
            contracts.validate_procedure_revocation(revocation, procedure=procedure)
        except contracts.ContractError as exc:
            raise ProcedureConflictError("revocation does not bind durable procedure") from exc
        connection = self._require_connection()
        revocation_id = str(revocation["revocation_id"])
        with connection:
            prior_revision = connection.execute(
                "SELECT * FROM procedure_revocations WHERE revision_id = ?",
                (revocation["revision_id"],),
            ).fetchone()
            if prior_revision is not None:
                restored = self._stored_record(prior_revision, contracts.validate_procedure_revocation)
                if restored["content_hash"] != revocation["content_hash"]:
                    raise ProcedureConflictError(
                        "procedure revision already has a different revocation tombstone"
                    )
                return restored
            connection.execute(
                """
                INSERT INTO procedure_revocations (
                    revocation_id, logical_id, revision_id, record, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    revocation["revocation_id"], revocation["logical_id"], revocation["revision_id"],
                    self._serialize_record(revocation), revocation["content_hash"], revocation["created_at"],
                ),
            )
            self._fence_publications_in_transaction(
                connection,
                logical_id=str(revocation["logical_id"]),
                revision_id=str(revocation["revision_id"]),
                status="revocation_pending",
            )
        restored = self.get_procedure_revocation(str(revocation["revision_id"]))
        assert restored is not None
        return restored

    def get_procedure_remote_operation(self, operation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_remote_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure remote operation not found: {operation_id}")
        return self._stored_record(row, contracts.validate_procedure_remote_operation)

    def create_procedure_remote_operation(
        self, operation: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        contracts.validate_procedure_remote_operation(operation)
        connection = self._require_connection()
        operation_id = str(operation["operation_id"])
        with connection:
            existing = connection.execute(
                "SELECT * FROM procedure_remote_operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if existing is not None:
                restored = self._stored_record(existing, contracts.validate_procedure_remote_operation)
                immutable_fields = (
                    "kind", "logical_id", "revision_id", "partition_id", "payload_id", "payload_digest",
                )
                if any(restored[field] != operation[field] for field in immutable_fields):
                    raise ProcedureConflictError(
                        f"procedure remote operation conflict for {operation_id}"
                    )
                return restored, False
            connection.execute(
                """
                INSERT INTO procedure_remote_operations (
                    operation_id, kind, logical_id, revision_id, partition_id, payload_id,
                    status, version, record, content_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation["operation_id"], operation["kind"], operation["logical_id"],
                    operation["revision_id"], operation["partition_id"], operation["payload_id"],
                    operation["status"], operation["version"], self._serialize_record(operation),
                    operation["content_hash"], operation["created_at"], operation["created_at"],
                ),
            )
        return self.get_procedure_remote_operation(operation_id), True

    def update_procedure_remote_operation(
        self,
        operation_id: str,
        *,
        status: str,
        expected_version: int,
        remote_receipt: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        existing = self.get_procedure_remote_operation(operation_id)
        if existing["version"] != expected_version:
            raise ProcedureConflictError(f"stale procedure remote operation update for {operation_id}")
        allowed = {
            "intent": {"ambiguous", "remote_committed", "fenced", "withdrawn", "revoked", "blocked"},
            "ambiguous": {"remote_committed", "fenced", "withdrawn", "revoked", "blocked"},
            "remote_committed": {"acknowledged", "fenced", "withdrawn", "revoked"},
            "acknowledged": {"fenced", "withdrawn", "revoked"},
            "fenced": {"withdrawn", "revoked"},
            "withdrawn": {"revoked"},
            "revoked": set(),
            # A stable operation that was blocked locally can be reconciled
            # only by later exact remote evidence, which promotes it through
            # remote_committed before a normal acknowledgement.
            "blocked": {"remote_committed", "fenced", "withdrawn", "revoked"},
            "revocation_pending": {"revoked", "withdrawn"},
        }
        if status == existing["status"] and (
            remote_receipt == existing.get("remote_receipt") and error == existing.get("error")
        ):
            return existing
        if status not in allowed.get(existing["status"], set()):
            raise ProcedureConflictError(
                f"procedure remote operation {operation_id} cannot transition from "
                f"{existing['status']!r} to {status!r}"
            )
        updated = contracts.revise_procedure_remote_operation(
            existing, status=status, remote_receipt=remote_receipt, error=error
        )
        connection = self._require_connection()
        with connection:
            cursor = connection.execute(
                """
                UPDATE procedure_remote_operations
                SET status = ?, version = ?, record = ?, content_hash = ?, updated_at = ?
                WHERE operation_id = ? AND version = ?
                """,
                (
                    updated["status"], updated["version"], self._serialize_record(updated),
                    updated["content_hash"], contracts.utc_now(), operation_id, expected_version,
                ),
            )
            if cursor.rowcount != 1:
                raise ProcedureConflictError(
                    f"stale procedure remote operation update for {operation_id}"
                )
        return self.get_procedure_remote_operation(operation_id)

    def list_procedure_remote_operations(
        self,
        *,
        revision_id: str | None = None,
        payload_id: str | None = None,
    ) -> list[dict[str, Any]]:
        connection = self._require_connection()
        clauses: list[str] = []
        values: list[Any] = []
        if revision_id is not None:
            clauses.append("revision_id = ?")
            values.append(revision_id)
        if payload_id is not None:
            clauses.append("payload_id = ?")
            values.append(payload_id)
        query = "SELECT * FROM procedure_remote_operations"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at, operation_id"
        return [
            self._stored_record(row, contracts.validate_procedure_remote_operation)
            for row in connection.execute(query, values).fetchall()
        ]

    def record_procedure_exposure(self, exposure: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_procedure_exposure(exposure)
        publication = self.get_procedure_publication(str(exposure["publication_id"]))
        if (
            publication["logical_id"] != exposure["logical_id"]
            or publication["revision_id"] != exposure["revision_id"]
        ):
            raise ProcedureConflictError("procedure exposure does not bind its publication")
        connection = self._require_connection()
        exposure_id = str(exposure["exposure_id"])
        existing = connection.execute(
            "SELECT * FROM procedure_exposures WHERE exposure_id = ?", (exposure_id,)
        ).fetchone()
        if existing is not None:
            restored = self._stored_record(existing, contracts.validate_procedure_exposure)
            if restored["content_hash"] != exposure["content_hash"]:
                raise ProcedureConflictError(f"procedure exposure conflict for {exposure_id}")
            return restored
        with connection:
            connection.execute(
                """
                INSERT INTO procedure_exposures (
                    exposure_id, publication_id, revision_id, record, content_hash, delivered_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    exposure["exposure_id"], exposure["publication_id"], exposure["revision_id"],
                    self._serialize_record(exposure), exposure["content_hash"], exposure["delivered_at"],
                ),
            )
        return self.get_procedure_exposure(exposure_id)

    def get_procedure_exposure(self, exposure_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT * FROM procedure_exposures WHERE exposure_id = ?", (exposure_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"procedure exposure not found: {exposure_id}")
        return self._stored_record(row, contracts.validate_procedure_exposure)

    def list_procedure_exposures(self, revision_id: str) -> list[dict[str, Any]]:
        connection = self._require_connection()
        rows = connection.execute(
            """
            SELECT * FROM procedure_exposures
            WHERE revision_id = ?
            ORDER BY delivered_at, exposure_id
            """,
            (revision_id,),
        ).fetchall()
        return [self._stored_record(row, contracts.validate_procedure_exposure) for row in rows]

    # -- STEP-04 preparation, search, disposition, child, and context state --

    def record_preparation(
        self, preparation: Mapping[str, Any],
        *, first_decision: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        contracts.validate_preparation(preparation)
        if first_decision is not None:
            contracts.validate_decision(first_decision)
            if first_decision["decision_id"] != preparation["decision_id"]:
                raise StoreError("first preparation and decision identity differ")
            for key in (
                "task_card_digest", "objective_id", "route", "plan_id",
                "plan_state", "plan_digest",
            ):
                if first_decision[key] != preparation[key]:
                    raise StoreError(f"first preparation and decision {key} differ")
        connection = self._require_connection()
        payload = self._serialize_record(preparation)
        # BEGIN IMMEDIATE makes the decision-wide read and the attempt write
        # one SQLite claim across separate callers/connections.  A delayed
        # caller can retry with the new minimum rather than overwrite it.
        try:
            connection.execute("BEGIN IMMEDIATE")
            if first_decision is not None:
                identity = {key: first_decision[key] for key in (
                    "task_card_digest", "objective_id", "route", "plan_id",
                    "plan_state", "plan_digest",
                )}
                owner = self.find_logical_decision(identity)
                if owner is not None:
                    if owner["decision_id"] != first_decision["decision_id"]:
                        raise PreparationConflictError(
                            "another caller captured this logical decision"
                        )
                    if self.list_preparations(owner["decision_id"]):
                        raise PreparationConflictError(
                            "another caller claimed the first preparation"
                        )
                    if (owner["content_hash"] != first_decision["content_hash"]
                            or owner["created_at"] != first_decision["created_at"]):
                        raise StoreError("standalone decision changed before first preparation")
                else:
                    for key in ("strategy", "configuration_digest"):
                        if first_decision[key] != preparation[key]:
                            raise StoreError(f"first preparation and decision {key} differ")
                    try:
                        connection.execute(
                            """INSERT INTO decisions (
                            decision_id, task_card_digest, objective_id, route, plan_id,
                            plan_state, plan_digest, strategy, configuration,
                            configuration_digest, state, content_hash, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                first_decision["decision_id"], first_decision["task_card_digest"],
                                first_decision["objective_id"], first_decision["route"],
                                first_decision["plan_id"], first_decision["plan_state"],
                                first_decision["plan_digest"], first_decision["strategy"],
                                json.dumps(first_decision["configuration"], sort_keys=True),
                                first_decision["configuration_digest"], first_decision["state"],
                                first_decision["content_hash"], first_decision["created_at"],
                                first_decision["created_at"],
                            ),
                        )
                    except sqlite3.IntegrityError as exc:
                        if "decisions.decision_id" in str(exc):
                            raise PreparationConflictError(
                                "another exact plan claimed the accepted decision identifier"
                            ) from exc
                        raise
            prior = self.list_preparations(str(preparation["decision_id"]))
            if prior and any(row["network_mode"] != preparation["network_mode"] for row in prior):
                raise PreparationConflictError("captured preparation network mode changed")
            if prior and any(
                row.get("network_resolution") != preparation.get("network_resolution")
                for row in prior
            ):
                raise PreparationConflictError("captured preparation network resolution changed")
            existing = next(
                (row for row in prior if row["preparation_id"] == preparation["preparation_id"]),
                None,
            )
            if existing is not None:
                stable_keys = (set(existing) | set(preparation)) - {
                    "status", "superseded_by", "content_hash"
                }
                if any(existing.get(key) != preparation.get(key) for key in stable_keys):
                    raise PreparationConflictError("preparation attempt identity already has different state")
                if existing.get("superseded_by") and (
                    preparation.get("superseded_by") != existing["superseded_by"]
                    or preparation["status"] != existing["status"]
                ):
                    raise PreparationConflictError("a superseded preparation cannot become current")
                connection.execute(
                    """UPDATE preparations SET status=?, superseded_by=?, record=?, updated_at=?
                       WHERE preparation_id=?""",
                    (
                        preparation["status"], preparation.get("superseded_by"),
                        payload, contracts.utc_now(), preparation["preparation_id"],
                    ),
                )
            else:
                if prior and preparation["budget_source"] == "unknown_time" and (
                    preparation["stage_allowance_seconds"] > 0
                ):
                    raise PreparationConflictError(
                        "unknown-time cheap pass is already claimed for this decision"
                    )
                attempts = [row.get("attempt") for row in prior]
                if any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 1
                    for value in attempts
                ):
                    raise StoreError("durable preparation attempt state is unreadable")
                if attempts and preparation["attempt"] <= max(attempts):
                    raise PreparationConflictError("preparation attempt was claimed by another caller")
                if preparation["budget_source"] == "trusted_deadline":
                    deadlines = []
                    for row in prior:
                        if row.get("budget_source") != "trusted_deadline":
                            continue
                        deadline = row.get("deadline_monotonic")
                        if (
                            isinstance(deadline, bool)
                            or not isinstance(deadline, (int, float))
                            or not math.isfinite(deadline)
                        ):
                            raise StoreError("durable preparation budget is unreadable")
                        deadlines.append(float(deadline))
                    if deadlines and preparation["deadline_monotonic"] > min(deadlines):
                        raise PreparationConflictError("preparation would extend the durable cutoff")
                supersedes = preparation.get("supersedes")
                if supersedes is not None:
                    old = next((row for row in prior if row["preparation_id"] == supersedes), None)
                    if old is None or old.get("superseded_by") or old.get("status") == "superseded":
                        raise PreparationConflictError("the superseded preparation is no longer current")
                    updated_old = dict(old)
                    updated_old["status"] = "superseded"
                    updated_old["superseded_by"] = preparation["preparation_id"]
                    updated_old["content_hash"] = contracts.content_hash(updated_old)
                    connection.execute(
                        """UPDATE preparations SET status=?, superseded_by=?, record=?, updated_at=?
                           WHERE preparation_id=?""",
                        (
                            "superseded", preparation["preparation_id"],
                            self._serialize_record(updated_old), contracts.utc_now(), supersedes,
                        ),
                    )
                connection.execute(
                    """
                    INSERT INTO preparations (
                        preparation_id, decision_id, task_card_digest, objective_id,
                        route, strategy, current_plan_state, status, supersedes,
                        superseded_by, record, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        preparation["preparation_id"], preparation["decision_id"],
                        preparation["task_card_digest"], preparation["objective_id"],
                        preparation["route"], preparation["strategy"],
                        preparation["current_plan_state"], preparation["status"],
                        supersedes, preparation.get("superseded_by"), payload,
                        preparation["created_at"], preparation["created_at"],
                    ),
                )
            connection.commit()
        except sqlite3.Error as exc:
            connection.rollback()
            raise StoreError(f"cannot record preparation: {exc}") from exc
        except Exception:
            connection.rollback()
            raise
        return self.get_preparation(str(preparation["preparation_id"]))

    def get_preparation(self, preparation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT record FROM preparations WHERE preparation_id = ?", (preparation_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"preparation not found: {preparation_id}")
        return self._stored_record(row, contracts.validate_preparation)

    def list_preparations(self, decision_id: str) -> list[dict[str, Any]]:
        connection = self._require_connection()
        rows = connection.execute(
            "SELECT record FROM preparations WHERE decision_id = ? ORDER BY created_at, preparation_id",
            (decision_id,),
        ).fetchall()
        return [self._stored_record(row, contracts.validate_preparation) for row in rows]

    def list_captured_preparations(self, decision_id: str) -> list[dict[str, Any]]:
        """Read captured policy separately from the optional budget gate."""

        rows = self._require_connection().execute(
            "SELECT record FROM preparations WHERE decision_id = ? ORDER BY created_at, preparation_id",
            (decision_id,),
        ).fetchall()
        return [self._stored_record(row, contracts.validate_preparation) for row in rows]

    def mark_preparation_superseded(
        self, preparation_id: str, *, superseded_by: str
    ) -> dict[str, Any]:
        if not isinstance(superseded_by, str) or not superseded_by:
            raise StoreError("superseded_by must be a nonempty string")
        current = self.get_preparation(preparation_id)
        superseded = dict(current)
        superseded["status"] = "superseded"
        superseded["superseded_by"] = superseded_by
        superseded["content_hash"] = contracts.content_hash(superseded)
        return self.record_preparation(superseded)

    def record_search_trace(self, trace: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_search_trace(trace)
        connection = self._require_connection()
        payload = self._serialize_record(trace)
        with connection:
            connection.execute(
                """
                INSERT INTO search_traces (preparation_id, outcome, record, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(preparation_id) DO UPDATE SET
                    outcome=excluded.outcome,
                    record=excluded.record
                """,
                (
                    trace["preparation_id"],
                    trace["outcome"],
                    payload,
                    contracts.utc_now(),
                ),
            )
        return self.get_search_trace(str(trace["preparation_id"]))

    def get_search_trace(self, preparation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT record FROM search_traces WHERE preparation_id = ?", (preparation_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"search trace not found: {preparation_id}")
        return self._stored_record(row, contracts.validate_search_trace)

    def record_search_candidate(
        self, preparation_id: str, candidate: Mapping[str, Any]
    ) -> dict[str, Any]:
        contracts.validate_candidate(candidate)
        connection = self._require_connection()
        payload = self._serialize_record(candidate)
        with connection:
            connection.execute(
                """
                INSERT INTO search_candidates (
                    candidate_id, preparation_id, kind, logical_id, revision_id,
                    disposition, score, record, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(candidate_id, preparation_id) DO UPDATE SET
                    disposition=excluded.disposition,
                    score=excluded.score,
                    record=excluded.record
                """,
                (
                    candidate["candidate_id"],
                    preparation_id,
                    candidate["kind"],
                    candidate["logical_id"],
                    candidate["revision_id"],
                    candidate["disposition"],
                    float(candidate["score"]),
                    payload,
                    contracts.utc_now(),
                ),
            )
        return dict(candidate)

    def list_search_candidates(self, preparation_id: str) -> list[dict[str, Any]]:
        connection = self._require_connection()
        rows = connection.execute(
            """
            SELECT record FROM search_candidates
            WHERE preparation_id = ?
            ORDER BY score DESC, kind, logical_id, revision_id
            """,
            (preparation_id,),
        ).fetchall()
        return [self._stored_record(row, contracts.validate_candidate) for row in rows]

    def record_plan_disposition(self, disposition: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_plan_disposition(disposition)
        connection = self._require_connection()
        payload = self._serialize_record(disposition)
        with connection:
            connection.execute(
                """
                INSERT INTO plan_dispositions (
                    disposition_id, decision_id, objective_id, branch, record,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(disposition_id) DO UPDATE SET
                    branch=excluded.branch,
                    record=excluded.record,
                    updated_at=excluded.updated_at
                """,
                (
                    disposition["disposition_id"],
                    disposition["decision_id"],
                    disposition["objective_id"],
                    disposition["branch"],
                    payload,
                    disposition["created_at"],
                    disposition["created_at"],
                ),
            )
        return self.get_plan_disposition(str(disposition["disposition_id"]))

    def get_plan_disposition(self, disposition_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT record FROM plan_dispositions WHERE disposition_id = ?", (disposition_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"plan disposition not found: {disposition_id}")
        return self._stored_record(row, contracts.validate_plan_disposition)

    def list_plan_dispositions(self, decision_id: str) -> list[dict[str, Any]]:
        connection = self._require_connection()
        rows = connection.execute(
            """
            SELECT record FROM plan_dispositions
            WHERE decision_id = ?
            ORDER BY created_at, disposition_id
            """,
            (decision_id,),
        ).fetchall()
        return [self._stored_record(row, contracts.validate_plan_disposition) for row in rows]

    def create_apc_child_operation(
        self, child_operation: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """Claim one exact child operation identity exactly once.

        ``created`` is ``False`` when that exact attempt identity already
        exists; callers must then inspect the durable status and only continue
        with a *new* bounded attempt when the existing claim is terminal.
        """

        contracts.validate_apc_child_operation(child_operation)
        connection = self._require_connection()
        payload = self._serialize_record(child_operation)
        observed = child_operation.get("observed_invocation")
        with connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO apc_child_operations (
                    child_operation_id, parent_decision_id, parent_objective_id,
                    request_digest, status, observed_invocation, result_digest,
                    record, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    child_operation["child_operation_id"],
                    child_operation.get("parent_decision_id"),
                    child_operation.get("parent_objective_id"),
                    child_operation["request_digest"],
                    child_operation["status"],
                    json.dumps(observed, sort_keys=True) if observed is not None else None,
                    child_operation.get("result_digest"),
                    payload,
                    child_operation["created_at"],
                    child_operation["created_at"],
                ),
            )
        return (
            self.get_apc_child_operation(str(child_operation["child_operation_id"])),
            cursor.rowcount == 1,
        )

    def list_apc_child_operations_by_request(
        self, request_digest: str
    ) -> list[dict[str, Any]]:
        """Return the ordered bounded attempts for one exact child request."""

        if not isinstance(request_digest, str) or not request_digest:
            raise StoreError("request_digest must be a nonempty string")
        connection = self._require_connection()
        rows = connection.execute(
            """
            SELECT record FROM apc_child_operations
            WHERE request_digest = ?
            ORDER BY created_at, child_operation_id
            """,
            (request_digest,),
        ).fetchall()
        return [
            self._stored_record(row, contracts.validate_apc_child_operation)
            for row in rows
        ]

    def record_apc_child_operation(self, child_operation: Mapping[str, Any]) -> dict[str, Any]:
        contracts.validate_apc_child_operation(child_operation)
        connection = self._require_connection()
        payload = self._serialize_record(child_operation)
        observed = child_operation.get("observed_invocation")
        with connection:
            connection.execute(
                """
                INSERT INTO apc_child_operations (
                    child_operation_id, parent_decision_id, parent_objective_id,
                    request_digest, status, observed_invocation, result_digest,
                    record, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(child_operation_id) DO UPDATE SET
                    status=excluded.status,
                    observed_invocation=excluded.observed_invocation,
                    result_digest=excluded.result_digest,
                    record=excluded.record,
                    updated_at=excluded.updated_at
                """,
                (
                    child_operation["child_operation_id"],
                    child_operation.get("parent_decision_id"),
                    child_operation.get("parent_objective_id"),
                    child_operation["request_digest"],
                    child_operation["status"],
                    json.dumps(observed, sort_keys=True) if observed is not None else None,
                    child_operation.get("result_digest"),
                    payload,
                    child_operation["created_at"],
                    child_operation["created_at"],
                ),
            )
        return self.get_apc_child_operation(str(child_operation["child_operation_id"]))

    def get_apc_child_operation(self, child_operation_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT record FROM apc_child_operations WHERE child_operation_id = ?",
            (child_operation_id,),
        ).fetchone()
        if row is None:
            raise StoreError(f"APC child operation not found: {child_operation_id}")
        return self._stored_record(row, contracts.validate_apc_child_operation)

    def list_apc_child_operations(self, decision_id: str) -> list[dict[str, Any]]:
        connection = self._require_connection()
        rows = connection.execute(
            """
            SELECT record FROM apc_child_operations
            WHERE parent_decision_id = ?
            ORDER BY created_at, child_operation_id
            """,
            (decision_id,),
        ).fetchall()
        return [self._stored_record(row, contracts.validate_apc_child_operation) for row in rows]

    def record_final_context(
        self, context: Mapping[str, Any], *, envelope_digest: str
    ) -> dict[str, Any]:
        contracts.validate_finalized_context(context)
        if not isinstance(envelope_digest, str) or not envelope_digest:
            raise StoreError("a finalized context requires its exact envelope digest")
        connection = self._require_connection()
        payload = self._serialize_record(context)
        with connection:
            connection.execute(
                """
                INSERT INTO final_contexts (
                    context_id, decision_id, plan_id, plan_digest,
                    envelope_digest, record, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(context_id) DO NOTHING
                """,
                (
                    context["context_id"],
                    context["decision_id"],
                    context["plan_id"],
                    context["plan_digest"],
                    envelope_digest,
                    payload,
                    context["created_at"],
                ),
            )
        persisted = self.get_final_context(str(context["context_id"]))
        row = connection.execute(
            "SELECT envelope_digest FROM final_contexts WHERE context_id = ?",
            (context["context_id"],),
        ).fetchone()
        if persisted != dict(context) or row["envelope_digest"] != envelope_digest:
            raise StoreError("final context identity already belongs to a different immutable record or envelope")
        return persisted

    def get_final_context(self, context_id: str) -> dict[str, Any]:
        connection = self._require_connection()
        row = connection.execute(
            "SELECT record FROM final_contexts WHERE context_id = ?", (context_id,)
        ).fetchone()
        if row is None:
            raise StoreError(f"final context not found: {context_id}")
        return self._stored_record(row, contracts.validate_finalized_context)

    def get_final_context_for_decision(self, decision_id: str) -> dict[str, Any] | None:
        connection = self._require_connection()
        row = connection.execute(
            """
            SELECT record FROM final_contexts
            WHERE decision_id = ?
            ORDER BY created_at DESC, context_id DESC
            LIMIT 1
            """,
            (decision_id,),
        ).fetchone()
        if row is None:
            return None
        return self._stored_record(row, contracts.validate_finalized_context)

    def start_native_usage(self, start: Mapping[str, Any]) -> dict[str, Any]:
        """Persist an actual call before counters arrive; exact replay is safe."""
        contracts.validate_native_usage_start(start)
        connection = self._require_connection()
        if start["decision_id"] is not None:
            row = connection.execute(
                "SELECT objective_id FROM decisions WHERE decision_id=?", (start["decision_id"],)
            ).fetchone()
            if row is None or row["objective_id"] != start["objective_id"]:
                raise NativeUsageConflictError("usage decision does not own the objective")
        with connection:
            connection.execute(
                "INSERT OR IGNORE INTO native_usage_starts "
                "(source, invocation_id, objective_id, maintenance_operation_id, record) "
                "VALUES (?, ?, ?, ?, ?)",
                (start["source"], start["invocation_id"], start["objective_id"],
                 start["maintenance_operation_id"], self._serialize_record(start)),
            )
            row = connection.execute(
                "SELECT record FROM native_usage_starts WHERE source=? AND invocation_id=?",
                (start["source"], start["invocation_id"]),
            ).fetchone()
            if json.loads(row["record"]) != dict(start):
                raise NativeUsageConflictError("native invocation already belongs to different evidence")
            if start["accepted_support"]:
                parent = connection.execute(
                    "SELECT record FROM native_usage_starts WHERE source=? AND invocation_id=?",
                    (start["parent_source"], start["parent_invocation_id"]),
                ).fetchone()
                if parent is None:
                    raise NativeUsageConflictError("accepted APC support parent is not started")
                parent_record = json.loads(parent["record"])
                if (parent_record["objective_id"] != start["objective_id"]
                        or parent_record["window_id"] != start["window_id"]
                        or parent_record["category"] != "online_adaptation"):
                    raise NativeUsageConflictError("accepted APC support parent has different ownership")
                peers = connection.execute(
                    "SELECT record FROM native_usage_starts WHERE objective_id=?",
                    (start["objective_id"],),
                ).fetchall()
                if any((item := json.loads(peer["record"]))["accepted_support"]
                       and item["parent_invocation_id"] == start["parent_invocation_id"]
                       and item["parent_source"] == start["parent_source"]
                       and (item["source"], item["invocation_id"]) != (start["source"], start["invocation_id"])
                       for peer in peers):
                    raise NativeUsageConflictError("adaptation already has an accepted APC support child")
        return self.get_native_usage(start["source"], start["invocation_id"])

    def record_native_usage_receipt(self, receipt: Mapping[str, Any]) -> dict[str, Any]:
        """Add one partial, cumulative, or incremental native receipt by its source id."""
        contracts.validate_native_usage_receipt(receipt)
        connection = self._require_connection()
        with connection:
            row = connection.execute(
                "SELECT record FROM native_usage_starts WHERE source=? AND invocation_id=?",
                (receipt["source"], receipt["invocation_id"]),
            ).fetchone()
            if row is None:
                raise NativeUsageConflictError("receipt has no known started invocation")
            start = json.loads(row["record"])
            for field in ("source", "invocation_id", "objective_id", "decision_id",
                          "maintenance_operation_id", "stage", "category", "window_id"):
                if receipt[field] != start[field]:
                    raise NativeUsageConflictError(f"native usage receipt conflicts on {field}")
            connection.execute(
                "INSERT OR IGNORE INTO native_usage_receipts "
                "(source, invocation_id, receipt_id, record) VALUES (?, ?, ?, ?)",
                (receipt["source"], receipt["invocation_id"], receipt["receipt_id"],
                 self._serialize_record(receipt)),
            )
            row = connection.execute(
                "SELECT record FROM native_usage_receipts "
                "WHERE source=? AND invocation_id=? AND receipt_id=?",
                (receipt["source"], receipt["invocation_id"], receipt["receipt_id"]),
            ).fetchone()
            if json.loads(row["record"]) != dict(receipt):
                raise NativeUsageConflictError("native receipt id already belongs to different evidence")
            # Projection validates compatible receipt modes before the write commits.
            return self.get_native_usage(receipt["source"], receipt["invocation_id"])

    def get_native_usage(self, source: str, invocation_id: str) -> dict[str, Any]:
        """Project one durable start and all its receipts without changing quality state."""
        connection = self._require_connection()
        row = connection.execute(
            "SELECT record FROM native_usage_starts WHERE source=? AND invocation_id=?",
            (source, invocation_id),
        ).fetchone()
        if row is None:
            raise StoreError(f"native invocation not found: {source}/{invocation_id}")
        start = json.loads(row["record"])
        contracts.validate_native_usage_start(start)
        rows = connection.execute(
            "SELECT record FROM native_usage_receipts WHERE source=? AND invocation_id=? ORDER BY receipt_id",
            (source, invocation_id),
        ).fetchall()
        measures_by_source: dict[tuple[str, str], dict[str, Any]] = {}
        modes: dict[tuple[str, str], str] = {}
        effective_binding = dict(start["binding"])
        complete = False
        for item in rows:
            receipt = json.loads(item["record"])
            contracts.validate_native_usage_receipt(receipt)
            for name, value in receipt["binding"].items():
                if value is not None:
                    if effective_binding[name] is not None and effective_binding[name] != value:
                        raise NativeUsageConflictError(f"native usage receipt conflicts on binding {name}")
                    effective_binding[name] = value
            complete = complete or receipt["complete"]
            for name, measure in receipt["measures"].items():
                source_key = (name, measure["unit"])
                previous_mode = modes.setdefault(source_key, receipt["mode"])
                if previous_mode != receipt["mode"]:
                    raise NativeUsageConflictError("mixed cumulative and incremental native measure")
                previous = measures_by_source.get(source_key)
                value = measure["value"]
                if previous is not None:
                    old_inclusion = previous["included_in_total"]
                    new_inclusion = measure["included_in_total"]
                    if (old_inclusion is not None and new_inclusion is not None
                            and old_inclusion != new_inclusion):
                        raise NativeUsageConflictError("native measure total inclusion conflicts")
                    value = (max(previous["value"], value) if receipt["mode"] == "cumulative"
                             else previous["value"] + value)
                    if new_inclusion is None:
                        measure = {**measure, "included_in_total": old_inclusion}
                measures_by_source[source_key] = {**measure, "value": value}
        measures: dict[str, dict[str, Any]] = {}
        for (name, unit), measure in measures_by_source.items():
            inclusion = ("included" if measure["included_in_total"] is True else
                         "excluded" if measure["included_in_total"] is False else "unknown")
            measures[f"{name}|{unit}|{inclusion}"] = measure
        return {**start, "coverage": "complete" if complete and measures else "incomplete",
                "effective_binding": effective_binding,
                "measures": measures, "receipt_count": len(rows)}

    def list_native_usage(
        self, *, objective_id: str | None = None,
        maintenance_operation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Inspect each started call under one caller-selected owner."""
        if (objective_id is None) == (maintenance_operation_id is None):
            raise StoreError("aggregate requires exactly one objective or maintenance operation")
        column, owner = (("objective_id", objective_id) if objective_id is not None
                         else ("maintenance_operation_id", maintenance_operation_id))
        connection = self._require_connection()
        rows = connection.execute(
            f"SELECT source, invocation_id FROM native_usage_starts WHERE {column}=? "
            "ORDER BY source, invocation_id", (owner,),
        ).fetchall()
        return [self.get_native_usage(row["source"], row["invocation_id"]) for row in rows]

    def aggregate_native_usage(
        self, *, objective_id: str | None = None,
        maintenance_operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Sum compatible measures and qualify each by coverage of all started calls."""
        usages = self.list_native_usage(
            objective_id=objective_id, maintenance_operation_id=maintenance_operation_id)
        total: dict[str, dict[str, Any]] = {}
        categories: dict[str, dict[str, Any]] = {}
        measure_counts: dict[str, int] = {}
        category_measure_counts: dict[str, dict[str, int]] = {}
        incomplete = []
        for usage in usages:
            category = categories.setdefault(usage["category"], {"invocations": 0, "measures": {}})
            category["invocations"] += 1
            if usage["coverage"] != "complete":
                incomplete.append({"source": usage["source"], "invocation_id": usage["invocation_id"]})
            for key, measure in usage["measures"].items():
                current = total.setdefault(key, {**measure, "value": 0})
                current["value"] += measure["value"]
                in_category = category["measures"].setdefault(key, {**measure, "value": 0})
                in_category["value"] += measure["value"]
                if usage["coverage"] == "complete":
                    measure_counts[key] = measure_counts.get(key, 0) + 1
                    counts = category_measure_counts.setdefault(usage["category"], {})
                    counts[key] = counts.get(key, 0) + 1
        for name, category in categories.items():
            counts = category_measure_counts.get(name, {})
            category["measure_coverage"] = {
                key: "complete" if counts.get(key, 0) == category["invocations"] else "incomplete"
                for key in category["measures"]
            }
        return {"invocations": len(usages), "by_category": categories,
                "measures": total,
                "measure_coverage": {
                    key: "complete" if measure_counts.get(key, 0) == len(usages) else "incomplete"
                    for key in total
                },
                "incomplete_invocations": incomplete,
                "coverage": "incomplete" if incomplete else "complete"}



__all__ = [
    "MemoryStore", "StoreError", "OperationConflictError", "OutcomeConflictError",
    "TrajectoryConflictError", "ExperienceConflictError", "ProcedureConflictError",
    "ProcedureDesignationConflictError", "PreparationConflictError",
    "NativeUsageConflictError",
]
