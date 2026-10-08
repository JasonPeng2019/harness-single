-- Accepted e2bd6bd STEP-03 _SCHEMA statements; no STEP-04 tables.
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
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
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
    );
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
    );
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
    );
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
    );
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
    );
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
    );
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
    );
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
    );
CREATE TABLE IF NOT EXISTS procedure_revisions (
        revision_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        origin_scope_digest TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
CREATE TABLE IF NOT EXISTS procedure_approvals (
        approval_id TEXT PRIMARY KEY,
        revision_id TEXT NOT NULL REFERENCES procedure_revisions(revision_id),
        logical_id TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
CREATE TABLE IF NOT EXISTS procedure_representations (
        representation_id TEXT PRIMARY KEY,
        revision_id TEXT NOT NULL REFERENCES procedure_revisions(revision_id),
        logical_id TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
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
    );
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
    );
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
    );
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
    );
CREATE TABLE IF NOT EXISTS procedure_revocations (
        revocation_id TEXT PRIMARY KEY,
        logical_id TEXT NOT NULL,
        revision_id TEXT NOT NULL UNIQUE REFERENCES procedure_revisions(revision_id),
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
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
    );
CREATE TABLE IF NOT EXISTS procedure_exposures (
        exposure_id TEXT PRIMARY KEY,
        publication_id TEXT NOT NULL REFERENCES procedure_publications(publication_id),
        revision_id TEXT NOT NULL,
        record TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        delivered_at TEXT NOT NULL
    );
