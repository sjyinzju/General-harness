-- Migration 023: Candidate Review Gate (I4.6).
--
-- Five new tables for the Candidate Review system:
--   candidate_snapshots     — immutable frozen Candidate snapshots
--   review_requests          — review lifecycle tracking
--   review_findings          — per-finding structured output
--   review_decisions         — terminal decision records
--   review_dossier_refs      — dossier artifact references
--
-- Additive only. Migrations 001–022 frozen.

-- ── Candidate Snapshot ─────────────────────────────────────────────────

CREATE TABLE candidate_snapshots (
    candidate_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    execution_id TEXT NOT NULL REFERENCES execution_attempts(id),
    executor_profile_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    candidate_tree_hash TEXT NOT NULL,
    diff_digest TEXT NOT NULL,
    task_spec_digest TEXT NOT NULL,
    evidence_digest TEXT NOT NULL,
    composite_digest TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_candidate_task ON candidate_snapshots(task_id);
CREATE INDEX idx_candidate_execution ON candidate_snapshots(execution_id);

-- ── Review Request ─────────────────────────────────────────────────────

CREATE TABLE review_requests (
    review_id TEXT PRIMARY KEY NOT NULL,
    candidate_id TEXT NOT NULL REFERENCES candidate_snapshots(candidate_id),
    reviewer_profile_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'requested'
        CHECK (state IN (
            'requested','preparing','prechecking','reviewing',
            'approved','rejected','blocked','cancelled','stale')),
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE INDEX idx_review_candidate ON review_requests(candidate_id);
CREATE INDEX idx_review_state ON review_requests(state);

CREATE UNIQUE INDEX idx_review_one_active_per_candidate
    ON review_requests(candidate_id)
    WHERE state NOT IN ('approved','rejected','blocked','cancelled','stale');

-- ── Review Finding ─────────────────────────────────────────────────────

CREATE TABLE review_findings (
    finding_id TEXT PRIMARY KEY NOT NULL,
    review_id TEXT NOT NULL REFERENCES review_requests(review_id),
    severity TEXT NOT NULL
        CHECK (severity IN ('critical','high','medium','low')),
    category TEXT NOT NULL
        CHECK (category IN (
            'requirement_mismatch','scope_violation','correctness',
            'safety','security','evidence_gap','test_gap',
            'architecture_violation','maintainability')),
    summary TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '',
    source_location TEXT,
    evidence_reference TEXT,
    blocking INTEGER NOT NULL DEFAULT 1 CHECK (blocking IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_finding_review ON review_findings(review_id);
CREATE INDEX idx_finding_severity ON review_findings(severity);

-- ── Review Decision ────────────────────────────────────────────────────

CREATE TABLE review_decisions (
    decision_id TEXT PRIMARY KEY NOT NULL,
    review_id TEXT NOT NULL REFERENCES review_requests(review_id),
    candidate_id TEXT NOT NULL REFERENCES candidate_snapshots(candidate_id),
    decision TEXT NOT NULL
        CHECK (decision IN ('approved','rejected','blocked','stale')),
    summary TEXT NOT NULL DEFAULT '',
    candidate_digest_at_decision TEXT NOT NULL,
    decision_digest TEXT NOT NULL,
    findings_count INTEGER NOT NULL DEFAULT 0,
    reviewer_output_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_decision_review ON review_decisions(review_id);
CREATE INDEX idx_decision_candidate ON review_decisions(candidate_id);

-- ── Review Dossier Reference ───────────────────────────────────────────

CREATE TABLE review_dossier_refs (
    dossier_id TEXT PRIMARY KEY NOT NULL,
    review_id TEXT NOT NULL REFERENCES review_requests(review_id),
    candidate_id TEXT NOT NULL REFERENCES candidate_snapshots(candidate_id),
    dossier_json TEXT NOT NULL DEFAULT '{}',
    dossier_digest TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_dossier_review ON review_dossier_refs(review_id);
CREATE UNIQUE INDEX idx_dossier_digest ON review_dossier_refs(dossier_digest);
