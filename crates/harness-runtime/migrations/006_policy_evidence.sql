-- Migration v6: Policy Evidence persistence (I2B-3 Workspace Policy).
-- Evidence records + findings; large diffs go to artifact spool, not SQLite.
-- Additive only — migrations 001-005 are frozen.

CREATE TABLE policy_evaluations (
    id TEXT PRIMARY KEY NOT NULL,
    evaluation_type TEXT NOT NULL,         -- command / file_scope / diff / secret_scan
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    worktree_id TEXT,
    fencing_token INTEGER,                -- epoch value, NOT the lease token
    policy_version INTEGER NOT NULL DEFAULT 1,
    input_fingerprint TEXT,               -- hash of command / scope args
    decision TEXT NOT NULL,               -- allowed / denied / require_approval
    reasons_json TEXT NOT NULL DEFAULT '[]',
    changed_path_count INTEGER,
    finding_count INTEGER,
    artifact_reference TEXT,              -- spool path for large diff content
    evaluator_identity TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_policy_evals_worktree ON policy_evaluations(worktree_id);
CREATE INDEX idx_policy_evals_fingerprint ON policy_evaluations(input_fingerprint);

CREATE TABLE policy_findings (
    id TEXT PRIMARY KEY NOT NULL,
    evaluation_id TEXT NOT NULL REFERENCES policy_evaluations(id) ON DELETE CASCADE,
    finding_type TEXT NOT NULL,
    file_path TEXT,
    line_number INTEGER,
    byte_range_start INTEGER,
    byte_range_end INTEGER,
    redacted_preview TEXT NOT NULL DEFAULT '',
    fingerprint TEXT                     -- hash of rule/value, NOT raw secret
);

CREATE INDEX idx_policy_findings_eval ON policy_findings(evaluation_id);
