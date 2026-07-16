-- Migration v7: Approval persistence boundary (I2B-3 Workspace Policy).
-- Stores command approval decisions keyed by the composite command
-- fingerprint so an approval cannot be reused for a different command
-- shape, and scoped to a fencing epoch so a stale lease owner's approvals
-- cannot be reused after takeover. Additive only — migrations 001-006 frozen.

CREATE TABLE policy_approvals (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    command_fingerprint TEXT NOT NULL,   -- composite key (exec|args|cwd|env)
    decision TEXT NOT NULL,              -- approved / denied / expired
    expiry TEXT,                         -- opaque ISO-ish timestamp string
    fencing_token INTEGER,               -- epoch the approval was recorded under
    evaluator_identity TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_policy_approvals_fp ON policy_approvals(command_fingerprint);
