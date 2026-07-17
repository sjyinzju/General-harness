-- Migration 015: Verification Approvals — command execution approvals for
-- shell and restricted commands. Additive only — migrations 001–014 frozen.
-- NO credentials, lease tokens, environment values, or secrets.

CREATE TABLE verification_approvals (
    approval_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    step_op_id TEXT NOT NULL,
    cmd_fingerprint TEXT NOT NULL,
    worktree_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL DEFAULT 0,
    single_use INTEGER NOT NULL DEFAULT 0,
    lifecycle TEXT NOT NULL DEFAULT 'pending',
    expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_approvals_run ON verification_approvals(verification_run_id);
CREATE INDEX idx_approvals_step ON verification_approvals(step_id);
CREATE INDEX idx_approvals_lifecycle ON verification_approvals(lifecycle);
