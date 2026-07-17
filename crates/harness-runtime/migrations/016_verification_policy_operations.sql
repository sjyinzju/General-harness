-- Migration 016: Verification Policy Operations — formal idempotency and
-- lifecycle tracking for deferred policy steps (Diff, FileScope, SecretScan,
-- Artifact, RequiredFile, ForbiddenChange, OutputMatcher, WorktreeCheck).
--
-- Additive only. Migrations 001–015 frozen. No FSM changes to Gate C.
--
-- Each policy step execution produces exactly one operation row.
-- Idempotency: (idempotency_key) UNIQUE. Same key + same request_hash →
--   returns existing result. Same key + different hash → conflict.
-- Lifecycle: pending → running → completed | failed | reconciliation_required.
-- Events reference policy_op_id via verification_step_events.step_op_id.
-- NO lease tokens, credentials, raw secrets, or full log dumps.

CREATE TABLE verification_policy_operations (
    policy_op_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id),
    step_id TEXT NOT NULL,
    step_kind TEXT NOT NULL,
    sequence_index INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    input_fingerprint TEXT,
    worktree_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    plan_fingerprint TEXT,
    policy_version INTEGER NOT NULL DEFAULT 1,
    validator_version TEXT NOT NULL DEFAULT '1.0',
    lifecycle TEXT NOT NULL DEFAULT 'pending',
    result_id TEXT,
    evidence_id TEXT,
    outcome_json TEXT,
    started_at TEXT,
    terminal_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_policy_ops_run ON verification_policy_operations(verification_run_id);
CREATE INDEX idx_policy_ops_step ON verification_policy_operations(step_id);
CREATE INDEX idx_policy_ops_lifecycle ON verification_policy_operations(lifecycle);
