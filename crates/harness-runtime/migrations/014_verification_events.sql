-- Migration 014: Verification Step Events — proper domain events for
-- verification step lifecycle. Additive only — migrations 001–013 frozen.
--
-- Each step transition produces at most one event of each type.
-- Events use idempotency_key for exactly-once semantics.
-- NO lease tokens, credentials, secrets, or full stdout/stderr.

CREATE TABLE verification_step_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    step_op_id TEXT NOT NULL REFERENCES verification_step_operations(op_id) ON DELETE CASCADE,
    execution_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    worktree_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    step_kind TEXT NOT NULL,
    detail_json TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_step_events_run ON verification_step_events(verification_run_id);
CREATE INDEX idx_step_events_step ON verification_step_events(step_id);
CREATE INDEX idx_step_events_op ON verification_step_events(step_op_id);
