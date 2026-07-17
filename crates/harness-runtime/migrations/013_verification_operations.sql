-- Migration 013: Verification Operations — ownership events, step operations,
-- and event idempotency linkage. Additive only — migrations 001–012 are frozen.
--
-- ownership_events records the exactly-once ownership-acquired fact for each
-- VerificationRun. Written atomically with the Created→Running transition.
--
-- step_operations records each verification step execution for idempotency
-- and crash-window recovery. Same key + same hash → existing result.
--
-- NO lease tokens, credentials, API keys, environment variable values,
-- raw secrets, or full log dumps are persisted in these tables.

-- ── Ownership Events ───────────────────────────────────────────────

CREATE TABLE verification_ownership_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id) ON DELETE CASCADE,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL,
    handoff_id TEXT NOT NULL,
    worktree_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    claim_group_id TEXT,
    fencing_token INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_ownership_events_run ON verification_ownership_events(verification_run_id);
CREATE INDEX idx_ownership_events_execution ON verification_ownership_events(execution_id);

-- ── Step Operations ────────────────────────────────────────────────

CREATE TABLE verification_step_operations (
    op_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id) ON DELETE CASCADE,
    step_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    step_config_hash TEXT NOT NULL,
    worktree_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    process_start_count INTEGER NOT NULL DEFAULT 0,
    process_pid INTEGER,
    process_exit_code INTEGER,
    output_artifact_ref TEXT,
    output_size_bytes INTEGER,
    output_truncated INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER,
    outcome_json TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_step_ops_run ON verification_step_operations(verification_run_id);
CREATE INDEX idx_step_ops_step ON verification_step_operations(step_id);
CREATE INDEX idx_step_ops_status ON verification_step_operations(status);

-- ── Step Process Identity ──────────────────────────────────────────

CREATE TABLE verification_step_processes (
    process_id TEXT PRIMARY KEY NOT NULL,
    op_id TEXT NOT NULL REFERENCES verification_step_operations(op_id) ON DELETE CASCADE,
    verification_run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    pid INTEGER,
    session_id TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    terminated_at TEXT,
    exit_code INTEGER,
    termination_reason TEXT
);

CREATE INDEX idx_step_processes_op ON verification_step_processes(op_id);
