-- Migration 010: Scheduler — dispatch, concurrency, reconciliation.
-- Additive only — migrations 001–009 are frozen.

-- ── Concurrency reservations ──────────────────────────────────────

CREATE TABLE scheduler_reservations (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    execution_id TEXT REFERENCES execution_attempts(id) ON DELETE SET NULL,
    profile_id TEXT,
    repository_id TEXT,
    dispatch_op_id TEXT,
    status TEXT NOT NULL DEFAULT 'active', -- active | released | expired
    acquired_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT,
    released_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Exactly one active reservation per task.
CREATE UNIQUE INDEX idx_reservation_one_per_task
    ON scheduler_reservations(task_id)
    WHERE status = 'active';

CREATE INDEX idx_reservations_profile ON scheduler_reservations(profile_id, status);
CREATE INDEX idx_reservations_repo ON scheduler_reservations(repository_id, status);
CREATE INDEX idx_reservations_expires ON scheduler_reservations(expires_at) WHERE status = 'active';

-- ── Dispatch operations (saga) ────────────────────────────────────

CREATE TABLE dispatch_operations (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    execution_id TEXT, -- set after execution attempt created
    selected_profile_id TEXT,
    worktree_id TEXT,
    lease_id TEXT,
    claim_group_id TEXT,
    agent_session_id TEXT,
    pid INTEGER,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'preparing', -- preparing|worktree_ready|lease_acquired|claims_acquired|agent_starting|agent_running|agent_completed|compensating|completed|failed
    stage TEXT NOT NULL DEFAULT 'init',
    stage_detail TEXT,
    outcome_json TEXT,
    compensation_json TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_dispatch_ops_task ON dispatch_operations(task_id);
CREATE INDEX idx_dispatch_ops_execution ON dispatch_operations(execution_id);
CREATE INDEX idx_dispatch_ops_status ON dispatch_operations(status);

-- ── Scheduler reconciliation log ──────────────────────────────────

CREATE TABLE scheduler_reconciliations (
    id TEXT PRIMARY KEY NOT NULL,
    anomaly_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    description TEXT NOT NULL,
    repair_action TEXT,
    repair_status TEXT NOT NULL DEFAULT 'detected', -- detected|repaired|skipped|failed
    idempotency_key TEXT NOT NULL UNIQUE,
    detected_at TEXT NOT NULL DEFAULT (datetime('now')),
    repaired_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_reconciliations_type ON scheduler_reconciliations(anomaly_type);
CREATE INDEX idx_reconciliations_entity ON scheduler_reconciliations(entity_type, entity_id);
