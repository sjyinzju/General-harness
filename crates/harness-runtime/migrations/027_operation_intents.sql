-- Migration 027: Operation Intents and Recovery Runs (I6.3/I6.4).
--
-- Tables:
--   operation_intents  — durable operation tracking for the control loop
--   recovery_runs      — startup reconciliation audit trail
--   recovery_actions   — per-aggregate recovery actions

-- ── Operation Intents ──────────────────────────────────────────────────────

CREATE TABLE operation_intents (
    operation_id TEXT PRIMARY KEY NOT NULL,
    request_id TEXT NOT NULL,
    operation_kind TEXT NOT NULL
        CHECK (operation_kind IN (
            'task_start','task_resume','task_cancel',
            'review_create','review_run',
            'integration_enqueue','integration_run_next','integration_cancel','integration_recover',
            'supervisor_stop','cancel','inspect')),
    aggregate_id TEXT NOT NULL,
    desired_action TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN (
            'pending','claimed','running','succeeded',
            'blocked','failed','cancelled','abandoned')),
    owner_instance_id TEXT,
    owner_fencing_token INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    error_message TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE INDEX idx_operation_intent_state ON operation_intents(state, created_at);
CREATE INDEX idx_operation_intent_aggregate ON operation_intents(aggregate_id, operation_kind);
CREATE INDEX idx_operation_intent_idempotency ON operation_intents(idempotency_key);
CREATE INDEX idx_operation_intent_owner ON operation_intents(owner_instance_id, owner_fencing_token);

-- ── Recovery Runs ──────────────────────────────────────────────────────────

CREATE TABLE recovery_runs (
    recovery_id TEXT PRIMARY KEY NOT NULL,
    supervisor_instance_id TEXT NOT NULL REFERENCES supervisor_instances(instance_id),
    fencing_token INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    state TEXT NOT NULL DEFAULT 'running'
        CHECK (state IN ('running','completed','failed')),
    scanned_count INTEGER NOT NULL DEFAULT 0,
    action_count INTEGER NOT NULL DEFAULT 0,
    blocked_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_recovery_run_instance ON recovery_runs(supervisor_instance_id);

-- ── Recovery Actions ───────────────────────────────────────────────────────

CREATE TABLE recovery_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recovery_id TEXT NOT NULL REFERENCES recovery_runs(recovery_id),
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    previous_state TEXT NOT NULL,
    observed_external_state TEXT,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    result TEXT,
    occurred_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_recovery_action_run ON recovery_actions(recovery_id);
CREATE INDEX idx_recovery_action_aggregate ON recovery_actions(aggregate_type, aggregate_id);
