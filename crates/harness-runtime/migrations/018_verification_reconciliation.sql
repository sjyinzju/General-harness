-- Migration 018: Verification Reconciliation — formal reconciliation operations
-- for deterministic recovery of verification finalization and resource release.
--
-- Additive only. Migrations 001–017 frozen. No FSM changes to Gate C.
--
-- Each reconciliation attempt produces exactly one operation row.
-- Idempotency: (idempotency_key) UNIQUE. Same key + same hash → existing result.
-- Lifecycle: pending → running → completed | blocked | reconciliation_required.

CREATE TABLE verification_reconciliation_operations (
    reconciliation_op_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    observed_state_fingerprint TEXT,
    classification TEXT NOT NULL DEFAULT 'NoOpAlreadyConsistent',
    planned_action TEXT,
    lifecycle TEXT NOT NULL DEFAULT 'pending',
    owner_kind TEXT NOT NULL DEFAULT 'verification',
    owner_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    result_fingerprint TEXT,
    last_error TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    terminal_at TEXT
);

CREATE INDEX idx_reconciliation_ops_run ON verification_reconciliation_operations(verification_run_id);
CREATE INDEX idx_reconciliation_ops_lifecycle ON verification_reconciliation_operations(lifecycle);
