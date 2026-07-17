-- Migration 017: Verification Finalization — formal finalization operations
-- and outcome persistence for deterministic VerificationRun terminalization.
--
-- Additive only. Migrations 001–016 frozen. No FSM changes to Gate C.
--
-- Each finalization attempt produces exactly one operation row.
-- Idempotency: (idempotency_key) UNIQUE. Same key + same request_hash →
--   returns existing outcome. Same key + different hash → conflict.
-- Lifecycle: pending → running → outcome_persisted → releasing_resources →
--   completed | reconciliation_required.

CREATE TABLE verification_finalization_operations (
    finalization_op_id TEXT PRIMARY KEY NOT NULL,
    verification_run_id TEXT NOT NULL REFERENCES verification_runs(run_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    target_outcome_fingerprint TEXT,
    plan_fingerprint TEXT,
    worktree_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    owner_kind TEXT NOT NULL DEFAULT 'verification',
    owner_id TEXT NOT NULL,
    lifecycle TEXT NOT NULL DEFAULT 'pending',
    -- Release progress tracking (JSON-safe, no secrets).
    release_progress_json TEXT,
    -- Outcome reference after persistence.
    outcome_summary TEXT,
    outcome_classification TEXT,
    dossier_json TEXT,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    outcome_persisted_at TEXT,
    resources_released_at TEXT,
    terminal_at TEXT
);

CREATE INDEX idx_finalization_ops_run ON verification_finalization_operations(verification_run_id);
CREATE INDEX idx_finalization_ops_lifecycle ON verification_finalization_operations(lifecycle);
