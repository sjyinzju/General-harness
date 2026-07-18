-- Migration 019: Verification Release Steps — durable per-step execution
-- authority for the finalization resource-release saga.
--
-- Additive only. Migrations 001–018 frozen. No FSM changes to Gate C.
--
-- Every resource side effect (Claim release, Lease release, Heartbeat
-- unregister, Handoff release, the ResourcesReleased event, and operation
-- completion) is guarded by one row here. A worker must CAS the row
-- pending → in_progress BEFORE executing the side effect; only the CAS
-- winner executes it, and completion is CAS'd in_progress → completed with
-- worker + fencing + version bound. ReleaseProgress JSON on
-- verification_finalization_operations remains a human-readable summary but
-- is no longer the execution authority.

CREATE TABLE verification_release_steps (
    release_step_id TEXT PRIMARY KEY NOT NULL,
    finalization_op_id TEXT NOT NULL REFERENCES verification_finalization_operations(finalization_op_id),
    step_kind TEXT NOT NULL CHECK (step_kind IN (
        'claim_release',
        'lease_release',
        'heartbeat_unregister',
        'handoff_release',
        'resources_released_event',
        'operation_completion')),
    step_order INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN (
        'pending',
        'in_progress',
        'completed',
        'failed',
        'reconciliation_required')),
    worker_id TEXT,
    owner_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    claimed_at TEXT,
    completed_at TEXT,
    failed_at TEXT,
    result_fingerprint TEXT,
    error_classification TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(finalization_op_id, step_kind)
);

CREATE INDEX idx_release_steps_op ON verification_release_steps(finalization_op_id);
CREATE INDEX idx_release_steps_state ON verification_release_steps(state);
