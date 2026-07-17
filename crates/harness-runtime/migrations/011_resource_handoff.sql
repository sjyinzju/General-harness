-- Migration 011: Resource Handoff — persistent handoff records for
-- I4-C Verification to discover and take over scheduler resources.
-- Additive only — migrations 001–010 are frozen.
--
-- A handoff record is created on successful Agent completion.
-- It links together the execution_id, worktree_id, lease_id, and
-- claim_group_id so that I4-C Verification can:
--   - inspect active resources
--   - take over ownership (CAS with fencing + version)
--   - renew leases
--   - cancel/stop heartbeats
--   - finalize after verification
--
-- NO lease tokens, API keys, auth tokens, or environment variable values
-- are persisted in this table.

CREATE TABLE resource_handoffs (
    handoff_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    execution_id TEXT NOT NULL REFERENCES execution_attempts(id) ON DELETE CASCADE,
    worktree_id TEXT,
    lease_id TEXT,
    claim_group_id TEXT,
    -- The fencing token at the time of handoff creation; used to validate
    -- takeover requests and reject stale fencing.
    fencing_token INTEGER NOT NULL DEFAULT 0,
    -- Who currently owns this handoff:
    --   scheduler       — still owned by the I4-B Scheduler
    --   verification    — taken over by I4-C Verification
    owner_kind TEXT NOT NULL DEFAULT 'scheduler',
    -- Opaque owner identifier (scheduler runtime id or verification run id).
    owner_id TEXT NOT NULL DEFAULT '',
    -- Handoff lifecycle status:
    --   scheduler_owned      — scheduler still in control
    --   verification_owned   — transferred to I4-C Verification
    --   released             — resources released, handoff closed
    --   lost                 — heartbeat lost, may need reconciliation
    --   reconciliation_required — anomaly detected
    status TEXT NOT NULL DEFAULT 'scheduler_owned',
    -- Last time the heartbeat was confirmed alive.
    heartbeat_last_seen_at TEXT,
    -- Structured detail for diagnostics (NEVER contains tokens or secrets).
    detail_json TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Lookup by execution_id (primary query path).
CREATE UNIQUE INDEX idx_handoff_execution
    ON resource_handoffs(execution_id);

-- Lookup by lease_id.
CREATE INDEX idx_handoff_lease
    ON resource_handoffs(lease_id);

-- Lookup by task_id.
CREATE INDEX idx_handoff_task
    ON resource_handoffs(task_id);

-- Lookup by status for reconciliation.
CREATE INDEX idx_handoff_status
    ON resource_handoffs(status);
