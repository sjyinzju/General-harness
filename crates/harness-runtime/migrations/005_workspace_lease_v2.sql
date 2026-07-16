-- Migration v5: Workspace Lease v2 (I2B-2 Lease ownership & fencing).
-- Extends the existing `workspace_leases` table (001_initial_schema) with
-- fencing tokens, lease epochs, and partial-unique-indexes that enforce
-- single-active-lease rules per worktree / task / execution.
-- Migrations 001–004 are unchanged.

-- ── lease_epoch on worktrees (monotonically increasing fencing source) ──────
ALTER TABLE worktrees ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0;

-- ── Extend workspace_leases ─────────────────────────────────────────────────

ALTER TABLE workspace_leases ADD COLUMN worktree_id TEXT REFERENCES worktrees(id);
ALTER TABLE workspace_leases ADD COLUMN project_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workspace_leases ADD COLUMN owner_supervisor_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workspace_leases ADD COLUMN lease_token TEXT;
ALTER TABLE workspace_leases ADD COLUMN fencing_token INTEGER;      -- monotonically increasing epoch
ALTER TABLE workspace_leases ADD COLUMN release_reason TEXT;       -- why Released
ALTER TABLE workspace_leases ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'));
ALTER TABLE workspace_leases ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));

-- Existing rows did not have lease tokens / fencing tokens / epoch values.
-- They cannot represent a functioning lease, so they become Expired.
UPDATE workspace_leases
   SET lifecycle = 'expired',
       expires_at = datetime('now'),
       updated_at = datetime('now')
 WHERE lease_token IS NULL AND lifecycle NOT IN ('released','expired');

-- ── Partial unique indexes: one active lease per entity ─────────────────────
-- These enforce the invariant globally; the Service additionally validates
-- inside the acquire transaction for consistent error reporting.

CREATE UNIQUE INDEX idx_leases_active_worktree
    ON workspace_leases(worktree_id) WHERE lifecycle NOT IN ('released','expired');

CREATE UNIQUE INDEX idx_leases_active_task
    ON workspace_leases(task_id) WHERE lifecycle NOT IN ('released','expired');

CREATE UNIQUE INDEX idx_leases_active_execution
    ON workspace_leases(owner_execution_id) WHERE lifecycle NOT IN ('released','expired');

-- The lease token itself is also a logical unique per active lease.
CREATE UNIQUE INDEX idx_leases_active_token
    ON workspace_leases(lease_token) WHERE lifecycle NOT IN ('released','expired');
