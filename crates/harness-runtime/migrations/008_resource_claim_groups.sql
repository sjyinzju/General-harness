-- Migration 008: Resource Claim Groups (I3 Resource Claim Kernel).
-- Adds a parent `resource_claim_groups` table for atomic multi-resource
-- acquisition, and extends the existing `resource_claims` table (created
-- in migration 001) with group linkage and lifecycle columns.
-- Additive only — migrations 001-007 are frozen.

-- ── New: resource_claim_groups (parent table) ───────────────────────

CREATE TABLE resource_claim_groups (
    group_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    execution_id TEXT REFERENCES execution_attempts(id) ON DELETE SET NULL,
    repository_identity TEXT NOT NULL DEFAULT '',
    worktree_id TEXT,
    lease_id TEXT,
    fencing_token INTEGER NOT NULL DEFAULT 0,
    request_hash TEXT NOT NULL,
    lifecycle TEXT NOT NULL DEFAULT 'active',   -- active | released | expired
    acquired_at TEXT NOT NULL DEFAULT (datetime('now')),
    heartbeat_at TEXT,
    expires_at TEXT,
    released_at TEXT,
    release_reason TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Lookup indexes.
CREATE INDEX idx_claim_groups_task ON resource_claim_groups(task_id);
CREATE INDEX idx_claim_groups_execution ON resource_claim_groups(execution_id);
CREATE INDEX idx_claim_groups_repo ON resource_claim_groups(repository_identity);
CREATE INDEX idx_claim_groups_worktree ON resource_claim_groups(worktree_id);
CREATE INDEX idx_claim_groups_lease ON resource_claim_groups(lease_id);
CREATE INDEX idx_claim_groups_active ON resource_claim_groups(lifecycle) WHERE lifecycle = 'active';
CREATE INDEX idx_claim_groups_hash ON resource_claim_groups(request_hash);

-- ── Extend resource_claims with group linkage ──────────────────────

ALTER TABLE resource_claims ADD COLUMN group_id TEXT REFERENCES resource_claim_groups(group_id);
ALTER TABLE resource_claims ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'active';
ALTER TABLE resource_claims ADD COLUMN created_at TEXT NOT NULL DEFAULT (datetime('now'));

CREATE INDEX idx_resource_claims_group ON resource_claims(group_id);
