-- Migration v4: Worktree records (I2B-1 WorktreeManager).
-- Additive only — migrations 001/002/003 are frozen as applied.

CREATE TABLE worktrees (
    id TEXT PRIMARY KEY NOT NULL,                -- worktree_id (wt-<task>-<execution>)
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    repository_root TEXT NOT NULL,               -- canonical main worktree root
    repository_identity TEXT NOT NULL,           -- canonical common git directory
    worktree_path TEXT NOT NULL,                 -- canonical linked worktree path
    branch_name TEXT NOT NULL,
    base_commit TEXT NOT NULL,                   -- full OID resolved at create time
    owner_supervisor_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,                  -- creating operation
    status TEXT NOT NULL DEFAULT 'active',       -- active|removing|removed|reconciliation_required
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    removed_at TEXT
);

-- One live worktree per path / per branch within a repository.
CREATE UNIQUE INDEX idx_worktrees_live_path
    ON worktrees(worktree_path) WHERE status != 'removed';
CREATE UNIQUE INDEX idx_worktrees_live_branch
    ON worktrees(repository_identity, branch_name) WHERE status != 'removed';
CREATE INDEX idx_worktrees_task ON worktrees(task_id);
