//! Worktree data model — runtime-owned types for WorktreeManager v1.

use std::path::PathBuf;

/// Request to create a task worktree. `operation_id` is the stable
/// idempotency anchor: replaying the same spec never creates a second
/// worktree.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorktreeSpec {
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    /// Path anywhere inside the target repository (resolved to canonical root).
    pub repository_root: PathBuf,
    /// Commit-ish resolved to a full OID during create.
    pub base_commit: String,
    /// Target path — must resolve under the harness-owned worktree root.
    pub worktree_path: PathBuf,
    pub branch_name: String,
    /// Stable logical operation id (used as the Operation idempotency key).
    pub operation_id: String,
    pub owner_supervisor_id: String,
}

/// Canonical identity of a managed worktree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeIdentity {
    pub worktree_id: String,
    /// Canonical common git directory — the repository identity.
    pub repository_identity: String,
    pub canonical_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeStatus {
    Active,
    Removing,
    Removed,
    ReconciliationRequired,
}

impl WorktreeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Removing => "removing",
            Self::Removed => "removed",
            Self::ReconciliationRequired => "reconciliation_required",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "removing" => Self::Removing,
            "removed" => Self::Removed,
            "reconciliation_required" => Self::ReconciliationRequired,
            _ => Self::Active,
        }
    }
}

/// Persisted record of a managed worktree (SQLite `worktrees` table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeRecord {
    pub worktree_id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub repository_root: String,
    pub repository_identity: String,
    pub worktree_path: String,
    pub branch_name: String,
    pub base_commit: String,
    pub owner_supervisor_id: String,
    pub operation_id: String,
    pub status: WorktreeStatus,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub enum WorktreeCreateOutcome {
    Created(WorktreeRecord),
    /// Idempotent replay: the same operation already completed.
    AlreadyExists(WorktreeRecord),
    /// Another owner currently holds the create operation claim.
    InProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorktreeRemoveOutcome {
    Removed,
    AlreadyRemoved,
    /// Dirty worktree and the policy did not authorize forced removal.
    RefusedDirty {
        changed_entries: usize,
    },
    /// Ownership could not be proven — never delete what we cannot verify.
    RefusedOwnershipUnverified {
        reason: String,
    },
    /// Another owner currently holds the remove operation claim.
    InProgress,
}

/// Removal policy. `force_dirty` must be an explicit caller decision.
#[derive(Debug, Clone, Copy, Default)]
pub struct WorktreeRemovePolicy {
    pub force_dirty: bool,
}

/// Live inspection of a worktree against its expected record.
#[derive(Debug, Clone)]
pub struct WorktreeInspection {
    pub path_exists: bool,
    /// Path resolves to the expected repository (common git dir matches).
    pub belongs_to_repository: bool,
    pub head_commit: Option<String>,
    /// HEAD equals the recorded base commit.
    pub head_equals_base: bool,
    /// HEAD is the base commit or a descendant of it.
    pub head_descends_from_base: bool,
    pub branch: Option<String>,
    pub branch_matches: bool,
    pub metadata_present: bool,
    pub metadata_matches: bool,
    /// None when the path is gone / not inspectable.
    pub dirty: Option<bool>,
    pub locked: bool,
    pub prunable: bool,
    /// Git administrative metadata broken (path exists but git cannot
    /// resolve it as a worktree of the repository).
    pub git_admin_missing: bool,
    /// Registered in git but the directory was moved or deleted by hand.
    pub moved_or_deleted: bool,
}
