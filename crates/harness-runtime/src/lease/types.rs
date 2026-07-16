//! Workspace Lease data types — runtime-owned, Gate C LeaseLifecycle compatible.

use std::time::Duration;

/// Result of a lease acquisition attempt.
#[derive(Debug, Clone)]
pub enum LeaseAcquireOutcome {
    Acquired(LeaseRecord),
    /// Idempotent replay: same idempotency key returned the existing lease.
    AlreadyAcquired(LeaseRecord),
    /// Another owner currently holds an active lease; includes the blocking
    /// lease id (NOT the token).
    Contested {
        existing_lease_id: String,
    },
    /// Worktree / execution does not exist or is in the wrong state.
    PreconditionFailed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseHeartbeatOutcome {
    Ok,
    /// Heartbeat bumped expires_at but the margin is low.
    AtRisk {
        expires_at: String,
    },
    TokenMismatch,
    FencingMismatch,
    Expired,
    NotActive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseReleaseOutcome {
    Released,
    AlreadyReleased,
    TokenMismatch,
    NotActive,
}

/// Active lease view returned by query methods (tokens included — callers
/// must preserve them; they are never logged).
#[derive(Clone)]
pub struct LeaseRecord {
    pub lease_id: String,
    pub worktree_id: Option<String>,
    pub project_id: String,
    pub task_id: String,
    pub owner_execution_id: Option<String>,
    pub owner_supervisor_id: String,
    pub lease_token: String,
    pub fencing_token: i64,
    pub lifecycle: String,
    pub acquired_at: String,
    pub heartbeat_at: Option<String>,
    pub expires_at: String,
    pub released_at: Option<String>,
    pub release_reason: Option<String>,
    pub version: i64,
}

// Custom Debug: the lease token must never appear in debug output.
impl std::fmt::Debug for LeaseRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseRecord")
            .field("lease_id", &self.lease_id)
            .field("worktree_id", &self.worktree_id)
            .field("project_id", &self.project_id)
            .field("task_id", &self.task_id)
            .field("owner_execution_id", &self.owner_execution_id)
            .field("owner_supervisor_id", &self.owner_supervisor_id)
            .field("lease_token", &"[REDACTED]")
            .field("fencing_token", &self.fencing_token)
            .field("lifecycle", &self.lifecycle)
            .field("acquired_at", &self.acquired_at)
            .field("heartbeat_at", &self.heartbeat_at)
            .field("expires_at", &self.expires_at)
            .field("released_at", &self.released_at)
            .field("release_reason", &self.release_reason)
            .field("version", &self.version)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct LeaseSpec {
    pub worktree_id: String,
    pub project_id: String,
    pub task_id: String,
    pub owner_execution_id: String,
    pub owner_supervisor_id: String,
    pub lease_duration: Duration,
    pub idempotency_key: String,
}

/// Describes the status of a Worktree from the Lease perspective.
#[derive(Debug, Clone)]
pub struct WorktreeLeaseStatus {
    pub active_lease: Option<LeaseRecord>,
    pub can_acquire: bool,
    pub blocked_by: Option<String>,
}

/// Configuration driving lease duration, heartbeat interval, and renewal
/// margin. Invariant: `heartbeat_interval` must be significantly less than
/// `lease_duration`.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub lease_duration: Duration,
    pub heartbeat_interval: Duration,
    /// Soft deadline after which heartbeat is still accepted but the lease is
    /// reported AtRisk; must be < lease_duration.
    pub renewal_margin: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(300),    // 5 min
            heartbeat_interval: Duration::from_secs(60), // 1 min
            renewal_margin: Duration::from_secs(120),    // 2 min
        }
    }
}

impl LeaseConfig {
    /// The hard deadline for accepting a heartbeat (lease_duration - a tiny
    /// grace). Heartbeats arriving after this point from the wall clock are
    /// refused even if the DB timestamp has not ticked over yet.
    pub fn heartbeat_deadline(&self) -> Duration {
        self.lease_duration.saturating_sub(Duration::from_secs(5))
    }
}
