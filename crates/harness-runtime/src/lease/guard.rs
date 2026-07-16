//! WorkspaceLeaseAccessValidator — runtime-owned trait that decouples
//! WorktreeManager from the full LeaseService. Injected at composition root.
//!
//! WorktreeManager calls `can_remove_worktree` before performing a remove.
//! When no lease service is configured (tests, development), a
//! `NoOpAccessValidator` allows all operations.

use harness_core::{CoreError, ErrorCode, ErrorSource};

/// Outcome of a lease-access check for worktree removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseAccessResult {
    /// Allowed — no active lease blocks the operation.
    Allowed,
    /// An active lease exists on this worktree. The caller must release or
    /// expire it before removal.
    BlockedByActiveLease {
        lease_id: String,
        /// Opaque — the caller does not receive the lease token.
        owner_supervisor_id: String,
    },
    /// The provided fencing token is stale (another owner now holds the
    /// lease).
    StaleFencingToken,
    /// The lease token / fencing token combination is invalid.
    Unauthorized,
}

/// Minimal information the WorktreeManager needs to pass to the validator.
#[derive(Debug, Clone)]
pub struct WorktreeAccessRequest {
    pub worktree_id: String,
    pub worktree_path: String,
    pub task_id: String,
    pub execution_id: String,
    pub owner_supervisor_id: String,
    /// Only populated when the caller is explicitly passing a lease
    /// credential (admin recovery or controlled force-remove).
    pub lease_credential: Option<LeaseCredential>,
}

/// Credentials that prove authority over a specific lease.
#[derive(Debug, Clone)]
pub struct LeaseCredential {
    pub lease_id: String,
    pub lease_token: String,
    pub fencing_token: i64,
}

/// Trait injected into WorktreeManager so it can gate remove on lease
/// status without importing the lease module.
#[async_trait::async_trait]
pub trait WorkspaceLeaseAccessValidator: Send + Sync {
    /// Check whether the given worktree can currently be removed.
    /// - No active lease → Allowed.
    /// - Active lease held by a different supervisor → BlockedByActiveLease.
    /// - Active lease with matching credential → Allowed (admin override).
    async fn can_remove_worktree(
        &self,
        request: &WorktreeAccessRequest,
    ) -> Result<LeaseAccessResult, CoreError>;

    /// Validate that the lease credential authorizes a force-remove.
    /// Only succeeds when lease_token + fencing_token match the active
    /// lease exactly AND the worktree identity is correct.
    async fn validate_force_credential(
        &self,
        worktree_id: &str,
        credential: &LeaseCredential,
    ) -> Result<bool, CoreError>;
}

/// Default validator for tests / no-lease deployments. Allows all
/// operations.
pub struct NoOpAccessValidator;

#[async_trait::async_trait]
impl WorkspaceLeaseAccessValidator for NoOpAccessValidator {
    async fn can_remove_worktree(
        &self,
        _request: &WorktreeAccessRequest,
    ) -> Result<LeaseAccessResult, CoreError> {
        Ok(LeaseAccessResult::Allowed)
    }

    async fn validate_force_credential(
        &self,
        _worktree_id: &str,
        _credential: &LeaseCredential,
    ) -> Result<bool, CoreError> {
        Ok(true)
    }
}

fn _ls_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
