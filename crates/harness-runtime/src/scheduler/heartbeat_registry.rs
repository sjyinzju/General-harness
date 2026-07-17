//! HeartbeatRegistry — runtime-owned registry for discoverable heartbeat
//! management and I4-C Verification resource handoff.
//!
//! Every successful dispatch registers its heartbeat here so that:
//! - The heartbeat is NOT a lost fire-and-forget tokio::spawn
//! - I4-C can `inspect` by execution_id or lease_id
//! - I4-C can `takeover` ownership (CAS with fencing)
//! - I4-C can `cancel` the heartbeat
//! - The reconciler can detect missing heartbeats
//!
//! The registry is the runtime view. The persisted `resource_handoffs`
//! table is the durable view. Both must agree after takeover.

use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use harness_core::{CoreError, ErrorCode, ErrorSource};

/// Status of a heartbeat in the runtime registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatStatus {
    /// Heartbeat is running and healthy.
    Healthy,
    /// Heartbeat has encountered transient errors but is still running.
    Degraded { last_error: String },
    /// Heartbeat has stopped (cancelled, expired, or lost).
    Stopped { reason: String },
    /// Heartbeat is missing from the registry (entry existed but runner vanished).
    Missing,
}

/// Owner kind for the handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerKind {
    Scheduler,
    Verification,
}

impl OwnerKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            OwnerKind::Scheduler => "scheduler",
            OwnerKind::Verification => "verification",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "verification" => OwnerKind::Verification,
            _ => OwnerKind::Scheduler,
        }
    }
}

/// A runtime entry tracking a dispatch's heartbeat and resources.
#[derive(Clone)]
pub struct HeartbeatEntry {
    pub execution_id: String,
    pub task_id: String,
    pub worktree_id: String,
    pub lease_id: String,
    pub claim_group_id: Option<String>,
    pub fencing_token: i64,
    pub owner_kind: OwnerKind,
    pub owner_id: String,
    pub status: HeartbeatStatus,
    pub last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    pub cancel_token: CancellationToken,
    pub last_error: Option<String>,
}

impl std::fmt::Debug for HeartbeatEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatEntry")
            .field("execution_id", &self.execution_id)
            .field("task_id", &self.task_id)
            .field("worktree_id", &self.worktree_id)
            .field("lease_id", &self.lease_id)
            .field("claim_group_id", &self.claim_group_id)
            .field("fencing_token", &self.fencing_token)
            .field("owner_kind", &self.owner_kind.as_str())
            .field("owner_id", &self.owner_id)
            .field("status", &self.status)
            .field("last_heartbeat_at", &self.last_heartbeat_at)
            .field("last_error", &self.last_error)
            .finish()
    }
}

/// Result of an inspect operation.
#[derive(Debug, Clone)]
pub struct InspectResult {
    pub execution_id: String,
    pub task_id: String,
    pub worktree_id: String,
    pub lease_id: String,
    pub claim_group_id: Option<String>,
    pub fencing_token: i64,
    pub owner_kind: String,
    pub owner_id: String,
    pub status: String,
    pub last_heartbeat_at: Option<String>,
    pub last_error: Option<String>,
}

/// Result of a takeover operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakeoverResult {
    /// Takeover succeeded.
    Acquired,
    /// Same owner already owns this handoff (idempotent).
    AlreadyOwned,
    /// Another owner already took over.
    Contested { current_owner: String },
    /// The expected fencing token doesn't match (stale fencing).
    StaleFencing { expected: i64, actual: i64 },
    /// The handoff is in a terminal state and cannot be taken over.
    Terminal { status: String },
    /// No handoff found for this execution.
    NotFound,
}

/// The runtime heartbeat registry.
///
/// Thread-safe via `Arc<RwLock<...>>`. Multiple concurrent readers are
/// supported; writes (register, takeover, cancel) are serialized.
pub struct HeartbeatRegistry {
    entries: RwLock<HashMap<String, HeartbeatEntry>>,
    /// Secondary index: lease_id → execution_id
    lease_index: RwLock<HashMap<String, String>>,
}

impl HeartbeatRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            lease_index: RwLock::new(HashMap::new()),
        }
    }

    /// Register a heartbeat entry after successful dispatch.
    /// Returns an error if an entry for this execution_id already exists
    /// (dispatch is idempotent — this should not happen in normal operation).
    pub async fn register(&self, entry: HeartbeatEntry) -> Result<(), CoreError> {
        let mut entries = self.entries.write().await;
        let mut lease_idx = self.lease_index.write().await;

        if entries.contains_key(&entry.execution_id) {
            return Err(CoreError::new(
                ErrorCode::ResourceConflict {
                    resource: format!("heartbeat-exec:{}", entry.execution_id),
                },
                "heartbeat already registered for this execution".to_string(),
                ErrorSource::System,
            ));
        }

        lease_idx.insert(entry.lease_id.clone(), entry.execution_id.clone());
        entries.insert(entry.execution_id.clone(), entry);
        Ok(())
    }

    /// Inspect a heartbeat by execution_id.
    pub async fn inspect(&self, execution_id: &str) -> Option<InspectResult> {
        let entries = self.entries.read().await;
        entries.get(execution_id).map(|e| self.to_result(e))
    }

    /// Inspect a heartbeat by lease_id.
    pub async fn inspect_by_lease(&self, lease_id: &str) -> Option<InspectResult> {
        let lease_idx = self.lease_index.read().await;
        let exec_id = lease_idx.get(lease_id)?;
        let entries = self.entries.read().await;
        entries.get(exec_id).map(|e| self.to_result(e))
    }

    /// Take over a heartbeat for I4-C Verification.
    ///
    /// Verifies the expected fencing token. Only succeeds if the current
    /// owner is Scheduler and the fencing token matches.
    /// Idempotent: same owner repeating takeover returns AlreadyOwned.
    pub async fn takeover(
        &self,
        execution_id: &str,
        verification_owner_id: &str,
        expected_fencing: i64,
    ) -> TakeoverResult {
        let mut entries = self.entries.write().await;

        let Some(entry) = entries.get_mut(execution_id) else {
            return TakeoverResult::NotFound;
        };

        // Check terminal statuses
        match entry.status {
            HeartbeatStatus::Stopped { ref reason } => {
                return TakeoverResult::Terminal {
                    status: format!("stopped: {reason}"),
                };
            }
            HeartbeatStatus::Missing => {
                return TakeoverResult::Terminal {
                    status: "missing".to_string(),
                };
            }
            _ => {}
        }

        // Already owned by this verification owner — idempotent
        if entry.owner_kind.as_str() == "verification" && entry.owner_id == verification_owner_id {
            return TakeoverResult::AlreadyOwned;
        }

        // Already owned by a different verification owner — contested
        if entry.owner_kind.as_str() == "verification" {
            return TakeoverResult::Contested {
                current_owner: entry.owner_id.clone(),
            };
        }

        // Check fencing token
        if entry.fencing_token != expected_fencing {
            return TakeoverResult::StaleFencing {
                expected: expected_fencing,
                actual: entry.fencing_token,
            };
        }

        // Transfer ownership
        entry.owner_kind = OwnerKind::Verification;
        entry.owner_id = verification_owner_id.to_string();
        TakeoverResult::Acquired
    }

    /// Cancel a heartbeat. Only the current owner can cancel.
    /// Verifies the expected fencing token.
    pub async fn cancel(
        &self,
        execution_id: &str,
        owner_id: &str,
        expected_fencing: i64,
    ) -> Result<(), CoreError> {
        let entries = self.entries.read().await;

        let Some(entry) = entries.get(execution_id) else {
            return Err(CoreError::new(
                ErrorCode::ConfigMissing,
                format!("heartbeat not found: {execution_id}"),
                ErrorSource::System,
            ));
        };

        if entry.owner_id != owner_id {
            return Err(CoreError::new(
                ErrorCode::ResourceConflict {
                    resource: format!("heartbeat-exec:{execution_id}"),
                },
                format!(
                    "cancel denied: owner mismatch (expected {}, got {})",
                    owner_id, entry.owner_id
                ),
                ErrorSource::System,
            ));
        }

        if entry.fencing_token != expected_fencing {
            return Err(CoreError::new(
                ErrorCode::ResourceConflict {
                    resource: format!("heartbeat-exec:{execution_id}"),
                },
                format!(
                    "cancel denied: fencing mismatch (expected {expected_fencing}, actual {})",
                    entry.fencing_token
                ),
                ErrorSource::System,
            ));
        }

        // Cancel the heartbeat runner
        entry.cancel_token.cancel();
        Ok(())
    }

    /// Update heartbeat status after a heartbeat attempt.
    pub async fn update_heartbeat_status(
        &self,
        execution_id: &str,
        success: bool,
        error: Option<String>,
    ) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(execution_id) {
            entry.last_heartbeat_at = Some(chrono::Utc::now());
            if success {
                entry.status = HeartbeatStatus::Healthy;
                entry.last_error = None;
            } else {
                entry.status = HeartbeatStatus::Degraded {
                    last_error: error.clone().unwrap_or_default(),
                };
                entry.last_error = error;
            }
        }
    }

    /// Mark a heartbeat as lost (runner vanished).
    pub async fn mark_lost(&self, execution_id: &str) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(execution_id) {
            entry.status = HeartbeatStatus::Missing;
        }
    }

    /// Remove an entry after verification finalization.
    pub async fn remove_after_finalization(&self, execution_id: &str) {
        let mut entries = self.entries.write().await;
        let mut lease_idx = self.lease_index.write().await;
        if let Some(entry) = entries.remove(execution_id) {
            lease_idx.remove(&entry.lease_id);
        }
    }

    /// Check if a registered heartbeat for an execution exists.
    pub async fn exists(&self, execution_id: &str) -> bool {
        let entries = self.entries.read().await;
        entries.contains_key(execution_id)
    }

    /// List all execution IDs with active heartbeats.
    pub async fn list_active(&self) -> Vec<String> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e.status,
                    HeartbeatStatus::Healthy | HeartbeatStatus::Degraded { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Cancel all heartbeats (for runtime shutdown).
    pub async fn cancel_all(&self) {
        let entries = self.entries.read().await;
        for (_, entry) in entries.iter() {
            entry.cancel_token.cancel();
        }
    }

    fn to_result(&self, entry: &HeartbeatEntry) -> InspectResult {
        let status_str = match &entry.status {
            HeartbeatStatus::Healthy => "healthy".to_string(),
            HeartbeatStatus::Degraded { last_error } => format!("degraded: {last_error}"),
            HeartbeatStatus::Stopped { reason } => format!("stopped: {reason}"),
            HeartbeatStatus::Missing => "missing".to_string(),
        };

        InspectResult {
            execution_id: entry.execution_id.clone(),
            task_id: entry.task_id.clone(),
            worktree_id: entry.worktree_id.clone(),
            lease_id: entry.lease_id.clone(),
            claim_group_id: entry.claim_group_id.clone(),
            fencing_token: entry.fencing_token,
            owner_kind: entry.owner_kind.as_str().to_string(),
            owner_id: entry.owner_id.clone(),
            status: status_str,
            last_heartbeat_at: entry
                .last_heartbeat_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
            last_error: entry.last_error.clone(),
        }
    }
}

impl Default for HeartbeatRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(exec_id: &str, lease_id: &str, fencing: i64) -> HeartbeatEntry {
        HeartbeatEntry {
            execution_id: exec_id.to_string(),
            task_id: format!("task-{exec_id}"),
            worktree_id: format!("wt-{exec_id}"),
            lease_id: lease_id.to_string(),
            claim_group_id: Some(format!("cg-{exec_id}")),
            fencing_token: fencing,
            owner_kind: OwnerKind::Scheduler,
            owner_id: "scheduler-main".to_string(),
            status: HeartbeatStatus::Healthy,
            last_heartbeat_at: Some(chrono::Utc::now()),
            cancel_token: CancellationToken::new(),
            last_error: None,
        }
    }

    #[tokio::test]
    async fn test_register_and_inspect() {
        let reg = HeartbeatRegistry::new();
        let entry = make_entry("exec-1", "lease-1", 1);
        reg.register(entry).await.unwrap();

        let result = reg.inspect("exec-1").await.unwrap();
        assert_eq!(result.execution_id, "exec-1");
        assert_eq!(result.lease_id, "lease-1");
        assert_eq!(result.status, "healthy");
    }

    #[tokio::test]
    async fn test_inspect_by_lease() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 1))
            .await
            .unwrap();

        let result = reg.inspect_by_lease("lease-1").await.unwrap();
        assert_eq!(result.execution_id, "exec-1");
    }

    #[tokio::test]
    async fn test_duplicate_register_rejected() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 1))
            .await
            .unwrap();
        let err = reg.register(make_entry("exec-1", "lease-2", 1)).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_takeover_success() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        let result = reg.takeover("exec-1", "verify-run-1", 5).await;
        assert_eq!(result, TakeoverResult::Acquired);

        let inspect = reg.inspect("exec-1").await.unwrap();
        assert_eq!(inspect.owner_kind, "verification");
        assert_eq!(inspect.owner_id, "verify-run-1");
    }

    #[tokio::test]
    async fn test_takeover_idempotent_same_owner() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        assert_eq!(
            reg.takeover("exec-1", "verify-run-1", 5).await,
            TakeoverResult::Acquired
        );
        assert_eq!(
            reg.takeover("exec-1", "verify-run-1", 5).await,
            TakeoverResult::AlreadyOwned
        );
    }

    #[tokio::test]
    async fn test_takeover_different_owner_contested() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        assert_eq!(
            reg.takeover("exec-1", "verify-run-1", 5).await,
            TakeoverResult::Acquired
        );
        let result = reg.takeover("exec-1", "verify-run-2", 5).await;
        assert!(matches!(result, TakeoverResult::Contested { .. }));
    }

    #[tokio::test]
    async fn test_takeover_stale_fencing_rejected() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        let result = reg.takeover("exec-1", "verify-run-1", 3).await;
        assert!(matches!(result, TakeoverResult::StaleFencing { .. }));
    }

    #[tokio::test]
    async fn test_takeover_not_found() {
        let reg = HeartbeatRegistry::new();
        let result = reg.takeover("nonexistent", "verify-run-1", 1).await;
        assert_eq!(result, TakeoverResult::NotFound);
    }

    #[tokio::test]
    async fn test_cancel_by_owner() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        let result = reg.cancel("exec-1", "scheduler-main", 5).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cancel_by_wrong_owner_rejected() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        let result = reg.cancel("exec-1", "wrong-owner", 5).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancel_stale_fencing_rejected() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        let result = reg.cancel("exec-1", "scheduler-main", 3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_heartbeat_status() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        reg.update_heartbeat_status("exec-1", false, Some("connection lost".to_string()))
            .await;

        let inspect = reg.inspect("exec-1").await.unwrap();
        assert!(inspect.status.contains("degraded"));
        assert!(inspect.last_error.unwrap().contains("connection lost"));
    }

    #[tokio::test]
    async fn test_mark_lost_and_remove() {
        let reg = HeartbeatRegistry::new();
        reg.register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        reg.mark_lost("exec-1").await;
        let inspect = reg.inspect("exec-1").await.unwrap();
        assert_eq!(inspect.status, "missing");

        reg.remove_after_finalization("exec-1").await;
        assert!(reg.inspect("exec-1").await.is_none());
        assert!(reg.inspect_by_lease("lease-1").await.is_none());
    }
}
