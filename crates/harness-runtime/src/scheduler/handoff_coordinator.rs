//! ResourceHandoffCoordinator — unified handoff coordination between durable
//! (HandoffRepository / SQLite) and runtime (HeartbeatRegistry / in-memory)
//! layers. This is the single entry point for I4-C Verification takeover.
//!
//! Every takeover is a two-phase coordinated operation:
//!   1. DB CAS (version-based optimistic locking) on resource_handoffs
//!   2. Runtime registry ownership update
//!
//! After both phases succeed, the coordinator re-reads both layers to confirm
//! consistency. If either phase fails, the coordinator does NOT return success
//! and marks the handoff for reconciliation.
//!
//! Safety invariants:
//!   - Never persist lease tokens
//!   - Never silently overwrite a mismatched owner
//!   - Never start Verification without confirmed consistency
//!   - Never re-acquire Lease/Claim during takeover

use std::sync::Arc;

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::handoff_repo::{HandoffRepository, TakeoverPersistResult};
use super::heartbeat_registry::{HeartbeatRegistry, TakeoverResult};

/// Outcome of a coordinated takeover attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatedTakeoverResult {
    /// Both layers successfully updated and confirmed consistent.
    Acquired {
        execution_id: String,
        owner_id: String,
        fencing_token: i64,
    },
    /// Same verification owner already owns this handoff (idempotent).
    AlreadyOwned,
    /// Another verification owner already took over.
    Contested { current_owner: String },
    /// The expected fencing token doesn't match.
    StaleFencing { expected: i64, actual: i64 },
    /// The handoff is in a terminal state.
    Terminal { status: String },
    /// No handoff found for this execution.
    NotFound,
    /// DB and runtime registry disagree — handoff marked for reconciliation.
    HandoffStateMismatch {
        db_owner_kind: String,
        db_owner_id: String,
        registry_owner_kind: Option<String>,
        registry_owner_id: Option<String>,
        detail: String,
    },
}

/// Result of a coordinated inspect operation.
#[derive(Debug, Clone)]
pub struct CoordinatedInspectResult {
    pub execution_id: String,
    pub task_id: String,
    pub worktree_id: Option<String>,
    pub lease_id: Option<String>,
    pub claim_group_id: Option<String>,
    pub fencing_token: i64,
    pub db_owner_kind: String,
    pub db_owner_id: String,
    pub db_status: String,
    pub registry_owner_kind: Option<String>,
    pub registry_owner_id: Option<String>,
    pub registry_status: Option<String>,
    pub consistent: bool,
}

/// Coordinates durable (DB) and runtime (registry) handoff ownership.
///
/// All takeover operations go through this coordinator to prevent
/// split-brain between the two layers.
pub struct ResourceHandoffCoordinator {
    repo: HandoffRepository,
    registry: Arc<HeartbeatRegistry>,
}

impl ResourceHandoffCoordinator {
    pub fn new(repo: HandoffRepository, registry: Arc<HeartbeatRegistry>) -> Self {
        Self { repo, registry }
    }

    // ── Inspect ──────────────────────────────────────────────────────

    /// Inspect both DB and runtime layers and report consistency.
    pub async fn inspect_consistent(
        &self,
        execution_id: &str,
    ) -> Result<CoordinatedInspectResult, CoreError> {
        let db = self.repo.get_by_execution(execution_id).await?;
        let reg = self.registry.inspect(execution_id).await;

        match (db, reg) {
            (Some(db_record), Some(reg_result)) => {
                let consistent = db_record.owner_kind == reg_result.owner_kind
                    && db_record.owner_id == reg_result.owner_id
                    && db_record.fencing_token == reg_result.fencing_token;
                Ok(CoordinatedInspectResult {
                    execution_id: execution_id.to_string(),
                    task_id: db_record.task_id,
                    worktree_id: db_record.worktree_id,
                    lease_id: db_record.lease_id,
                    claim_group_id: db_record.claim_group_id,
                    fencing_token: db_record.fencing_token,
                    db_owner_kind: db_record.owner_kind,
                    db_owner_id: db_record.owner_id,
                    db_status: db_record.status,
                    registry_owner_kind: Some(reg_result.owner_kind),
                    registry_owner_id: Some(reg_result.owner_id),
                    registry_status: Some(reg_result.status),
                    consistent,
                })
            }
            (Some(db_record), None) => Ok(CoordinatedInspectResult {
                execution_id: execution_id.to_string(),
                task_id: db_record.task_id,
                worktree_id: db_record.worktree_id,
                lease_id: db_record.lease_id,
                claim_group_id: db_record.claim_group_id,
                fencing_token: db_record.fencing_token,
                db_owner_kind: db_record.owner_kind,
                db_owner_id: db_record.owner_id,
                db_status: db_record.status,
                registry_owner_kind: None,
                registry_owner_id: None,
                registry_status: None,
                consistent: false,
            }),
            (None, Some(_reg_result)) => Ok(CoordinatedInspectResult {
                execution_id: execution_id.to_string(),
                task_id: String::new(),
                worktree_id: None,
                lease_id: None,
                claim_group_id: None,
                fencing_token: 0,
                db_owner_kind: String::new(),
                db_owner_id: String::new(),
                db_status: "missing".to_string(),
                registry_owner_kind: Some(_reg_result.owner_kind),
                registry_owner_id: Some(_reg_result.owner_id),
                registry_status: Some(_reg_result.status),
                consistent: false,
            }),
            (None, None) => Ok(CoordinatedInspectResult {
                execution_id: execution_id.to_string(),
                task_id: String::new(),
                worktree_id: None,
                lease_id: None,
                claim_group_id: None,
                fencing_token: 0,
                db_owner_kind: String::new(),
                db_owner_id: String::new(),
                db_status: "missing".to_string(),
                registry_owner_kind: None,
                registry_owner_id: None,
                registry_status: None,
                consistent: true, // both absent = consistent
            }),
        }
    }

    // ── Coordinated Takeover ─────────────────────────────────────────

    /// Take over a handoff for I4-C Verification.
    ///
    /// Order:
    ///   1. Read DB handoff + runtime heartbeat
    ///   2. Validate consistency (owner, fencing) between layers
    ///   3. DB CAS: SchedulerOwned → VerificationOwned (version-based)
    ///   4. Runtime registry takeover
    ///   5. Re-read both layers → confirm consistency
    ///   6. Return Acquired only if both layers agree
    ///
    /// If DB CAS succeeds but registry takeover fails:
    ///   - Marks DB as reconciliation_required
    ///   - Returns HandoffStateMismatch (NOT Acquired)
    ///
    /// If layers are already inconsistent before takeover:
    ///   - Returns HandoffStateMismatch
    ///   - Does NOT attempt takeover
    pub async fn takeover_for_verification(
        &self,
        execution_id: &str,
        verification_owner_id: &str,
        expected_fencing: i64,
    ) -> CoordinatedTakeoverResult {
        // ── 1. Read both layers ───────────────────────────────────
        let db_record = match self.repo.get_by_execution(execution_id).await {
            Ok(Some(r)) => r,
            Ok(None) => return CoordinatedTakeoverResult::NotFound,
            Err(_) => return CoordinatedTakeoverResult::NotFound,
        };

        let reg_result = self.registry.inspect(execution_id).await;

        // ── 2. Pre-takeover consistency check ─────────────────────
        if let Some(ref reg) = reg_result {
            if db_record.owner_kind != reg.owner_kind || db_record.owner_id != reg.owner_id {
                return CoordinatedTakeoverResult::HandoffStateMismatch {
                    db_owner_kind: db_record.owner_kind.clone(),
                    db_owner_id: db_record.owner_id.clone(),
                    registry_owner_kind: Some(reg.owner_kind.clone()),
                    registry_owner_id: Some(reg.owner_id.clone()),
                    detail: "pre-takeover: DB and registry owners disagree".to_string(),
                };
            }
            if db_record.fencing_token != reg.fencing_token {
                return CoordinatedTakeoverResult::HandoffStateMismatch {
                    db_owner_kind: db_record.owner_kind.clone(),
                    db_owner_id: db_record.owner_id.clone(),
                    registry_owner_kind: Some(reg.owner_kind.clone()),
                    registry_owner_id: Some(reg.owner_id.clone()),
                    detail: format!(
                        "pre-takeover: fencing mismatch DB={} registry={}",
                        db_record.fencing_token, reg.fencing_token
                    ),
                };
            }
        }

        // Validate fencing
        if db_record.fencing_token != expected_fencing {
            return CoordinatedTakeoverResult::StaleFencing {
                expected: expected_fencing,
                actual: db_record.fencing_token,
            };
        }

        // ── 3. DB CAS ────────────────────────────────────────────
        let db_result = match self
            .repo
            .takeover(execution_id, verification_owner_id, db_record.version)
            .await
        {
            Ok(r) => r,
            Err(_) => return CoordinatedTakeoverResult::NotFound,
        };

        match db_result {
            TakeoverPersistResult::Acquired => { /* proceed to registry phase */ }
            TakeoverPersistResult::AlreadyOwned => {
                // Registry might already be updated; confirm consistency
                if let Some(ref reg) = reg_result {
                    if reg.owner_kind == "verification" && reg.owner_id == verification_owner_id {
                        return CoordinatedTakeoverResult::AlreadyOwned;
                    }
                }
                // DB says AlreadyOwned but registry doesn't match → repair registry
                let _ = self
                    .registry
                    .takeover(execution_id, verification_owner_id, expected_fencing)
                    .await;
                return CoordinatedTakeoverResult::AlreadyOwned;
            }
            TakeoverPersistResult::Contested { current_owner } => {
                return CoordinatedTakeoverResult::Contested { current_owner };
            }
            TakeoverPersistResult::VersionConflict => {
                return CoordinatedTakeoverResult::StaleFencing {
                    expected: expected_fencing,
                    actual: db_record.fencing_token,
                };
            }
            TakeoverPersistResult::TerminalState => {
                return CoordinatedTakeoverResult::Terminal {
                    status: db_record.status,
                };
            }
            TakeoverPersistResult::NotFound => {
                return CoordinatedTakeoverResult::NotFound;
            }
        }

        // ── 4. Runtime registry takeover ──────────────────────────
        let reg_takeover = self
            .registry
            .takeover(execution_id, verification_owner_id, expected_fencing)
            .await;

        match reg_takeover {
            TakeoverResult::Acquired | TakeoverResult::AlreadyOwned => {
                // Success — proceed to consistency confirmation
            }
            other => {
                // Registry takeover failed after DB CAS succeeded.
                // Compensation: mark DB as reconciliation_required.
                let detail = format!(
                    "DB CAS succeeded but registry takeover returned {:?}",
                    other
                );
                let _ = self
                    .repo
                    .mark_reconciliation_required(execution_id, &detail)
                    .await;
                return CoordinatedTakeoverResult::HandoffStateMismatch {
                    db_owner_kind: "verification".to_string(),
                    db_owner_id: verification_owner_id.to_string(),
                    registry_owner_kind: None,
                    registry_owner_id: None,
                    detail,
                };
            }
        }

        // ── 5. Post-takeover consistency confirmation ─────────────
        let post_inspect = match self.inspect_consistent(execution_id).await {
            Ok(r) => r,
            Err(_) => {
                let _ = self
                    .repo
                    .mark_reconciliation_required(execution_id, "post-takeover inspect failed")
                    .await;
                return CoordinatedTakeoverResult::HandoffStateMismatch {
                    db_owner_kind: "verification".to_string(),
                    db_owner_id: verification_owner_id.to_string(),
                    registry_owner_kind: None,
                    registry_owner_id: None,
                    detail: "post-takeover inspect failed".to_string(),
                };
            }
        };

        if !post_inspect.consistent {
            let _ = self
                .repo
                .mark_reconciliation_required(execution_id, "post-takeover layers inconsistent")
                .await;
            return CoordinatedTakeoverResult::HandoffStateMismatch {
                db_owner_kind: post_inspect.db_owner_kind,
                db_owner_id: post_inspect.db_owner_id,
                registry_owner_kind: post_inspect.registry_owner_kind,
                registry_owner_id: post_inspect.registry_owner_id,
                detail: "post-takeover: DB and registry owners disagree".to_string(),
            };
        }

        // ── 6. Success ────────────────────────────────────────────
        CoordinatedTakeoverResult::Acquired {
            execution_id: execution_id.to_string(),
            owner_id: verification_owner_id.to_string(),
            fencing_token: expected_fencing,
        }
    }

    // ── Cancel after verification ──────────────────────────────────

    /// Cancel a handoff after verification is complete.
    /// Checks both DB owner and registry owner before cancelling.
    pub async fn cancel_after_verification(
        &self,
        execution_id: &str,
        verification_owner_id: &str,
        expected_fencing: i64,
    ) -> Result<(), CoreError> {
        // Verify DB ownership
        let db = self
            .repo
            .get_by_execution(execution_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::ConfigMissing,
                    format!("handoff not found: {execution_id}"),
                    ErrorSource::System,
                )
            })?;

        if db.owner_kind != "verification" || db.owner_id != verification_owner_id {
            return Err(CoreError::new(
                ErrorCode::ResourceConflict {
                    resource: format!("handoff-exec:{execution_id}"),
                },
                format!(
                    "cancel denied: DB owner mismatch (expected verification/{verification_owner_id}, got {}/{})",
                    db.owner_kind, db.owner_id
                ),
                ErrorSource::System,
            ));
        }

        if db.fencing_token != expected_fencing {
            return Err(CoreError::new(
                ErrorCode::ResourceConflict {
                    resource: format!("handoff-exec:{execution_id}"),
                },
                format!(
                    "cancel denied: fencing mismatch (expected {expected_fencing}, got {})",
                    db.fencing_token
                ),
                ErrorSource::System,
            ));
        }

        // Cancel runtime heartbeat
        self.registry
            .cancel(execution_id, verification_owner_id, expected_fencing)
            .await?;

        // Mark DB as released
        self.repo
            .mark_released(execution_id, "verification-complete")
            .await?;

        Ok(())
    }

    // ── Mark reconciliation required ───────────────────────────────

    /// Mark a handoff as requiring reconciliation with structured detail.
    pub async fn mark_reconciliation_required(
        &self,
        execution_id: &str,
        reason: &str,
    ) -> Result<(), CoreError> {
        self.repo
            .mark_reconciliation_required(execution_id, reason)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::scheduler::handoff_repo::CreateHandoffParams;
    use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};
    use tokio_util::sync::CancellationToken;

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

    async fn setup_coordinator() -> (ResourceHandoffCoordinator, Arc<HeartbeatRegistry>, Database) {
        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('proj-1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('task-1','proj-1','test','submitted')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('exec-1','task-1',1,'completed')")
            .execute(&db.pool)
            .await
            .unwrap();

        let repo = HandoffRepository::new(db.pool.clone());
        let registry = Arc::new(HeartbeatRegistry::new());

        // Seed DB handoff
        repo.create(
            "ho-1",
            "proj-1",
            "task-1",
            CreateHandoffParams {
                execution_id: "exec-1",
                worktree_id: Some("wt-1"),
                lease_id: Some("lease-1"),
                claim_group_id: Some("cg-1"),
                fencing_token: 5,
                owner_id: "scheduler-main",
            },
        )
        .await
        .unwrap();

        // Seed registry
        registry
            .register(make_entry("exec-1", "lease-1", 5))
            .await
            .unwrap();

        let coordinator = ResourceHandoffCoordinator::new(repo, registry.clone());
        (coordinator, registry, db)
    }

    #[tokio::test]
    async fn test_coordinated_takeover_success() {
        let (coord, _reg, _db) = setup_coordinator().await;

        let result = coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;
        assert!(matches!(result, CoordinatedTakeoverResult::Acquired { .. }));

        // Post-takeover: both layers must agree
        let inspect = coord.inspect_consistent("exec-1").await.unwrap();
        assert!(inspect.consistent);
        assert_eq!(inspect.db_owner_kind, "verification");
        assert_eq!(inspect.db_owner_id, "verify-run-1");
        assert_eq!(
            inspect.registry_owner_kind,
            Some("verification".to_string())
        );
        assert_eq!(inspect.registry_owner_id, Some("verify-run-1".to_string()));
    }

    #[tokio::test]
    async fn test_coordinated_takeover_idempotent() {
        let (coord, _reg, _db) = setup_coordinator().await;

        // First takeover
        let r1 = coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;
        assert!(matches!(r1, CoordinatedTakeoverResult::Acquired { .. }));

        // Second takeover same owner
        let r2 = coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;
        assert_eq!(r2, CoordinatedTakeoverResult::AlreadyOwned);
    }

    #[tokio::test]
    async fn test_coordinated_takeover_contested() {
        let (coord, _reg, _db) = setup_coordinator().await;

        // First owner takes over
        coord
            .takeover_for_verification("exec-1", "verify-run-a", 5)
            .await;

        // Second owner tries
        let result = coord
            .takeover_for_verification("exec-1", "verify-run-b", 5)
            .await;
        assert!(matches!(
            result,
            CoordinatedTakeoverResult::Contested { .. }
        ));
    }

    #[tokio::test]
    async fn test_coordinated_takeover_stale_fencing() {
        let (coord, _reg, _db) = setup_coordinator().await;

        let result = coord
            .takeover_for_verification("exec-1", "verify-run-1", 99)
            .await;
        assert!(matches!(
            result,
            CoordinatedTakeoverResult::StaleFencing { .. }
        ));
    }

    #[tokio::test]
    async fn test_db_registry_mismatch_detected() {
        let (coord, registry, _db) = setup_coordinator().await;

        // Artificially change registry owner without updating DB
        registry.takeover("exec-1", "rogue-owner", 5).await;

        // Now try coordinated takeover — must detect mismatch
        let result = coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;
        assert!(
            matches!(
                result,
                CoordinatedTakeoverResult::HandoffStateMismatch { .. }
            ),
            "should detect pre-existing mismatch, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_cancel_checks_both_owners() {
        let (coord, _reg, _db) = setup_coordinator().await;

        // Take over first
        coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;

        // Cancel with correct owner
        let result = coord
            .cancel_after_verification("exec-1", "verify-run-1", 5)
            .await;
        assert!(result.is_ok());

        // Verify DB is released
        let repo = HandoffRepository::new(_db.pool.clone());
        let record = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        assert_eq!(record.status, "released");
    }

    #[tokio::test]
    async fn test_cancel_wrong_owner_rejected() {
        let (coord, _reg, _db) = setup_coordinator().await;

        coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;

        let result = coord
            .cancel_after_verification("exec-1", "wrong-owner", 5)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancel_wrong_fencing_rejected() {
        let (coord, _reg, _db) = setup_coordinator().await;

        coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;

        let result = coord
            .cancel_after_verification("exec-1", "verify-run-1", 99)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lease_token_absent_from_all_outputs() {
        let (coord, _reg, _db) = setup_coordinator().await;

        let result = coord
            .takeover_for_verification("exec-1", "verify-run-1", 5)
            .await;

        // Debug representation must not contain "lease_token"
        let debug = format!("{:?}", result);
        assert!(
            !debug.contains("lease_token"),
            "debug output must not contain lease_token: {debug}"
        );

        // Inspect result must not contain lease_token
        let inspect = coord.inspect_consistent("exec-1").await.unwrap();
        let inspect_debug = format!("{:?}", inspect);
        assert!(
            !inspect_debug.contains("lease_token"),
            "inspect output must not contain lease_token: {inspect_debug}"
        );
    }
}
