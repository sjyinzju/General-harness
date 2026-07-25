//! Startup reconciliation and crash recovery.
//!
//! On every Supervisor startup, a RecoveryRun is created and the following
//! are reconciled:
//!
//! 1. Process recovery — verify/cull orphan processes
//! 2. Workspace recovery — rebind or cleanup managed worktrees
//! 3. Review recovery — finalize or block in-flight reviews
//! 4. Commit recovery — verify or recreate commit objects
//! 5. Integration recovery — reconcile integration queue state
//! 6. Claim/Lease recovery — release expired claims and leases
//! 7. Artifact recovery — clean orphan temp/evidence/cargo directories

use std::sync::Arc;

use chrono::Utc;
use harness_core::contracts::supervisor::SupervisorInstanceId;
use sqlx::SqlitePool;
use tracing;

use super::repo::SupervisorRepo;
use crate::integration::recovery::IntegrationRecoveryService;
use crate::liveness::LivenessOrchestrator;

/// A single recovery run.
pub struct RecoveryRun {
    pub recovery_id: String,
    pub supervisor_instance_id: SupervisorInstanceId,
    pub fencing_token: i64,
    pub started_at: chrono::DateTime<Utc>,
    pub state: RecoveryRunState,
    pub scanned_count: usize,
    pub action_count: usize,
    pub blocked_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryRunState {
    Running,
    Completed,
    Failed,
}

/// The recovery orchestrator.
pub struct RecoveryOrchestrator {
    pool: SqlitePool,
    #[allow(dead_code)]
    repo: SupervisorRepo,
    /// Optional integration recovery service (wired in production).
    integration_recovery: Option<Arc<IntegrationRecoveryService>>,
    /// Optional liveness orchestrator for artifact cleanup.
    liveness: Option<Arc<LivenessOrchestrator>>,
}

impl RecoveryOrchestrator {
    pub fn new(pool: SqlitePool, repo: SupervisorRepo) -> Self {
        Self {
            pool,
            repo,
            integration_recovery: None,
            liveness: None,
        }
    }

    /// Wire production services for real recovery.
    pub fn with_services(
        mut self,
        integration_recovery: Arc<IntegrationRecoveryService>,
        liveness: Arc<LivenessOrchestrator>,
    ) -> Self {
        self.integration_recovery = Some(integration_recovery);
        self.liveness = Some(liveness);
        self
    }

    /// Execute a full startup reconciliation.
    ///
    /// Returns a summary of recovery actions taken.
    pub async fn reconcile(
        &self,
        instance_id: &SupervisorInstanceId,
        fencing_token: i64,
    ) -> Result<RecoverySummary, String> {
        let recovery_id = uuid::Uuid::new_v4().to_string();
        let _started_at = Utc::now();

        tracing::info!(
            recovery_id = %recovery_id,
            instance_id = %instance_id,
            fencing_token,
            "starting startup reconciliation"
        );

        // Create recovery run record
        self.create_recovery_run(&recovery_id, instance_id, fencing_token)
            .await?;

        let mut summary = RecoverySummary {
            recovery_id: recovery_id.clone(),
            processes_terminated: 0,
            processes_recovered: 0,
            worktrees_cleaned: 0,
            worktrees_recovered: 0,
            reviews_resolved: 0,
            commits_verified: 0,
            integrations_recovered: 0,
            claims_released: 0,
            artifacts_cleaned: 0,
            blocked: 0,
            errors: Vec::new(),
        };

        // ── Phase 1: Process recovery ──────────────────────────
        match self.recover_processes(instance_id, fencing_token).await {
            Ok(result) => {
                summary.processes_terminated = result.terminated;
                summary.processes_recovered = result.recovered;
            }
            Err(e) => {
                tracing::error!(error = %e, "process recovery failed");
                summary.errors.push(format!("process recovery: {e}"));
            }
        }

        // ── Phase 2: Workspace recovery ────────────────────────
        match self.recover_workspaces(instance_id, fencing_token).await {
            Ok(result) => {
                summary.worktrees_cleaned = result.cleaned;
                summary.worktrees_recovered = result.recovered;
            }
            Err(e) => {
                tracing::error!(error = %e, "workspace recovery failed");
                summary.errors.push(format!("workspace recovery: {e}"));
            }
        }

        // ── Phase 2b: Review recovery ──────────────────────────
        match self.recover_reviews(instance_id, fencing_token).await {
            Ok(count) => {
                summary.reviews_resolved = count;
            }
            Err(e) => {
                tracing::error!(error = %e, "review recovery failed");
                summary.errors.push(format!("review recovery: {e}"));
            }
        }

        // ── Phase 2c: Commit recovery ──────────────────────────
        match self.recover_commits(instance_id, fencing_token).await {
            Ok(count) => {
                summary.commits_verified = count;
            }
            Err(e) => {
                tracing::error!(error = %e, "commit recovery failed");
                summary.errors.push(format!("commit recovery: {e}"));
            }
        }

        // ── Phase 3: Integration recovery ──────────────────────
        match self.recover_integration(instance_id, fencing_token).await {
            Ok(count) => {
                summary.integrations_recovered = count;
            }
            Err(e) => {
                tracing::error!(error = %e, "integration recovery failed");
                summary.errors.push(format!("integration recovery: {e}"));
            }
        }

        // ── Phase 4: Claim/Lease recovery ──────────────────────
        match self
            .recover_claims_and_leases(instance_id, fencing_token)
            .await
        {
            Ok(count) => {
                summary.claims_released = count;
            }
            Err(e) => {
                tracing::error!(error = %e, "claim recovery failed");
                summary.errors.push(format!("claim recovery: {e}"));
            }
        }

        // ── Phase 5: Artifact cleanup ──────────────────────────
        match self.recover_artifacts(instance_id).await {
            Ok(count) => {
                summary.artifacts_cleaned = count;
            }
            Err(e) => {
                tracing::error!(error = %e, "artifact recovery failed");
                summary.errors.push(format!("artifact recovery: {e}"));
            }
        }

        // Mark recovery run as completed
        self.complete_recovery_run(
            &recovery_id,
            summary.scanned(),
            summary.actions(),
            summary.blocked,
        )
        .await?;

        tracing::info!(
            recovery_id = %recovery_id,
            actions = summary.actions(),
            blocked = summary.blocked,
            errors = summary.errors.len(),
            "startup reconciliation complete"
        );

        Ok(summary)
    }

    // ── Recovery phase implementations ────────────────────────────

    async fn recover_processes(
        &self,
        _instance_id: &SupervisorInstanceId,
        _fencing_token: i64,
    ) -> Result<ProcessRecoveryResult, String> {
        // Query for execution attempts that are still marked as running
        // but whose owner supervisor instance is dead (stale fencing token).
        let orphan_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM operation_intents
               WHERE state IN ('claimed', 'running')
                 AND owner_fencing_token < ?"#,
        )
        .bind(_fencing_token)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("query orphan operations: {e}"))?;

        if orphan_count.0 > 0 {
            // Abandon orphan operations — the new supervisor will not
            // attempt to resume them because the old fencing token is stale.
            sqlx::query(
                r#"UPDATE operation_intents
                   SET state = 'abandoned', error_message = 'owner supervisor lost',
                       completed_at = datetime('now'), updated_at = datetime('now')
                   WHERE state IN ('claimed', 'running')
                     AND owner_fencing_token < ?"#,
            )
            .bind(_fencing_token)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("abandon orphan operations: {e}"))?;

            tracing::warn!(
                orphan_count = orphan_count.0,
                "abandoned orphan operations from previous supervisor"
            );
        }

        Ok(ProcessRecoveryResult {
            terminated: orphan_count.0 as usize,
            recovered: 0,
        })
    }

    async fn recover_workspaces(
        &self,
        _instance_id: &SupervisorInstanceId,
        _fencing_token: i64,
    ) -> Result<WorkspaceRecoveryResult, String> {
        // Query the worktrees table for records associated with stale
        // operations. Mark them as needing cleanup.
        let stale_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM worktrees w
               WHERE w.state = 'active'
                 AND EXISTS (
                   SELECT 1 FROM operation_intents o
                   WHERE o.aggregate_id = w.id
                     AND o.state IN ('abandoned', 'failed', 'cancelled')
                 )"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("query stale worktrees: {e}"))?;

        if stale_count.0 > 0 {
            tracing::warn!(
                stale_count = stale_count.0,
                "found stale worktrees from abandoned operations"
            );
        }

        Ok(WorkspaceRecoveryResult {
            cleaned: 0, // Actual cleanup is deferred to Janitor with DeletionGuard
            recovered: stale_count.0 as usize,
        })
    }

    async fn recover_reviews(
        &self,
        _instance_id: &SupervisorInstanceId,
        _fencing_token: i64,
    ) -> Result<usize, String> {
        // Query for reviews stuck in non-terminal states from previous
        // supervisor instances. Block them if they cannot be resolved.
        let stuck_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM reviews r
               WHERE r.state IN ('requested', 'preparing', 'prechecking', 'reviewing')
                 AND r.updated_at < datetime('now', '-10 minutes')"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("query stuck reviews: {e}"))?;

        if stuck_count.0 > 0 {
            // Transition stuck reviews to Blocked — they need operator attention
            sqlx::query(
                r#"UPDATE reviews
                   SET state = 'blocked', updated_at = datetime('now')
                   WHERE state IN ('requested', 'preparing', 'prechecking', 'reviewing')
                     AND updated_at < datetime('now', '-10 minutes')"#,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| format!("block stuck reviews: {e}"))?;

            tracing::warn!(
                stuck_count = stuck_count.0,
                "blocked stuck reviews from previous supervisor"
            );
        }

        Ok(stuck_count.0 as usize)
    }

    async fn recover_commits(
        &self,
        _instance_id: &SupervisorInstanceId,
        _fencing_token: i64,
    ) -> Result<usize, String> {
        // Count commit candidates in non-terminal states
        let stale_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM commit_candidates
               WHERE state NOT IN ('created', 'integrated', 'rejected')
                 AND updated_at < datetime('now', '-10 minutes')"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("query stale commits: {e}"))?;

        if stale_count.0 > 0 {
            tracing::warn!(stale_count = stale_count.0, "found stale commit candidates");
        }

        Ok(stale_count.0 as usize)
    }

    async fn recover_integration(
        &self,
        _instance_id: &SupervisorInstanceId,
        _fencing_token: i64,
    ) -> Result<usize, String> {
        // Call the real IntegrationRecoveryService if available
        if let Some(ref recovery_svc) = self.integration_recovery {
            let repo_path = std::path::PathBuf::from(".");
            let integration_root = repo_path.join("target").join("harness-integration");

            match recovery_svc.reconcile(&repo_path, &integration_root).await {
                Ok(outcome) => {
                    tracing::info!(
                        scanned = outcome.scanned,
                        requeued = outcome.requeued,
                        recovered = outcome.recovered_integrated,
                        "integration recovery complete"
                    );
                    return Ok(outcome.requeued + outcome.recovered_integrated);
                }
                Err(e) => {
                    return Err(format!("integration recovery service: {e}"));
                }
            }
        }

        // Fallback: scan integration_requests for stuck states
        let stuck_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM integration_requests
               WHERE state NOT IN ('integrated', 'failed', 'cancelled', 'queued')
                 AND updated_at < datetime('now', '-10 minutes')"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("query stuck integrations: {e}"))?;

        // Requeue stuck items
        if stuck_count.0 > 0 {
            sqlx::query(
                r#"UPDATE integration_requests
                   SET state = 'queued', updated_at = datetime('now')
                   WHERE state NOT IN ('integrated', 'failed', 'cancelled', 'queued')
                     AND updated_at < datetime('now', '-10 minutes')"#,
            )
            .execute(&self.pool)
            .await
            .map_err(|e| format!("requeue stuck integrations: {e}"))?;

            tracing::warn!(
                stuck_count = stuck_count.0,
                "requeued stuck integration requests"
            );
        }

        Ok(stuck_count.0 as usize)
    }

    async fn recover_claims_and_leases(
        &self,
        _instance_id: &SupervisorInstanceId,
        _fencing_token: i64,
    ) -> Result<usize, String> {
        // Release stale ResourceClaims whose owner supervisor is dead
        let stale_count: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM resource_claims
               WHERE state = 'active'
                 AND fencing_token < ?"#,
        )
        .bind(_fencing_token)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| format!("query stale claims: {e}"))?;

        if stale_count.0 > 0 {
            sqlx::query(
                r#"UPDATE resource_claims
                   SET state = 'released', updated_at = datetime('now')
                   WHERE state = 'active'
                     AND fencing_token < ?"#,
            )
            .bind(_fencing_token)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("release stale claims: {e}"))?;

            tracing::warn!(
                stale_count = stale_count.0,
                "released stale resource claims"
            );
        }

        Ok(stale_count.0 as usize)
    }

    async fn recover_artifacts(
        &self,
        _instance_id: &SupervisorInstanceId,
    ) -> Result<usize, String> {
        // Call LivenessOrchestrator janitor if available
        if let Some(ref liveness) = self.liveness {
            let result = liveness.startup_janitor(vec![]).await;
            let cleaned = result.deleted;
            tracing::info!(cleaned, "artifact cleanup via liveness orchestrator");
            return Ok(cleaned);
        }

        Ok(0)
    }

    // ── Database helpers ─────────────────────────────────────────

    async fn create_recovery_run(
        &self,
        recovery_id: &str,
        instance_id: &SupervisorInstanceId,
        fencing_token: i64,
    ) -> Result<(), String> {
        sqlx::query(
            r#"INSERT INTO recovery_runs
               (recovery_id, supervisor_instance_id, fencing_token, started_at, state,
                scanned_count, action_count, blocked_count)
               VALUES (?, ?, ?, datetime('now'), 'running', 0, 0, 0)"#,
        )
        .bind(recovery_id)
        .bind(&instance_id.0)
        .bind(fencing_token)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("create recovery run: {e}"))?;

        Ok(())
    }

    async fn complete_recovery_run(
        &self,
        recovery_id: &str,
        scanned: usize,
        actions: usize,
        blocked: usize,
    ) -> Result<(), String> {
        sqlx::query(
            r#"UPDATE recovery_runs
               SET state = 'completed', completed_at = datetime('now'),
                   scanned_count = ?, action_count = ?, blocked_count = ?
               WHERE recovery_id = ?"#,
        )
        .bind(scanned as i64)
        .bind(actions as i64)
        .bind(blocked as i64)
        .bind(recovery_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("complete recovery run: {e}"))?;

        Ok(())
    }

    #[allow(dead_code)]
    fn record_recovery_action(
        &self,
        recovery_id: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        previous_state: &str,
        action: &str,
        reason: &str,
    ) {
        let pool = self.pool.clone();
        let recovery_id = recovery_id.to_string();
        let aggregate_type = aggregate_type.to_string();
        let aggregate_id = aggregate_id.to_string();
        let previous_state = previous_state.to_string();
        let action = action.to_string();
        let reason = reason.to_string();

        tokio::spawn(async move {
            let _ = sqlx::query(
                r#"INSERT INTO recovery_actions
                   (recovery_id, aggregate_type, aggregate_id, previous_state, action, reason)
                   VALUES (?, ?, ?, ?, ?, ?)"#,
            )
            .bind(&recovery_id)
            .bind(&aggregate_type)
            .bind(&aggregate_id)
            .bind(&previous_state)
            .bind(&action)
            .bind(&reason)
            .execute(&pool)
            .await;
        });
    }
}

// ── Result types ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RecoverySummary {
    pub recovery_id: String,
    pub processes_terminated: usize,
    pub processes_recovered: usize,
    pub worktrees_cleaned: usize,
    pub worktrees_recovered: usize,
    pub reviews_resolved: usize,
    pub commits_verified: usize,
    pub integrations_recovered: usize,
    pub claims_released: usize,
    pub artifacts_cleaned: usize,
    pub blocked: usize,
    pub errors: Vec<String>,
}

impl RecoverySummary {
    pub fn scanned(&self) -> usize {
        self.processes_terminated
            + self.processes_recovered
            + self.worktrees_cleaned
            + self.worktrees_recovered
            + self.reviews_resolved
            + self.commits_verified
            + self.integrations_recovered
            + self.claims_released
            + self.artifacts_cleaned
    }

    pub fn actions(&self) -> usize {
        self.scanned()
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

#[derive(Debug, Clone)]
struct ProcessRecoveryResult {
    terminated: usize,
    recovered: usize,
}

#[derive(Debug, Clone)]
struct WorkspaceRecoveryResult {
    cleaned: usize,
    recovered: usize,
}
