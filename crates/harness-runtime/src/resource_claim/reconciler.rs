//! ResourceClaimReconciler — detects and repairs claim-group inconsistencies.
//!
//! Cross-checks claim groups, claim rows, workspace leases, fencing epochs,
//! execution lifecycles, task lifecycles, and worktree status.

use harness_core::resource_claim::{
    AccessMode, ExistingClaim, LogicalResourceKey, ResourceIdentity, ResourceKind,
    ResourceOverlapEngine,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Categories of reconciliation findings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimAnomaly {
    /// Active claim group whose `expires_at` has passed.
    ActiveButExpired {
        group_id: String,
        expires_at: String,
    },
    /// Active claim group whose associated lease is Released.
    ActiveLeaseReleased { group_id: String, lease_id: String },
    /// Active claim group whose associated lease is Expired.
    ActiveLeaseExpired { group_id: String, lease_id: String },
    /// Active claim group with a stale fencing token (not equal to current epoch).
    StaleFencingToken {
        group_id: String,
        claim_fencing: i64,
        current_epoch: i64,
    },
    /// Active claim group whose owner execution is terminal.
    OwnerExecutionTerminal {
        group_id: String,
        execution_id: String,
        lifecycle: String,
    },
    /// Active claim group whose owner execution doesn't exist.
    OwnerExecutionLost {
        group_id: String,
        execution_id: String,
    },
    /// Active claim group whose worktree is missing from the DB.
    WorktreeMissing {
        group_id: String,
        worktree_id: String,
    },
    /// Active claim group whose worktree has been removed.
    WorktreeRemoved {
        group_id: String,
        worktree_id: String,
    },
    /// Claim group with no child claim rows.
    ClaimGroupWithoutRows { group_id: String },
    /// Orphan claim rows (NULL group_id or group doesn't exist).
    ClaimRowsWithoutGroup {
        claim_id: String,
        group_id: Option<String>,
    },
    /// Inconsistent claim row count vs expected.
    IncompleteClaimGroup { group_id: String },
    /// Multiple active claim groups with conflicting resources.
    MultipleConflictingActiveGroups {
        group_id_a: String,
        group_id_b: String,
    },
    /// Repository identity mismatch between group and worktree.
    RepositoryIdentityMismatch { group_id: String },
}

/// Outcome of a reconciliation run.
#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    pub anomalies: Vec<ClaimAnomaly>,
    pub expired: Vec<String>,
    pub fixed_rows: Vec<String>,
}

pub struct ResourceClaimReconciler {
    pool: SqlitePool,
}

impl ResourceClaimReconciler {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Run a full reconciliation scan.
    ///
    /// Detects anomalies and auto-fixes safe cases:
    /// - Expired groups → Expired.
    /// - Groups with terminal owners → Expired.
    /// - Orphan claim rows or empty groups → marked but not auto-deleted.
    pub async fn reconcile(&self) -> Result<ReconciliationReport, CoreError> {
        let mut report = ReconciliationReport {
            anomalies: Vec::new(),
            expired: Vec::new(),
            fixed_rows: Vec::new(),
        };

        // 1. Active but expired.
        self.detect_active_but_expired(&mut report).await?;

        // 2. Active with released/expired lease.
        self.detect_active_with_bad_lease(&mut report).await?;

        // 3. Stale fencing tokens.
        self.detect_stale_fencing(&mut report).await?;

        // 4. Terminal or lost owners.
        self.detect_terminal_owners(&mut report).await?;

        // 5. Missing/removed worktrees.
        self.detect_worktree_issues(&mut report).await?;

        // 6. Claim groups without rows.
        self.detect_groups_without_rows(&mut report).await?;

        // 7. Orphan claim rows.
        self.detect_rows_without_groups(&mut report).await?;

        // 8. Incomplete claim groups (active group with mixed-lifecycle rows).
        self.detect_incomplete_claim_groups(&mut report).await?;

        // 9. Repository identity mismatch between group and worktree.
        self.detect_repository_identity_mismatch(&mut report)
            .await?;

        // 10. Conflicting active groups.
        self.detect_conflicting_active(&mut report).await?;

        Ok(report)
    }

    async fn detect_active_but_expired(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT group_id, expires_at FROM resource_claim_groups WHERE lifecycle = 'active' AND expires_at IS NOT NULL AND expires_at < ?",
        )
        .bind(&now)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, expires_at) in &rows {
            report.anomalies.push(ClaimAnomaly::ActiveButExpired {
                group_id: group_id.clone(),
                expires_at: expires_at.clone(),
            });
            // Safe fix: expire the group.
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }
        Ok(())
    }

    async fn detect_active_with_bad_lease(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT g.group_id, g.lease_id, l.lifecycle FROM resource_claim_groups g LEFT JOIN workspace_leases l ON g.lease_id = l.id WHERE g.lifecycle = 'active' AND g.lease_id IS NOT NULL AND (l.id IS NULL OR l.lifecycle IN ('released', 'expired'))",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, lease_id, lifecycle) in &rows {
            if lifecycle == "released" {
                report.anomalies.push(ClaimAnomaly::ActiveLeaseReleased {
                    group_id: group_id.clone(),
                    lease_id: lease_id.clone(),
                });
            } else {
                report.anomalies.push(ClaimAnomaly::ActiveLeaseExpired {
                    group_id: group_id.clone(),
                    lease_id: lease_id.clone(),
                });
            }
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }
        Ok(())
    }

    async fn detect_stale_fencing(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT g.group_id, g.fencing_token, w.lease_epoch FROM resource_claim_groups g LEFT JOIN worktrees w ON g.worktree_id = w.id WHERE g.lifecycle = 'active' AND g.worktree_id IS NOT NULL AND g.fencing_token != w.lease_epoch",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, claim_fencing, current_epoch) in &rows {
            report.anomalies.push(ClaimAnomaly::StaleFencingToken {
                group_id: group_id.clone(),
                claim_fencing: *claim_fencing,
                current_epoch: *current_epoch,
            });
        }
        Ok(())
    }

    async fn detect_terminal_owners(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        // Terminal executions.
        let exec_rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT g.group_id, g.execution_id, e.lifecycle FROM resource_claim_groups g JOIN execution_attempts e ON g.execution_id = e.id WHERE g.lifecycle = 'active' AND e.lifecycle IN ('completed', 'failed', 'lost', 'cancelled')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, execution_id, lifecycle) in &exec_rows {
            report.anomalies.push(ClaimAnomaly::OwnerExecutionTerminal {
                group_id: group_id.clone(),
                execution_id: execution_id.clone(),
                lifecycle: lifecycle.clone(),
            });
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }

        // Missing executions.
        let lost_rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT g.group_id, g.execution_id FROM resource_claim_groups g WHERE g.lifecycle = 'active' AND g.execution_id IS NOT NULL AND g.execution_id != '' AND g.execution_id NOT IN (SELECT id FROM execution_attempts)",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, execution_id) in &lost_rows {
            report.anomalies.push(ClaimAnomaly::OwnerExecutionLost {
                group_id: group_id.clone(),
                execution_id: execution_id.clone(),
            });
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }

        Ok(())
    }

    async fn detect_worktree_issues(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        // Missing worktrees.
        let missing_rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT g.group_id, g.worktree_id FROM resource_claim_groups g WHERE g.lifecycle = 'active' AND g.worktree_id IS NOT NULL AND g.worktree_id NOT IN (SELECT id FROM worktrees)",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, worktree_id) in &missing_rows {
            report.anomalies.push(ClaimAnomaly::WorktreeMissing {
                group_id: group_id.clone(),
                worktree_id: worktree_id.clone(),
            });
        }

        // Removed worktrees.
        let removed_rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT g.group_id, g.worktree_id FROM resource_claim_groups g JOIN worktrees w ON g.worktree_id = w.id WHERE g.lifecycle = 'active' AND w.status IN ('removed', 'removing')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id, worktree_id) in &removed_rows {
            report.anomalies.push(ClaimAnomaly::WorktreeRemoved {
                group_id: group_id.clone(),
                worktree_id: worktree_id.clone(),
            });
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }

        Ok(())
    }

    async fn detect_groups_without_rows(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT g.group_id FROM resource_claim_groups g WHERE g.lifecycle = 'active' AND g.group_id NOT IN (SELECT DISTINCT group_id FROM resource_claims WHERE group_id IS NOT NULL)",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id,) in &rows {
            report.anomalies.push(ClaimAnomaly::ClaimGroupWithoutRows {
                group_id: group_id.clone(),
            });
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }
        Ok(())
    }

    async fn detect_rows_without_groups(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT id, group_id FROM resource_claims WHERE group_id IS NULL OR group_id NOT IN (SELECT group_id FROM resource_claim_groups)",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (claim_id, group_id) in &rows {
            report.anomalies.push(ClaimAnomaly::ClaimRowsWithoutGroup {
                claim_id: claim_id.clone(),
                group_id: group_id.clone(),
            });
        }
        Ok(())
    }

    /// Detect active groups where some child claim rows have a different
    /// lifecycle than the group (indicating a partial/incomplete state change).
    async fn detect_incomplete_claim_groups(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT g.group_id FROM resource_claim_groups g JOIN resource_claims rc ON rc.group_id = g.group_id WHERE g.lifecycle = 'active' AND rc.lifecycle != 'active'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id,) in &rows {
            report.anomalies.push(ClaimAnomaly::IncompleteClaimGroup {
                group_id: group_id.clone(),
            });
            // Auto-fix: expire the inconsistent group.
            self.expire_group(group_id).await?;
            report.expired.push(group_id.clone());
        }
        Ok(())
    }

    /// Detect active groups whose repository_identity does not match
    /// the linked worktree's repository_identity.
    async fn detect_repository_identity_mismatch(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT g.group_id FROM resource_claim_groups g JOIN worktrees w ON g.worktree_id = w.id WHERE g.lifecycle = 'active' AND g.worktree_id IS NOT NULL AND g.repository_identity != '' AND w.repository_identity != '' AND g.repository_identity != w.repository_identity",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        for (group_id,) in &rows {
            report
                .anomalies
                .push(ClaimAnomaly::RepositoryIdentityMismatch {
                    group_id: group_id.clone(),
                });
            // Report only — do not auto-fix. This may be a legitimate
            // reconfiguration and expiring claims prematurely could disrupt
            // active tasks.
        }
        Ok(())
    }

    async fn detect_conflicting_active(
        &self,
        report: &mut ReconciliationReport,
    ) -> Result<(), CoreError> {
        // Load all active claim views and check for internal conflicts.
        // This is a heavy check — only run when requested.
        let claim_rows: Vec<ActiveClaimRow> = sqlx::query_as(
            "SELECT rc.resource_kind, rc.normalized_resource, rc.access_mode, rc.group_id, g.task_id, g.execution_id, g.repository_identity FROM resource_claims rc JOIN resource_claim_groups g ON rc.group_id = g.group_id WHERE g.lifecycle = 'active'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let claims: Vec<ExistingClaim> = claim_rows
            .iter()
            .map(|r| {
                let kind = match r.resource_kind.as_str() {
                    "exact_file" => ResourceKind::ExactFile,
                    "directory_prefix" => ResourceKind::DirectoryPrefix,
                    "repository_wide" => ResourceKind::RepositoryWide,
                    _ => ResourceKind::Logical,
                };
                let identity = match kind {
                    ResourceKind::Logical => ResourceIdentity::Logical {
                        key: LogicalResourceKey::new(&r.normalized_resource)
                            .unwrap_or_else(|_| LogicalResourceKey::new("invalid").unwrap()),
                    },
                    _ => ResourceIdentity::Path {
                        repository_identity: r.repository_identity.clone(),
                        kind,
                        normalized_path: r.normalized_resource.clone(),
                    },
                };
                ExistingClaim {
                    identity,
                    mode: match r.access_mode.as_str() {
                        "write" => AccessMode::Write,
                        _ => AccessMode::Read,
                    },
                    group_id: r.group_id.clone(),
                    task_id: r.task_id.clone(),
                    execution_id: r.execution_id.clone(),
                }
            })
            .collect();

        let conflicts = ResourceOverlapEngine::detect_conflicting_active(&claims);
        for (a, b, _reason) in &conflicts {
            report
                .anomalies
                .push(ClaimAnomaly::MultipleConflictingActiveGroups {
                    group_id_a: a.group_id.clone(),
                    group_id_b: b.group_id.clone(),
                });
        }
        Ok(())
    }

    async fn expire_group(&self, group_id: &str) -> Result<(), CoreError> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        sqlx::query(
            "UPDATE resource_claim_groups SET lifecycle = 'expired', released_at = ?, updated_at = ?, version = version + 1 WHERE group_id = ? AND lifecycle = 'active'",
        )
        .bind(&now)
        .bind(&now)
        .bind(group_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query(
            "UPDATE resource_claims SET lifecycle = 'expired' WHERE group_id = ? AND lifecycle = 'active'",
        )
        .bind(group_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct ActiveClaimRow {
    resource_kind: String,
    normalized_resource: String,
    access_mode: String,
    group_id: String,
    task_id: String,
    execution_id: Option<String>,
    repository_identity: String,
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}
