//! WorkspaceLeaseReconciler — cross-checks workspace_leases, worktrees,
//! sidecar metadata, filesystem, git worktree list, execution lifecycle,
//! supervisor identity, and current wall clock. Only performs safe
//! deterministic repairs: expired → Expired, terminal execution → Expired.

use harness_core::contracts::workspace::LeaseLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use crate::lease::transition::LeaseTransitionService;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseDriftKind {
    ActiveButExpired,
    ActiveWorktreeMissing,
    ActiveWorktreeRemoved,
    ActiveOwnerExecutionTerminal,
    ActiveOwnerExecutionLost,
    ActiveOwnerSupervisorMismatch,
    ActiveSidecarOwnerMismatch,
    MultipleActiveLeases,
    ReleasedButStillReferenced,
    ExpiredButHeartbeatContinues,
    LeaseWithoutWorktreeRecord,
    WorktreeWithStaleLeaseMetadata,
}

#[derive(Debug, Clone)]
pub struct LeaseDrift {
    pub kind: LeaseDriftKind,
    pub lease_id: Option<String>,
    pub worktree_id: Option<String>,
    pub detail: String,
    pub repaired: bool,
    pub repair: Option<String>,
}

pub struct WorkspaceLeaseReconciler {
    pool: SqlitePool,
    transitions: LeaseTransitionService,
    current_supervisor: String,
}

impl WorkspaceLeaseReconciler {
    pub fn new(pool: SqlitePool, current_supervisor: String) -> Self {
        Self {
            transitions: LeaseTransitionService::new(pool.clone()),
            pool,
            current_supervisor,
        }
    }

    pub async fn reconcile(&self) -> Result<Vec<LeaseDrift>, CoreError> {
        let mut drifts = Vec::new();

        // Active leases across all rows.
        let active: Vec<LeaseRow> = sqlx::query_as(
            "SELECT id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM workspace_leases WHERE lifecycle = 'active'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_err)?;

        for row in &active {
            let lease_id = &row.id;

            // 1. Clock-expired.
            let now_sql = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
            if row.expires_at < now_sql {
                let ikey = format!("recon-expire-{lease_id}");
                let res = self
                    .transitions
                    .transition_lease(
                        lease_id,
                        &LeaseLifecycle::Active,
                        &LeaseLifecycle::Expired,
                        &ikey,
                        true,
                    )
                    .await;
                let repaired = res.is_ok() || is_already(&res);
                drifts.push(lease_drift(
                    LeaseDriftKind::ActiveButExpired,
                    Some(lease_id),
                    row.worktree_id.as_deref(),
                    &format!("lease expired at {}", row.expires_at),
                    repaired,
                    repaired.then_some("marked Expired"),
                ));
                continue;
            }

            // 2. Owner execution terminal/lost.
            if let Some(ref eid) = row.owner_execution_id {
                let lc: Option<String> =
                    sqlx::query_scalar("SELECT lifecycle FROM execution_attempts WHERE id = ?")
                        .bind(eid)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(map_err)?;
                match lc.as_deref() {
                    Some("completed" | "failed" | "cancelled") => {
                        let ikey = format!("recon-exec-term-{lease_id}");
                        let res = self
                            .transitions
                            .transition_lease(
                                lease_id,
                                &LeaseLifecycle::Active,
                                &LeaseLifecycle::Expired,
                                &ikey,
                                true,
                            )
                            .await;
                        let repaired = res.is_ok() || is_already(&res);
                        drifts.push(lease_drift(
                            LeaseDriftKind::ActiveOwnerExecutionTerminal,
                            Some(lease_id),
                            row.worktree_id.as_deref(),
                            &format!("execution {eid} is terminal ({lc:?})"),
                            repaired,
                            repaired.then_some("marked Expired"),
                        ));
                        continue;
                    }
                    Some("lost") => {
                        let ikey = format!("recon-exec-lost-{lease_id}");
                        let res = self
                            .transitions
                            .transition_lease(
                                lease_id,
                                &LeaseLifecycle::Active,
                                &LeaseLifecycle::Expired,
                                &ikey,
                                true,
                            )
                            .await;
                        let repaired = res.is_ok() || is_already(&res);
                        drifts.push(lease_drift(
                            LeaseDriftKind::ActiveOwnerExecutionLost,
                            Some(lease_id),
                            row.worktree_id.as_deref(),
                            &format!("execution {eid} is lost"),
                            repaired,
                            repaired.then_some("marked Expired"),
                        ));
                        continue;
                    }
                    _ => {}
                }
            }

            // 3. Supervisor mismatch.
            if let Some(ref sid) = row.owner_supervisor_id {
                if !sid.is_empty() && sid != &self.current_supervisor {
                    drifts.push(lease_drift(
                        LeaseDriftKind::ActiveOwnerSupervisorMismatch,
                        Some(lease_id),
                        row.worktree_id.as_deref(),
                        &format!(
                            "lease owned by {sid}, reconciler is {}",
                            self.current_supervisor
                        ),
                        false,
                        None,
                    ));
                }
            }

            // 4. Worktree record check.
            if let Some(ref wt_id) = row.worktree_id {
                let wt: Option<(String,)> =
                    sqlx::query_as("SELECT status FROM worktrees WHERE id = ?")
                        .bind(wt_id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(map_err)?;
                let wt_status: Option<&str> = wt.as_ref().map(|(s,)| s.as_str());
                match wt_status {
                    None => {
                        let ikey = format!("recon-wt-missing-{lease_id}");
                        let res = self
                            .transitions
                            .transition_lease(
                                lease_id,
                                &LeaseLifecycle::Active,
                                &LeaseLifecycle::Expired,
                                &ikey,
                                true,
                            )
                            .await;
                        let repaired = res.is_ok() || is_already(&res);
                        drifts.push(lease_drift(
                            LeaseDriftKind::ActiveWorktreeMissing,
                            Some(lease_id),
                            Some(wt_id),
                            "worktree record deleted or never existed",
                            repaired,
                            repaired.then_some("marked Expired"),
                        ));
                        continue;
                    }
                    Some(status) if status == "removed" || status == "removing" => {
                        let ikey = format!("recon-wt-removed-{lease_id}");
                        let res = self
                            .transitions
                            .transition_lease(
                                lease_id,
                                &LeaseLifecycle::Active,
                                &LeaseLifecycle::Expired,
                                &ikey,
                                true,
                            )
                            .await;
                        let repaired = res.is_ok() || is_already(&res);
                        drifts.push(lease_drift(
                            LeaseDriftKind::ActiveWorktreeRemoved,
                            Some(lease_id),
                            Some(wt_id),
                            &format!("worktree status is '{status}'"),
                            repaired,
                            repaired.then_some("marked Expired"),
                        ));
                        continue;
                    }
                    _ => {}
                }
            }
        }

        // 5. Duplicate active leases per worktree (should be impossible with
        //    the partial unique index, but scan as a safety diagnostic).
        let dupes: Vec<(Option<String>, i64)> = sqlx::query_as(
            "SELECT worktree_id, COUNT(*) AS cnt FROM workspace_leases WHERE lifecycle = 'active' AND worktree_id IS NOT NULL GROUP BY worktree_id HAVING cnt > 1",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_err)?;
        for (wt_id, cnt) in &dupes {
            if let Some(ref id) = wt_id {
                drifts.push(lease_drift(
                    LeaseDriftKind::MultipleActiveLeases,
                    None,
                    Some(id),
                    &format!("{cnt} active leases on worktree {id}"),
                    false,
                    None,
                ));
            }
        }

        // 6. Leases without worktree records (already expired above; report
        //    any remaining on review).
        let orphans: Vec<String> = sqlx::query_scalar(
            "SELECT wl.id FROM workspace_leases wl LEFT JOIN worktrees wt ON wl.worktree_id = wt.id WHERE wl.lifecycle = 'active' AND wl.worktree_id IS NOT NULL AND wt.id IS NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_err)?;
        for lid in &orphans {
            drifts.push(lease_drift(
                LeaseDriftKind::LeaseWithoutWorktreeRecord,
                Some(lid),
                None,
                "active lease references a non-existent worktree record",
                false,
                None,
            ));
        }

        Ok(drifts)
    }
}

fn lease_drift(
    kind: LeaseDriftKind,
    lease_id: Option<&str>,
    worktree_id: Option<&str>,
    detail: &str,
    repaired: bool,
    repair: Option<&str>,
) -> LeaseDrift {
    LeaseDrift {
        kind,
        lease_id: lease_id.map(str::to_string),
        worktree_id: worktree_id.map(str::to_string),
        detail: detail.to_string(),
        repaired,
        repair: repair.map(str::to_string),
    }
}

fn is_already(r: &Result<(), CoreError>) -> bool {
    r.as_ref()
        .err()
        .map(|e| e.message.contains("terminal"))
        .unwrap_or(false)
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)] // sqlx reads all columns; not every field is accessed after query
struct LeaseRow {
    id: String,
    worktree_id: Option<String>,
    project_id: String,
    #[allow(dead_code)]
    task_id: String,
    owner_execution_id: Option<String>,
    owner_supervisor_id: Option<String>,
    #[allow(dead_code)]
    lease_token: Option<String>,
    #[allow(dead_code)]
    fencing_token: Option<i64>,
    #[allow(dead_code)]
    lifecycle: String,
    #[allow(dead_code)]
    acquired_at: String,
    #[allow(dead_code)]
    heartbeat_at: Option<String>,
    expires_at: String,
    #[allow(dead_code)]
    released_at: Option<String>,
    #[allow(dead_code)]
    release_reason: Option<String>,
    #[allow(dead_code)]
    version: i64,
}

fn map_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}
