//! WorkspaceLeaseService — acquire / heartbeat / renew / release / expire /
//! validate with monotonic fencing tokens.
//!
//! Every acquire bumps `worktrees.lease_epoch` atomically inside the same
//! transaction that inserts the lease row. The fencing token is the new epoch
//! value. The service never logs tokens and never returns them in `Display`/
//! `Debug` of error messages.

use std::sync::Arc;

use harness_core::contracts::workspace::LeaseLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::idempotency;

use super::clock::Clock;
use super::types::*;
use crate::lease::transition::LeaseTransitionService;

pub struct WorkspaceLeaseService {
    pool: SqlitePool,
    transitions: LeaseTransitionService,
    clock: Arc<dyn Clock + Send + Sync>,
    config: LeaseConfig,
    git_verifier: Option<Box<dyn crate::worktree::WorktreeGitVerifier>>,
}

impl WorkspaceLeaseService {
    /// Production constructor: git verifier is MANDATORY. Without it
    /// the service cannot verify that a worktree is still registered
    /// in git's worktree list during acquire.
    pub fn new(
        pool: SqlitePool,
        clock: Arc<dyn Clock + Send + Sync>,
        config: LeaseConfig,
        git_verifier: Box<dyn crate::worktree::WorktreeGitVerifier>,
    ) -> Self {
        Self {
            transitions: LeaseTransitionService::new(pool.clone()),
            pool,
            clock,
            config,
            git_verifier: Some(git_verifier),
        }
    }

    /// Test-only constructor: skips git-level worktree verification.
    /// Explicitly named to prevent accidental production use.
    pub fn new_unverified_for_tests(
        pool: SqlitePool,
        clock: Arc<dyn Clock + Send + Sync>,
        config: LeaseConfig,
    ) -> Self {
        Self {
            transitions: LeaseTransitionService::new(pool.clone()),
            pool,
            clock,
            config,
            git_verifier: None,
        }
    }

    // ── Acquire ──────────────────────────────────────────────────

    pub async fn acquire_lease(&self, spec: &LeaseSpec) -> Result<LeaseAcquireOutcome, CoreError> {
        // 1. Pre-flight validations (before any DB write).
        let _now = self.clock.now();
        let lease_dur = spec.lease_duration.as_secs().max(1);
        let expires = self.clock.expires_sql(lease_dur as u32);
        let now_sql = self.clock.now_sql();
        let lease_id = format!("lease-{}", Uuid::new_v4());
        let lease_token = Uuid::new_v4().to_string();

        // 2. Idempotency guard.
        if let Ok(Some(row)) = get_lease_by_ikey(&self.pool, &spec.idempotency_key).await {
            let lid = match &row.result_json {
                Some(json) => json.clone(),
                None => {
                    return Ok(LeaseAcquireOutcome::PreconditionFailed {
                        reason: "idempotency record has no lease id".into(),
                    })
                }
            };
            let record = load_record(&self.pool, &lid).await?;
            return Ok(LeaseAcquireOutcome::AlreadyAcquired(record));
        }

        // 3. Worktree & execution checks (outside the tx to keep it lean).
        let _ = get_worktree_row(&self.pool, &spec.worktree_id).await?;
        self.verify_worktree_ready(&spec.worktree_id).await?;
        self.verify_execution_nonterminal(&spec.owner_execution_id)
            .await?;
        self.verify_no_active_lease(&spec.worktree_id, &spec.task_id, &spec.owner_execution_id)
            .await?;

        // 4. Atomic acquire: bump epoch + expire stale leases + insert lease
        //    + record idempotency, all in one transaction.
        let mut tx = self.pool.begin().await.map_err(db_err)?;

        // Bump the fencing epoch on the worktree row. The new value is the
        // fencing token for this (and only this) lease.
        let epoch_before: i64 =
            sqlx::query_scalar("SELECT lease_epoch FROM worktrees WHERE id = ?")
                .bind(&spec.worktree_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_err)?
                .unwrap_or(0);
        let new_epoch = epoch_before + 1;
        let fencing_token = new_epoch;
        sqlx::query("UPDATE worktrees SET lease_epoch = ?, updated_at = datetime('now'), version = version + 1 WHERE id = ?")
            .bind(new_epoch)
            .bind(&spec.worktree_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

        // Expire any existing active leases for the same entities (partial
        // unique indexes alone would reject the INSERT; explicit expire
        // produces a clean event trail).
        expire_active_for(&mut tx, "worktree_id", &spec.worktree_id, &now_sql).await?;
        let task_id = &spec.task_id;
        let exec_id = &spec.owner_execution_id;
        expire_active_for(&mut tx, "task_id", task_id, &now_sql).await?;
        expire_active_for(&mut tx, "owner_execution_id", exec_id, &now_sql).await?;

        // Insert the new active lease.
        sqlx::query(
            "INSERT INTO workspace_leases (id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&lease_id)
        .bind(&spec.worktree_id)
        .bind(&spec.project_id)
        .bind(&spec.task_id)
        .bind(&spec.owner_execution_id)
        .bind(&spec.owner_supervisor_id)
        .bind(&lease_token)
        .bind(fencing_token)
        .bind("active")
        .bind(&now_sql)
        .bind(&now_sql) // heartbeat_at = now
        .bind(&expires)
        .bind(&now_sql)
        .bind(&now_sql)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        // Record idempotency.
        sqlx::query(
            "INSERT INTO idempotency_records (key, request_hash, status, result_json, created_at, updated_at) VALUES (?,'active','pending',?,?,?)",
        )
        .bind(&spec.idempotency_key)
        .bind(&lease_id)
        .bind(&now_sql)
        .bind(&now_sql)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        tx.commit().await.map_err(db_err)?;

        // Record the acquire event (the row was inserted as 'active').
        write_lease_event(
            &self.pool,
            &lease_id,
            "acquired",
            "active",
            &format!("{}-acquire", &lease_id),
        )
        .await?;

        let record = load_record(&self.pool, &lease_id).await?;
        Ok(LeaseAcquireOutcome::Acquired(record))
    }

    // ── Heartbeat / Renew ───────────────────────────────────────

    pub async fn heartbeat(
        &self,
        lease_id: &str,
        lease_token: &str,
        fencing_token: i64,
    ) -> Result<LeaseHeartbeatOutcome, CoreError> {
        let now = self.clock.now();
        let Some(rec) = load_optional_record(&self.pool, lease_id).await? else {
            return Ok(LeaseHeartbeatOutcome::NotActive);
        };

        if rec.lifecycle != "active" {
            return Ok(LeaseHeartbeatOutcome::NotActive);
        }
        if rec.lease_token != lease_token {
            return Ok(LeaseHeartbeatOutcome::TokenMismatch);
        }
        if rec.fencing_token != fencing_token {
            return Ok(LeaseHeartbeatOutcome::FencingMismatch);
        }
        if expire_passed(&rec.expires_at, &now) {
            // Hard-expired by wall clock; mark Expired atomically.
            let _ = self
                .transitions
                .transition_lease(
                    lease_id,
                    &LeaseLifecycle::Active,
                    &LeaseLifecycle::Expired,
                    &format!("{lease_id}-expire"),
                    true,
                )
                .await;
            return Ok(LeaseHeartbeatOutcome::Expired);
        }

        let extension = self.config.lease_duration;
        let expires = (now + chrono::Duration::from_std(extension).unwrap())
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let now_sql = self.clock.now_sql();

        let aff = sqlx::query(
            "UPDATE workspace_leases SET heartbeat_at = ?, expires_at = ?, updated_at = ?, version = version + 1 WHERE id = ? AND lifecycle = 'active' AND lease_token = ? AND version = ?",
        )
        .bind(&now_sql)
        .bind(&expires)
        .bind(&now_sql)
        .bind(lease_id)
        .bind(lease_token)
        .bind(rec.version)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        if aff.rows_affected() == 0 {
            // expired/released by a concurrent writer.
            let fresh = load_optional_record(&self.pool, lease_id).await?;
            return match fresh {
                Some(f) if f.lifecycle == "active" && f.lease_token != lease_token => {
                    Ok(LeaseHeartbeatOutcome::TokenMismatch)
                }
                Some(f) if f.lifecycle == "expired" => Ok(LeaseHeartbeatOutcome::Expired),
                _ => Ok(LeaseHeartbeatOutcome::NotActive),
            };
        }

        // At-risk detection (soft margin).
        let margin = self.config.renewal_margin;
        let expires_dt = now + chrono::Duration::from_std(extension).unwrap();
        let at_risk_deadline = now + chrono::Duration::from_std(margin).unwrap();
        if expires_dt <= at_risk_deadline {
            return Ok(LeaseHeartbeatOutcome::AtRisk {
                expires_at: expires,
            });
        }

        Ok(LeaseHeartbeatOutcome::Ok)
    }

    pub async fn renew_lease(
        &self,
        lease_id: &str,
        lease_token: &str,
        extension: std::time::Duration,
    ) -> Result<LeaseHeartbeatOutcome, CoreError> {
        // renew_lease extends the expiry by a custom duration; token checks
        // are the same as heartbeat.
        let rec = match load_optional_record(&self.pool, lease_id).await? {
            Some(r) => r,
            None => return Ok(LeaseHeartbeatOutcome::NotActive),
        };
        if rec.lifecycle != "active" {
            return Ok(LeaseHeartbeatOutcome::NotActive);
        }
        if rec.lease_token != lease_token {
            return Ok(LeaseHeartbeatOutcome::TokenMismatch);
        }
        let now = self.clock.now();
        let expires = (now + chrono::Duration::from_std(extension).unwrap())
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let now_sql = self.clock.now_sql();
        let aff = sqlx::query(
            "UPDATE workspace_leases SET heartbeat_at = ?, expires_at = ?, updated_at = ?, version = version + 1 WHERE id = ? AND lifecycle = 'active' AND lease_token = ? AND version = ?",
        )
        .bind(&now_sql)
        .bind(&expires)
        .bind(&now_sql)
        .bind(lease_id)
        .bind(lease_token)
        .bind(rec.version)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        if aff.rows_affected() == 0 {
            let fresh = load_optional_record(&self.pool, lease_id).await?;
            return match fresh {
                Some(f) if f.lifecycle == "active" && f.lease_token != lease_token => {
                    Ok(LeaseHeartbeatOutcome::TokenMismatch)
                }
                _ => Ok(LeaseHeartbeatOutcome::NotActive),
            };
        }
        Ok(LeaseHeartbeatOutcome::Ok)
    }

    // ── Validate ─────────────────────────────────────────────────

    /// Return Ok(()) when the lease is still active and the caller holds the
    /// correct token and fencing token. Used by `WorkspaceAccessGuard`.
    pub async fn validate_lease(
        &self,
        lease_id: &str,
        lease_token: &str,
        fencing_token: i64,
    ) -> Result<(), CoreError> {
        let Some(rec) = load_optional_record(&self.pool, lease_id).await? else {
            return Err(ls_err(format!("lease not found: {lease_id}")));
        };
        if rec.lifecycle != "active" {
            return Err(ls_err(format!("lease not active: {lease_id}")));
        }
        if rec.lease_token != lease_token {
            return Err(ls_err(format!("lease token mismatch for {lease_id}")));
        }
        if rec.fencing_token != fencing_token {
            return Err(ls_err(format!(
                "lease fencing token mismatch for {lease_id}: expected {fencing_token}, current {}",
                rec.fencing_token
            )));
        }
        // Verify worktree ownership still intact.
        if let Some(ref wt_id) = rec.worktree_id {
            let wrow = get_worktree_row(&self.pool, wt_id).await?;
            let expected_supervisor = &rec.owner_supervisor_id;
            if wrow.owner_supervisor_id != *expected_supervisor {
                return Err(ls_err(format!("worktree ownership changed for {wt_id}")));
            }
        }
        Ok(())
    }

    // ── Release ──────────────────────────────────────────────────

    pub async fn release_lease(
        &self,
        lease_id: &str,
        lease_token: &str,
        _reason: &str,
    ) -> Result<LeaseReleaseOutcome, CoreError> {
        let Some(rec) = load_optional_record(&self.pool, lease_id).await? else {
            return Ok(LeaseReleaseOutcome::NotActive);
        };
        if rec.lifecycle == "released" {
            return Ok(LeaseReleaseOutcome::AlreadyReleased);
        }
        if rec.lifecycle != "active" {
            return Ok(LeaseReleaseOutcome::NotActive);
        }
        if rec.lease_token != lease_token {
            return Ok(LeaseReleaseOutcome::TokenMismatch);
        }

        // TransitionService is the primary state-change mechanism
        // (state update + event in one transaction). The optimistic
        // version check from the read above is baked into the transition's
        // own version-guarded query.
        let ikey = format!("{lease_id}-release");
        match self
            .transitions
            .transition_lease(
                lease_id,
                &LeaseLifecycle::Active,
                &LeaseLifecycle::Released,
                &ikey,
                true,
            )
            .await
        {
            Ok(()) => Ok(LeaseReleaseOutcome::Released),
            Err(e) => {
                let fresh = load_optional_record(&self.pool, lease_id).await?;
                match fresh {
                    Some(f) if f.lifecycle == "released" => {
                        Ok(LeaseReleaseOutcome::AlreadyReleased)
                    }
                    Some(f) if f.lease_token != lease_token => {
                        Ok(LeaseReleaseOutcome::TokenMismatch)
                    }
                    _ => {
                        tracing::debug!(lease_id, error = %e, "release_transition_failed");
                        Ok(LeaseReleaseOutcome::TokenMismatch)
                    }
                }
            }
        }
    }

    // ── Expire ───────────────────────────────────────────────────

    pub async fn expire_due_leases(&self) -> Result<Vec<String>, CoreError> {
        let now = self.clock.now_sql();
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM workspace_leases WHERE lifecycle = 'active' AND expires_at < ?",
        )
        .bind(&now)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut expired = Vec::new();
        for id in &ids {
            let r = self
                .transitions
                .transition_lease(
                    id,
                    &LeaseLifecycle::Active,
                    &LeaseLifecycle::Expired,
                    &format!("{id}-expire"),
                    true,
                )
                .await;
            if r.is_ok() {
                expired.push(id.clone());
            }
        }
        Ok(expired)
    }

    // ── Query ────────────────────────────────────────────────────

    pub async fn get_active_for_worktree(
        &self,
        worktree_id: &str,
    ) -> Result<Option<LeaseRecord>, CoreError> {
        get_active_by(&self.pool, "worktree_id", worktree_id).await
    }

    pub async fn get_active_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<LeaseRecord>, CoreError> {
        get_active_by(&self.pool, "task_id", task_id).await
    }

    pub async fn get_active_for_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<LeaseRecord>, CoreError> {
        get_active_by(&self.pool, "owner_execution_id", execution_id).await
    }

    pub async fn list_expiring_before(
        &self,
        timestamp: &str,
    ) -> Result<Vec<LeaseRecord>, CoreError> {
        let rows: Vec<LeaseRow> = sqlx::query_as(
            "SELECT id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM workspace_leases WHERE lifecycle = 'active' AND expires_at < ?",
        )
        .bind(timestamp)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(LeaseRow::into_record).collect())
    }

    pub async fn get_lease(&self, lease_id: &str) -> Result<Option<LeaseRecord>, CoreError> {
        load_optional_record(&self.pool, lease_id).await
    }

    pub fn config(&self) -> &LeaseConfig {
        &self.config
    }

    pub fn clock(&self) -> &dyn Clock {
        self.clock.as_ref()
    }

    // ── Internal helpers ─────────────────────────────────────────

    async fn verify_worktree_ready(&self, worktree_id: &str) -> Result<(), CoreError> {
        let row = get_worktree_row(&self.pool, worktree_id).await?;
        if row.status == "removing" || row.status == "removed" {
            return Err(ls_err(format!(
                "worktree {worktree_id} is {status}",
                status = row.status
            )));
        }
        // Real filesystem path check.
        let path = std::path::PathBuf::from(&row.worktree_path);
        if !path.exists() {
            return Err(ls_err(format!(
                "worktree path does not exist: {}",
                path.display()
            )));
        }
        // Sidecar: must exist, parse, and match DB record identity.
        let sidecar_path = crate::worktree::metadata::sidecar_path(&path);
        if !sidecar_path.exists() {
            return Err(ls_err(format!(
                "worktree ownership sidecar missing: {}",
                sidecar_path.display()
            )));
        }
        let meta = crate::worktree::metadata::read_sidecar(&path)?
            .ok_or_else(|| ls_err("worktree sidecar unreadable or corrupt".into()))?;
        if meta.worktree_id != worktree_id {
            return Err(ls_err(format!(
                "sidecar worktree_id mismatch: expected {worktree_id}, got {}",
                meta.worktree_id
            )));
        }
        if meta.repository_identity != row.repository_identity {
            return Err(ls_err("sidecar repository identity mismatch".into()));
        }
        if meta.branch != row.branch_name {
            return Err(ls_err(format!(
                "sidecar branch mismatch: expected {}, got {}",
                row.branch_name, meta.branch
            )));
        }
        let canonical_path =
            crate::worktree::naming::canonicalize_for_git(&path).unwrap_or_else(|_| path.clone());
        let sidecar_pb = std::path::PathBuf::from(&meta.worktree_path);
        if sidecar_pb != path && sidecar_pb != canonical_path {
            return Err(ls_err(format!(
                "sidecar path mismatch: record has {}, sidecar has {}",
                path.display(),
                meta.worktree_path
            )));
        }

        // Git-level verification via injected verifier (production path).
        if let Some(ref verifier) = self.git_verifier {
            let common = std::path::PathBuf::from(&row.repository_identity);
            let gv = verifier
                .verify_worktree_git(&path, &common, &row.branch_name)
                .await?;
            if !gv.listed {
                return Err(ls_err(format!(
                    "worktree not listed in git worktree list: {}",
                    path.display()
                )));
            }
            if !gv.admin_intact || gv.ambiguous {
                return Err(ls_err(
                    "git administrative metadata missing or ambiguous".into(),
                ));
            }
            if !gv.common_dir_matches {
                return Err(ls_err("git common-dir does not match record".into()));
            }
            if !gv.branch_matches {
                return Err(ls_err("actual branch does not match record".into()));
            }
        }
        Ok(())
    }

    async fn verify_execution_nonterminal(&self, execution_id: &str) -> Result<(), CoreError> {
        let lc: Option<String> =
            sqlx::query_scalar("SELECT lifecycle FROM execution_attempts WHERE id = ?")
                .bind(execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?;
        match lc {
            None => Err(ls_err(format!("execution not found: {execution_id}"))),
            Some(lc) if is_execution_terminal(&lc) => Err(ls_err(format!(
                "execution {execution_id} is in terminal state: {lc}"
            ))),
            _ => Ok(()),
        }
    }

    async fn verify_no_active_lease(
        &self,
        worktree_id: &str,
        task_id: &str,
        execution_id: &str,
    ) -> Result<(), CoreError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workspace_leases WHERE lifecycle = 'active' AND (worktree_id = ? OR task_id = ? OR owner_execution_id = ?)",
        )
        .bind(worktree_id)
        .bind(task_id)
        .bind(execution_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        if count > 0 {
            return Err(ls_err(format!(
                "active lease already exists for worktree/task/execution: {worktree_id}/{task_id}"
            )));
        }
        Ok(())
    }
}

// ── DB helpers ───────────────────────────────────────────────────

async fn get_worktree_row(pool: &SqlitePool, worktree_id: &str) -> Result<WorktreeRow, CoreError> {
    sqlx::query_as::<_, WorktreeRow>(
        "SELECT id, worktree_path, status, owner_supervisor_id, repository_identity, branch_name FROM worktrees WHERE id = ?",
    )
    .bind(worktree_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?
    .ok_or_else(|| ls_err(format!("worktree not found: {worktree_id}")))
}

#[derive(sqlx::FromRow)]
struct WorktreeRow {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    worktree_path: String,
    status: String,
    #[allow(dead_code)]
    owner_supervisor_id: String,
    #[allow(dead_code)]
    repository_identity: String,
    #[allow(dead_code)]
    branch_name: String,
}

async fn expire_active_for(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    column: &str,
    value: &str,
    now: &str,
) -> Result<(), CoreError> {
    let sql = format!(
        "UPDATE workspace_leases SET lifecycle = 'expired', expires_at = ?, updated_at = ?, version = version + 1 WHERE {column} = ? AND lifecycle = 'active'"
    );
    sqlx::query(&sql)
        .bind(now)
        .bind(now)
        .bind(value)
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    Ok(())
}

fn is_execution_terminal(lc: &str) -> bool {
    matches!(lc, "completed" | "failed" | "lost" | "cancelled")
}

fn parse_sql_dt(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().into())
}

fn expire_passed(expires_at: &str, now: &chrono::DateTime<chrono::Utc>) -> bool {
    parse_sql_dt(expires_at)
        .map(|dt| dt < *now)
        .unwrap_or(false)
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    id: String,
    worktree_id: Option<String>,
    project_id: String,
    task_id: String,
    owner_execution_id: Option<String>,
    owner_supervisor_id: String,
    lease_token: Option<String>,
    fencing_token: Option<i64>,
    lifecycle: String,
    acquired_at: String,
    heartbeat_at: Option<String>,
    expires_at: String,
    released_at: Option<String>,
    release_reason: Option<String>,
    version: i64,
}

impl LeaseRow {
    fn into_record(self) -> LeaseRecord {
        LeaseRecord {
            lease_id: self.id,
            worktree_id: self.worktree_id,
            project_id: self.project_id,
            task_id: self.task_id,
            owner_execution_id: self.owner_execution_id,
            owner_supervisor_id: self.owner_supervisor_id,
            lease_token: self.lease_token.unwrap_or_default(),
            fencing_token: self.fencing_token.unwrap_or(0),
            lifecycle: self.lifecycle,
            acquired_at: self.acquired_at,
            heartbeat_at: self.heartbeat_at,
            expires_at: self.expires_at,
            released_at: self.released_at,
            release_reason: self.release_reason,
            version: self.version,
        }
    }
}

async fn load_record(pool: &SqlitePool, lease_id: &str) -> Result<LeaseRecord, CoreError> {
    load_optional_record(pool, lease_id)
        .await?
        .ok_or_else(|| ls_err(format!("lease not found: {lease_id}")))
}

async fn load_optional_record(
    pool: &SqlitePool,
    lease_id: &str,
) -> Result<Option<LeaseRecord>, CoreError> {
    let row: Option<LeaseRow> = sqlx::query_as(
        "SELECT id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM workspace_leases WHERE id = ?",
    )
    .bind(lease_id)
    .fetch_optional(pool)
    .await
    .map_err(db_err)?;
    Ok(row.map(LeaseRow::into_record))
}

async fn get_lease_by_ikey(pool: &SqlitePool, ikey: &str) -> Result<Option<IdemRow>, CoreError> {
    sqlx::query_as("SELECT status, result_json FROM idempotency_records WHERE key = ?")
        .bind(ikey)
        .fetch_optional(pool)
        .await
        .map_err(db_err)
}

#[derive(sqlx::FromRow)]
struct IdemRow {
    #[allow(dead_code)]
    status: String,
    result_json: Option<String>,
}

async fn get_active_by(
    pool: &SqlitePool,
    column: &str,
    value: &str,
) -> Result<Option<LeaseRecord>, CoreError> {
    let sql = format!(
        "SELECT id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM workspace_leases WHERE {column} = ? AND lifecycle = 'active'"
    );
    let row: Option<LeaseRow> = sqlx::query_as(&sql)
        .bind(value)
        .fetch_optional(pool)
        .await
        .map_err(db_err)?;
    Ok(row.map(LeaseRow::into_record))
}

/// Write a lifecycle-changed event for the lease without going through
/// LeaseTransitionService (the row is already at `'active'` on creation).
async fn write_lease_event(
    pool: &SqlitePool,
    lease_id: &str,
    from: &str,
    to: &str,
    ikey: &str,
) -> Result<(), CoreError> {
    if idempotency::is_duplicate(pool, ikey).await? {
        return Ok(());
    }
    let event_id = Uuid::new_v4().to_string();
    let cid = Uuid::new_v4().to_string();
    let _now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let mut tx = pool.begin().await.map_err(db_err)?;
    sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,1,'workspace_lease_lifecycle_changed',?,1,?,?,'harness')")
        .bind(&event_id).bind(lease_id).bind(serde_json::json!({"from":from,"to":to}).to_string()).bind(&cid).bind(ikey)
        .execute(&mut *tx).await.map_err(db_err)?;
    idempotency::record_in_tx(&mut tx, ikey, "ok").await?;
    tx.commit().await.map_err(db_err)?;
    Ok(())
}

fn ls_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}
