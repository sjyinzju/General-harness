//! ResourceClaimRepo — persistence layer for claim groups.
//!
//! All acquire/replace operations run inside a SQLite `BEGIN IMMEDIATE`
//! transaction that serializes writers. The overlap engine is invoked
//! within the transaction so conflict detection and insertion form a
//! single atomic window.

use harness_core::resource_claim::{
    AccessMode, ClaimConflict, ClaimDecision, ClaimGroupSpec, ClaimLifecycle,
    ExistingClaim, ResourceIdentity, ResourceKind, ResourceOverlapEngine,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::idempotency;

/// Guard carrying live lease credentials. The `lease_token` is a secret:
/// it must never be persisted, logged, or rendered. `fencing_token` is the
/// public monotonic epoch and is safe to store.
#[derive(Clone)]
pub struct ClaimGuard {
    pub lease_id: String,
    pub lease_token: String,
    pub fencing_token: i64,
    pub worktree_id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
}

impl std::fmt::Debug for ClaimGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimGuard")
            .field("lease_id", &self.lease_id)
            .field("lease_token", &"[REDACTED]")
            .field("fencing_token", &self.fencing_token)
            .field("worktree_id", &self.worktree_id)
            .field("project_id", &self.project_id)
            .field("task_id", &self.task_id)
            .field("execution_id", &self.execution_id)
            .finish()
    }
}

/// Outcome of a claim group acquisition attempt.
#[derive(Debug, Clone)]
pub enum AcquireOutcome {
    /// Successfully acquired a new group.
    Acquired(ClaimGroupRecord),
    /// Idempotent replay: same idempotency key returned the existing group.
    AlreadyAcquired(ClaimGroupRecord),
    /// One or more claims conflict with existing active claims.
    Conflict {
        conflicts: Vec<ClaimConflict>,
    },
    /// Idempotency key exists but with a different request hash.
    IdempotencyConflict,
    /// The spec was invalid (empty group, bad paths, etc.).
    InvalidSpec {
        reason: String,
    },
}

/// A persisted claim group with its child claim rows.
#[derive(Debug, Clone)]
pub struct ClaimGroupRecord {
    pub group_id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub repository_identity: String,
    pub worktree_id: Option<String>,
    pub lease_id: Option<String>,
    pub fencing_token: i64,
    pub request_hash: String,
    pub lifecycle: ClaimLifecycle,
    pub acquired_at: String,
    pub heartbeat_at: Option<String>,
    pub expires_at: Option<String>,
    pub released_at: Option<String>,
    pub release_reason: Option<String>,
    pub version: i64,
    pub claims: Vec<ClaimRowRecord>,
}

/// A single claim row within a group.
#[derive(Debug, Clone)]
pub struct ClaimRowRecord {
    pub claim_id: String,
    pub group_id: String,
    pub resource_kind: String,
    pub normalized_resource: String,
    pub access_mode: String,
    pub lifecycle: ClaimLifecycle,
}

/// Active claim view used by the overlap engine.
#[derive(Debug, Clone)]
pub struct ActiveClaimView {
    pub identity: ResourceIdentity,
    pub mode: AccessMode,
    pub group_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
}

pub struct ResourceClaimRepo {
    pool: SqlitePool,
}

impl ResourceClaimRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Check whether a [`ClaimGroupSpec`] conflicts with any active claims.
    /// This is a read-only operation and does not reserve resources.
    pub async fn check_conflicts(
        &self,
        spec: &ClaimGroupSpec,
    ) -> Result<ClaimDecision, CoreError> {
        let active = self.load_active_claims().await?;
        let existing: Vec<ExistingClaim> = active
            .iter()
            .map(|a| ExistingClaim {
                identity: a.identity.clone(),
                mode: a.mode,
                group_id: a.group_id.clone(),
                task_id: a.task_id.clone(),
                execution_id: a.execution_id.clone(),
            })
            .collect();
        Ok(ResourceOverlapEngine::check_conflicts(spec, &existing))
    }

    /// Atomically acquire a claim group.
    ///
    /// Uses `BEGIN IMMEDIATE` to serialize writers. Within the transaction:
    /// 1. Read all active claims
    /// 2. Normalize the spec
    /// 3. Check conflicts against active claims
    /// 4. If compatible, insert group + claim rows + idempotency record
    /// 5. Emit DomainEvent
    pub async fn acquire_group(
        &self,
        spec: &ClaimGroupSpec,
        guard: &ClaimGuard,
        idempotency_key: &str,
    ) -> Result<AcquireOutcome, CoreError> {
        // 1. Normalize the spec (needed for hash comparison).
        let normalized = match spec.normalize() {
            Ok(n) => n,
            Err(reason) => return Ok(AcquireOutcome::InvalidSpec { reason }),
        };

        // 2. Idempotency check: same key, same hash → AlreadyAcquired;
        //    same key, different hash → IdempotencyConflict.
        if let Some(record) = self.get_idempotent_group(idempotency_key).await? {
            let stored_hash = self
                .get_ikey_hash(idempotency_key)
                .await?
                .unwrap_or_default();
            if stored_hash == normalized.request_hash {
                return Ok(AcquireOutcome::AlreadyAcquired(record));
            }
            return Ok(AcquireOutcome::IdempotencyConflict);
        }

        // 4. BEGIN IMMEDIATE — serializes all claim acquisitions.
        let mut tx = self.pool.begin().await.map_err(db_err)?;

        // Execute IMMEDIATE by doing a write.
        sqlx::query("CREATE TEMP TABLE IF NOT EXISTS _claim_serialize (x INTEGER)")
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

        // 5. Load all active claims within the transaction.
        let active = self.load_active_claims_in_tx(&mut tx).await?;
        let existing: Vec<ExistingClaim> = active
            .iter()
            .map(|a| ExistingClaim {
                identity: a.identity.clone(),
                mode: a.mode,
                group_id: a.group_id.clone(),
                task_id: a.task_id.clone(),
                execution_id: a.execution_id.clone(),
            })
            .collect();

        // 6. Conflict check.
        let decision = ResourceOverlapEngine::check_conflicts(spec, &existing);
        match decision {
            ClaimDecision::Conflict { conflicts } => {
                return Ok(AcquireOutcome::Conflict { conflicts });
            }
            ClaimDecision::InvalidSpec { reason } => {
                return Ok(AcquireOutcome::InvalidSpec { reason });
            }
            ClaimDecision::Compatible => {} // proceed
        }

        // 7. Insert group.
        let group_id = format!("rcg-{}", Uuid::new_v4());
        let now = now_sql();
        let expires = guard_expires_sql();

        sqlx::query(
            "INSERT INTO resource_claim_groups (group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&group_id)
        .bind(&spec.project_id)
        .bind(&spec.task_id)
        .bind(&spec.execution_id)
        .bind(&spec.repository_identity)
        .bind(&spec.worktree_id)
        .bind(&guard.lease_id)
        .bind(guard.fencing_token)
        .bind(&normalized.request_hash)
        .bind("active")
        .bind(&now)
        .bind(&now)
        .bind(&expires)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        // 8. Insert claim rows.
        for (identity, mode) in &normalized.claims {
            let claim_id = format!("rc-{}", Uuid::new_v4());
            let (kind_str, norm_res) = identity_to_kind_and_resource(identity);
            let mode_str = access_mode_str(*mode);

            sqlx::query(
                "INSERT INTO resource_claims (id, project_id, task_id, execution_id, resource_kind, normalized_resource, access_mode, status, group_id, lifecycle, acquired_at, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            )
            .bind(&claim_id)
            .bind(&spec.project_id)
            .bind(&spec.task_id)
            .bind(&spec.execution_id)
            .bind(&kind_str)
            .bind(&norm_res)
            .bind(&mode_str)
            .bind("active")
            .bind(&group_id)
            .bind("active")
            .bind(&now)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        }

        // 9. Record idempotency.
        sqlx::query(
            "INSERT INTO idempotency_records (key, request_hash, status, result_json, created_at, updated_at) VALUES (?,?,'pending',?,?,?)",
        )
        .bind(idempotency_key)
        .bind(&normalized.request_hash)
        .bind(&group_id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        tx.commit().await.map_err(db_err)?;

        // 10. Emit event (outside the tx to avoid holding the lock).
        self.emit_claim_event(
            &group_id,
            "resource_claim_group_acquired",
            "active",
            &format!("{group_id}-acquire"),
        )
        .await?;

        // 11. Reload and return.
        let record = self.get_group(&group_id).await?;
        Ok(AcquireOutcome::Acquired(record))
    }

    /// Get a claim group by ID.
    pub async fn get_group(&self, group_id: &str) -> Result<ClaimGroupRecord, CoreError> {
        let row: GroupRow = sqlx::query_as(
            "SELECT group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM resource_claim_groups WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?
        .ok_or_else(|| CoreError::new(ErrorCode::PersistenceError, format!("claim group not found: {group_id}"), ErrorSource::System))?;

        let claims = self.load_group_claims(group_id).await?;
        Ok(group_row_to_record(row, claims))
    }

    /// List active claim groups for a task.
    pub async fn list_active_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<ClaimGroupRecord>, CoreError> {
        let rows: Vec<GroupRow> = sqlx::query_as(
            "SELECT group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM resource_claim_groups WHERE task_id = ? AND lifecycle = 'active'",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut records = Vec::new();
        for row in rows {
            let claims = self.load_group_claims(&row.group_id).await?;
            records.push(group_row_to_record(row, claims));
        }
        Ok(records)
    }

    /// List active claim groups for an execution.
    pub async fn list_active_for_execution(
        &self,
        execution_id: &str,
    ) -> Result<Vec<ClaimGroupRecord>, CoreError> {
        let rows: Vec<GroupRow> = sqlx::query_as(
            "SELECT group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM resource_claim_groups WHERE execution_id = ? AND lifecycle = 'active'",
        )
        .bind(execution_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut records = Vec::new();
        for row in rows {
            let claims = self.load_group_claims(&row.group_id).await?;
            records.push(group_row_to_record(row, claims));
        }
        Ok(records)
    }

    /// List active claim groups for a repository.
    pub async fn list_active_for_repository(
        &self,
        repository_identity: &str,
    ) -> Result<Vec<ClaimGroupRecord>, CoreError> {
        let rows: Vec<GroupRow> = sqlx::query_as(
            "SELECT group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM resource_claim_groups WHERE repository_identity = ? AND lifecycle = 'active'",
        )
        .bind(repository_identity)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut records = Vec::new();
        for row in rows {
            let claims = self.load_group_claims(&row.group_id).await?;
            records.push(group_row_to_record(row, claims));
        }
        Ok(records)
    }

    /// Renew a claim group (extend heartbeat and expiry).
    /// Only the current lease owner with the correct fencing token can renew.
    pub async fn renew_group(
        &self,
        group_id: &str,
        guard: &ClaimGuard,
        _duration_secs: u32,
    ) -> Result<(), CoreError> {
        let now = now_sql();
        let expires = guard_expires_sql();

        let aff = sqlx::query(
            "UPDATE resource_claim_groups SET heartbeat_at = ?, expires_at = ?, updated_at = ?, version = version + 1 WHERE group_id = ? AND lifecycle = 'active' AND lease_id = ? AND fencing_token = ? AND version = (SELECT version FROM resource_claim_groups WHERE group_id = ?)",
        )
        .bind(&now)
        .bind(&expires)
        .bind(&now)
        .bind(group_id)
        .bind(&guard.lease_id)
        .bind(guard.fencing_token)
        .bind(group_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        if aff.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::WorkspaceLeaseExpired,
                format!("claim group renew failed: {group_id}"),
                ErrorSource::System,
            ));
        }
        Ok(())
    }

    /// Release a claim group.
    /// Only the current lease owner with the correct fencing token can release.
    pub async fn release_group(
        &self,
        group_id: &str,
        guard: &ClaimGuard,
        reason: &str,
    ) -> Result<(), CoreError> {
        let now = now_sql();

        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let aff = sqlx::query(
            "UPDATE resource_claim_groups SET lifecycle = 'released', released_at = ?, release_reason = ?, updated_at = ?, version = version + 1 WHERE group_id = ? AND lifecycle = 'active' AND lease_id = ? AND fencing_token = ?",
        )
        .bind(&now)
        .bind(reason)
        .bind(&now)
        .bind(group_id)
        .bind(&guard.lease_id)
        .bind(guard.fencing_token)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        if aff.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::WorkspaceLeaseExpired,
                format!("claim group release failed (not found, not active, or wrong guard): {group_id}"),
                ErrorSource::System,
            ));
        }

        // Also mark child claim rows as released.
        sqlx::query(
            "UPDATE resource_claims SET lifecycle = 'released' WHERE group_id = ? AND lifecycle = 'active'",
        )
        .bind(group_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        tx.commit().await.map_err(db_err)?;

        self.emit_claim_event(
            group_id,
            "resource_claim_group_released",
            "released",
            &format!("{group_id}-release-{}", Uuid::new_v4()),
        )
        .await?;

        Ok(())
    }

    /// Expire all claim groups whose `expires_at` has passed.
    pub async fn expire_due_groups(&self, now: &str) -> Result<Vec<String>, CoreError> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT group_id FROM resource_claim_groups WHERE lifecycle = 'active' AND expires_at < ?",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut expired = Vec::new();
        for id in &ids {
            let mut tx = self.pool.begin().await.map_err(db_err)?;

            let aff = sqlx::query(
                "UPDATE resource_claim_groups SET lifecycle = 'expired', released_at = ?, updated_at = ?, version = version + 1 WHERE group_id = ? AND lifecycle = 'active' AND expires_at < ?",
            )
            .bind(now)
            .bind(now)
            .bind(id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

            if aff.rows_affected() > 0 {
                sqlx::query(
                    "UPDATE resource_claims SET lifecycle = 'expired' WHERE group_id = ? AND lifecycle = 'active'",
                )
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;

                tx.commit().await.map_err(db_err)?;

                self.emit_claim_event(
                    id,
                    "resource_claim_group_expired",
                    "expired",
                    &format!("{id}-expire"),
                )
                .await?;

                expired.push(id.clone());
            }
        }
        Ok(expired)
    }

    /// Replace an existing claim group with a new spec atomically.
    ///
    /// Old group → Released; new group → Active. Both in one transaction.
    /// If the new spec conflicts, the old group is preserved.
    pub async fn replace_group(
        &self,
        old_group_id: &str,
        new_spec: &ClaimGroupSpec,
        guard: &ClaimGuard,
        idempotency_key: &str,
    ) -> Result<AcquireOutcome, CoreError> {
        // 1. Normalize the new spec.
        let normalized = match new_spec.normalize() {
            Ok(n) => n,
            Err(reason) => return Ok(AcquireOutcome::InvalidSpec { reason }),
        };

        // 2. Idempotency check: same key, same hash → AlreadyAcquired.
        if let Some(record) = self.get_idempotent_group(idempotency_key).await? {
            let stored_hash = self
                .get_ikey_hash(idempotency_key)
                .await?
                .unwrap_or_default();
            if stored_hash == normalized.request_hash {
                return Ok(AcquireOutcome::AlreadyAcquired(record));
            }
            return Ok(AcquireOutcome::IdempotencyConflict);
        }

        // 3. BEGIN IMMEDIATE.
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        sqlx::query("CREATE TEMP TABLE IF NOT EXISTS _claim_serialize (x INTEGER)")
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

        // 4. Verify old group exists and is active and owned by this guard.
        let old: Option<GroupRow> = sqlx::query_as(
            "SELECT group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, released_at, release_reason, version FROM resource_claim_groups WHERE group_id = ? AND lifecycle = 'active' AND lease_id = ? AND fencing_token = ?",
        )
        .bind(old_group_id)
        .bind(&guard.lease_id)
        .bind(guard.fencing_token)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_err)?;

        let _old_row = match old {
            Some(r) => r,
            None => {
                return Err(CoreError::new(
                    ErrorCode::WorkspaceLeaseExpired,
                    format!("old claim group not found or guard mismatch: {old_group_id}"),
                    ErrorSource::System,
                ));
            }
        };

        // 5. Load active claims EXCEPT the old group's claims (since they'll be released).
        let all_active = self.load_active_claims_in_tx(&mut tx).await?;
        let external_active: Vec<_> = all_active
            .iter()
            .filter(|a| a.group_id != old_group_id)
            .collect();

        let existing: Vec<ExistingClaim> = external_active
            .iter()
            .map(|a| ExistingClaim {
                identity: a.identity.clone(),
                mode: a.mode,
                group_id: a.group_id.clone(),
                task_id: a.task_id.clone(),
                execution_id: a.execution_id.clone(),
            })
            .collect();

        // 6. Check conflicts against external active claims.
        let decision = ResourceOverlapEngine::check_conflicts(new_spec, &existing);
        match decision {
            ClaimDecision::Conflict { conflicts } => {
                return Ok(AcquireOutcome::Conflict { conflicts });
            }
            ClaimDecision::InvalidSpec { reason } => {
                return Ok(AcquireOutcome::InvalidSpec { reason });
            }
            ClaimDecision::Compatible => {} // proceed
        }

        // 7. Release old group.
        let now = now_sql();
        sqlx::query(
            "UPDATE resource_claim_groups SET lifecycle = 'released', released_at = ?, release_reason = 'replaced', updated_at = ?, version = version + 1 WHERE group_id = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(old_group_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        sqlx::query(
            "UPDATE resource_claims SET lifecycle = 'released' WHERE group_id = ? AND lifecycle = 'active'",
        )
        .bind(old_group_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        // 8. Insert new group.
        let new_group_id = format!("rcg-{}", Uuid::new_v4());
        let expires = guard_expires_sql();

        sqlx::query(
            "INSERT INTO resource_claim_groups (group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, heartbeat_at, expires_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&new_group_id)
        .bind(&new_spec.project_id)
        .bind(&new_spec.task_id)
        .bind(&new_spec.execution_id)
        .bind(&new_spec.repository_identity)
        .bind(&new_spec.worktree_id)
        .bind(&guard.lease_id)
        .bind(guard.fencing_token)
        .bind(&normalized.request_hash)
        .bind("active")
        .bind(&now)
        .bind(&now)
        .bind(&expires)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        for (identity, mode) in &normalized.claims {
            let claim_id = format!("rc-{}", Uuid::new_v4());
            let (kind_str, norm_res) = identity_to_kind_and_resource(identity);
            let mode_str = access_mode_str(*mode);

            sqlx::query(
                "INSERT INTO resource_claims (id, project_id, task_id, execution_id, resource_kind, normalized_resource, access_mode, status, group_id, lifecycle, acquired_at, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            )
            .bind(&claim_id)
            .bind(&new_spec.project_id)
            .bind(&new_spec.task_id)
            .bind(&new_spec.execution_id)
            .bind(&kind_str)
            .bind(&norm_res)
            .bind(&mode_str)
            .bind("active")
            .bind(&new_group_id)
            .bind("active")
            .bind(&now)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        }

        // 9. Record idempotency.
        sqlx::query(
            "INSERT INTO idempotency_records (key, request_hash, status, result_json, created_at, updated_at) VALUES (?,?,'pending',?,?,?)",
        )
        .bind(idempotency_key)
        .bind(&normalized.request_hash)
        .bind(&new_group_id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        tx.commit().await.map_err(db_err)?;

        self.emit_claim_event(
            &new_group_id,
            "resource_claim_group_replaced",
            "active",
            &format!("{new_group_id}-replace"),
        )
        .await?;

        let record = self.get_group(&new_group_id).await?;
        Ok(AcquireOutcome::Acquired(record))
    }

    /// Load all active claim views for conflict checking.
    async fn load_active_claims(&self) -> Result<Vec<ActiveClaimView>, CoreError> {
        let rows: Vec<ActiveClaimRow> = sqlx::query_as(
            "SELECT rc.resource_kind, rc.normalized_resource, rc.access_mode, rc.group_id, g.task_id, g.execution_id, g.repository_identity FROM resource_claims rc JOIN resource_claim_groups g ON rc.group_id = g.group_id WHERE g.lifecycle = 'active'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows_to_active_views(rows))
    }

    /// Load all active claim views within a transaction.
    async fn load_active_claims_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<Vec<ActiveClaimView>, CoreError> {
        let rows: Vec<ActiveClaimRow> = sqlx::query_as(
            "SELECT rc.resource_kind, rc.normalized_resource, rc.access_mode, rc.group_id, g.task_id, g.execution_id, g.repository_identity FROM resource_claims rc JOIN resource_claim_groups g ON rc.group_id = g.group_id WHERE g.lifecycle = 'active'",
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(db_err)?;

        Ok(rows_to_active_views(rows))
    }

    /// Load all claim rows for a group.
    async fn load_group_claims(
        &self,
        group_id: &str,
    ) -> Result<Vec<ClaimRowRecord>, CoreError> {
        let rows: Vec<ClaimRow> = sqlx::query_as(
            "SELECT id, group_id, resource_kind, normalized_resource, access_mode, lifecycle FROM resource_claims WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows
            .into_iter()
            .map(|r| ClaimRowRecord {
                claim_id: r.id,
                group_id: r.group_id,
                resource_kind: r.resource_kind,
                normalized_resource: r.normalized_resource,
                access_mode: r.access_mode,
                lifecycle: parse_claim_lifecycle(&r.lifecycle),
            })
            .collect())
    }

    /// Check if an idempotency key already resolves to a group.
    async fn get_idempotent_group(
        &self,
        ikey: &str,
    ) -> Result<Option<ClaimGroupRecord>, CoreError> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT status, result_json FROM idempotency_records WHERE key = ?",
        )
        .bind(ikey)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        match row {
            Some((_status, result_json)) if !result_json.is_empty() => {
                match self.get_group(&result_json).await {
                    Ok(record) => Ok(Some(record)),
                    Err(_) => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Get the request hash for an existing idempotency key.
    async fn get_ikey_hash(&self, ikey: &str) -> Result<Option<String>, CoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT request_hash FROM idempotency_records WHERE key = ?",
        )
        .bind(ikey)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.0))
    }

    /// Emit a lifecycle event for a claim group.
    async fn emit_claim_event(
        &self,
        group_id: &str,
        event_type: &str,
        to_lifecycle: &str,
        ikey: &str,
    ) -> Result<(), CoreError> {
        if idempotency::is_duplicate(&self.pool, ikey).await? {
            return Ok(());
        }
        // Get the next stream_version for this stream.
        let max_ver: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(stream_version) FROM event_log WHERE stream_id = ?",
        )
        .bind(group_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?
        .flatten();
        let next_ver = max_ver.unwrap_or(0) + 1;

        let event_id = Uuid::new_v4().to_string();
        let cid = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        sqlx::query(
            "INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,?,?,1,?,?,'harness')",
        )
        .bind(&event_id)
        .bind(group_id)
        .bind(next_ver)
        .bind(event_type)
        .bind(serde_json::json!({"group_id": group_id, "to": to_lifecycle}).to_string())
        .bind(&cid)
        .bind(ikey)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        idempotency::record_in_tx(&mut tx, ikey, "ok").await?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }
}

// ── Row types ────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct GroupRow {
    group_id: String,
    project_id: String,
    task_id: String,
    execution_id: Option<String>,
    repository_identity: String,
    worktree_id: Option<String>,
    lease_id: Option<String>,
    fencing_token: i64,
    request_hash: String,
    lifecycle: String,
    acquired_at: String,
    heartbeat_at: Option<String>,
    expires_at: Option<String>,
    released_at: Option<String>,
    release_reason: Option<String>,
    version: i64,
}

#[derive(sqlx::FromRow)]
struct ClaimRow {
    id: String,
    group_id: String,
    resource_kind: String,
    normalized_resource: String,
    access_mode: String,
    lifecycle: String,
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

// ── Conversion helpers ───────────────────────────────────────────────

fn rows_to_active_views(rows: Vec<ActiveClaimRow>) -> Vec<ActiveClaimView> {
    rows.into_iter()
        .map(|r| {
            let kind = parse_resource_kind(&r.resource_kind);
            let identity = match kind {
                ResourceKind::Logical => ResourceIdentity::Logical {
                    key: harness_core::resource_claim::LogicalResourceKey::new(
                        &r.normalized_resource,
                    )
                    .unwrap_or_else(|_| {
                        harness_core::resource_claim::LogicalResourceKey::new(
                            "invalid",
                        )
                        .unwrap()
                    }),
                },
                _ => ResourceIdentity::Path {
                    repository_identity: r.repository_identity,
                    kind,
                    normalized_path: r.normalized_resource,
                },
            };
            ActiveClaimView {
                identity,
                mode: parse_access_mode(&r.access_mode),
                group_id: r.group_id,
                task_id: r.task_id,
                execution_id: r.execution_id,
            }
        })
        .collect()
}

fn group_row_to_record(row: GroupRow, claims: Vec<ClaimRowRecord>) -> ClaimGroupRecord {
    ClaimGroupRecord {
        group_id: row.group_id,
        project_id: row.project_id,
        task_id: row.task_id,
        execution_id: row.execution_id.unwrap_or_default(),
        repository_identity: row.repository_identity,
        worktree_id: row.worktree_id,
        lease_id: row.lease_id,
        fencing_token: row.fencing_token,
        request_hash: row.request_hash,
        lifecycle: parse_claim_lifecycle(&row.lifecycle),
        acquired_at: row.acquired_at,
        heartbeat_at: row.heartbeat_at,
        expires_at: row.expires_at,
        released_at: row.released_at,
        release_reason: row.release_reason,
        version: row.version,
        claims,
    }
}

fn identity_to_kind_and_resource(identity: &ResourceIdentity) -> (String, String) {
    match identity {
        ResourceIdentity::Path {
            kind,
            normalized_path,
            ..
        } => {
            let kind_str = match kind {
                ResourceKind::ExactFile => "exact_file".to_string(),
                ResourceKind::DirectoryPrefix => "directory_prefix".to_string(),
                ResourceKind::RepositoryWide => "repository_wide".to_string(),
                ResourceKind::Logical => "logical".to_string(),
            };
            (kind_str, normalized_path.clone())
        }
        ResourceIdentity::Logical { key } => ("logical".to_string(), key.to_string()),
    }
}

fn access_mode_str(mode: AccessMode) -> String {
    match mode {
        AccessMode::Read => "read".to_string(),
        AccessMode::Write => "write".to_string(),
    }
}

fn parse_resource_kind(s: &str) -> ResourceKind {
    match s {
        "exact_file" => ResourceKind::ExactFile,
        "directory_prefix" => ResourceKind::DirectoryPrefix,
        "repository_wide" => ResourceKind::RepositoryWide,
        "logical" => ResourceKind::Logical,
        _ => ResourceKind::ExactFile, // fallback for legacy data
    }
}

fn parse_access_mode(s: &str) -> AccessMode {
    match s {
        "write" => AccessMode::Write,
        _ => AccessMode::Read,
    }
}

fn parse_claim_lifecycle(s: &str) -> ClaimLifecycle {
    match s {
        "released" => ClaimLifecycle::Released,
        "expired" => ClaimLifecycle::Expired,
        _ => ClaimLifecycle::Active,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn now_sql() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn guard_expires_sql() -> String {
    // Default TTL of 300 seconds (5 minutes), mirrored from LeaseConfig default.
    (chrono::Utc::now() + chrono::Duration::seconds(300))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}
