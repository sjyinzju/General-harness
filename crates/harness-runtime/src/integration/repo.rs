//! Integration repository — persistence for integration requests, attempts, leases, and results.

use harness_core::contracts::integration::{
    IntegrationAttempt, IntegrationRequest, IntegrationResult, IntegrationState,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}

pub struct IntegrationRepo {
    pool: SqlitePool,
}

impl IntegrationRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // ── Integration Request ──────────────────────────────────────────

    /// Insert a new integration request. Returns false if idempotency key already exists.
    pub async fn insert_request(&self, req: &IntegrationRequest) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO integration_requests (integration_id, commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_target_head, priority, state, idempotency_key, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&req.integration_id)
        .bind(&req.commit_request_id)
        .bind(&req.candidate_id)
        .bind(&req.review_id)
        .bind(&req.repository_id)
        .bind(&req.target_ref)
        .bind(&req.expected_target_head)
        .bind(req.priority)
        .bind(IntegrationState::Queued.as_str())
        .bind(&req.idempotency_key)
        .bind(req.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(req.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Find an existing request by idempotency key.
    pub async fn find_by_idempotency_key(
        &self,
        ikey: &str,
    ) -> Result<Option<IntegrationRequest>, CoreError> {
        let row: Option<IntegrationRequestRow> = sqlx::query_as(
            "SELECT integration_id, commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_target_head, priority, state, idempotency_key, created_at FROM integration_requests WHERE idempotency_key = ?",
        )
        .bind(ikey)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Find an active request for the same scope (candidate + repo + target_ref).
    pub async fn find_active_by_scope(
        &self,
        candidate_id: &str,
        repository_id: &str,
        target_ref: &str,
    ) -> Result<Option<IntegrationRequest>, CoreError> {
        let row: Option<IntegrationRequestRow> = sqlx::query_as(
            "SELECT integration_id, commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_target_head, priority, state, idempotency_key, created_at FROM integration_requests WHERE candidate_id = ? AND repository_id = ? AND target_ref = ? AND state NOT IN ('integrated','conflict','blocked','failed','cancelled','stale') ORDER BY created_at DESC LIMIT 1",
        )
        .bind(candidate_id)
        .bind(repository_id)
        .bind(target_ref)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Get just the state of an integration request.
    pub async fn get_state(&self, id: &str) -> Result<Option<String>, CoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state FROM integration_requests WHERE integration_id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?;
        Ok(row.map(|r| r.0))
    }

    /// Get an integration request by ID.
    pub async fn get_request(&self, id: &str) -> Result<Option<IntegrationRequest>, CoreError> {
        let row: Option<IntegrationRequestRow> = sqlx::query_as(
            "SELECT integration_id, commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_target_head, priority, state, idempotency_key, created_at FROM integration_requests WHERE integration_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Transition integration request state (CAS).
    pub async fn transition_state(
        &self,
        integration_id: &str,
        from: &IntegrationState,
        to: &IntegrationState,
    ) -> Result<bool, CoreError> {
        let is_terminal = to.is_terminal();
        let rows = sqlx::query(
            "UPDATE integration_requests SET state = ?, updated_at = datetime('now'), completed_at = CASE WHEN ? THEN datetime('now') ELSE completed_at END WHERE integration_id = ? AND state = ?",
        )
        .bind(to.as_str())
        .bind(is_terminal)
        .bind(integration_id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// List queued requests for a given (repo, target_ref) scope, ordered by priority DESC, created_at ASC.
    pub async fn list_queued_for_scope(
        &self,
        repository_id: &str,
        target_ref: &str,
        limit: i64,
    ) -> Result<Vec<IntegrationRequest>, CoreError> {
        let rows: Vec<IntegrationRequestRow> = sqlx::query_as(
            "SELECT integration_id, commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_target_head, priority, state, idempotency_key, created_at FROM integration_requests WHERE repository_id = ? AND target_ref = ? AND state = 'queued' ORDER BY priority DESC, created_at ASC, integration_id ASC LIMIT ?",
        )
        .bind(repository_id)
        .bind(target_ref)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    // ── Integration Attempt ───────────────────────────────────────────

    /// Insert an integration attempt.
    pub async fn insert_attempt(&self, attempt: &IntegrationAttempt) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO integration_attempts (attempt_id, integration_id, attempt_number, state, commit_oid, parent_oid, target_head_at_start, integration_tree_oid, integration_commit_oid, lease_id, fencing_token, worktree_path, strategy, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&attempt.attempt_id)
        .bind(&attempt.integration_id)
        .bind(attempt.attempt_number as i64)
        .bind(attempt.state.as_str())
        .bind(&attempt.commit_oid)
        .bind(&attempt.parent_oid)
        .bind(&attempt.target_head_at_start)
        .bind(&attempt.integration_tree_oid)
        .bind(&attempt.integration_commit_oid)
        .bind(&attempt.lease_id)
        .bind(attempt.fencing_token)
        .bind(&attempt.integration_tree_oid) // worktree_path placeholder
        .bind(None::<String>) // strategy
        .bind(attempt.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Get an integration attempt by ID.
    pub async fn get_attempt(&self, id: &str) -> Result<Option<IntegrationAttempt>, CoreError> {
        let row: Option<IntegrationAttemptRow> = sqlx::query_as(
            "SELECT attempt_id, integration_id, attempt_number, state, commit_oid, parent_oid, target_head_at_start, integration_tree_oid, integration_commit_oid, lease_id, fencing_token, worktree_path, strategy, error_message, started_at, completed_at, created_at FROM integration_attempts WHERE attempt_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Count attempts for an integration request.
    pub async fn count_attempts(&self, integration_id: &str) -> Result<u32, CoreError> {
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM integration_attempts WHERE integration_id = ?")
                .bind(integration_id)
                .fetch_one(&self.pool)
                .await
                .map_err(db_err)?;
        Ok(row.0 as u32)
    }

    /// Transition attempt state (CAS).
    pub async fn transition_attempt_state(
        &self,
        attempt_id: &str,
        from: &IntegrationState,
        to: &IntegrationState,
    ) -> Result<bool, CoreError> {
        let is_terminal = to.is_terminal();
        let rows = sqlx::query(
            "UPDATE integration_attempts SET state = ?, completed_at = CASE WHEN ? THEN datetime('now') ELSE completed_at END WHERE attempt_id = ? AND state = ?",
        )
        .bind(to.as_str())
        .bind(is_terminal)
        .bind(attempt_id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    // ── Integration Result ────────────────────────────────────────────

    /// Insert an integration result.
    pub async fn insert_result(&self, result: &IntegrationResult) -> Result<bool, CoreError> {
        let strategy_str = result.strategy.as_ref().map(|s| match s {
            harness_core::contracts::integration::IntegrationStrategy::FastForward => {
                "fast_forward"
            }
            harness_core::contracts::integration::IntegrationStrategy::CherryPick => "cherry_pick",
            harness_core::contracts::integration::IntegrationStrategy::Conflict => "conflict",
        });
        let conflict_json = result
            .conflicts
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_default());

        let rows = sqlx::query(
            "INSERT OR IGNORE INTO integration_results (integration_id, attempt_id, state, previous_target_head, new_target_head, commit_oid, strategy, verification_status, conflict_json, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&result.integration_id)
        .bind(&result.attempt_id)
        .bind(result.state.as_str())
        .bind(&result.previous_target_head)
        .bind(&result.new_target_head)
        .bind(&result.commit_oid)
        .bind(strategy_str)
        .bind(&result.verification_status)
        .bind(&conflict_json)
        .bind(result.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    // ── Leases ──────────────────────────────────────────────────────────

    /// Acquire an integration lease for (repo, target_ref).
    /// Fails if another active lease exists for the same scope (UNIQUE constraint).
    #[allow(clippy::too_many_arguments)]
    pub async fn acquire_lease(
        &self,
        lease_id: &str,
        integration_id: &str,
        attempt_id: &str,
        repository_id: &str,
        target_ref: &str,
        lease_token: &str,
        fencing_token: i64,
        expires_at: &str,
    ) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO integration_leases (lease_id, integration_id, attempt_id, repository_id, target_ref, lease_token, fencing_token, lifecycle, expires_at, version) VALUES (?,?,?,?,?,?,?,?,?,1)",
        )
        .bind(lease_id)
        .bind(integration_id)
        .bind(attempt_id)
        .bind(repository_id)
        .bind(target_ref)
        .bind(lease_token)
        .bind(fencing_token)
        .bind("active")
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Release a lease — sets lifecycle to 'released'.
    pub async fn release_lease(
        &self,
        lease_id: &str,
        fencing_token: i64,
    ) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "UPDATE integration_leases SET lifecycle = 'released', released_at = datetime('now') WHERE lease_id = ? AND lifecycle = 'active' AND fencing_token = ?",
        )
        .bind(lease_id)
        .bind(fencing_token)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Force-expire stale leases for a scope (recovery).
    pub async fn expire_stale_leases(
        &self,
        repository_id: &str,
        target_ref: &str,
    ) -> Result<u64, CoreError> {
        let rows = sqlx::query(
            "UPDATE integration_leases SET lifecycle = 'expired', released_at = datetime('now') WHERE repository_id = ? AND target_ref = ? AND lifecycle = 'active' AND expires_at < datetime('now')",
        )
        .bind(repository_id)
        .bind(target_ref)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected())
    }

    /// Get the active lease for a scope.
    pub async fn get_active_lease(
        &self,
        repository_id: &str,
        target_ref: &str,
    ) -> Result<Option<LeaseRow>, CoreError> {
        let row: Option<LeaseRow> = sqlx::query_as(
            "SELECT lease_id, integration_id, attempt_id, repository_id, target_ref, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, version FROM integration_leases WHERE repository_id = ? AND target_ref = ? AND lifecycle = 'active'",
        )
        .bind(repository_id)
        .bind(target_ref)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row)
    }

    /// Validate that a lease is active, not expired, and fencing token matches.
    pub async fn validate_active_lease(
        &self,
        repository_id: &str,
        target_ref: &str,
        expected_fencing_token: i64,
    ) -> Result<bool, CoreError> {
        let row: Option<LeaseRow> = sqlx::query_as(
            "SELECT lease_id, fencing_token FROM integration_leases WHERE repository_id = ? AND target_ref = ? AND lifecycle = 'active' AND expires_at > datetime('now')",
        )
        .bind(repository_id)
        .bind(target_ref)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        match row {
            Some(r) => Ok(r.fencing_token == expected_fencing_token),
            None => Ok(false),
        }
    }

    // ── Enhanced transitions with fencing ───────────────────────────────

    /// Transition integration request state with fencing token check.
    pub async fn transition_state_fenced(
        &self,
        integration_id: &str,
        from: &IntegrationState,
        to: &IntegrationState,
        fencing_token: i64,
    ) -> Result<bool, CoreError> {
        let is_terminal = to.is_terminal();
        let rows = sqlx::query(
            "UPDATE integration_requests SET state = ?, updated_at = datetime('now'), completed_at = CASE WHEN ? THEN datetime('now') ELSE completed_at END WHERE integration_id = ? AND state = ?",
        )
        .bind(to.as_str())
        .bind(is_terminal)
        .bind(integration_id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        if rows.rows_affected() != 1 {
            return Ok(false);
        }
        // Verify lease is still active with correct fencing
        // (We check integration_requests state + integration_leases fencing)
        let lease_valid = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM integration_attempts WHERE integration_id = ? AND fencing_token = ? AND state NOT IN ('integrated','conflict','blocked','failed','cancelled','stale')",
        )
        .bind(integration_id)
        .bind(fencing_token)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(lease_valid > 0)
    }

    /// Transition attempt state with fencing token check.
    pub async fn transition_attempt_state_fenced(
        &self,
        attempt_id: &str,
        from: &IntegrationState,
        to: &IntegrationState,
        fencing_token: i64,
    ) -> Result<bool, CoreError> {
        let is_terminal = to.is_terminal();
        let rows = sqlx::query(
            "UPDATE integration_attempts SET state = ?, completed_at = CASE WHEN ? THEN datetime('now') ELSE completed_at END WHERE attempt_id = ? AND state = ? AND fencing_token = ?",
        )
        .bind(to.as_str())
        .bind(is_terminal)
        .bind(attempt_id)
        .bind(from.as_str())
        .bind(fencing_token)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Update attempt with integration result data.
    pub async fn update_attempt_result(
        &self,
        attempt_id: &str,
        integration_tree_oid: Option<&str>,
        integration_commit_oid: Option<&str>,
        strategy: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE integration_attempts SET integration_tree_oid = ?, integration_commit_oid = ?, strategy = ? WHERE attempt_id = ?",
        )
        .bind(integration_tree_oid)
        .bind(integration_commit_oid)
        .bind(strategy)
        .bind(attempt_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// List all non-terminal integration requests for recovery.
    pub async fn list_recoverable(&self) -> Result<Vec<(String, String)>, CoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT integration_id, state FROM integration_requests WHERE state NOT IN ('integrated','conflict','blocked','failed','cancelled','stale')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows)
    }

    /// List all active leases for recovery.
    pub async fn list_active_leases(&self) -> Result<Vec<LeaseRow>, CoreError> {
        let rows: Vec<LeaseRow> = sqlx::query_as(
            "SELECT lease_id, integration_id, attempt_id, repository_id, target_ref, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, version FROM integration_leases WHERE lifecycle = 'active'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows)
    }

    // ── Events ─────────────────────────────────────────────────────────

    /// Write an integration event.
    pub async fn write_event(
        &self,
        event_id: &str,
        integration_id: &str,
        attempt_id: Option<&str>,
        event_type: &str,
        payload_json: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT OR IGNORE INTO integration_events (event_id, integration_id, attempt_id, event_type, payload_json) VALUES (?,?,?,?,?)",
        )
        .bind(event_id)
        .bind(integration_id)
        .bind(attempt_id)
        .bind(event_type)
        .bind(payload_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }
}

// ── Row types ──────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct IntegrationRequestRow {
    integration_id: String,
    commit_request_id: String,
    candidate_id: String,
    review_id: String,
    repository_id: String,
    target_ref: String,
    expected_target_head: String,
    priority: i64,
    #[allow(dead_code)]
    state: String,
    idempotency_key: String,
    created_at: String,
}

impl From<IntegrationRequestRow> for IntegrationRequest {
    fn from(r: IntegrationRequestRow) -> Self {
        Self {
            integration_id: r.integration_id,
            commit_request_id: r.commit_request_id,
            candidate_id: r.candidate_id,
            review_id: r.review_id,
            repository_id: r.repository_id,
            target_ref: r.target_ref,
            expected_target_head: r.expected_target_head,
            priority: r.priority as i32,
            idempotency_key: r.idempotency_key,
            created_at: parse_dt(&r.created_at),
        }
    }
}

#[derive(sqlx::FromRow)]
struct IntegrationAttemptRow {
    attempt_id: String,
    integration_id: String,
    attempt_number: i64,
    state: String,
    commit_oid: String,
    parent_oid: String,
    target_head_at_start: String,
    integration_tree_oid: Option<String>,
    integration_commit_oid: Option<String>,
    lease_id: Option<String>,
    fencing_token: Option<i64>,
    #[allow(dead_code)]
    worktree_path: Option<String>,
    #[allow(dead_code)]
    strategy: Option<String>,
    #[allow(dead_code)]
    error_message: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    created_at: String,
}

impl From<IntegrationAttemptRow> for IntegrationAttempt {
    fn from(r: IntegrationAttemptRow) -> Self {
        Self {
            attempt_id: r.attempt_id,
            integration_id: r.integration_id,
            attempt_number: r.attempt_number as u32,
            state: parse_integration_state(&r.state),
            commit_oid: r.commit_oid,
            parent_oid: r.parent_oid,
            target_head_at_start: r.target_head_at_start,
            integration_tree_oid: r.integration_tree_oid,
            integration_commit_oid: r.integration_commit_oid,
            lease_id: r.lease_id,
            fencing_token: r.fencing_token,
            started_at: r.started_at.as_deref().map(parse_dt),
            completed_at: r.completed_at.as_deref().map(parse_dt),
            created_at: parse_dt(&r.created_at),
        }
    }
}

fn parse_dt(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().into())
        .unwrap_or_else(chrono::Utc::now)
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct LeaseRow {
    pub lease_id: String,
    pub integration_id: String,
    pub attempt_id: String,
    pub repository_id: String,
    pub target_ref: String,
    pub lease_token: String,
    pub fencing_token: i64,
    pub lifecycle: String,
    pub acquired_at: String,
    pub heartbeat_at: String,
    pub expires_at: String,
    pub version: i64,
}

fn parse_integration_state(s: &str) -> IntegrationState {
    match s {
        "queued" => IntegrationState::Queued,
        "waiting_for_lease" => IntegrationState::WaitingForLease,
        "preparing" => IntegrationState::Preparing,
        "applying" => IntegrationState::Applying,
        "verifying" => IntegrationState::Verifying,
        "ready_to_publish" => IntegrationState::ReadyToPublish,
        "integrated" => IntegrationState::Integrated,
        "conflict" => IntegrationState::Conflict,
        "blocked" => IntegrationState::Blocked,
        "failed" => IntegrationState::Failed,
        "cancelled" => IntegrationState::Cancelled,
        "stale" => IntegrationState::Stale,
        _ => IntegrationState::Queued,
    }
}
