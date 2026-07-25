//! Supervisor persistence — database operations for supervisor instances,
//! leases, and events.

use chrono::{DateTime, Utc};
use harness_core::contracts::supervisor::{
    SupervisorEvent, SupervisorInstance, SupervisorInstanceId, SupervisorState,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Repository for supervisor persistence operations.
#[derive(Clone)]
pub struct SupervisorRepo {
    pool: SqlitePool,
}

impl SupervisorRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get the database pool (for use in transactions by other modules).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // ── Instance operations ────────────────────────────────────────────

    /// Insert a new supervisor instance record.
    pub async fn insert_instance(&self, instance: &SupervisorInstance) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO supervisor_instances
               (instance_id, state_directory_id, pid, process_started_at, boot_nonce,
                state, fencing_token, started_at, heartbeat_at, lease_expires_at,
                protocol_version, binary_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&instance.instance_id.0)
        .bind(&instance.state_directory_id)
        .bind(instance.pid as i64)
        .bind(format_time(&instance.process_started_at))
        .bind(&instance.boot_nonce)
        .bind(state_str(instance.state))
        .bind(instance.fencing_token)
        .bind(format_time(&instance.started_at))
        .bind(format_time(&instance.heartbeat_at))
        .bind(format_time(&instance.lease_expires_at))
        .bind(&instance.protocol_version)
        .bind(&instance.binary_version)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("insert supervisor instance: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(())
    }

    /// Get a supervisor instance by ID.
    pub async fn get_instance(
        &self,
        instance_id: &SupervisorInstanceId,
    ) -> Result<Option<SupervisorInstance>, CoreError> {
        let row = sqlx::query_as::<_, SupervisorInstanceRow>(
            r#"SELECT instance_id, state_directory_id, pid, process_started_at, boot_nonce,
                      state, fencing_token, started_at, heartbeat_at, lease_expires_at,
                      protocol_version, binary_version
               FROM supervisor_instances WHERE instance_id = ?"#,
        )
        .bind(&instance_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get supervisor instance: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(|r| r.into()))
    }

    /// Get the active (non-terminal) instance for a state directory.
    pub async fn get_active_instance_for_dir(
        &self,
        state_directory_id: &str,
    ) -> Result<Option<SupervisorInstance>, CoreError> {
        let row = sqlx::query_as::<_, SupervisorInstanceRow>(
            r#"SELECT instance_id, state_directory_id, pid, process_started_at, boot_nonce,
                      state, fencing_token, started_at, heartbeat_at, lease_expires_at,
                      protocol_version, binary_version
               FROM supervisor_instances
               WHERE state_directory_id = ?
                 AND state NOT IN ('stopped', 'failed')
               ORDER BY started_at DESC LIMIT 1"#,
        )
        .bind(state_directory_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get active instance for dir: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(|r| r.into()))
    }

    /// Update the instance state without appending an event.
    /// Used for transitions that don't need events (e.g., Starting, Recovering).
    pub async fn update_state_no_event(
        &self,
        instance_id: &SupervisorInstanceId,
        new_state: SupervisorState,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"UPDATE supervisor_instances
               SET state = ?, updated_at = datetime('now')
               WHERE instance_id = ?"#,
        )
        .bind(state_str(new_state))
        .bind(&instance_id.0)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("update supervisor state (no event): {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Update the instance state and append an event in the same transaction.
    pub async fn update_state_and_append_event(
        &self,
        instance_id: &SupervisorInstanceId,
        new_state: SupervisorState,
        event: &SupervisorEvent,
    ) -> Result<(), CoreError> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("begin transaction: {e}"),
                ErrorSource::System,
            )
        })?;

        let event_type = event_type_str(event);
        let payload_json = serde_json::to_string(event).map_err(|e| {
            CoreError::new(
                ErrorCode::SerializationError,
                format!("serialize supervisor event: {e}"),
                ErrorSource::Harness,
            )
        })?;

        // Update instance state
        sqlx::query(
            r#"UPDATE supervisor_instances
               SET state = ?, updated_at = datetime('now')
               WHERE instance_id = ?"#,
        )
        .bind(state_str(new_state))
        .bind(&instance_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("update supervisor state: {e}"),
                ErrorSource::System,
            )
        })?;

        // Get next sequence number
        let seq: (i64,) = sqlx::query_as(
            r#"SELECT COALESCE(MAX(sequence_num), 0) + 1
               FROM supervisor_events WHERE instance_id = ?"#,
        )
        .bind(&instance_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get next sequence: {e}"),
                ErrorSource::System,
            )
        })?;

        // Append event
        sqlx::query(
            r#"INSERT INTO supervisor_events
               (instance_id, event_type, payload_json, occurred_at, sequence_num)
               VALUES (?, ?, ?, datetime('now'), ?)"#,
        )
        .bind(&instance_id.0)
        .bind(event_type)
        .bind(&payload_json)
        .bind(seq.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("append supervisor event: {e}"),
                ErrorSource::System,
            )
        })?;

        tx.commit().await.map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("commit transaction: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(())
    }

    /// Append a supervisor event (without state update).
    pub async fn append_event(
        &self,
        instance_id: &SupervisorInstanceId,
        event_type: &str,
        payload_json: &str,
    ) -> Result<(), CoreError> {
        let seq: (i64,) = sqlx::query_as(
            r#"SELECT COALESCE(MAX(sequence_num), 0) + 1
               FROM supervisor_events WHERE instance_id = ?"#,
        )
        .bind(&instance_id.0)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get next sequence: {e}"),
                ErrorSource::System,
            )
        })?;

        sqlx::query(
            r#"INSERT INTO supervisor_events
               (instance_id, event_type, payload_json, occurred_at, sequence_num)
               VALUES (?, ?, ?, datetime('now'), ?)"#,
        )
        .bind(&instance_id.0)
        .bind(event_type)
        .bind(payload_json)
        .bind(seq.0)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("append supervisor event: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(())
    }

    /// Update the fencing token for an instance.
    pub async fn update_fencing_token(
        &self,
        instance_id: &SupervisorInstanceId,
        new_token: i64,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"UPDATE supervisor_instances
               SET fencing_token = ?, updated_at = datetime('now')
               WHERE instance_id = ?"#,
        )
        .bind(new_token)
        .bind(&instance_id.0)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("update fencing token: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(())
    }

    /// Update heartbeat timestamp and lease expiry.
    /// Uses CAS: only updates if fencing_token matches expected value.
    /// Returns the number of rows updated (0 = CAS failure).
    pub async fn heartbeat_cas(
        &self,
        instance_id: &SupervisorInstanceId,
        expected_fencing_token: i64,
        new_lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, CoreError> {
        let result = sqlx::query(
            r#"UPDATE supervisor_instances
               SET heartbeat_at = datetime('now'),
                   lease_expires_at = ?,
                   updated_at = datetime('now')
               WHERE instance_id = ?
                 AND fencing_token = ?
                 AND state IN ('ready', 'recovering', 'draining')"#,
        )
        .bind(format_time(&new_lease_expires_at))
        .bind(&instance_id.0)
        .bind(expected_fencing_token)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("heartbeat cas: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(result.rows_affected() > 0)
    }

    // ── Lease operations ───────────────────────────────────────────────

    /// Acquire a supervisor lease. Uses UNIQUE partial index to ensure
    /// only one active lease per state_directory_id.
    /// Uses INSERT OR IGNORE pattern: if the partial unique index fires,
    /// the insert is silently skipped.
    pub async fn acquire_lease(
        &self,
        instance_id: &SupervisorInstanceId,
        state_directory_id: &str,
        fencing_token: i64,
        expires_at: DateTime<Utc>,
    ) -> Result<(), CoreError> {
        // Check if an active lease already exists first (the partial index
        // makes INSERT fail, so we check-before-insert for clear error handling).
        // But the UNIQUE partial index is the real guard.
        sqlx::query(
            r#"INSERT INTO supervisor_leases
               (state_directory_id, instance_id, fencing_token, acquired_at, expires_at, is_active)
               VALUES (?, ?, ?, datetime('now'), ?, 1)"#,
        )
        .bind(state_directory_id)
        .bind(&instance_id.0)
        .bind(fencing_token)
        .bind(format_time(&expires_at))
        .execute(&self.pool)
        .await
        .map_err(|e| {
            // If the partial unique index fires, translate to a Conflict error
            let msg = e.to_string();
            if msg.contains("UNIQUE constraint failed")
                && msg.contains("idx_supervisor_lease_one_active")
            {
                CoreError::new(
                    ErrorCode::Conflict,
                    format!(
                        "active lease already exists for state directory '{state_directory_id}'"
                    ),
                    ErrorSource::Harness,
                )
            } else if msg.contains("UNIQUE constraint failed") {
                CoreError::new(
                    ErrorCode::Conflict,
                    format!("lease conflict for state directory '{state_directory_id}': {msg}"),
                    ErrorSource::Harness,
                )
            } else {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("acquire supervisor lease: {e}"),
                    ErrorSource::System,
                )
            }
        })?;

        Ok(())
    }

    /// Get the current active lease for a state directory.
    pub async fn get_active_lease(
        &self,
        state_directory_id: &str,
    ) -> Result<Option<SupervisorLeaseRow>, CoreError> {
        let row = sqlx::query_as::<_, SupervisorLeaseRow>(
            r#"SELECT id, state_directory_id, instance_id, fencing_token, acquired_at,
                      expires_at, is_active
               FROM supervisor_leases
               WHERE state_directory_id = ? AND is_active = 1"#,
        )
        .bind(state_directory_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get active lease: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row)
    }

    /// Release (deactivate) a supervisor lease. Uses CAS: only releases
    /// if fencing_token and instance_id match.
    pub async fn release_lease_cas(
        &self,
        instance_id: &SupervisorInstanceId,
        expected_fencing_token: i64,
    ) -> Result<bool, CoreError> {
        let result = sqlx::query(
            r#"UPDATE supervisor_leases
               SET is_active = 0, updated_at = datetime('now')
               WHERE instance_id = ?
                 AND fencing_token = ?
                 AND is_active = 1"#,
        )
        .bind(&instance_id.0)
        .bind(expected_fencing_token)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("release lease cas: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(result.rows_affected() > 0)
    }

    /// Force-deactivate a stale lease (used during takeover).
    /// No CAS — this is only called after verifying the owner is dead.
    pub async fn force_deactivate_lease(
        &self,
        state_directory_id: &str,
    ) -> Result<Option<SupervisorLeaseRow>, CoreError> {
        // Read the old lease before deactivating
        let old_lease = self.get_active_lease(state_directory_id).await?;

        sqlx::query(
            r#"UPDATE supervisor_leases
               SET is_active = 0, updated_at = datetime('now')
               WHERE state_directory_id = ? AND is_active = 1"#,
        )
        .bind(state_directory_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("force deactivate lease: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(old_lease)
    }

    /// Check if a write operation with the given fencing token is allowed.
    /// Each table write should validate that the current active lease
    /// has a fencing token >= the writer's token.
    pub async fn validate_fencing_for_write(
        &self,
        state_directory_id: &str,
        writer_fencing_token: i64,
    ) -> Result<bool, CoreError> {
        let row = sqlx::query_as::<_, (i64,)>(
            r#"SELECT fencing_token FROM supervisor_leases
               WHERE state_directory_id = ? AND is_active = 1"#,
        )
        .bind(state_directory_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("validate fencing: {e}"),
                ErrorSource::System,
            )
        })?;

        match row {
            Some((current_token,)) => Ok(writer_fencing_token >= current_token),
            None => Ok(false), // No active lease — no writes allowed
        }
    }
}

// ── Row types ────────────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow)]
struct SupervisorInstanceRow {
    instance_id: String,
    state_directory_id: String,
    pid: i64,
    process_started_at: String,
    boot_nonce: String,
    state: String,
    fencing_token: i64,
    started_at: String,
    heartbeat_at: String,
    lease_expires_at: String,
    protocol_version: String,
    binary_version: String,
}

impl From<SupervisorInstanceRow> for SupervisorInstance {
    fn from(r: SupervisorInstanceRow) -> Self {
        SupervisorInstance {
            instance_id: SupervisorInstanceId(r.instance_id),
            state_directory_id: r.state_directory_id,
            pid: r.pid as u32,
            process_started_at: parse_time(&r.process_started_at),
            boot_nonce: r.boot_nonce,
            state: parse_state(&r.state),
            fencing_token: r.fencing_token,
            started_at: parse_time(&r.started_at),
            heartbeat_at: parse_time(&r.heartbeat_at),
            lease_expires_at: parse_time(&r.lease_expires_at),
            protocol_version: r.protocol_version,
            binary_version: r.binary_version,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct SupervisorLeaseRow {
    pub id: i64,
    pub state_directory_id: String,
    pub instance_id: String,
    pub fencing_token: i64,
    pub acquired_at: String,
    pub expires_at: String,
    pub is_active: i64,
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn format_time(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn parse_time(s: &str) -> DateTime<Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3fZ")
        .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        .or_else(|_| chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.with_timezone(&Utc)))
        .unwrap_or_else(|_| Utc::now())
}

fn state_str(state: SupervisorState) -> &'static str {
    match state {
        SupervisorState::Created => "created",
        SupervisorState::Starting => "starting",
        SupervisorState::AcquiringOwnership => "acquiring_ownership",
        SupervisorState::Recovering => "recovering",
        SupervisorState::Ready => "ready",
        SupervisorState::Draining => "draining",
        SupervisorState::Stopping => "stopping",
        SupervisorState::Stopped => "stopped",
        SupervisorState::Failed => "failed",
        SupervisorState::TakingOver => "taking_over",
    }
}

fn parse_state(s: &str) -> SupervisorState {
    match s {
        "created" => SupervisorState::Created,
        "starting" => SupervisorState::Starting,
        "acquiring_ownership" => SupervisorState::AcquiringOwnership,
        "recovering" => SupervisorState::Recovering,
        "ready" => SupervisorState::Ready,
        "draining" => SupervisorState::Draining,
        "stopping" => SupervisorState::Stopping,
        "stopped" => SupervisorState::Stopped,
        "failed" => SupervisorState::Failed,
        "taking_over" => SupervisorState::TakingOver,
        _ => SupervisorState::Created,
    }
}

fn event_type_str(event: &SupervisorEvent) -> &'static str {
    match event {
        SupervisorEvent::SupervisorStarting { .. } => "supervisor_starting",
        SupervisorEvent::SupervisorOwnershipAcquired { .. } => "supervisor_ownership_acquired",
        SupervisorEvent::SupervisorOwnershipRejected { .. } => "supervisor_ownership_rejected",
        SupervisorEvent::SupervisorHeartbeat { .. } => "supervisor_heartbeat",
        SupervisorEvent::SupervisorReady { .. } => "supervisor_ready",
        SupervisorEvent::SupervisorDraining { .. } => "supervisor_draining",
        SupervisorEvent::SupervisorStopping { .. } => "supervisor_stopping",
        SupervisorEvent::SupervisorStopped { .. } => "supervisor_stopped",
        SupervisorEvent::SupervisorLeaseLost { .. } => "supervisor_lease_lost",
        SupervisorEvent::SupervisorStaleOwnerDetected { .. } => "supervisor_stale_owner_detected",
        SupervisorEvent::SupervisorTakeoverStarted { .. } => "supervisor_takeover_started",
        SupervisorEvent::SupervisorTakeoverCompleted { .. } => "supervisor_takeover_completed",
        SupervisorEvent::SupervisorFailed { .. } => "supervisor_failed",
    }
}
