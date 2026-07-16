//! Lease transition — state + DomainEvent in one SQLite transaction.
//! Uses the Gate C frozen `LeaseLifecycle` + `LeaseFsm`.
//!
//! High-frequency heartbeats update state + version but only sample event
//! writes (every N heartbeats or every M seconds) to bound the event log
//! volume. Acquire / release / expire always write an audit event.

use harness_core::contracts::workspace::LeaseLifecycle;
use harness_core::state_machine::lease_fsm::LeaseFsm;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::idempotency;

pub struct LeaseTransitionService {
    pool: SqlitePool,
}

impl LeaseTransitionService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Transition lifecycle state + append event. High-frequency heartbeats
    /// may pass `write_event: false` to skip the event log insert.
    pub async fn transition_lease(
        &self,
        lease_id: &str,
        from: &LeaseLifecycle,
        to: &LeaseLifecycle,
        idempotency_key: &str,
        write_event: bool,
    ) -> Result<(), CoreError> {
        if idempotency::is_duplicate(&self.pool, idempotency_key).await? {
            return Ok(());
        }
        if from.is_terminal() {
            return Err(ls_err(format!(
                "terminal lease cannot transition: {lease_id} {from:?} -> {to:?}"
            )));
        }
        if !LeaseFsm::can_transition(from, to) {
            return Err(ls_err(format!(
                "illegal lease transition: {lease_id} {from:?} -> {to:?}"
            )));
        }
        let from_s = serde_json::to_string(from)
            .unwrap()
            .trim_matches('"')
            .to_string();
        let to_s = serde_json::to_string(to)
            .unwrap()
            .trim_matches('"')
            .to_string();

        // Read current lifecycle + version.
        let (current_lc, version): (String, i64) =
            sqlx::query_as("SELECT lifecycle, version FROM workspace_leases WHERE id = ?")
                .bind(lease_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?
                .ok_or_else(|| ls_err(format!("lease not found: {lease_id}")))?;

        // Idempotent: if already at the target state, record the
        // idempotency key and (when requested) write the event, then
        // return Ok. This handles the case where the lifecycle was
        // updated by a direct UPDATE and transition_lease is called
        // solely to write the audit event.
        if current_lc == to_s {
            let mut tx = self.pool.begin().await.map_err(db_err)?;
            if write_event {
                let event_id = Uuid::new_v4().to_string();
                let cid = Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,'workspace_lease_lifecycle_changed',?,1,?,?,'harness')",
                )
                .bind(&event_id)
                .bind(lease_id)
                .bind(version + 1)
                .bind(serde_json::json!({"from":from_s,"to":to_s}).to_string())
                .bind(&cid)
                .bind(idempotency_key)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
            }
            idempotency::record_in_tx(&mut tx, idempotency_key, "ok").await?;
            tx.commit().await.map_err(db_err)?;
            return Ok(());
        }
        if current_lc != from_s {
            return Err(ls_err(format!(
                "lease lifecycle mismatch: expected {from_s}, got {current_lc}"
            )));
        }

        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let aff = sqlx::query(
            "UPDATE workspace_leases SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND lifecycle = ? AND version = ?",
        )
        .bind(&to_s)
        .bind(lease_id)
        .bind(&from_s)
        .bind(version)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        if aff.rows_affected() == 0 {
            return Err(ls_err(format!(
                "lease transition optimistic conflict: {lease_id} version={version}"
            )));
        }

        if write_event {
            let event_id = Uuid::new_v4().to_string();
            let cid = Uuid::new_v4().to_string();
            let sv = version + 1;
            sqlx::query(
                "INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?)",
            )
            .bind(&event_id)
            .bind(lease_id)
            .bind(sv)
            .bind("workspace_lease_lifecycle_changed")
            .bind(serde_json::json!({"from":from_s,"to":to_s}).to_string())
            .bind(1i64)
            .bind(&cid)
            .bind(idempotency_key)
            .bind("harness")
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        }

        idempotency::record_in_tx(&mut tx, idempotency_key, "ok").await?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }
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
