//! TransitionService — atomic lifecycle transitions with event logging.

use harness_core::contracts::project::ProjectLifecycle;
use harness_core::contracts::task::TaskLifecycle;
use harness_core::state_machine::execution_fsm::ExecutionFsm;
use harness_core::state_machine::project_fsm::ProjectFsm;
use harness_core::state_machine::task_fsm::TaskFsm;
use harness_core::state_machine::ExecutionLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::idempotency;

pub struct TransitionService {
    pool: SqlitePool,
}

impl TransitionService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Transition a Project lifecycle. Validates, updates, appends event — all in one transaction.
    pub async fn transition_project(
        &self,
        project_id: &str,
        from: &ProjectLifecycle,
        to: &ProjectLifecycle,
        idempotency_key: &str,
    ) -> Result<(), CoreError> {
        // Pre-check idempotency
        if idempotency::is_duplicate(&self.pool, idempotency_key).await? {
            return Ok(());
        }

        // Validate
        if !ProjectFsm::can_transition(from, to) {
            return Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: format!("{from:?}"),
                    to: format!("{to:?}"),
                },
                "illegal project transition",
                ErrorSource::System,
            ));
        }

        let from_str = serde_json::to_string(from)
            .unwrap()
            .trim_matches('"')
            .to_string();
        let to_str = serde_json::to_string(to)
            .unwrap()
            .trim_matches('"')
            .to_string();

        // Read current version
        let (current_lifecycle, version): (String, i64) =
            sqlx::query_as("SELECT lifecycle, version FROM projects WHERE id = ?")
                .bind(project_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?
                .ok_or_else(|| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        "project not found",
                        ErrorSource::System,
                    )
                })?;

        if current_lifecycle != from_str {
            return Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: current_lifecycle,
                    to: to_str,
                },
                "expected different current lifecycle",
                ErrorSource::System,
            ));
        }

        let event_id = Uuid::new_v4().to_string();
        let correlation_id = Uuid::new_v4().to_string();
        let stream_version = version + 1;

        // Transaction: update state + append event + record idempotency
        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let affected = sqlx::query(
            "UPDATE projects SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND lifecycle = ? AND version = ?",
        )
        .bind(&to_str)
        .bind(project_id)
        .bind(&from_str)
        .bind(version)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        if affected.rows_affected() == 0 {
            // Distinguish: lifecycle changed vs version conflict
            let (actual_lc, actual_ver): (String, i64) =
                sqlx::query_as("SELECT lifecycle, version FROM projects WHERE id = ?")
                    .bind(project_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db_err)?
                    .map(|r: (String, i64)| r)
                    .unwrap_or_else(|| (String::new(), 0));
            if actual_lc != from_str {
                return Err(CoreError::new(
                    ErrorCode::InvalidStateTransition {
                        from: from_str,
                        to: to_str,
                    },
                    format!("lifecycle already changed to {actual_lc} by another transaction"),
                    ErrorSource::System,
                ));
            }
            // Same lifecycle, different version → optimistic lock conflict
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                format!("optimistic_version_conflict: expected={version}, actual={actual_ver}"),
                ErrorSource::System,
            )
            .with_diagnostic(format!("project:{project_id}:version_mismatch")));
        }

        sqlx::query(
            "INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(&event_id)
        .bind(project_id)
        .bind(stream_version)
        .bind("project_lifecycle_changed")
        .bind(&serde_json::json!({"from": from_str, "to": to_str}).to_string())
        .bind(1i64) // schema_version
        .bind(&correlation_id)
        .bind(idempotency_key)
        .bind("harness")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        idempotency::record_in_tx(&mut tx, idempotency_key, "ok").await?;
        tx.commit().await.map_err(db_err)?;

        Ok(())
    }

    /// Transition a Task lifecycle. Same atomic pattern.
    pub async fn transition_task(
        &self,
        task_id: &str,
        from: &TaskLifecycle,
        to: &TaskLifecycle,
        idempotency_key: &str,
    ) -> Result<(), CoreError> {
        if idempotency::is_duplicate(&self.pool, idempotency_key).await? {
            return Ok(());
        }

        if !TaskFsm::can_transition(from, to) {
            return Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: format!("{from:?}"),
                    to: format!("{to:?}"),
                },
                "illegal task transition",
                ErrorSource::System,
            ));
        }

        let from_s = serde_json::to_string(from)
            .unwrap()
            .trim_matches('"')
            .to_string();
        let to_s = serde_json::to_string(to)
            .unwrap()
            .trim_matches('"')
            .to_string();

        let (current_lc, version): (String, i64) =
            sqlx::query_as("SELECT lifecycle, version FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?
                .ok_or_else(|| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        "task not found",
                        ErrorSource::System,
                    )
                })?;

        if current_lc != from_s {
            return Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: current_lc,
                    to: to_s,
                },
                "expected different lifecycle",
                ErrorSource::System,
            ));
        }

        let event_id = Uuid::new_v4().to_string();
        let cid = Uuid::new_v4().to_string();
        let sv = version + 1;

        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let aff = sqlx::query("UPDATE tasks SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND lifecycle = ? AND version = ?")
            .bind(&to_s).bind(task_id).bind(&from_s).bind(version).execute(&mut *tx).await.map_err(db_err)?;

        if aff.rows_affected() == 0 {
            let (actual_lc, actual_ver): (String, i64) =
                sqlx::query_as("SELECT lifecycle, version FROM tasks WHERE id = ?")
                    .bind(task_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db_err)?
                    .map(|r: (String, i64)| r)
                    .unwrap_or_else(|| (String::new(), 0));
            if actual_lc != from_s {
                return Err(CoreError::new(
                    ErrorCode::InvalidStateTransition {
                        from: from_s,
                        to: to_s,
                    },
                    format!("lifecycle already changed to {actual_lc} by another transaction"),
                    ErrorSource::System,
                ));
            }
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                format!("optimistic_version_conflict: expected={version}, actual={actual_ver}"),
                ErrorSource::System,
            )
            .with_diagnostic(format!("task:{task_id}:version_mismatch")));
        }

        sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(&event_id).bind(task_id).bind(sv).bind("task_lifecycle_changed")
            .bind(&serde_json::json!({"from": from_s, "to": to_s}).to_string()).bind(1i64).bind(&cid).bind(idempotency_key).bind("harness")
            .execute(&mut *tx).await.map_err(db_err)?;

        idempotency::record_in_tx(&mut tx, idempotency_key, "ok").await?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    /// Transition an Execution lifecycle. Terminal execs cannot be modified.
    pub async fn transition_execution(
        &self,
        execution_id: &str,
        to: &ExecutionLifecycle,
        reason: Option<&str>,
        idempotency_key: &str,
    ) -> Result<(), CoreError> {
        if idempotency::is_duplicate(&self.pool, idempotency_key).await? {
            return Ok(());
        }

        let (current_lc, version): (String, i64) =
            sqlx::query_as("SELECT lifecycle, version FROM execution_attempts WHERE id = ?")
                .bind(execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?
                .ok_or_else(|| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        "execution not found",
                        ErrorSource::System,
                    )
                })?;

        let from = parse_exec_lc(&current_lc);

        if from.is_terminal() {
            return Err(CoreError::new(
                ErrorCode::EntityTerminal {
                    entity_id: execution_id.into(),
                },
                "terminal execution cannot be modified",
                ErrorSource::System,
            ));
        }

        if !ExecutionFsm::can_transition(&from, to) {
            return Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: format!("{from:?}"),
                    to: format!("{to:?}"),
                },
                "illegal execution transition",
                ErrorSource::System,
            ));
        }

        let to_s = serde_json::to_string(to)
            .unwrap()
            .trim_matches('"')
            .to_string();
        let event_id = Uuid::new_v4().to_string();
        let cid = Uuid::new_v4().to_string();
        let sv = version + 1;

        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let aff = sqlx::query("UPDATE execution_attempts SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND version = ?")
            .bind(&to_s).bind(execution_id).bind(version).execute(&mut *tx).await.map_err(db_err)?;

        if aff.rows_affected() == 0 {
            let (actual_lc, actual_ver): (String, i64) =
                sqlx::query_as("SELECT lifecycle, version FROM execution_attempts WHERE id = ?")
                    .bind(execution_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db_err)?
                    .map(|r: (String, i64)| r)
                    .unwrap_or_else(|| (String::new(), 0));
            if actual_lc != current_lc {
                return Err(CoreError::new(
                    ErrorCode::InvalidStateTransition {
                        from: current_lc,
                        to: to_s,
                    },
                    format!("lifecycle already changed to {actual_lc}"),
                    ErrorSource::System,
                ));
            }
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                format!("optimistic_version_conflict: expected={version}, actual={actual_ver}"),
                ErrorSource::System,
            )
            .with_diagnostic(format!("execution:{execution_id}:version_mismatch")));
        }

        sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(&event_id).bind(execution_id).bind(sv).bind("execution_lifecycle_changed")
            .bind(&serde_json::json!({"from": current_lc, "to": to_s, "reason": reason}).to_string()).bind(1i64).bind(&cid).bind(idempotency_key).bind("harness")
            .execute(&mut *tx).await.map_err(db_err)?;

        idempotency::record_in_tx(&mut tx, idempotency_key, "ok").await?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}

fn parse_exec_lc(s: &str) -> ExecutionLifecycle {
    serde_json::from_str(&format!("\"{s}\"")).unwrap_or(ExecutionLifecycle::Created)
}
