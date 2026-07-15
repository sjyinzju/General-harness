//! Operation/Saga — two-phase external effect management.

use harness_core::contracts::repository::OperationRecord;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::repo::op_row;

pub struct OperationManager {
    pool: SqlitePool,
}

impl OperationManager {
    pub fn new(pool: SqlitePool) -> Self { Self { pool } }

    /// Phase 1: Record operation intent. Returns operation_id.
    pub async fn begin(&self, task_id: &str, op_type: &str, payload: &serde_json::Value, idempotency_key: &str) -> Result<String, CoreError> {
        let id = Uuid::new_v4().to_string();
        let op_id = format!("op-{id}");
        sqlx::query("INSERT INTO operations (id, operation_id, operation_type, task_id, status, payload_json, idempotency_key) VALUES (?,?,?,?,?,?,?)")
            .bind(&id).bind(&op_id).bind(op_type).bind(task_id).bind("pending").bind(&payload.to_string()).bind(idempotency_key)
            .execute(&self.pool).await.map_err(db_err)?;
        Ok(op_id)
    }

    /// Phase 3: Record success.
    pub async fn complete(&self, operation_id: &str, result: &serde_json::Value) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE operations SET status = 'completed', result_json = ?, completed_at = datetime('now') WHERE operation_id = ? AND status IN ('pending','running')")
            .bind(&result.to_string()).bind(operation_id).execute(&self.pool).await.map_err(db_err)?;
        if r.rows_affected() == 0 {
            return Err(CoreError::new(ErrorCode::PersistenceError, "operation not found or already terminal", ErrorSource::System));
        }
        Ok(())
    }

    /// Record failure.
    pub async fn fail(&self, operation_id: &str, reason: &str) -> Result<(), CoreError> {
        sqlx::query("UPDATE operations SET status = 'failed', result_json = ?, completed_at = datetime('now') WHERE operation_id = ? AND status IN ('pending','running')")
            .bind(reason).bind(operation_id).execute(&self.pool).await.map_err(db_err)?;
        Ok(())
    }

    /// Find stale operations for reconciliation (older than N seconds, still pending/running).
    pub async fn find_stale(&self, older_than_secs: u32) -> Result<Vec<OperationRecord>, CoreError> {
        let rows: Vec<op_row::OpRow> = sqlx::query_as(
            "SELECT id, operation_id, operation_type, task_id, status, payload_json, result_json, idempotency_key, started_at, completed_at FROM operations WHERE status IN ('pending','running') AND started_at < datetime('now', ?)"
        ).bind(format!("-{older_than_secs} seconds")).fetch_all(&self.pool).await.map_err(db_err)?;
        Ok(rows.into_iter().map(|r| OperationRecord {
            id: r.id, operation_id: r.operation_id, operation_type: r.operation_type,
            task_id: r.task_id, status: r.status, payload_json: r.payload_json,
            result_json: r.result_json, version: 1, idempotency_key: r.idempotency_key,
            started_at: r.started_at, completed_at: r.completed_at,
        }).collect())
    }
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System)
}
