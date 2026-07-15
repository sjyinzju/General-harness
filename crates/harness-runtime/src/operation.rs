//! Operation/Saga with claim ownership — prevents concurrent reconciler execution.

use harness_core::contracts::repository::OperationRecord;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct OperationManager {
    pool: SqlitePool,
}

impl OperationManager {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn begin(
        &self,
        task_id: &str,
        op_type: &str,
        payload: &serde_json::Value,
        idempotency_key: &str,
    ) -> Result<String, CoreError> {
        let id = Uuid::new_v4().to_string();
        let op_id = format!("op-{id}");
        let now = now_sql();
        sqlx::query("INSERT INTO operations (id, operation_id, operation_type, task_id, status, payload_json, idempotency_key, started_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(&op_id).bind(op_type).bind(task_id).bind("pending").bind(payload.to_string()).bind(idempotency_key).bind(&now).bind(&now)
            .execute(&self.pool).await.map_err(db_err)?;
        Ok(op_id)
    }

    // ── Claim API ─────────────────────────────────

    pub async fn try_claim_operation(
        &self,
        operation_id: &str,
        lease_secs: u32,
    ) -> Result<Option<String>, CoreError> {
        let token = Uuid::new_v4().to_string();
        let now = now_sql();
        let expires = expires_sql(lease_secs);
        let claimed_by = format!("reconciler-{}", Uuid::new_v4());

        let result = sqlx::query(
            "UPDATE operations SET claimed_by=?, claim_token=?, claim_expires_at=?, attempt_count=attempt_count+1, updated_at=? WHERE operation_id=? AND status IN ('pending','running','reconciliation_required') AND (claim_token IS NULL OR claim_expires_at < ?)"
        ).bind(&claimed_by).bind(&token).bind(&expires).bind(&now).bind(operation_id).bind(&now)
        .execute(&self.pool).await.map_err(db_err)?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }
        Ok(Some(token))
    }

    pub async fn renew_operation_claim(
        &self,
        operation_id: &str,
        token: &str,
        lease_secs: u32,
    ) -> Result<(), CoreError> {
        let expires = expires_sql(lease_secs);
        let r = sqlx::query("UPDATE operations SET claim_expires_at=?, updated_at=? WHERE operation_id=? AND claim_token=? AND status IN ('pending','running','reconciliation_required')")
            .bind(&expires).bind(now_sql()).bind(operation_id).bind(token).execute(&self.pool).await.map_err(db_err)?;
        if r.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                "operation_claim_conflict",
                ErrorSource::System,
            ));
        }
        Ok(())
    }

    pub async fn release_operation_claim(
        &self,
        operation_id: &str,
        token: &str,
    ) -> Result<(), CoreError> {
        sqlx::query("UPDATE operations SET claim_token=NULL, claim_expires_at=NULL, updated_at=? WHERE operation_id=? AND claim_token=?")
            .bind(now_sql()).bind(operation_id).bind(token).execute(&self.pool).await.map_err(db_err)?;
        Ok(())
    }

    pub async fn complete_claimed_operation(
        &self,
        operation_id: &str,
        token: &str,
        result: &serde_json::Value,
    ) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE operations SET status='completed', result_json=?, claim_token=NULL, claim_expires_at=NULL, completed_at=?, updated_at=? WHERE operation_id=? AND claim_token=? AND status IN ('pending','running','reconciliation_required')")
            .bind(result.to_string()).bind(now_sql()).bind(now_sql()).bind(operation_id).bind(token).execute(&self.pool).await.map_err(db_err)?;
        if r.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                "operation_claim_conflict: token mismatch or already terminal",
                ErrorSource::System,
            ));
        }
        Ok(())
    }

    pub async fn fail_claimed_operation(
        &self,
        operation_id: &str,
        token: &str,
        reason: &str,
    ) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE operations SET status='failed', result_json=?, claim_token=NULL, claim_expires_at=NULL, last_error=?, completed_at=?, updated_at=? WHERE operation_id=? AND claim_token=? AND status IN ('pending','running','reconciliation_required')")
            .bind(reason).bind(reason).bind(now_sql()).bind(now_sql()).bind(operation_id).bind(token).execute(&self.pool).await.map_err(db_err)?;
        if r.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                "operation_claim_conflict",
                ErrorSource::System,
            ));
        }
        Ok(())
    }

    pub async fn mark_reconciliation_required(&self, operation_id: &str) -> Result<(), CoreError> {
        sqlx::query("UPDATE operations SET status='reconciliation_required', updated_at=? WHERE operation_id=? AND status IN ('pending','running')")
            .bind(now_sql()).bind(operation_id).execute(&self.pool).await.map_err(db_err)?;
        Ok(())
    }

    // ── Legacy ────────────────────────────────────

    pub async fn complete(
        &self,
        operation_id: &str,
        result: &serde_json::Value,
    ) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE operations SET status='completed', result_json=?, completed_at=?, updated_at=? WHERE operation_id=? AND status IN ('pending','running')")
            .bind(result.to_string()).bind(now_sql()).bind(now_sql()).bind(operation_id).execute(&self.pool).await.map_err(db_err)?;
        if r.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::PersistenceError,
                "operation not found or already terminal",
                ErrorSource::System,
            ));
        }
        Ok(())
    }

    pub async fn fail(&self, operation_id: &str, reason: &str) -> Result<(), CoreError> {
        sqlx::query("UPDATE operations SET status='failed', result_json=?, last_error=?, completed_at=?, updated_at=? WHERE operation_id=? AND status IN ('pending','running')")
            .bind(reason).bind(reason).bind(now_sql()).bind(now_sql()).bind(operation_id).execute(&self.pool).await.map_err(db_err)?;
        Ok(())
    }

    pub async fn find_stale(
        &self,
        older_than_secs: u32,
    ) -> Result<Vec<OperationRecord>, CoreError> {
        let rows: Vec<crate::repo::op_row::OpRow> = sqlx::query_as(
            "SELECT id, operation_id, operation_type, task_id, status, payload_json, result_json, idempotency_key, started_at, completed_at FROM operations WHERE status IN ('pending','running','reconciliation_required') AND started_at < datetime('now', ?)"
        ).bind(format!("-{older_than_secs} seconds")).fetch_all(&self.pool).await.map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|r| OperationRecord {
                id: r.id,
                operation_id: r.operation_id,
                operation_type: r.operation_type,
                task_id: r.task_id,
                status: r.status,
                payload_json: r.payload_json,
                result_json: r.result_json,
                version: 1,
                idempotency_key: r.idempotency_key,
                started_at: r.started_at,
                completed_at: r.completed_at,
            })
            .collect())
    }
}

fn now_sql() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}
fn expires_sql(secs: u32) -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(secs as i64))
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
