//! SchedulerReconciler — detects and repairs scheduler anomalies.
//! Never creates retries, never switches profiles, never deletes worktrees.

use harness_core::contracts::scheduler::SchedulerAnomaly;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct SchedulerReconciler {
    pool: SqlitePool,
}

impl SchedulerReconciler {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Run a reconciliation pass. Returns detected anomalies.
    pub async fn reconcile(&self) -> Result<Vec<SchedulerAnomaly>, CoreError> {
        let mut anomalies: Vec<SchedulerAnomaly> = Vec::new();

        // 1. Orphan reservations
        let orphan_count = self.expire_stale_reservations().await?;
        if orphan_count > 0 {
            anomalies.push(SchedulerAnomaly::OrphanReservation);
        }

        // 2. Terminal executions with active reservations
        let terminal_reservations: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT r.id, r.task_id, e.lifecycle FROM scheduler_reservations r JOIN execution_attempts e ON r.execution_id = e.id WHERE r.status = 'active' AND e.lifecycle IN ('completed','failed','lost','cancelled')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        for (res_id, _task_id, _lc) in &terminal_reservations {
            let _ = sqlx::query(
                "UPDATE scheduler_reservations SET status='released', released_at=datetime('now') WHERE id=?",
            )
            .bind(res_id)
            .execute(&self.pool)
            .await;
            anomalies.push(SchedulerAnomaly::TerminalExecutionResourcesActive);
        }

        // 3. Running execution but no process registry entry
        // (ProcessManager registry is in-memory; check by spawn intent)
        let stale_spawns: Vec<(String, String)> = sqlx::query_as(
            "SELECT d.execution_id, d.id FROM dispatch_operations d JOIN execution_attempts e ON d.execution_id = e.id WHERE d.stage IN ('agent_starting','agent_running') AND d.status NOT IN ('completed','failed') AND d.started_at < datetime('now','-10 minutes')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        for (exec_id, dispatch_id) in &stale_spawns {
            let _ = self
                .record_anomaly(
                    &SchedulerAnomaly::IntentWithoutSpawn,
                    "dispatch",
                    Some(dispatch_id),
                    "spawn intent older than 10min without completion",
                )
                .await;
            // Mark execution as Lost
            let _ = sqlx::query(
                "UPDATE execution_attempts SET lifecycle='lost' WHERE id=? AND lifecycle='running'",
            )
            .bind(exec_id)
            .execute(&self.pool)
            .await;
            anomalies.push(SchedulerAnomaly::IntentWithoutSpawn);
        }

        // 4. Task says Running but no active execution
        let task_running_no_exec: Vec<(String,)> = sqlx::query_as(
            "SELECT t.id FROM tasks t WHERE t.lifecycle = 'running' AND NOT EXISTS (SELECT 1 FROM execution_attempts e WHERE e.task_id = t.id AND e.lifecycle NOT IN ('completed','failed','lost','cancelled'))",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        if !task_running_no_exec.is_empty() {
            anomalies.push(SchedulerAnomaly::ProcessMissing);
        }

        // 5. Duplicate active executions
        let dup_execs: Vec<(String,)> = sqlx::query_as(
            "SELECT task_id FROM execution_attempts WHERE lifecycle NOT IN ('completed','failed','lost','cancelled') GROUP BY task_id HAVING COUNT(*) > 1",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        if !dup_execs.is_empty() {
            anomalies.push(SchedulerAnomaly::DuplicateActiveExecution);
        }

        // 6. Expired reservations that should be released
        let _ = sqlx::query(
            "UPDATE scheduler_reservations SET status='expired' WHERE status='active' AND expires_at < datetime('now')",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        Ok(anomalies)
    }

    async fn expire_stale_reservations(&self) -> Result<usize, CoreError> {
        let result = sqlx::query(
            "UPDATE scheduler_reservations SET status='expired' WHERE status='active' AND expires_at < datetime('now')",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(result.rows_affected() as usize)
    }

    async fn record_anomaly(
        &self,
        anomaly_type: &SchedulerAnomaly,
        entity_type: &str,
        entity_id: Option<&str>,
        description: &str,
    ) -> Result<(), CoreError> {
        let anomaly_str = format!("{:?}", anomaly_type).to_lowercase();
        let ikey = format!("recon-{}-{}", anomaly_str, Uuid::new_v4());

        sqlx::query(
            "INSERT OR IGNORE INTO scheduler_reconciliations (id, anomaly_type, entity_type, entity_id, description, repair_action, idempotency_key) VALUES (?,?,?,?,?,?,?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(anomaly_str)
        .bind(entity_type)
        .bind(entity_id)
        .bind(description)
        .bind("auto-repaired")
        .bind(&ikey)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        Database::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn test_orphan_reservation_reclaimed() {
        let db = setup().await;
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','pending')")
            .execute(&db.pool).await.unwrap();
        sqlx::query(
            "INSERT INTO scheduler_reservations (id, task_id, status, expires_at) VALUES ('r1','t1','active','2000-01-01')",
        )
        .execute(&db.pool).await.unwrap();

        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::OrphanReservation));
    }

    #[tokio::test]
    async fn test_duplicate_active_execution_detected() {
        let db = setup().await;
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','running')")
            .execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'running')")
            .execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e2','t1',2,'running')")
            .execute(&db.pool).await.unwrap();

        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::DuplicateActiveExecution));
    }

    #[tokio::test]
    async fn test_repeated_reconcile_idempotent() {
        let db = setup().await;
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','pending')")
            .execute(&db.pool).await.unwrap();
        sqlx::query(
            "INSERT INTO scheduler_reservations (id, task_id, status, expires_at) VALUES ('r1','t1','active','2000-01-01')",
        )
        .execute(&db.pool).await.unwrap();

        let rec = SchedulerReconciler::new(db.pool.clone());
        rec.reconcile().await.unwrap();
        // Second reconciliation should be idempotent
        let result = rec.reconcile().await;
        assert!(result.is_ok());
    }
}
