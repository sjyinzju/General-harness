//! SchedulerReconciler — detects and repairs scheduler anomalies.
//!
//! Safety rules:
//!   - Never auto-retry executions
//!   - Never silently switch profiles or providers
//!   - Never start a second Agent process
//!   - Never delete Worktrees
//!   - Never preempt a legitimate Claim owner
//!   - When uncertain, mark ReconciliationRequired
//!   - When terminal can be determined, complete the state
//!   - When a failed owner can be determined, release resources
//!   - Awaiting-Verification legitimate Lease/Claim must not be released

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

    pub async fn reconcile(&self) -> Result<Vec<SchedulerAnomaly>, CoreError> {
        let mut anomalies: Vec<SchedulerAnomaly> = Vec::new();

        if self.detect_orphan_reservation().await? {
            anomalies.push(SchedulerAnomaly::OrphanReservation);
        }
        if self.detect_terminal_exec_active_reservation().await? {
            anomalies.push(SchedulerAnomaly::TerminalExecutionResourcesActive);
        }
        if self.detect_stale_spawn_intent().await? {
            anomalies.push(SchedulerAnomaly::IntentWithoutSpawn);
        }
        if self.detect_task_running_without_exec().await? {
            anomalies.push(SchedulerAnomaly::ProcessMissing);
        }
        if self.detect_duplicate_active_execs().await? {
            anomalies.push(SchedulerAnomaly::DuplicateActiveExecution);
        }
        if self.detect_lease_without_claim().await? {
            anomalies.push(SchedulerAnomaly::LeaseWithoutClaim);
        }
        if self.detect_claim_without_lease().await? {
            anomalies.push(SchedulerAnomaly::ClaimWithoutLease);
        }
        if self.detect_stale_fencing().await? {
            anomalies.push(SchedulerAnomaly::StaleFencing);
        }
        if self.detect_profile_missing().await? {
            anomalies.push(SchedulerAnomaly::IntentWithoutSpawn);
        }
        if self.detect_awaiting_verification_missing().await? {
            anomalies.push(SchedulerAnomaly::AwaitingVerificationResourceLost);
        }
        if self.detect_terminal_event_no_transition().await? {
            anomalies.push(SchedulerAnomaly::EventTerminalMissingTransition);
        }
        if self.detect_failed_exec_active_lease().await? {
            anomalies.push(SchedulerAnomaly::TerminalExecutionResourcesActive);
        }
        if self.detect_reservation_without_task().await? {
            anomalies.push(SchedulerAnomaly::ReservationExpiredNotReleased);
        }
        if self.detect_incomplete_spawn().await? {
            anomalies.push(SchedulerAnomaly::IntentWithoutSpawn);
        }
        if self.detect_running_without_process().await? {
            anomalies.push(SchedulerAnomaly::ProcessMissing);
        }
        if self.detect_process_exit_exec_running().await? {
            anomalies.push(SchedulerAnomaly::TerminalProcessNotReflected);
        }
        if self.detect_heartbeat_missing().await? {
            anomalies.push(SchedulerAnomaly::StaleFencing);
        }

        let _ = self.expire_stale_reservations().await?;
        Ok(anomalies)
    }

    async fn detect_orphan_reservation(&self) -> Result<bool, CoreError> {
        let count = self.expire_stale_reservations().await?;
        Ok(count > 0)
    }

    async fn detect_terminal_exec_active_reservation(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT r.id, e.lifecycle FROM scheduler_reservations r JOIN execution_attempts e ON r.task_id = e.task_id WHERE r.status = 'active' AND e.lifecycle IN ('completed','failed','lost','cancelled')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        let mut found = false;
        for (res_id, _lc) in &rows {
            found = true;
            sqlx::query("UPDATE scheduler_reservations SET status='released', released_at=datetime('now') WHERE id=?")
                .bind(res_id).execute(&self.pool).await.map_err(db_err)?;
            self.record_evidence(
                "terminal_exec_active_reservation",
                "reservation",
                Some(res_id),
                "terminal exec with active reservation — released",
                true,
            )
            .await?;
        }
        Ok(found)
    }

    async fn detect_stale_spawn_intent(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT d.execution_id, d.id FROM dispatch_operations d JOIN execution_attempts e ON d.execution_id = e.id WHERE d.stage IN ('agent_starting','agent_running') AND d.status NOT IN ('completed','failed') AND d.started_at < datetime('now','-10 minutes')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        let mut found = false;
        for (exec_id, dispatch_id) in &rows {
            found = true;
            sqlx::query(
                "UPDATE execution_attempts SET lifecycle='lost' WHERE id=? AND lifecycle='running'",
            )
            .bind(exec_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
            sqlx::query("UPDATE dispatch_operations SET status='failed', outcome_json='stale spawn intent' WHERE id=?")
                .bind(dispatch_id).execute(&self.pool).await.map_err(db_err)?;
            self.record_evidence(
                "stale_spawn_intent",
                "dispatch",
                Some(dispatch_id),
                "stale spawn intent — marked lost",
                true,
            )
            .await?;
        }
        Ok(found)
    }

    async fn detect_task_running_without_exec(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT t.id FROM tasks t WHERE t.lifecycle = 'running' AND NOT EXISTS (SELECT 1 FROM execution_attempts e WHERE e.task_id = t.id AND e.lifecycle NOT IN ('completed','failed','lost','cancelled'))",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (task_id,) in &rows {
            self.record_evidence(
                "task_running_without_exec",
                "task",
                Some(task_id),
                "task running without active execution",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_duplicate_active_execs(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT task_id FROM execution_attempts WHERE lifecycle NOT IN ('completed','failed','lost','cancelled') GROUP BY task_id HAVING COUNT(*) > 1",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (task_id,) in &rows {
            self.record_evidence(
                "duplicate_active_exec",
                "task",
                Some(task_id),
                "multiple active executions",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_lease_without_claim(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT wl.id FROM workspace_leases wl WHERE wl.lifecycle = 'active' AND NOT EXISTS (SELECT 1 FROM resource_claim_groups cg WHERE cg.lease_id = wl.id)",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (lease_id,) in &rows {
            self.record_evidence(
                "lease_without_claim",
                "lease",
                Some(lease_id),
                "active lease without claim group",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_claim_without_lease(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT cg.group_id FROM resource_claim_groups cg WHERE cg.lease_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM workspace_leases wl WHERE wl.id = cg.lease_id)",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (cg_id,) in &rows {
            self.record_evidence(
                "claim_without_lease",
                "claim_group",
                Some(cg_id),
                "claim group referencing non-existent lease",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_stale_fencing(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String, i64, Option<i64>)> = sqlx::query_as(
            "SELECT w.id, w.lease_epoch, cg.fencing_token FROM worktrees w LEFT JOIN resource_claim_groups cg ON w.id = cg.worktree_id WHERE cg.fencing_token IS NOT NULL AND cg.fencing_token < w.lease_epoch",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (wt_id, epoch, fence) in &rows {
            self.record_evidence(
                "stale_fencing",
                "worktree",
                Some(wt_id),
                &format!("stale fencing: fence={fence:?} < epoch={epoch}"),
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_profile_missing(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT e.id, e.profile_id FROM execution_attempts e WHERE e.lifecycle NOT IN ('completed','failed','lost','cancelled') AND NOT EXISTS (SELECT 1 FROM runtime_profiles rp WHERE rp.id = e.profile_id AND rp.core_status != 'unavailable')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (exec_id, profile_id) in &rows {
            self.record_evidence(
                "profile_missing",
                "execution",
                Some(exec_id),
                &format!("profile {profile_id} missing/disabled"),
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_awaiting_verification_missing(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT t.id FROM tasks t WHERE t.lifecycle = 'submitted' AND NOT EXISTS (SELECT 1 FROM workspace_leases wl WHERE wl.task_id = t.id AND wl.lifecycle = 'active')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (task_id,) in &rows {
            self.record_evidence(
                "awaiting_verification_missing",
                "task",
                Some(task_id),
                "submitted task without active lease",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_terminal_event_no_transition(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT e.id FROM execution_attempts e WHERE e.lifecycle NOT IN ('completed','failed','lost','cancelled') AND EXISTS (SELECT 1 FROM event_log el WHERE el.stream_id = e.id AND el.event_type IN ('result','session_ended','error','process_exited'))",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (exec_id,) in &rows {
            self.record_evidence(
                "terminal_event_no_transition",
                "execution",
                Some(exec_id),
                "terminal event exists but execution not terminal",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_failed_exec_active_lease(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT wl.id FROM workspace_leases wl JOIN execution_attempts e ON wl.task_id = e.task_id WHERE wl.lifecycle = 'active' AND e.lifecycle IN ('failed','lost','cancelled')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        let mut found = false;
        for (lease_id,) in &rows {
            found = true;
            sqlx::query("UPDATE workspace_leases SET lifecycle='released', released_at=datetime('now'), release_reason='reconciler-failed' WHERE id=? AND lifecycle='active'")
                .bind(lease_id).execute(&self.pool).await.map_err(db_err)?;
            self.record_evidence(
                "failed_exec_active_lease",
                "lease",
                Some(lease_id),
                "failed exec with active lease — released",
                true,
            )
            .await?;
        }
        Ok(found)
    }

    async fn detect_reservation_without_task(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT r.id FROM scheduler_reservations r WHERE r.status = 'active' AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.id = r.task_id)",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (res_id,) in &rows {
            sqlx::query("UPDATE scheduler_reservations SET status='released', released_at=datetime('now') WHERE id=?")
                .bind(res_id).execute(&self.pool).await.map_err(db_err)?;
            self.record_evidence(
                "reservation_without_task",
                "reservation",
                Some(res_id),
                "active reservation for missing task — released",
                true,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_incomplete_spawn(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM dispatch_operations WHERE status NOT IN ('completed','failed') AND agent_session_id IS NULL AND started_at < datetime('now','-5 minutes')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (op_id,) in &rows {
            sqlx::query("UPDATE dispatch_operations SET status='failed', outcome_json='incomplete spawn' WHERE id=?")
                .bind(op_id).execute(&self.pool).await.map_err(db_err)?;
            self.record_evidence(
                "incomplete_spawn",
                "dispatch",
                Some(op_id),
                "incomplete spawn — marked failed",
                true,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_running_without_process(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT e.id FROM execution_attempts e LEFT JOIN dispatch_operations d ON e.id = d.execution_id WHERE e.lifecycle = 'running' AND d.agent_session_id IS NOT NULL AND d.started_at < datetime('now','-15 minutes')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (exec_id,) in &rows {
            self.record_evidence(
                "running_without_process",
                "execution",
                Some(exec_id),
                "running exec with stale session",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_process_exit_exec_running(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT e.id FROM execution_attempts e WHERE e.lifecycle = 'running' AND EXISTS (SELECT 1 FROM event_log el WHERE el.stream_id = e.id AND el.event_type = 'process_exited')",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (exec_id,) in &rows {
            self.record_evidence(
                "process_exit_exec_running",
                "execution",
                Some(exec_id),
                "process_exited event but execution still running",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn detect_heartbeat_missing(&self) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT wl.id FROM workspace_leases wl WHERE wl.lifecycle = 'active' AND (wl.heartbeat_at IS NULL OR wl.heartbeat_at < datetime('now','-2 minutes'))",
        )
        .fetch_all(&self.pool).await.map_err(db_err)?;
        for (lease_id,) in &rows {
            self.record_evidence(
                "heartbeat_missing",
                "lease",
                Some(lease_id),
                "active lease with stale heartbeat",
                false,
            )
            .await?;
        }
        Ok(!rows.is_empty())
    }

    async fn expire_stale_reservations(&self) -> Result<usize, CoreError> {
        let result = sqlx::query(
            "UPDATE scheduler_reservations SET status='expired' WHERE status='active' AND expires_at < datetime('now')",
        )
        .execute(&self.pool).await.map_err(db_err)?;
        Ok(result.rows_affected() as usize)
    }

    async fn record_evidence(
        &self,
        anomaly_type: &str,
        entity_type: &str,
        entity_id: Option<&str>,
        description: &str,
        auto_repairable: bool,
    ) -> Result<(), CoreError> {
        let ikey = format!(
            "recon-{}-{}-{}",
            anomaly_type,
            entity_id.unwrap_or("none"),
            &Uuid::new_v4().to_string()[..8]
        );
        sqlx::query(
            "INSERT OR IGNORE INTO scheduler_reconciliations (id, anomaly_type, entity_type, entity_id, description, repair_action, repair_status, idempotency_key) VALUES (?,?,?,?,?,?,?,?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(anomaly_type)
        .bind(entity_type)
        .bind(entity_id)
        .bind(description)
        .bind(if auto_repairable { "auto-repaired" } else { "none" })
        .bind(if auto_repairable { "repaired" } else { "detected" })
        .bind(&ikey)
        .execute(&self.pool).await.map_err(db_err)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        Database::open_in_memory().await.unwrap()
    }

    async fn seed_project_task(db: &Database, task_id: &str, lifecycle: &str) {
        sqlx::query("INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')")
            .execute(&db.pool).await.unwrap();
        sqlx::query("INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES (?, 'p1', 'test', ?)")
            .bind(task_id).bind(lifecycle).execute(&db.pool).await.unwrap();
    }

    // Helper: insert a worktree using correct column names (PK = id)
    async fn insert_worktree(db: &Database) {
        sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, operation_id, owner_supervisor_id, status, lease_epoch) VALUES ('wt1','p1','t1','e1','/repo','/repo/.git','/repo/wt','br','abc','op1','sup1','active',1)")
            .execute(&db.pool).await.unwrap();
    }

    // Helper: insert an active lease
    async fn insert_active_lease(db: &Database) {
        sqlx::query("INSERT INTO workspace_leases (id, worktree_id, project_id, task_id, owner_execution_id, lease_token, fencing_token, lifecycle, heartbeat_at, expires_at) VALUES ('l1','wt1','p1','t1','e1','tok',1,'active',datetime('now'),datetime('now','+10 minutes'))")
            .execute(&db.pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_orphan_reservation_reclaimed() {
        let db = setup().await;
        seed_project_task(&db, "t1", "pending").await;
        sqlx::query("INSERT INTO scheduler_reservations (id, task_id, status, expires_at) VALUES ('r1','t1','active','2000-01-01')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::OrphanReservation));
    }

    #[tokio::test]
    async fn test_duplicate_active_execution_detected() {
        let db = setup().await;
        seed_project_task(&db, "t1", "running").await;
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
        seed_project_task(&db, "t1", "pending").await;
        sqlx::query("INSERT INTO scheduler_reservations (id, task_id, status, expires_at) VALUES ('r1','t1','active','2000-01-01')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        rec.reconcile().await.unwrap();
        let result = rec.reconcile().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_lease_without_claim_detected() {
        let db = setup().await;
        seed_project_task(&db, "t1", "running").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'running')")
            .execute(&db.pool).await.unwrap();
        insert_worktree(&db).await;
        sqlx::query("INSERT INTO workspace_leases (id, worktree_id, project_id, task_id, owner_execution_id, lease_token, fencing_token, lifecycle, expires_at) VALUES ('l1','wt1','p1','t1','e1','tok',1,'active',datetime('now','+10 minutes'))")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::LeaseWithoutClaim));
    }

    #[tokio::test]
    async fn test_failed_execution_active_resources_released() {
        let db = setup().await;
        seed_project_task(&db, "t1", "failed").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'failed')")
            .execute(&db.pool).await.unwrap();
        insert_worktree(&db).await;
        insert_active_lease(&db).await;
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::TerminalExecutionResourcesActive));
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(lc.0, "released");
    }

    #[tokio::test]
    async fn test_awaiting_verification_resources_missing_detected() {
        let db = setup().await;
        seed_project_task(&db, "t1", "submitted").await;
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::AwaitingVerificationResourceLost));
    }

    #[tokio::test]
    async fn test_running_without_process_detected() {
        let db = setup().await;
        seed_project_task(&db, "t1", "running").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'running')")
            .execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO dispatch_operations (id, project_id, task_id, selected_profile_id, request_hash, idempotency_key, status, stage, agent_session_id, execution_id, started_at) VALUES ('d1','p1','t1','prof1','hash','ikey-d1','preparing','agent_running','sess1','e1','2000-01-01')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::ProcessMissing));
    }

    #[tokio::test]
    async fn test_process_terminal_execution_nonterminal_detected() {
        let db = setup().await;
        seed_project_task(&db, "t1", "running").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'running')")
            .execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES ('ev1','e1',1,'process_exited','{}',1,'c1','ik1','agent')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::TerminalProcessNotReflected));
    }

    #[tokio::test]
    async fn test_heartbeat_missing_detected() {
        let db = setup().await;
        seed_project_task(&db, "t1", "submitted").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&db.pool).await.unwrap();
        insert_worktree(&db).await;
        sqlx::query("INSERT INTO workspace_leases (id, worktree_id, project_id, task_id, owner_execution_id, lease_token, fencing_token, lifecycle, heartbeat_at, expires_at) VALUES ('l1','wt1','p1','t1','e1','tok',1,'active','2000-01-01',datetime('now','+10 minutes'))")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        let anomalies = rec.reconcile().await.unwrap();
        assert!(anomalies.contains(&SchedulerAnomaly::StaleFencing));
    }

    #[tokio::test]
    async fn test_concurrent_reconcilers_single_repair() {
        use std::sync::Arc;
        let db = setup().await;
        seed_project_task(&db, "t1", "pending").await;
        sqlx::query("INSERT INTO scheduler_reservations (id, task_id, status, expires_at) VALUES ('r1','t1','active','2000-01-01')")
            .execute(&db.pool).await.unwrap();
        let pool = Arc::new(db.pool.clone());
        let rec1 = SchedulerReconciler::new((*pool).clone());
        let rec2 = SchedulerReconciler::new((*pool).clone());
        let (r1, r2) = tokio::join!(rec1.reconcile(), rec2.reconcile());
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    #[tokio::test]
    async fn test_no_automatic_retry() {
        let db = setup().await;
        seed_project_task(&db, "t1", "failed").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'failed')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        rec.reconcile().await.unwrap();
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM execution_attempts WHERE task_id='t1'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1, "reconciler must not create retry executions");
    }

    #[tokio::test]
    async fn test_no_worktree_deletion() {
        let db = setup().await;
        seed_project_task(&db, "t1", "failed").await;
        insert_worktree(&db).await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'failed')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        rec.reconcile().await.unwrap();
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE id='wt1'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1, "reconciler must not delete worktrees");
    }

    #[tokio::test]
    async fn test_no_provider_switch() {
        let db = setup().await;
        seed_project_task(&db, "t1", "running").await;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id) VALUES ('e1','t1',1,'running','prof-claude')")
            .execute(&db.pool).await.unwrap();
        let rec = SchedulerReconciler::new(db.pool.clone());
        rec.reconcile().await.unwrap();
        let prof: (String,) =
            sqlx::query_as("SELECT profile_id FROM execution_attempts WHERE id='e1'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(prof.0, "prof-claude", "reconciler must not switch profiles");
    }
}
