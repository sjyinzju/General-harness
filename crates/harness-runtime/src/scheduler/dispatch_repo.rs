//! DispatchRepository — persistent dispatch operation storage with transactional
//! idempotency arbitration. The authoritative decision (create vs. duplicate vs.
//! conflict) is made inside a SQLite write transaction.

use harness_core::contracts::scheduler::{DispatchOutcome, DispatchStatus};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Row type for dispatch_operations queries — avoids clippy::type_complexity.
type DispatchOpRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    String,
    String,
    String,
);

/// Persisted shape of a dispatch operation row.
#[derive(Debug, Clone)]
pub struct DispatchRecord {
    pub id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
    pub selected_profile_id: Option<String>,
    pub worktree_id: Option<String>,
    pub lease_id: Option<String>,
    pub claim_group_id: Option<String>,
    pub agent_session_id: Option<String>,
    pub pid: Option<i64>,
    pub request_hash: String,
    pub status: String,
    pub stage: String,
    pub idempotency_key: String,
}

/// Outcome of an idempotent dispatch intent insertion.
#[derive(Debug, Clone)]
pub enum IntentOutcome {
    /// Fresh intent created.
    Created {
        op_id: String,
        idempotency_key: String,
    },
    /// Same key + same request_hash → return existing result.
    Duplicate { existing: DispatchOutcome },
    /// Same key + different request_hash → conflict.
    IdempotencyConflict {
        existing_op_id: String,
        existing_hash: String,
        new_hash: String,
    },
}

pub struct DispatchRepository {
    pool: SqlitePool,
}

impl DispatchRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Atomically record a dispatch intent or detect a duplicate/conflict.
    ///
    /// The idempotency identity binds:
    /// - idempotency_key (derived from project_id, task_id, profile_id, worktree identity,
    ///   claim request hash, scheduler request identity)
    /// - request_hash (fingerprint of the full request)
    ///
    /// Arbitration is the single atomic INSERT on the UNIQUE idempotency_key
    /// (not a read-then-write transaction: a deferred tx upgrading read→write
    /// under writer contention dies with SQLITE_BUSY_SNAPSHOT, which
    /// busy_timeout does not retry):
    /// 1. Existing operation with the same idempotency_key:
    ///    a. Same request_hash → return the existing outcome (duplicate)
    ///    b. Different request_hash → return IdempotencyConflict
    /// 2. Otherwise → atomic insert; the ON CONFLICT loser re-reads the
    ///    winner's row (never a bare UNIQUE error).
    #[allow(clippy::too_many_arguments)]
    pub async fn record_intent(
        &self,
        op_id: &str,
        project_id: &str,
        task_id: &str,
        profile_id: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<IntentOutcome, CoreError> {
        // Fast path: existing operation with same idempotency_key.
        let existing: Option<(String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, request_hash, status, execution_id FROM dispatch_operations WHERE idempotency_key = ?",
        )
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("check duplicate: {e}"),
                ErrorSource::System,
            )
        })?;

        if let Some((existing_id, existing_hash, existing_status, existing_exec_id)) = existing {
            // Same key exists — check hash
            if existing_hash == request_hash {
                // Same request — return existing outcome
                let outcome = DispatchOutcome {
                    dispatch_op_id: existing_id,
                    task_id: task_id.to_string(),
                    execution_id: existing_exec_id,
                    status: match existing_status.as_str() {
                        "agent_completed" | "completed" => DispatchStatus::Completed,
                        "failed" => DispatchStatus::Failed,
                        _ => DispatchStatus::Preparing,
                    },
                    terminal_outcome: None,
                    compensation_actions: vec!["idempotent-replay".to_string()],
                };
                return Ok(IntentOutcome::Duplicate { existing: outcome });
            } else {
                // Different hash → conflict
                return Ok(IntentOutcome::IdempotencyConflict {
                    existing_op_id: existing_id,
                    existing_hash,
                    new_hash: request_hash.to_string(),
                });
            }
        }

        // No existing operation — atomic winner insert. A concurrent racer
        // that loses the ON CONFLICT re-reads the durable row and returns
        // Duplicate/Conflict; a bare UNIQUE error never escapes.
        let inserted = sqlx::query(
            "INSERT INTO dispatch_operations (id, project_id, task_id, selected_profile_id, request_hash, idempotency_key, status, stage) VALUES (?,?,?,?,?,?,'preparing','init') ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(op_id)
        .bind(project_id)
        .bind(task_id)
        .bind(profile_id)
        .bind(request_hash)
        .bind(idempotency_key)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("insert intent: {e}"),
                ErrorSource::System,
            )
        })?;

        if inserted.rows_affected() == 0 {
            // Lost the insert race: read the winner's durable row.
            let existing: Option<(String, String, String, Option<String>)> = sqlx::query_as(
                "SELECT id, request_hash, status, execution_id FROM dispatch_operations WHERE idempotency_key = ?",
            )
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("reread after conflict: {e}"),
                    ErrorSource::System,
                )
            })?;
            let (existing_id, existing_hash, existing_status, existing_exec_id) = existing
                .ok_or_else(|| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        "intent row vanished after conflict",
                        ErrorSource::System,
                    )
                })?;
            if existing_hash == request_hash {
                let outcome = DispatchOutcome {
                    dispatch_op_id: existing_id,
                    task_id: task_id.to_string(),
                    execution_id: existing_exec_id,
                    status: match existing_status.as_str() {
                        "agent_completed" | "completed" => DispatchStatus::Completed,
                        "failed" => DispatchStatus::Failed,
                        _ => DispatchStatus::Preparing,
                    },
                    terminal_outcome: None,
                    compensation_actions: vec!["idempotent-replay".to_string()],
                };
                return Ok(IntentOutcome::Duplicate { existing: outcome });
            }
            return Ok(IntentOutcome::IdempotencyConflict {
                existing_op_id: existing_id,
                existing_hash,
                new_hash: request_hash.to_string(),
            });
        }

        Ok(IntentOutcome::Created {
            op_id: op_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
        })
    }

    /// Update dispatch stage and optionally link execution_id.
    pub async fn update_stage(
        &self,
        op_id: &str,
        stage: &str,
        exec_id: Option<&str>,
    ) -> Result<(), CoreError> {
        if let Some(eid) = exec_id {
            sqlx::query("UPDATE dispatch_operations SET stage=?, execution_id=? WHERE id=?")
                .bind(stage)
                .bind(eid)
                .bind(op_id)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        format!("update stage: {e}"),
                        ErrorSource::System,
                    )
                })?;
        } else {
            sqlx::query("UPDATE dispatch_operations SET stage=? WHERE id=?")
                .bind(stage)
                .bind(op_id)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        format!("update stage: {e}"),
                        ErrorSource::System,
                    )
                })?;
        }
        Ok(())
    }

    /// Update dispatch terminal status.
    pub async fn update_status(
        &self,
        op_id: &str,
        status: &str,
        outcome_json: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE dispatch_operations SET status=?, outcome_json=?, completed_at=datetime('now') WHERE id=?",
        )
        .bind(status)
        .bind(outcome_json)
        .bind(op_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("update status: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Record spawn evidence after successful agent start.
    pub async fn record_spawn_evidence(
        &self,
        op_id: &str,
        session_id: &str,
        pid: Option<i64>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE dispatch_operations SET agent_session_id=?, pid=?, stage='agent_running', started_at=datetime('now') WHERE id=?",
        )
        .bind(session_id)
        .bind(pid)
        .bind(op_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("record spawn evidence: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Record resource ownership links.
    pub async fn record_resources(
        &self,
        op_id: &str,
        worktree_id: Option<&str>,
        lease_id: Option<&str>,
        claim_group_id: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE dispatch_operations SET worktree_id=?, lease_id=?, claim_group_id=? WHERE id=?",
        )
        .bind(worktree_id)
        .bind(lease_id)
        .bind(claim_group_id)
        .bind(op_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("record resources: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Load a dispatch operation by idempotency key for crash-window recovery.
    pub async fn load_by_ikey(&self, ikey: &str) -> Result<Option<DispatchRecord>, CoreError> {
        let row: Option<DispatchOpRow> = sqlx::query_as(
            "SELECT id, project_id, task_id, execution_id, selected_profile_id, worktree_id, lease_id, claim_group_id, agent_session_id, pid, request_hash, status, stage, idempotency_key FROM dispatch_operations WHERE idempotency_key = ?",
        )
        .bind(ikey)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("load by ikey: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(
            |(
                id,
                project_id,
                task_id,
                execution_id,
                selected_profile_id,
                worktree_id,
                lease_id,
                claim_group_id,
                agent_session_id,
                pid,
                request_hash,
                status,
                stage,
                idempotency_key,
            )| {
                DispatchRecord {
                    id,
                    project_id,
                    task_id,
                    execution_id,
                    selected_profile_id,
                    worktree_id,
                    lease_id,
                    claim_group_id,
                    agent_session_id,
                    pid,
                    request_hash,
                    status,
                    stage,
                    idempotency_key,
                }
            },
        ))
    }

    /// Load dispatch by execution_id for crash-window recovery.
    pub async fn load_by_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<DispatchRecord>, CoreError> {
        let row: Option<DispatchOpRow> = sqlx::query_as(
            "SELECT id, project_id, task_id, execution_id, selected_profile_id, worktree_id, lease_id, claim_group_id, agent_session_id, pid, request_hash, status, stage, idempotency_key FROM dispatch_operations WHERE execution_id = ?",
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("load by execution: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(
            |(
                id,
                project_id,
                task_id,
                execution_id,
                selected_profile_id,
                worktree_id,
                lease_id,
                claim_group_id,
                agent_session_id,
                pid,
                request_hash,
                status,
                stage,
                idempotency_key,
            )| {
                DispatchRecord {
                    id,
                    project_id,
                    task_id,
                    execution_id,
                    selected_profile_id,
                    worktree_id,
                    lease_id,
                    claim_group_id,
                    agent_session_id,
                    pid,
                    request_hash,
                    status,
                    stage,
                    idempotency_key,
                }
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        let db = Database::open_in_memory().await.unwrap();
        seed_prereqs(&db.pool).await;
        db
    }

    /// File-backed variant for concurrency tests: WAL + busy_timeout give
    /// deterministic writer serialization (the shared-cache in-memory pool
    /// surfaces table-lock errors under concurrent transactions).
    async fn setup_file_backed() -> (Database, tempfile::TempDir) {
        let td = tempfile::tempdir().unwrap();
        let db = Database::open(&td.path().join("dispatch_repo.db"))
            .await
            .unwrap();
        seed_prereqs(&db.pool).await;
        (db, td)
    }

    async fn seed_prereqs(pool: &SqlitePool) {
        // Create prerequisite records for FK constraints
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('proj-1','test','active')",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('task-1','proj-1','test','pending')")
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_record_intent_creates_new() {
        let db = setup().await;
        let repo = DispatchRepository::new(db.pool.clone());
        let result = repo
            .record_intent("op-1", "proj-1", "task-1", "prof-1", "ikey-1", "hash-aaa")
            .await
            .unwrap();
        assert!(matches!(result, IntentOutcome::Created { .. }));
    }

    #[tokio::test]
    async fn test_same_key_same_hash_returns_duplicate() {
        let db = setup().await;
        let repo = DispatchRepository::new(db.pool.clone());

        // First insert
        let _ = repo
            .record_intent("op-1", "proj-1", "task-1", "prof-1", "ikey-1", "hash-aaa")
            .await
            .unwrap();

        // Same key + same hash
        let result = repo
            .record_intent("op-2", "proj-1", "task-1", "prof-1", "ikey-1", "hash-aaa")
            .await
            .unwrap();
        assert!(matches!(result, IntentOutcome::Duplicate { .. }));
    }

    #[tokio::test]
    async fn test_same_key_different_hash_returns_conflict() {
        let db = setup().await;
        let repo = DispatchRepository::new(db.pool.clone());

        let _ = repo
            .record_intent("op-1", "proj-1", "task-1", "prof-1", "ikey-1", "hash-aaa")
            .await
            .unwrap();

        let result = repo
            .record_intent("op-2", "proj-1", "task-1", "prof-1", "ikey-1", "hash-bbb")
            .await
            .unwrap();
        assert!(matches!(result, IntentOutcome::IdempotencyConflict { .. }));
    }

    #[tokio::test]
    async fn test_record_spawn_evidence() {
        let db = setup().await;
        let repo = DispatchRepository::new(db.pool.clone());

        let _ = repo
            .record_intent("op-1", "proj-1", "task-1", "prof-1", "ikey-1", "hash-aaa")
            .await
            .unwrap();

        repo.record_spawn_evidence("op-1", "session-abc", Some(12345))
            .await
            .unwrap();

        let record = repo.load_by_ikey("ikey-1").await.unwrap().unwrap();
        assert_eq!(record.agent_session_id, Some("session-abc".to_string()));
        assert_eq!(record.pid, Some(12345));
        assert_eq!(record.stage, "agent_running");
    }

    #[tokio::test]
    async fn test_record_resources() {
        let db = setup().await;
        let repo = DispatchRepository::new(db.pool.clone());

        let _ = repo
            .record_intent("op-1", "proj-1", "task-1", "prof-1", "ikey-1", "hash-aaa")
            .await
            .unwrap();

        repo.record_resources("op-1", Some("wt-1"), Some("lease-1"), Some("cg-1"))
            .await
            .unwrap();

        let record = repo.load_by_ikey("ikey-1").await.unwrap().unwrap();
        assert_eq!(record.worktree_id, Some("wt-1".to_string()));
        assert_eq!(record.lease_id, Some("lease-1".to_string()));
        assert_eq!(record.claim_group_id, Some("cg-1".to_string()));
    }

    #[tokio::test]
    async fn test_concurrent_same_key_one_winner() {
        let (db, _td) = setup_file_backed().await;
        let repo1 = DispatchRepository::new(db.pool.clone());
        let repo2 = DispatchRepository::new(db.pool.clone());

        // Both try concurrently with same key — only one winner
        let (r1, r2) = tokio::join!(
            repo1.record_intent(
                "op-a",
                "proj-1",
                "task-1",
                "prof-1",
                "ikey-conc",
                "hash-aaa"
            ),
            repo2.record_intent(
                "op-b",
                "proj-1",
                "task-1",
                "prof-1",
                "ikey-conc",
                "hash-aaa"
            ),
        );

        // At least one must be Duplicate (the loser)
        let has_duplicate = matches!(r1, Ok(IntentOutcome::Duplicate { .. }))
            || matches!(r2, Ok(IntentOutcome::Duplicate { .. }));
        let has_created = matches!(r1, Ok(IntentOutcome::Created { .. }))
            || matches!(r2, Ok(IntentOutcome::Created { .. }));
        assert!(
            has_duplicate,
            "second concurrent should be duplicate: {r1:?} / {r2:?}"
        );
        assert!(
            has_created,
            "first concurrent should create: {r1:?} / {r2:?}"
        );
    }
}
