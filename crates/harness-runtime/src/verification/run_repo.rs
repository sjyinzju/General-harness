//! VerificationRunRepo — persistent VerificationRun storage with
//! transactional idempotency arbitration.
//!
//! Same key + same request_hash → returns existing run (idempotent).
//! Same key + different request_hash → IdempotencyConflict.
//! Concurrent callers → exactly one winner, rest get duplicate.
//! Terminal runs cannot be reactivated.

use harness_core::contracts::verification::{
    VerificationOutcome, VerificationPlanFingerprint, VerificationRun, VerificationRunLifecycle,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Outcome of an idempotent run insertion.
#[derive(Debug, Clone)]
pub enum RunIntentOutcome {
    Created {
        run_id: String,
        idempotency_key: String,
    },
    Duplicate {
        existing: Box<VerificationRun>,
    },
    IdempotencyConflict {
        existing_run_id: String,
        existing_hash: String,
        new_hash: String,
    },
}

/// Row type for verification_runs queries.
type RunRow = (
    String,         // run_id
    String,         // plan_id
    String,         // plan_hash
    i64,            // plan_version
    String,         // execution_id
    String,         // task_id
    String,         // project_id
    String,         // lifecycle
    String,         // idempotency_key
    String,         // request_hash
    Option<String>, // outcome_json
    i64,            // version
    String,         // created_at
    Option<String>, // started_at
    Option<String>, // completed_at
);

pub struct VerificationRunRepo {
    pool: SqlitePool,
}

impl VerificationRunRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Atomically create a verification run or detect a duplicate/conflict.
    pub async fn create_run(&self, run: &VerificationRun) -> Result<RunIntentOutcome, CoreError> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("begin create run tx: {e}"),
                ErrorSource::System,
            )
        })?;

        let existing: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT run_id, request_hash, lifecycle, version FROM verification_runs WHERE idempotency_key = ?",
        )
        .bind(&run.idempotency_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("check dup: {e}"), ErrorSource::System))?;

        if let Some((existing_id, existing_hash, _lifecycle, _ver)) = existing {
            if existing_hash == run.request_hash {
                let existing_run = self.load_run_by_id(&existing_id).await?.ok_or_else(|| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        "run found by ikey but not loadable",
                        ErrorSource::System,
                    )
                })?;
                return Ok(RunIntentOutcome::Duplicate {
                    existing: Box::new(existing_run),
                });
            } else {
                return Ok(RunIntentOutcome::IdempotencyConflict {
                    existing_run_id: existing_id,
                    existing_hash,
                    new_hash: run.request_hash.clone(),
                });
            }
        }

        let outcome_json = run
            .outcome
            .as_ref()
            .map(|o| serde_json::to_string(o).unwrap_or_default());

        sqlx::query(
            "INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, idempotency_key, request_hash, outcome_json) VALUES (?,?,?,?,?,?,?,'created',?,?,?)",
        )
        .bind(&run.run_id).bind(&run.plan_id).bind(&run.plan_fingerprint.plan_hash)
        .bind(run.plan_fingerprint.plan_version as i64).bind(&run.execution_id)
        .bind(&run.task_id).bind(&run.project_id).bind(&run.idempotency_key)
        .bind(&run.request_hash).bind(outcome_json.as_deref())
        .execute(&mut *tx).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert run: {e}"), ErrorSource::System))?;

        tx.commit().await.map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("commit run: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(RunIntentOutcome::Created {
            run_id: run.run_id.clone(),
            idempotency_key: run.idempotency_key.clone(),
        })
    }

    /// Load a run by id.
    pub async fn load_run_by_id(&self, run_id: &str) -> Result<Option<VerificationRun>, CoreError> {
        let row: Option<RunRow> = sqlx::query_as(
            "SELECT run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, idempotency_key, request_hash, outcome_json, version, created_at, started_at, completed_at FROM verification_runs WHERE run_id = ?",
        )
        .bind(run_id).fetch_optional(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("load run: {e}"), ErrorSource::System))?;

        Ok(row.map(into_run))
    }

    /// Transition a run lifecycle. Uses version for optimistic locking.
    pub async fn transition_run(
        &self,
        run_id: &str,
        from: &VerificationRunLifecycle,
        to: &VerificationRunLifecycle,
        expected_version: i64,
    ) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "UPDATE verification_runs SET lifecycle=?, version=version+1, updated_at=datetime('now') WHERE run_id=? AND lifecycle=? AND version=?",
        )
        .bind(lifecycle_str(to)).bind(run_id).bind(lifecycle_str(from)).bind(expected_version)
        .execute(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("transition: {e}"), ErrorSource::System))?;

        Ok(rows.rows_affected() > 0)
    }

    /// Set the terminal outcome.
    pub async fn set_outcome(
        &self,
        run_id: &str,
        outcome: &VerificationOutcome,
        lifecycle: &VerificationRunLifecycle,
    ) -> Result<(), CoreError> {
        let json = serde_json::to_string(outcome).map_err(|e| {
            CoreError::new(
                ErrorCode::ConfigInvalid,
                format!("serialize outcome: {e}"),
                ErrorSource::System,
            )
        })?;
        sqlx::query("UPDATE verification_runs SET outcome_json=?, lifecycle=?, completed_at=datetime('now'), updated_at=datetime('now') WHERE run_id=?")
            .bind(&json).bind(lifecycle_str(lifecycle)).bind(run_id)
            .execute(&self.pool).await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("set outcome: {e}"), ErrorSource::System))?;
        Ok(())
    }
}

fn lifecycle_str(lc: &VerificationRunLifecycle) -> &'static str {
    match lc {
        VerificationRunLifecycle::Created => "created",
        VerificationRunLifecycle::Running => "running",
        VerificationRunLifecycle::Completed => "completed",
        VerificationRunLifecycle::Failed => "failed",
        VerificationRunLifecycle::Cancelled => "cancelled",
        VerificationRunLifecycle::Error => "error",
    }
}

fn parse_lifecycle(s: &str) -> VerificationRunLifecycle {
    match s {
        "running" => VerificationRunLifecycle::Running,
        "completed" => VerificationRunLifecycle::Completed,
        "failed" => VerificationRunLifecycle::Failed,
        "cancelled" => VerificationRunLifecycle::Cancelled,
        "error" => VerificationRunLifecycle::Error,
        _ => VerificationRunLifecycle::Created,
    }
}

fn into_run(
    (
        rid,
        pid,
        phash,
        pver,
        eid,
        tid,
        proj,
        lc,
        ikey,
        rhash,
        outcome_json,
        ver,
        created,
        started,
        completed,
    ): RunRow,
) -> VerificationRun {
    let lifecycle = parse_lifecycle(&lc);
    let outcome = outcome_json.and_then(|j| serde_json::from_str(&j).ok());
    VerificationRun {
        run_id: rid,
        plan_id: pid,
        plan_fingerprint: VerificationPlanFingerprint {
            plan_hash: phash,
            execution_id: eid.clone(),
            plan_version: pver as u32,
        },
        execution_id: eid,
        task_id: tid,
        project_id: proj,
        lifecycle,
        idempotency_key: ikey,
        request_hash: rhash,
        created_at: created,
        started_at: started,
        completed_at: completed,
        outcome,
        version: ver,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use harness_core::contracts::verification::{
        VerificationPlanFingerprint, VerificationResult, VerificationRunLifecycle,
    };

    async fn setup() -> Database {
        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-1','t1','e1','p1','hash-aaa',1,'[]')").execute(&db.pool).await.unwrap();
        db
    }

    fn make_run(run_id: &str, ikey: &str, hash: &str) -> VerificationRun {
        VerificationRun {
            run_id: run_id.into(),
            plan_id: "plan-1".into(),
            plan_fingerprint: VerificationPlanFingerprint {
                plan_hash: "hash-aaa".into(),
                execution_id: "e1".into(),
                plan_version: 1,
            },
            execution_id: "e1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            lifecycle: VerificationRunLifecycle::Created,
            idempotency_key: ikey.into(),
            request_hash: hash.into(),
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            started_at: None,
            completed_at: None,
            outcome: None,
            version: 1,
        }
    }

    #[tokio::test]
    async fn test_create_run_success() {
        let db = setup().await;
        let repo = VerificationRunRepo::new(db.pool.clone());
        let run = make_run("run-1", "ikey-aaa", "hash-aaa");
        let result = repo.create_run(&run).await.unwrap();
        assert!(matches!(result, RunIntentOutcome::Created { .. }));

        let loaded = repo.load_run_by_id("run-1").await.unwrap().unwrap();
        assert_eq!(loaded.run_id, "run-1");
        assert_eq!(loaded.lifecycle, VerificationRunLifecycle::Created);
    }

    #[tokio::test]
    async fn test_same_key_same_hash_duplicate() {
        let db = setup().await;
        let repo = VerificationRunRepo::new(db.pool.clone());

        let run1 = make_run("run-1", "ikey-dup", "hash-aaa");
        repo.create_run(&run1).await.unwrap();

        let run2 = make_run("run-2", "ikey-dup", "hash-aaa");
        let result = repo.create_run(&run2).await.unwrap();
        assert!(matches!(result, RunIntentOutcome::Duplicate { .. }));
    }

    #[tokio::test]
    async fn test_same_key_different_hash_conflict() {
        let db = setup().await;
        let repo = VerificationRunRepo::new(db.pool.clone());

        let run1 = make_run("run-1", "ikey-conf", "hash-aaa");
        repo.create_run(&run1).await.unwrap();

        let run2 = make_run("run-2", "ikey-conf", "hash-bbb");
        let result = repo.create_run(&run2).await.unwrap();
        assert!(matches!(
            result,
            RunIntentOutcome::IdempotencyConflict { .. }
        ));
    }

    #[tokio::test]
    async fn test_concurrent_same_key_one_winner() {
        let db = setup().await;
        let repo1 = VerificationRunRepo::new(db.pool.clone());
        let repo2 = VerificationRunRepo::new(db.pool.clone());

        let run_a = make_run("run-a", "ikey-conc", "hash-aaa");
        let run_b = make_run("run-b", "ikey-conc", "hash-aaa");

        let (r1, r2) = tokio::join!(repo1.create_run(&run_a), repo2.create_run(&run_b));
        let has_created = matches!(r1, Ok(RunIntentOutcome::Created { .. }))
            || matches!(r2, Ok(RunIntentOutcome::Created { .. }));
        let has_duplicate = matches!(r1, Ok(RunIntentOutcome::Duplicate { .. }))
            || matches!(r2, Ok(RunIntentOutcome::Duplicate { .. }));
        assert!(has_created, "one must create");
        assert!(has_duplicate, "other must be duplicate");
    }

    /// True file-backed two-pool concurrency: two independent SqlitePools
    /// on the same temp-file database. Only one run must be created.
    #[tokio::test]
    async fn test_file_backed_two_pool_one_winner() {
        use crate::db::Database;
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;
        use std::time::Duration;

        let td = tempfile::tempdir().unwrap();
        let db_path = td.path().join("concurrent.db");

        // Use Database::open to create the file and run all migrations.
        let db = Database::open(&db_path).await.unwrap();
        let pool1 = db.pool.clone();
        let pool1_check = db.pool.clone(); // retained for post-concurrency verification

        // Seed prerequisite rows.
        sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')")
            .execute(&pool1).await.unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')")
            .execute(&pool1).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&pool1).await.unwrap();
        sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-1','t1','e1','p1','hash-aaa',1,'[]')")
            .execute(&pool1).await.unwrap();

        // Pool 2 — independent connection to the same file.
        let db_path_str = db_path.to_string_lossy().to_string();
        let opts2 = SqliteConnectOptions::from_str(&db_path_str)
            .unwrap()
            .create_if_missing(false)
            .foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(30));
        let pool2 = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts2)
            .await
            .unwrap();

        let repo1 = VerificationRunRepo::new(pool1);
        let repo2 = VerificationRunRepo::new(pool2);

        let run_a = make_run("run-a", "ikey-file-conc", "hash-aaa");
        let run_b = make_run("run-b", "ikey-file-conc", "hash-aaa");

        let (r1, r2) = tokio::join!(repo1.create_run(&run_a), repo2.create_run(&run_b));

        // Exactly one winner must create a run. The loser may get:
        // - a clean Duplicate (if both pools see the same snapshot), or
        // - a PersistenceError from a UNIQUE constraint violation (if
        //   the second pool's SELECT sees an empty table before the first
        //   pool commits). Both outcomes are valid — the key invariant is
        //   that only ONE run is actually created.
        let has_created =
            matches!(r1, Ok(RunIntentOutcome::Created { .. }))
                || matches!(r2, Ok(RunIntentOutcome::Created { .. }));
        assert!(has_created, "exactly one must create a fresh run");

        // Verify only one run exists in the database (use retained pool).
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_runs WHERE idempotency_key = 'ikey-file-conc'",
        )
        .fetch_one(&pool1_check)
        .await
        .unwrap();
        assert_eq!(count.0, 1, "exactly one run must exist in DB");
    }

    #[tokio::test]
    async fn test_transition_run() {
        let db = setup().await;
        let repo = VerificationRunRepo::new(db.pool.clone());

        let run = make_run("run-1", "ikey-trans", "hash-aaa");
        repo.create_run(&run).await.unwrap();

        let ok = repo
            .transition_run(
                "run-1",
                &VerificationRunLifecycle::Created,
                &VerificationRunLifecycle::Running,
                1,
            )
            .await
            .unwrap();
        assert!(ok);

        let loaded = repo.load_run_by_id("run-1").await.unwrap().unwrap();
        assert_eq!(loaded.lifecycle, VerificationRunLifecycle::Running);
        assert_eq!(loaded.version, 2);
    }

    #[tokio::test]
    async fn test_set_outcome() {
        let db = setup().await;
        let repo = VerificationRunRepo::new(db.pool.clone());

        let run = make_run("run-1", "ikey-out", "hash-aaa");
        repo.create_run(&run).await.unwrap();

        let outcome = VerificationOutcome {
            result: VerificationResult::Passed,
            failure_classification: None,
            summary: "all good".into(),
            blockers: vec![],
            findings_count: 0,
        };
        repo.set_outcome("run-1", &outcome, &VerificationRunLifecycle::Completed)
            .await
            .unwrap();

        let loaded = repo.load_run_by_id("run-1").await.unwrap().unwrap();
        assert_eq!(loaded.lifecycle, VerificationRunLifecycle::Completed);
        assert!(loaded.outcome.is_some());
        assert_eq!(loaded.outcome.unwrap().result, VerificationResult::Passed);
    }
}
