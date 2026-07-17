//! VerificationPlanRepo — persistent storage for VerificationPlans.
//!
//! Plans are created once per execution and are immutable after creation.
//! Only one plan per execution (enforced by UNIQUE index on execution_id).

use harness_core::contracts::verification::{
    VerificationPlan, VerificationPlanFingerprint, VerificationStep,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::content_validator::VerificationContentValidator;

type PlanRow = (
    String, // plan_id
    String, // task_id
    String, // execution_id
    String, // project_id
    String, // plan_hash
    i64,    // plan_version
    String, // steps_json
    String, // created_at
);

pub struct VerificationPlanRepo {
    pool: SqlitePool,
}

impl VerificationPlanRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Create a new verification plan. Returns an error if a plan already
    /// exists for this execution (UNIQUE constraint on execution_id).
    pub async fn create_plan(&self, plan: &VerificationPlan) -> Result<(), CoreError> {
        // Validate each step before any SQL — fail-closed security boundary.
        for step in &plan.steps {
            if !step.config_json.is_empty() && step.config_json != "{}" {
                VerificationContentValidator::validate_detail_json(&step.config_json)?;
            }
            VerificationContentValidator::validate_text(&step.description)?;
        }

        let steps_json = serde_json::to_string(&plan.steps).map_err(|e| {
            CoreError::new(
                ErrorCode::ConfigInvalid,
                format!("serialize plan steps: {e}"),
                ErrorSource::System,
            )
        })?;

        sqlx::query(
            "INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES (?,?,?,?,?,?,?)",
        )
        .bind(&plan.plan_id)
        .bind(&plan.task_id)
        .bind(&plan.execution_id)
        .bind(&plan.project_id)
        .bind(&plan.fingerprint.plan_hash)
        .bind(plan.plan_version as i64)
        .bind(&steps_json)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("create verification plan: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(())
    }

    /// Load a plan by its id.
    pub async fn get_plan(&self, plan_id: &str) -> Result<Option<VerificationPlan>, CoreError> {
        let row: Option<PlanRow> =
            sqlx::query_as(
                "SELECT plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json, created_at FROM verification_plans WHERE plan_id = ?",
            )
            .bind(plan_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("get plan: {e}"),
                    ErrorSource::System,
                )
            })?;

        Ok(row.map(|(pid, tid, eid, proj, hash, ver, steps, created)| {
            let steps: Vec<VerificationStep> = serde_json::from_str(&steps).unwrap_or_default();
            VerificationPlan {
                plan_id: pid,
                task_id: tid,
                execution_id: eid.clone(),
                project_id: proj,
                steps,
                fingerprint: VerificationPlanFingerprint {
                    plan_hash: hash,
                    execution_id: eid,
                    plan_version: ver as u32,
                },
                plan_version: ver as u32,
                created_at: created,
            }
        }))
    }

    /// Load a plan by execution_id.
    pub async fn get_plan_by_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<VerificationPlan>, CoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT plan_id FROM verification_plans WHERE execution_id = ?")
                .bind(execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        format!("get plan by execution: {e}"),
                        ErrorSource::System,
                    )
                })?;

        match row {
            Some((plan_id,)) => self.get_plan(&plan_id).await,
            None => Ok(None),
        }
    }

    /// Check if a plan exists for an execution.
    pub async fn plan_exists(&self, execution_id: &str) -> Result<bool, CoreError> {
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_plans WHERE execution_id = ?")
                .bind(execution_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        format!("check plan exists: {e}"),
                        ErrorSource::System,
                    )
                })?;

        Ok(row.0 > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use harness_core::contracts::verification::{
        VerificationPlanFingerprint, VerificationStep, VerificationStepKind,
    };

    async fn setup() -> Database {
        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        db
    }

    fn make_plan(plan_id: &str, exec_id: &str) -> VerificationPlan {
        VerificationPlan {
            plan_id: plan_id.to_string(),
            task_id: "t1".to_string(),
            execution_id: exec_id.to_string(),
            project_id: "p1".to_string(),
            steps: vec![VerificationStep {
                step_id: format!("step-{plan_id}-1"),
                plan_id: plan_id.to_string(),
                kind: VerificationStepKind::GitDiffCheck,
                description: "check diff".to_string(),
                required: true,
                sequence_index: 0,
                config_json: "{}".to_string(),
            }],
            fingerprint: VerificationPlanFingerprint {
                plan_hash: format!("hash-{plan_id}"),
                execution_id: exec_id.to_string(),
                plan_version: 1,
            },
            plan_version: 1,
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        }
    }

    #[tokio::test]
    async fn test_create_and_get_plan() {
        let db = setup().await;
        let repo = VerificationPlanRepo::new(db.pool.clone());

        let plan = make_plan("plan-1", "e1");
        repo.create_plan(&plan).await.unwrap();

        let loaded = repo.get_plan("plan-1").await.unwrap().unwrap();
        assert_eq!(loaded.plan_id, "plan-1");
        assert_eq!(loaded.execution_id, "e1");
        assert_eq!(loaded.steps.len(), 1);
        assert_eq!(loaded.steps[0].kind, VerificationStepKind::GitDiffCheck);
    }

    #[tokio::test]
    async fn test_duplicate_plan_rejected() {
        let db = setup().await;
        let repo = VerificationPlanRepo::new(db.pool.clone());

        let plan = make_plan("plan-1", "e1");
        repo.create_plan(&plan).await.unwrap();

        // Second plan for same execution must fail (UNIQUE constraint).
        let plan2 = make_plan("plan-2", "e1");
        let result = repo.create_plan(&plan2).await;
        assert!(
            result.is_err(),
            "duplicate plan for same execution must be rejected"
        );
    }

    #[tokio::test]
    async fn test_get_plan_by_execution() {
        let db = setup().await;
        let repo = VerificationPlanRepo::new(db.pool.clone());

        let plan = make_plan("plan-1", "e1");
        repo.create_plan(&plan).await.unwrap();

        let loaded = repo.get_plan_by_execution("e1").await.unwrap().unwrap();
        assert_eq!(loaded.plan_id, "plan-1");

        assert!(repo
            .get_plan_by_execution("nonexistent")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_plan_exists() {
        let db = setup().await;
        let repo = VerificationPlanRepo::new(db.pool.clone());

        assert!(!repo.plan_exists("e1").await.unwrap());

        let plan = make_plan("plan-1", "e1");
        repo.create_plan(&plan).await.unwrap();

        assert!(repo.plan_exists("e1").await.unwrap());
    }

    // ── Repository-level validator enforcement tests ─────────────────

    #[tokio::test]
    async fn test_reject_secret_in_step_config() {
        let db = setup().await;
        let repo = VerificationPlanRepo::new(db.pool.clone());
        let mut plan = make_plan("plan-sec", "e2");
        // Need unique execution_id since the setup already uses "e1".
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e2','t1',2,'completed')")
            .execute(&db.pool).await.unwrap();
        plan.execution_id = "e2".to_string();
        plan.fingerprint.execution_id = "e2".to_string();
        plan.steps[0].config_json = r#"{"api_key":"sk-secret-in-config"}"#.to_string();

        let result = repo.create_plan(&plan).await;
        assert!(result.is_err(), "secret in step config must be rejected by repo");

        // Zero rows persisted.
        assert!(!repo.plan_exists("e2").await.unwrap());
    }

    #[tokio::test]
    async fn test_reject_secret_in_step_description() {
        let db = setup().await;
        let repo = VerificationPlanRepo::new(db.pool.clone());
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e3','t1',3,'completed')")
            .execute(&db.pool).await.unwrap();
        let mut plan = make_plan("plan-desc", "e3");
        plan.execution_id = "e3".to_string();
        plan.fingerprint.execution_id = "e3".to_string();
        plan.steps[0].description = "check with token ghp_secret123".to_string();

        assert!(repo.create_plan(&plan).await.is_err());
        assert!(!repo.plan_exists("e3").await.unwrap());
    }
}
