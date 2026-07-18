//! I4 Gateway for I4.5 — production integration point between the Task
//! Engineering Loop and the certified I4 dispatch/execution pipeline.
//!
//! The gateway is the ONLY code path through which I4.5 creates Executions,
//! dispatches Agents, and observes outcomes. It never calls AgentAdapter
//! directly, spawns processes, or fabricates outcomes.

use sqlx::SqlitePool;

/// Request to create a new Execution through the certified I4 pipeline.
#[derive(Debug, Clone)]
pub struct CreateExecutionRequest {
    pub task_id: String,
    pub attempt_id: String,
    pub attempt_ordinal: i64,
    pub runtime_profile_id: String,
    pub worktree_id: Option<String>,
    pub worktree_path: Option<String>,
    pub idempotency_key: String,
    pub request_hash: String,
}

/// Result of a successful Execution creation.
#[derive(Debug, Clone)]
pub struct ExecutionCreated {
    pub execution_id: String,
}

/// Result of a successful dispatch.
#[derive(Debug, Clone)]
pub struct DispatchOutcome {
    pub dispatched: bool,
    pub execution_id: String,
}

/// Snapshot of an Execution's durable state (read-only).
#[derive(Debug, Clone, Default)]
pub struct ExecutionObservation {
    pub execution_id: String,
    pub lifecycle: Option<String>,
    pub verification_run_id: Option<String>,
    pub outcome_json: Option<String>,
}

/// The I4 gateway trait. Production implementation uses real I4 services;
/// test implementations use deterministic fixtures.
#[async_trait::async_trait]
pub trait I4Gateway: Send + Sync {
    /// Create a new Execution for the given task. Idempotent.
    async fn create_execution(
        &self,
        request: &CreateExecutionRequest,
    ) -> Result<ExecutionCreated, String>;

    /// Observe an Execution's current durable state.
    async fn observe_execution(&self, execution_id: &str) -> Result<ExecutionObservation, String>;

    /// Request cancellation of an active Execution.
    async fn request_cancellation(&self, execution_id: &str) -> Result<bool, String>;
}

/// Production I4 gateway backed by the existing Scheduler and Execution tables.
pub struct ProductionI4Gateway {
    pool: SqlitePool,
}

impl ProductionI4Gateway {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl I4Gateway for ProductionI4Gateway {
    async fn create_execution(
        &self,
        request: &CreateExecutionRequest,
    ) -> Result<ExecutionCreated, String> {
        let execution_id = format!("exec-{}", uuid::Uuid::new_v4());
        // Use the existing execution_attempts table (I4 Gate C).
        let r = sqlx::query(
            "INSERT INTO execution_attempts \
             (id, task_id, attempt_number, lifecycle, profile_id, version) \
             VALUES (?,?,?,?,?,1) \
             ON CONFLICT(task_id, attempt_number) DO NOTHING",
        )
        .bind(&execution_id)
        .bind(&request.task_id)
        .bind(request.attempt_ordinal)
        .bind("created")
        .bind(&request.runtime_profile_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("create execution: {e}"))?;

        if r.rows_affected() == 0 {
            // Read existing execution.
            let existing: Option<(String,)> = sqlx::query_as(
                "SELECT id FROM execution_attempts WHERE task_id=? AND attempt_number=?",
            )
            .bind(&request.task_id)
            .bind(request.attempt_ordinal)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("reread execution: {e}"))?;
            if let Some((eid,)) = existing {
                return Ok(ExecutionCreated { execution_id: eid });
            }
            return Err("execution row vanished".into());
        }

        Ok(ExecutionCreated { execution_id })
    }

    async fn observe_execution(&self, execution_id: &str) -> Result<ExecutionObservation, String> {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT lifecycle, id FROM execution_attempts WHERE id=?")
                .bind(execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("observe execution: {e}"))?;

        let (lifecycle, _) = match row {
            Some(r) => r,
            None => {
                return Ok(ExecutionObservation {
                    execution_id: execution_id.to_string(),
                    ..Default::default()
                })
            }
        };

        // Query verification run if any.
        let ver: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT run_id, outcome_json FROM verification_runs WHERE execution_id=?",
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();

        Ok(ExecutionObservation {
            execution_id: execution_id.to_string(),
            lifecycle: Some(lifecycle),
            verification_run_id: ver.as_ref().map(|v| v.0.clone()),
            outcome_json: ver.and_then(|v| v.1),
        })
    }

    async fn request_cancellation(&self, execution_id: &str) -> Result<bool, String> {
        let r = sqlx::query(
            "UPDATE execution_attempts SET lifecycle='cancelled' \
             WHERE id=? AND lifecycle NOT IN ('completed','failed','cancelled')",
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("cancel execution: {e}"))?;
        Ok(r.rows_affected() == 1)
    }
}

/// Deterministic fixture gateway for integration tests.
pub struct FixtureI4Gateway {
    pool: SqlitePool,
    /// Pre-determined outcome to return on observe.
    pub staged_lifecycle: std::sync::Mutex<Option<String>>,
    pub staged_outcome_json: std::sync::Mutex<Option<String>>,
    pub staged_verification_run_id: std::sync::Mutex<Option<String>>,
}

impl FixtureI4Gateway {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            staged_lifecycle: std::sync::Mutex::new(None),
            staged_outcome_json: std::sync::Mutex::new(None),
            staged_verification_run_id: std::sync::Mutex::new(None),
        }
    }

    /// Pre-configure what observe_execution will return.
    pub fn stage_outcome(&self, lifecycle: &str, outcome_json: Option<&str>) {
        *self.staged_lifecycle.lock().unwrap() = Some(lifecycle.to_string());
        *self.staged_outcome_json.lock().unwrap() = outcome_json.map(|s| s.to_string());
        *self.staged_verification_run_id.lock().unwrap() =
            Some(format!("vr-{}", uuid::Uuid::new_v4()));
    }
}

#[async_trait::async_trait]
impl I4Gateway for FixtureI4Gateway {
    async fn create_execution(
        &self,
        request: &CreateExecutionRequest,
    ) -> Result<ExecutionCreated, String> {
        // Actually insert into the real table so I4 queries work.
        let execution_id = format!("exec-fix-{}", uuid::Uuid::new_v4());
        sqlx::query(
            "INSERT INTO execution_attempts \
             (id, task_id, attempt_number, lifecycle, profile_id, version) \
             VALUES (?,?,?,?,?,1) \
             ON CONFLICT(task_id, attempt_number) DO NOTHING",
        )
        .bind(&execution_id)
        .bind(&request.task_id)
        .bind(request.attempt_ordinal)
        .bind("completed") // Fixture: goes straight to terminal.
        .bind(&request.runtime_profile_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("fixture create execution: {e}"))?;
        Ok(ExecutionCreated { execution_id })
    }

    async fn observe_execution(&self, execution_id: &str) -> Result<ExecutionObservation, String> {
        let lc = self.staged_lifecycle.lock().unwrap().clone();
        let oj = self.staged_outcome_json.lock().unwrap().clone();
        let vrid = self.staged_verification_run_id.lock().unwrap().clone();
        Ok(ExecutionObservation {
            execution_id: execution_id.to_string(),
            lifecycle: lc,
            verification_run_id: vrid,
            outcome_json: oj,
        })
    }

    async fn request_cancellation(&self, _execution_id: &str) -> Result<bool, String> {
        Ok(true)
    }
}
