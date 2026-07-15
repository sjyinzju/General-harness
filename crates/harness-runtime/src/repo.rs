//! Repository implementations using SQLx.

use async_trait::async_trait;
use harness_core::contracts::project::{Project, ProjectLifecycle};
use harness_core::contracts::repository::{
    EventLogEntry, EventLogRepository, ExecutionRecord, ExecutionRepository,
    OperationRecord, OperationRepository, ProjectRepository, TaskRepository,
    WorkspaceLeaseRecord, WorkspaceLeaseRepository,
};
use harness_core::contracts::task::{Task, TaskLifecycle};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

// ── ProjectRepository ─────────────────────────────

pub struct SqlProjectRepo { pool: SqlitePool }

impl SqlProjectRepo {
    pub fn new(pool: SqlitePool) -> Self { Self { pool } }
}

#[async_trait]
impl ProjectRepository for SqlProjectRepo {
    async fn create(&self, p: &Project) -> Result<(), CoreError> {
        sqlx::query("INSERT INTO projects (id, objective, lifecycle, goal_contract_version, plan_version) VALUES (?,?,?,?,?)")
            .bind(&p.id).bind(&p.objective).bind("created").bind(p.goal_contract_version.map(|v| v as i64)).bind(p.plan_version.map(|v| v as i64))
            .execute(&self.pool).await.map_err(map_err)?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Project>, CoreError> {
        let row = sqlx::query_as::<_, ProjectRow>("SELECT id, objective, lifecycle, goal_contract_version, plan_version FROM projects WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await.map_err(map_err)?;
        Ok(row.map(|r| Project { id: r.id, objective: r.objective, lifecycle: parse_pl(&r.lifecycle), goal_contract_version: r.goal_contract_version.map(|v| v as u32), plan_version: r.plan_version.map(|v| v as u32) }))
    }

    async fn update_lifecycle(&self, id: &str, from: &ProjectLifecycle, to: &ProjectLifecycle, version: u32, ikey: &str) -> Result<(), CoreError> {
        let from_str = serde_json::to_string(from).unwrap().trim_matches('"').to_string();
        let to_str = serde_json::to_string(to).unwrap().trim_matches('"').to_string();
        let result = sqlx::query("UPDATE projects SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND lifecycle = ? AND version = ?")
            .bind(&to_str).bind(id).bind(&from_str).bind(version)
            .execute(&self.pool).await.map_err(map_err)?;
        if result.rows_affected() == 0 {
            return Err(CoreError::new(ErrorCode::InvalidStateTransition { from: from_str, to: to_str }, "optimistic lock conflict", ErrorSource::System));
        }
        Ok(())
    }

    async fn list_non_terminal(&self) -> Result<Vec<Project>, CoreError> {
        let rows = sqlx::query_as::<_, ProjectRow>("SELECT id, objective, lifecycle, goal_contract_version, plan_version FROM projects WHERE lifecycle NOT IN ('done','cancelled','failed')")
            .fetch_all(&self.pool).await.map_err(map_err)?;
        Ok(rows.into_iter().map(|r| Project { id: r.id, objective: r.objective, lifecycle: parse_pl(&r.lifecycle), goal_contract_version: r.goal_contract_version.map(|v| v as u32), plan_version: r.plan_version.map(|v| v as u32) }).collect())
    }
}

#[derive(sqlx::FromRow)]
struct ProjectRow { id: String, objective: String, lifecycle: String, goal_contract_version: Option<i64>, plan_version: Option<i64> }

// ── TaskRepository ────────────────────────────────

pub struct SqlTaskRepo { pool: SqlitePool }

impl SqlTaskRepo { pub fn new(pool: SqlitePool) -> Self { Self { pool } } }

#[async_trait]
impl TaskRepository for SqlTaskRepo {
    async fn create(&self, t: &Task) -> Result<(), CoreError> {
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle, retry_count, max_retries) VALUES (?,?,?,?,?,?)")
            .bind(&t.id).bind(&t.project_id).bind(&t.goal).bind("pending").bind(t.retry_count as i64).bind(t.max_retries as i64)
            .execute(&self.pool).await.map_err(map_err)?;
        for dep in &t.dependencies {
            sqlx::query("INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?,?)")
                .bind(&t.id).bind(&dep.task_id).execute(&self.pool).await.map_err(map_err)?;
        }
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Task>, CoreError> {
        let row = sqlx::query_as::<_, TaskRow>("SELECT id, project_id, goal, lifecycle, retry_count, max_retries, current_execution_id FROM tasks WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await.map_err(map_err)?;
        match row {
            Some(r) => {
                let deps = sqlx::query_as::<_, DepRow>("SELECT depends_on_task_id FROM task_dependencies WHERE task_id = ?")
                    .bind(&r.id).fetch_all(&self.pool).await.map_err(map_err)?;
                Ok(Some(Task { id: r.id, project_id: r.project_id, goal: r.goal, lifecycle: parse_tl(&r.lifecycle), retry_count: r.retry_count as u32, max_retries: r.max_retries as u32, current_execution_id: r.current_execution_id, dependencies: deps.into_iter().map(|d| harness_core::contracts::task::TaskDependency { task_id: d.depends_on_task_id }).collect() }))
            }
            None => Ok(None),
        }
    }

    async fn update_lifecycle(&self, id: &str, from: &TaskLifecycle, to: &TaskLifecycle, version: u32, ikey: &str) -> Result<(), CoreError> {
        let from_s = serde_json::to_string(from).unwrap().trim_matches('"').to_string();
        let to_s = serde_json::to_string(to).unwrap().trim_matches('"').to_string();
        let r = sqlx::query("UPDATE tasks SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND lifecycle = ? AND version = ?")
            .bind(&to_s).bind(id).bind(&from_s).bind(version).execute(&self.pool).await.map_err(map_err)?;
        if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::InvalidStateTransition { from: from_s, to: to_s }, "conflict", ErrorSource::System)); }
        Ok(())
    }

    async fn set_current_execution(&self, task_id: &str, execution_id: &str, version: u32) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE tasks SET current_execution_id = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND version = ?")
            .bind(execution_id).bind(task_id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::InvalidStateTransition { from: "?".into(), to: "?".into() }, "conflict", ErrorSource::System)); }
        Ok(())
    }

    async fn increment_retry_count(&self, id: &str, version: u32) -> Result<u32, CoreError> {
        let r = sqlx::query("UPDATE tasks SET retry_count = retry_count + 1, version = version + 1, updated_at = datetime('now') WHERE id = ? AND version = ?")
            .bind(id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::InvalidStateTransition { from: "?".into(), to: "?".into() }, "conflict", ErrorSource::System)); }
        let row: (i64,) = sqlx::query_as("SELECT retry_count FROM tasks WHERE id = ?").bind(id).fetch_one(&self.pool).await.map_err(map_err)?;
        Ok(row.0 as u32)
    }

    async fn list_by_project(&self, project_id: &str) -> Result<Vec<Task>, CoreError> {
        let rows = sqlx::query_as::<_, TaskRow>("SELECT id, project_id, goal, lifecycle, retry_count, max_retries, current_execution_id FROM tasks WHERE project_id = ?")
            .bind(project_id).fetch_all(&self.pool).await.map_err(map_err)?;
        let mut tasks = Vec::new();
        for r in rows {
            let deps = sqlx::query_as::<_, DepRow>("SELECT depends_on_task_id FROM task_dependencies WHERE task_id = ?").bind(&r.id).fetch_all(&self.pool).await.map_err(map_err)?;
            tasks.push(Task { id: r.id, project_id: r.project_id, goal: r.goal, lifecycle: parse_tl(&r.lifecycle), retry_count: r.retry_count as u32, max_retries: r.max_retries as u32, current_execution_id: r.current_execution_id, dependencies: deps.into_iter().map(|d| harness_core::contracts::task::TaskDependency { task_id: d.depends_on_task_id }).collect() });
        }
        Ok(tasks)
    }

    async fn list_non_terminal_by_project(&self, project_id: &str) -> Result<Vec<Task>, CoreError> {
        let rows = sqlx::query_as::<_, TaskRow>("SELECT id, project_id, goal, lifecycle, retry_count, max_retries, current_execution_id FROM tasks WHERE project_id = ? AND lifecycle NOT IN ('done','cancelled','superseded','failed')")
            .bind(project_id).fetch_all(&self.pool).await.map_err(map_err)?;
        let mut tasks = Vec::new();
        for r in rows { tasks.push(Task { id: r.id, project_id: r.project_id, goal: r.goal, lifecycle: parse_tl(&r.lifecycle), retry_count: r.retry_count as u32, max_retries: r.max_retries as u32, current_execution_id: r.current_execution_id, dependencies: vec![] }); }
        Ok(tasks)
    }
}

#[derive(sqlx::FromRow)] struct TaskRow { id: String, project_id: String, goal: String, lifecycle: String, retry_count: i64, max_retries: i64, current_execution_id: Option<String> }
#[derive(sqlx::FromRow)] struct DepRow { depends_on_task_id: String }

// ── ExecutionRepository ───────────────────────────

pub struct SqlExecutionRepo { pool: SqlitePool }
impl SqlExecutionRepo { pub fn new(pool: SqlitePool) -> Self { Self { pool } } }

#[async_trait]
impl ExecutionRepository for SqlExecutionRepo {
    async fn create(&self, e: &ExecutionRecord) -> Result<(), CoreError> {
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, agent_session_id, native_session_id, pid) VALUES (?,?,?,?,?,?,?,?)")
            .bind(&e.id).bind(&e.task_id).bind(e.attempt_number as i64).bind(&e.lifecycle).bind(&e.profile_id).bind(&e.agent_session_id).bind(&e.native_session_id).bind(e.pid.map(|p| p as i64))
            .execute(&self.pool).await.map_err(map_err)?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<ExecutionRecord>, CoreError> {
        let row = sqlx::query_as::<_, ExecRow>("SELECT id, task_id, attempt_number, lifecycle, profile_id, agent_session_id, native_session_id, pid FROM execution_attempts WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await.map_err(map_err)?;
        Ok(row.map(|r| ExecutionRecord { id: r.id, task_id: r.task_id, attempt_number: r.attempt_number as u32, lifecycle: r.lifecycle, profile_id: r.profile_id, agent_session_id: r.agent_session_id, native_session_id: r.native_session_id, pid: r.pid.map(|p| p as u32), version: 1, created_at: String::new(), updated_at: String::new() }))
    }

    async fn update_lifecycle(&self, id: &str, to: &str, reason: Option<&str>, version: u32, ikey: &str) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE execution_attempts SET lifecycle = ?, version = version + 1, updated_at = datetime('now') WHERE id = ? AND version = ?")
            .bind(to).bind(id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::InvalidStateTransition { from: "?".into(), to: to.into() }, "conflict", ErrorSource::System)); }
        Ok(())
    }

    async fn list_by_task(&self, task_id: &str) -> Result<Vec<ExecutionRecord>, CoreError> {
        let rows = sqlx::query_as::<_, ExecRow>("SELECT id, task_id, attempt_number, lifecycle, profile_id, agent_session_id, native_session_id, pid FROM execution_attempts WHERE task_id = ? ORDER BY attempt_number")
            .bind(task_id).fetch_all(&self.pool).await.map_err(map_err)?;
        Ok(rows.into_iter().map(|r| ExecutionRecord { id: r.id, task_id: r.task_id, attempt_number: r.attempt_number as u32, lifecycle: r.lifecycle, profile_id: r.profile_id, agent_session_id: r.agent_session_id, native_session_id: r.native_session_id, pid: r.pid.map(|p| p as u32), version: 1, created_at: String::new(), updated_at: String::new() }).collect())
    }

    async fn get_active_for_task(&self, task_id: &str) -> Result<Option<ExecutionRecord>, CoreError> {
        let row = sqlx::query_as::<_, ExecRow>("SELECT id, task_id, attempt_number, lifecycle, profile_id, agent_session_id, native_session_id, pid FROM execution_attempts WHERE task_id = ? AND lifecycle NOT IN ('completed','failed','lost','cancelled') LIMIT 1")
            .bind(task_id).fetch_optional(&self.pool).await.map_err(map_err)?;
        Ok(row.map(|r| ExecutionRecord { id: r.id, task_id: r.task_id, attempt_number: r.attempt_number as u32, lifecycle: r.lifecycle, profile_id: r.profile_id, agent_session_id: r.agent_session_id, native_session_id: r.native_session_id, pid: r.pid.map(|p| p as u32), version: 1, created_at: String::new(), updated_at: String::new() }))
    }
}
#[derive(sqlx::FromRow)] struct ExecRow { id: String, task_id: String, attempt_number: i64, lifecycle: String, profile_id: String, agent_session_id: Option<String>, native_session_id: Option<String>, pid: Option<i64> }

// ── WorkspaceLeaseRepository ──────────────────────

pub struct SqlLeaseRepo { pool: SqlitePool }
impl SqlLeaseRepo { pub fn new(pool: SqlitePool) -> Self { Self { pool } } }

#[async_trait]
impl WorkspaceLeaseRepository for SqlLeaseRepo {
    async fn acquire(&self, l: &harness_core::contracts::workspace::WorkspaceLease) -> Result<(), CoreError> {
        sqlx::query("INSERT INTO workspace_leases (id, task_id, lifecycle, worktree_path, branch_name, expires_at) VALUES (?,?,?,?,?,?)")
            .bind(&l.id).bind(&l.task_id).bind("acquired").bind(&l.worktree_path).bind(&l.branch_name).bind(&l.expires_at)
            .execute(&self.pool).await.map_err(map_err)?;
        Ok(())
    }
    async fn get(&self, id: &str) -> Result<Option<WorkspaceLeaseRecord>, CoreError> {
        let row = sqlx::query_as::<_, LeaseRow>("SELECT id, task_id, lifecycle, worktree_path, branch_name FROM workspace_leases WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await.map_err(map_err)?;
        Ok(row.map(|r| WorkspaceLeaseRecord { id: r.id, task_id: r.task_id, lifecycle: r.lifecycle, worktree_path: r.worktree_path, branch_name: r.branch_name, version: 1, acquired_at: String::new(), expires_at: String::new(), released_at: None }))
    }
    async fn release(&self, id: &str, version: u32) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE workspace_leases SET lifecycle = 'released', version = version + 1, released_at = datetime('now') WHERE id = ? AND version = ?")
            .bind(id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::WorkspaceLeaseExpired, "conflict", ErrorSource::System)); }
        Ok(())
    }
    async fn expire(&self, id: &str, version: u32) -> Result<(), CoreError> {
        sqlx::query("UPDATE workspace_leases SET lifecycle = 'expired', version = version + 1 WHERE id = ? AND version = ?")
            .bind(id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        Ok(())
    }
    async fn find_active_for_task(&self, task_id: &str) -> Result<Option<WorkspaceLeaseRecord>, CoreError> {
        let row = sqlx::query_as::<_, LeaseRow>("SELECT id, task_id, lifecycle, worktree_path, branch_name FROM workspace_leases WHERE task_id = ? AND lifecycle IN ('acquired','active') LIMIT 1")
            .bind(task_id).fetch_optional(&self.pool).await.map_err(map_err)?;
        Ok(row.map(|r| WorkspaceLeaseRecord { id: r.id, task_id: r.task_id, lifecycle: r.lifecycle, worktree_path: r.worktree_path, branch_name: r.branch_name, version: 1, acquired_at: String::new(), expires_at: String::new(), released_at: None }))
    }
    async fn find_expired(&self) -> Result<Vec<WorkspaceLeaseRecord>, CoreError> {
        let rows = sqlx::query_as::<_, LeaseRow>("SELECT id, task_id, lifecycle, worktree_path, branch_name FROM workspace_leases WHERE lifecycle IN ('acquired','active') AND expires_at < datetime('now')")
            .fetch_all(&self.pool).await.map_err(map_err)?;
        Ok(rows.into_iter().map(|r| WorkspaceLeaseRecord { id: r.id, task_id: r.task_id, lifecycle: r.lifecycle, worktree_path: r.worktree_path, branch_name: r.branch_name, version: 1, acquired_at: String::new(), expires_at: String::new(), released_at: None }).collect())
    }
}
#[derive(sqlx::FromRow)] struct LeaseRow { id: String, task_id: String, lifecycle: String, worktree_path: String, branch_name: String }

// ── EventLogRepository ────────────────────────────

pub struct SqlEventLogRepo { pool: SqlitePool }
impl SqlEventLogRepo { pub fn new(pool: SqlitePool) -> Self { Self { pool } } }

#[async_trait]
impl EventLogRepository for SqlEventLogRepo {
    async fn append(&self, events: &[EventLogEntry]) -> Result<(), CoreError> {
        for e in events {
            sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, causation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?,?)")
                .bind(&e.id).bind(&e.stream_id).bind(e.stream_version as i64).bind(&e.event_type).bind(&e.payload_json).bind(e.schema_version as i64).bind(&e.correlation_id).bind(&e.causation_id).bind(&e.idempotency_key).bind(&e.source)
                .execute(&self.pool).await.map_err(map_err)?;
        }
        Ok(())
    }
    async fn get_by_stream(&self, stream_id: &str, since_version: Option<u32>) -> Result<Vec<EventLogEntry>, CoreError> {
        let sv = since_version.unwrap_or(0) as i64;
        let rows = sqlx::query_as::<_, EventRow>("SELECT id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, causation_id, idempotency_key, source, created_at FROM event_log WHERE stream_id = ? AND stream_version > ? ORDER BY stream_version")
            .bind(stream_id).bind(sv).fetch_all(&self.pool).await.map_err(map_err)?;
        Ok(rows.into_iter().map(|r| EventLogEntry { id: r.id, stream_id: r.stream_id, stream_version: r.stream_version as u32, event_type: r.event_type, payload_json: r.payload_json, schema_version: r.schema_version as u32, correlation_id: r.correlation_id, causation_id: r.causation_id, idempotency_key: r.idempotency_key, source: r.source, timestamp: r.created_at }).collect())
    }
    async fn get_by_correlation(&self, cid: &str) -> Result<Vec<EventLogEntry>, CoreError> {
        let rows = sqlx::query_as::<_, EventRow>("SELECT id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, causation_id, idempotency_key, source, created_at FROM event_log WHERE correlation_id = ? ORDER BY created_at")
            .bind(cid).fetch_all(&self.pool).await.map_err(map_err)?;
        Ok(rows.into_iter().map(|r| EventLogEntry { id: r.id, stream_id: r.stream_id, stream_version: r.stream_version as u32, event_type: r.event_type, payload_json: r.payload_json, schema_version: r.schema_version as u32, correlation_id: r.correlation_id, causation_id: r.causation_id, idempotency_key: r.idempotency_key, source: r.source, timestamp: r.created_at }).collect())
    }
}
#[derive(sqlx::FromRow)] struct EventRow { id: String, stream_id: String, stream_version: i64, event_type: String, payload_json: String, schema_version: i64, correlation_id: String, causation_id: Option<String>, idempotency_key: String, source: String, created_at: String }

// ── OperationRepository ───────────────────────────

pub struct SqlOperationRepo { pool: SqlitePool }
impl SqlOperationRepo { pub fn new(pool: SqlitePool) -> Self { Self { pool } } }

#[async_trait]
impl OperationRepository for SqlOperationRepo {
    async fn create_pending(&self, op: &OperationRecord) -> Result<(), CoreError> {
        sqlx::query("INSERT INTO operations (id, operation_id, operation_type, task_id, status, payload_json, idempotency_key) VALUES (?,?,?,?,?,?,?)")
            .bind(&op.id).bind(&op.operation_id).bind(&op.operation_type).bind(&op.task_id).bind("pending").bind(&op.payload_json).bind(&op.idempotency_key)
            .execute(&self.pool).await.map_err(map_err)?;
        Ok(())
    }
    async fn complete(&self, operation_id: &str, result_json: &str, version: u32) -> Result<(), CoreError> {
        let r = sqlx::query("UPDATE operations SET status = 'completed', result_json = ?, version = version + 1, completed_at = datetime('now') WHERE operation_id = ? AND status IN ('pending','running') AND version = ?")
            .bind(result_json).bind(operation_id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::PersistenceError, "operation not found or already completed", ErrorSource::System)); }
        Ok(())
    }
    async fn fail(&self, operation_id: &str, reason: &str, version: u32) -> Result<(), CoreError> {
        sqlx::query("UPDATE operations SET status = 'failed', result_json = ?, version = version + 1, completed_at = datetime('now') WHERE operation_id = ? AND status IN ('pending','running') AND version = ?")
            .bind(reason).bind(operation_id).bind(version).execute(&self.pool).await.map_err(map_err)?;
        Ok(())
    }
    async fn find_stale(&self, older_than_secs: u32) -> Result<Vec<OperationRecord>, CoreError> {
        let rows = sqlx::query_as::<_, OpRow>("SELECT id, operation_id, operation_type, task_id, status, payload_json, result_json, idempotency_key, started_at, completed_at FROM operations WHERE status IN ('pending','running') AND started_at < datetime('now', ?)")
            .bind(format!("-{older_than_secs} seconds")).fetch_all(&self.pool).await.map_err(map_err)?;
        Ok(rows.into_iter().map(|r| OperationRecord { id: r.id, operation_id: r.operation_id, operation_type: r.operation_type, task_id: r.task_id, status: r.status, payload_json: r.payload_json, result_json: r.result_json, version: 1, idempotency_key: r.idempotency_key, started_at: r.started_at, completed_at: r.completed_at }).collect())
    }
    async fn get_by_operation_id(&self, oid: &str) -> Result<Option<OperationRecord>, CoreError> {
        let row = sqlx::query_as::<_, OpRow>("SELECT id, operation_id, operation_type, task_id, status, payload_json, result_json, idempotency_key, started_at, completed_at FROM operations WHERE operation_id = ?")
            .bind(oid).fetch_optional(&self.pool).await.map_err(map_err)?;
        Ok(row.map(|r| OperationRecord { id: r.id, operation_id: r.operation_id, operation_type: r.operation_type, task_id: r.task_id, status: r.status, payload_json: r.payload_json, result_json: r.result_json, version: 1, idempotency_key: r.idempotency_key, started_at: r.started_at, completed_at: r.completed_at }))
    }
}
#[derive(sqlx::FromRow)] struct OpRow { id: String, operation_id: String, operation_type: String, task_id: String, status: String, payload_json: String, result_json: Option<String>, idempotency_key: String, started_at: String, completed_at: Option<String> }

// ── Helpers ───────────────────────────────────────

// Re-export row types for use by event_log and operation modules
pub mod event_row {
    #[derive(sqlx::FromRow)] pub struct EventRow { pub id: String, pub stream_id: String, pub stream_version: i64, pub event_type: String, pub payload_json: String, pub schema_version: i64, pub correlation_id: String, pub causation_id: Option<String>, pub idempotency_key: String, pub source: String, pub created_at: String }
}
pub mod op_row {
    #[derive(sqlx::FromRow)] pub struct OpRow { pub id: String, pub operation_id: String, pub operation_type: String, pub task_id: String, pub status: String, pub payload_json: String, pub result_json: Option<String>, pub idempotency_key: String, pub started_at: String, pub completed_at: Option<String> }
}

fn row_to_entry(r: event_row::EventRow) -> harness_core::contracts::repository::EventLogEntry {
    harness_core::contracts::repository::EventLogEntry { id: r.id, stream_id: r.stream_id, stream_version: r.stream_version as u32, event_type: r.event_type, payload_json: r.payload_json, schema_version: r.schema_version as u32, correlation_id: r.correlation_id, causation_id: r.causation_id, idempotency_key: r.idempotency_key, source: r.source, timestamp: r.created_at }
}

fn map_err(e: sqlx::Error) -> CoreError {
    CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System)
}

fn parse_pl(s: &str) -> ProjectLifecycle {
    serde_json::from_str(&format!("\"{s}\"")).unwrap_or(ProjectLifecycle::Created)
}

fn parse_tl(s: &str) -> TaskLifecycle {
    serde_json::from_str(&format!("\"{s}\"")).unwrap_or(TaskLifecycle::Pending)
}
