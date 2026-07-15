//! Repository contracts — v1 FROZEN (Gate C).
//! Persistence boundaries. Implementations in harness-runtime.

use async_trait::async_trait;
use crate::error::CoreError;
use super::project::{Project, ProjectLifecycle};
use super::task::{Task, TaskLifecycle};
use super::workspace::WorkspaceLease;

// ── Project ──────────────────────────────────────

#[async_trait]
pub trait ProjectRepository: Send + Sync {
    async fn create(&self, project: &Project) -> Result<(), CoreError>;
    async fn get(&self, id: &str) -> Result<Option<Project>, CoreError>;
    async fn update_lifecycle(
        &self, id: &str, from: &ProjectLifecycle, to: &ProjectLifecycle,
        version: u32, idempotency_key: &str,
    ) -> Result<(), CoreError>;
    async fn list_non_terminal(&self) -> Result<Vec<Project>, CoreError>;
}

// ── Task ─────────────────────────────────────────

#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn create(&self, task: &Task) -> Result<(), CoreError>;
    async fn get(&self, id: &str) -> Result<Option<Task>, CoreError>;
    async fn update_lifecycle(
        &self, id: &str, from: &TaskLifecycle, to: &TaskLifecycle,
        version: u32, idempotency_key: &str,
    ) -> Result<(), CoreError>;
    async fn set_current_execution(&self, task_id: &str, execution_id: &str, version: u32) -> Result<(), CoreError>;
    async fn increment_retry_count(&self, id: &str, version: u32) -> Result<u32, CoreError>;
    async fn list_by_project(&self, project_id: &str) -> Result<Vec<Task>, CoreError>;
    async fn list_non_terminal_by_project(&self, project_id: &str) -> Result<Vec<Task>, CoreError>;
}

// ── Execution ────────────────────────────────────

#[async_trait]
pub trait ExecutionRepository: Send + Sync {
    async fn create(&self, execution: &ExecutionRecord) -> Result<(), CoreError>;
    async fn get(&self, id: &str) -> Result<Option<ExecutionRecord>, CoreError>;
    async fn update_lifecycle(
        &self, id: &str, to: &str, reason: Option<&str>,
        version: u32, idempotency_key: &str,
    ) -> Result<(), CoreError>;
    async fn list_by_task(&self, task_id: &str) -> Result<Vec<ExecutionRecord>, CoreError>;
    async fn get_active_for_task(&self, task_id: &str) -> Result<Option<ExecutionRecord>, CoreError>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionRecord {
    pub id: String,
    pub task_id: String,
    pub attempt_number: u32,
    pub lifecycle: String,
    pub profile_id: String,
    pub agent_session_id: Option<String>,
    pub native_session_id: Option<String>,
    pub pid: Option<u32>,
    pub version: u32,
    pub created_at: String,
    pub updated_at: String,
}

// ── WorkspaceLease ───────────────────────────────

#[async_trait]
pub trait WorkspaceLeaseRepository: Send + Sync {
    async fn acquire(&self, lease: &WorkspaceLease) -> Result<(), CoreError>;
    async fn get(&self, id: &str) -> Result<Option<WorkspaceLeaseRecord>, CoreError>;
    async fn release(&self, id: &str, version: u32) -> Result<(), CoreError>;
    async fn expire(&self, id: &str, version: u32) -> Result<(), CoreError>;
    async fn find_active_for_task(&self, task_id: &str) -> Result<Option<WorkspaceLeaseRecord>, CoreError>;
    async fn find_expired(&self) -> Result<Vec<WorkspaceLeaseRecord>, CoreError>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceLeaseRecord {
    pub id: String,
    pub task_id: String,
    pub lifecycle: String,
    pub worktree_path: String,
    pub branch_name: String,
    pub version: u32,
    pub acquired_at: String,
    pub expires_at: String,
    pub released_at: Option<String>,
}

// ── EventLog ─────────────────────────────────────

#[async_trait]
pub trait EventLogRepository: Send + Sync {
    async fn append(&self, events: &[EventLogEntry]) -> Result<(), CoreError>;
    async fn get_by_stream(&self, stream_id: &str, since_version: Option<u32>) -> Result<Vec<EventLogEntry>, CoreError>;
    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventLogEntry>, CoreError>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventLogEntry {
    pub id: String,
    pub stream_id: String,
    pub stream_version: u32,
    pub event_type: String,
    pub payload_json: String,
    pub schema_version: u32,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub idempotency_key: String,
    pub timestamp: String,
    pub source: String,
}

// ── Operation ────────────────────────────────────

#[async_trait]
pub trait OperationRepository: Send + Sync {
    async fn create_pending(&self, op: &OperationRecord) -> Result<(), CoreError>;
    async fn complete(&self, operation_id: &str, result_json: &str, version: u32) -> Result<(), CoreError>;
    async fn fail(&self, operation_id: &str, reason: &str, version: u32) -> Result<(), CoreError>;
    async fn find_stale(&self, older_than_secs: u32) -> Result<Vec<OperationRecord>, CoreError>;
    async fn get_by_operation_id(&self, operation_id: &str) -> Result<Option<OperationRecord>, CoreError>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OperationRecord {
    pub id: String,
    pub operation_id: String,
    pub operation_type: String,
    pub task_id: String,
    pub status: String,
    pub payload_json: String,
    pub result_json: Option<String>,
    pub version: u32,
    pub idempotency_key: String,
    pub started_at: String,
    pub completed_at: Option<String>,
}
