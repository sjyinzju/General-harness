//! Task contract — v1 FROZEN (Gate C).

use serde::{Deserialize, Serialize};

/// Task lifecycle — 8 non-terminal + 4 terminal = 12 states.
/// Terminal states have NO successor transitions.
/// Retry creates a NEW Execution Attempt; old Executions are immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskLifecycle {
    /// Waiting for dependencies to be satisfied
    Pending,
    /// Dependencies satisfied, waiting for Scheduler
    Ready,
    /// Resources allocated (lease + claims), waiting for Agent start
    Dispatched,
    /// An Execution Attempt is currently running
    Running,
    /// Agent requested scope expansion or user input
    AwaitingInput,
    /// Waiting for retry after Execution failure (within max_retries)
    /// A new Execution Attempt will be created; old Execution is immutable.
    RetryPending,
    /// Agent returned TaskResult, pending verification
    Submitted,
    /// Verification passed
    Verified,
    // ── Terminal (no successor transitions) ──
    /// All work complete: committed + integrated
    Done,
    /// Cancelled by user or orchestrator
    Cancelled,
    /// Superseded by a newer task or change request
    Superseded,
    /// Permanently failed (max retries exhausted or unrecoverable)
    Failed,
}

impl TaskLifecycle {
    /// Terminal states have NO valid successor transitions.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Done | Self::Cancelled | Self::Superseded | Self::Failed
        )
    }

    /// Whether the owning Task can schedule a retry.
    /// This does NOT mean the current state can transition —
    /// it means a new Execution Attempt can be created for this Task.
    pub fn can_retry(&self) -> bool {
        // RetryPending is where we explicitly wait for retry
        // Running/Submitted/Verified/AwaitingInput may also need retry
        // if the Execution fails unexpectedly
        matches!(
            self,
            Self::RetryPending
                | Self::Running
                | Self::Submitted
                | Self::AwaitingInput
                | Self::Verified
        )
    }

    /// Whether this state implies an active Execution exists.
    pub fn has_active_execution(&self) -> bool {
        matches!(self, Self::Running | Self::AwaitingInput)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub project_id: String,
    pub goal: String,
    pub lifecycle: TaskLifecycle,
    pub dependencies: Vec<TaskDependency>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub current_execution_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    pub task_id: String,
}
