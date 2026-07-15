use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskLifecycle {
    Pending,
    Ready,
    Dispatched,
    Running,
    AwaitingInput,
    Submitted,
    Verified,
    // Terminal
    Done,
    Cancelled,
    Superseded,
    Failed,
}

impl TaskLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Done | Self::Cancelled | Self::Superseded | Self::Failed
        )
    }

    pub fn allows_retry(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub project_id: String,
    pub goal: String,
    pub lifecycle: TaskLifecycle,
    pub dependencies: Vec<TaskDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    pub task_id: String,
}
