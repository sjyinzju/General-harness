pub mod commit_fsm;
pub mod execution_fsm;
pub mod integration_fsm;
pub mod lease_fsm;
pub mod project_fsm;
pub mod review_fsm;
pub mod task_fsm;

pub use commit_fsm::CommitFsm;
pub use execution_fsm::ExecutionFsm;
pub use integration_fsm::IntegrationFsm;
pub use lease_fsm::LeaseFsm;
pub use project_fsm::ProjectFsm;
pub use review_fsm::ReviewFsm;
pub use task_fsm::TaskFsm;

/// Execution Attempt lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLifecycle {
    Created,
    Running,
    // Terminal
    Completed,
    Failed,
    Lost,
    Cancelled,
}

impl ExecutionLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Lost | Self::Cancelled
        )
    }
}
