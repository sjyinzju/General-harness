pub mod execution_fsm;
pub mod lease_fsm;
pub mod project_fsm;
pub mod task_fsm;

pub use execution_fsm::ExecutionFsm;
pub use lease_fsm::LeaseFsm;
pub use project_fsm::ProjectFsm;
pub use task_fsm::TaskFsm;

/// Execution Attempt lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
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
