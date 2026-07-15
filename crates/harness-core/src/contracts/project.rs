use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectLifecycle {
    Created,
    Clarifying,
    GoalLocked,
    Planning,
    AwaitingApproval,
    Active,
    Integrating,
    Verifying,
    Delivering,
    // Terminal
    Done,
    Cancelled,
    Failed,
}

impl ProjectLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub objective: String,
    pub lifecycle: ProjectLifecycle,
    pub goal_contract_version: Option<u32>,
    pub plan_version: Option<u32>,
}
