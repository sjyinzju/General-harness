use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseLifecycle {
    Acquired,
    Active,
    // Terminal
    Released,
    Expired,
}

impl LeaseLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Released | Self::Expired)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceLease {
    pub id: String,
    pub task_id: String,
    pub lifecycle: LeaseLifecycle,
    pub worktree_path: String,
    pub branch_name: String,
}
