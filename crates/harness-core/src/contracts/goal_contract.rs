use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalContractVersion {
    pub version: u32,
    pub objective: String,
    pub deliverables: Vec<String>,
    pub acceptance: Vec<String>,
    pub constraints: Vec<String>,
    pub non_goals: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRequest {
    pub id: String,
    pub reason: String,
    pub new_goal_contract_version: u32,
    pub plan_revision: u32,
    pub status: String,
}
