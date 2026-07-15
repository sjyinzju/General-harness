use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnvelope {
    pub task_id: String,
    pub project_id: String,
    pub task_goal: String,
    pub scope: FileScope,
    pub resource_claims: Vec<ResourceClaim>,
    pub dependencies: Vec<String>,
    pub acceptance_checks: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub output_schema: String,
    pub budget: TaskBudget,
    pub goal_contract_version: u32,
    pub plan_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileScope {
    pub allowed_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub readable_paths: Vec<String>,
    pub scope_expansion_allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceClaim {
    pub resource_type: String,
    pub resource_path: Option<String>,
    pub resource_name: Option<String>,
    pub access_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskBudget {
    pub max_turns: u32,
    pub max_time_ms: u64,
    pub max_cost_cents: Option<u32>,
}
