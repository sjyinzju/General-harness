use serde::{Deserialize, Serialize};

/// Worker Agent's declaration — NOT a fact until verified by Harness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub status: String,
    pub summary: String,
    pub changed_files: Vec<String>,
    pub checks: Vec<TaskResultCheck>,
    pub blockers: Vec<String>,
    pub risks: Vec<String>,
    pub proposed_followups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultCheck {
    pub command: String,
    pub exit_code: i32,
    pub output_ref: Option<String>,
}
