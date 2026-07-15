use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfile {
    pub id: String,
    pub agent_kind: String,
    pub adapter_kind: String,
    pub agent_version: String,
    pub binary_path: String,
    pub provider: String,
    pub model: String,
    pub base_url: Option<String>,
    pub auth_mode: String,
    pub auth_state: String,
    pub capabilities: CapabilitySet,
    pub probe: Option<ProbeResult>,
    pub status: RuntimeProfileStatus,
    pub concurrency_max: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Required capabilities (all adapters MUST support).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequiredCapabilities {
    pub execute: bool,
    pub working_directory: bool,
    pub stream_output: bool,
    pub process_exit: bool,
    pub cancellation: bool,
    pub timeout: bool,
    pub final_result: bool,
}

/// Optional capabilities (detected via Probe).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionalCapabilities {
    pub native_session_resume: bool,
    pub structured_output: bool,
    pub tool_events: bool,
    pub file_change_events: bool,
    pub reasoning_summary: bool,
    pub interactive_approval: bool,
    pub usage_reporting: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitySet {
    pub required: RequiredCapabilities,
    pub optional: OptionalCapabilities,
    pub workspace_modes: Vec<String>,
    pub supported_languages: Vec<String>,
    pub mcp_tools: Vec<String>,
    pub supported_platforms: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub status: String,
    pub tested_at: Option<DateTime<Utc>>,
    pub checks: ProbeChecks,
    pub error_summary: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeChecks {
    pub read_repo: bool,
    pub create_file: bool,
    pub execute_test: bool,
    pub structured_output: bool,
    pub interrupt_and_resume: bool,
    pub budget_stop: bool,
    pub accepts_task_envelope: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeProfileStatus {
    Detected,
    Configured,
    Authenticated,
    Probed,
    Available,
    Degraded,
    Unavailable,
}
