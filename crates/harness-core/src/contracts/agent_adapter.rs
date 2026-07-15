//! AgentAdapter contract — CANDIDATE, not frozen.
//! Will be revised after Codex and Claude CLI spikes.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;

use super::agent_event::AgentEvent;
use super::runtime_profile::RuntimeProfile;
use super::task_envelope::TaskEnvelope;

#[derive(Debug)]
pub struct DetectionResult {
    pub found: bool,
    pub binary_path: Option<PathBuf>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct AgentConfigInfo {
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub auth_mode: String,
    pub config_file_path: Option<PathBuf>,
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub struct AuthCheckResult {
    pub authenticated: bool,
    pub method: Option<String>,
    pub provider: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct SessionOptions {
    pub working_directory: PathBuf,
    pub env: HashMap<String, String>,
    pub timeout: Duration,
    pub max_turns: Option<u32>,
    pub resume_session_id: Option<String>,
    pub model_override: Option<String>,
    pub effort_override: Option<String>,
    pub extra_args: Vec<String>,
}

/// Core Agent Adapter trait — CANDIDATE.
#[async_trait]
pub trait AgentAdapter: Send + Sync {
    fn kind(&self) -> &'static str;

    // Discovery
    async fn detect(
        &self,
        binary_path: Option<&std::path::Path>,
    ) -> Result<DetectionResult, String>;
    async fn get_version(&self) -> Result<String, String>;
    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, String>;
    async fn check_authentication(&self) -> Result<AuthCheckResult, String>;
    async fn probe(
        &self,
        temp_dir: &std::path::Path,
    ) -> Result<super::runtime_profile::ProbeResult, String>;

    // Execution
    async fn start_session(
        &self,
        profile: &RuntimeProfile,
        opts: &SessionOptions,
    ) -> Result<Box<dyn AgentSession>, String>;
}

/// Core Agent Session trait — CANDIDATE.
#[async_trait]
pub trait AgentSession: Send {
    fn session_id(&self) -> &str;
    fn is_active(&self) -> bool;

    async fn send_task(&mut self, envelope: &TaskEnvelope) -> Result<(), String>;
    /// Receive events via callback. Returns when session ends.
    async fn receive_events(
        &mut self,
        on_event: &(dyn Fn(AgentEvent) + Send + Sync),
    ) -> Result<(), String>;
    async fn interrupt(&self) -> Result<(), String>;
    async fn cancel(&self) -> Result<(), String>;
    async fn dispose(&mut self) -> Result<(), String>;
}
