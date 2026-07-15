//! Process types — runtime-owned, no core dependency beyond error types.
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ProcessSpec {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub working_directory: PathBuf,
    pub env_overrides: HashMap<String, String>,
    pub env_removals: Vec<String>,
    pub stdin_mode: StdinMode,
    pub timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub stdout_capture: CapturePolicy,
    pub stderr_capture: CapturePolicy,
    pub output_byte_limit: usize,
    pub execution_id: String,
    pub runtime_profile_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StdinMode {
    Closed,
    Pipe,
    OneShot(String),
}

#[derive(Debug, Clone)]
pub enum CapturePolicy {
    Pipe,
    Spool { max_memory_bytes: usize },
    Discard,
}

#[derive(Debug, Clone)]
pub struct ProcessHandle {
    pub execution_id: String,
    pub pid: u32,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub state: std::sync::Arc<tokio::sync::RwLock<ProcessState>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessState {
    Starting,
    Running,
    Completed { outcome: ProcessOutcome },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessOutcome {
    pub termination: ProcessTermination,
    pub exit_code: Option<i32>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub duration_ms: u64,
    pub stdout_ref: Option<String>,
    pub stderr_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessTermination {
    Completed,
    NonZeroExit,
    Timeout,
    Cancelled,
    Killed,
    SpawnFailed,
    Lost,
}

#[derive(Debug, Clone)]
pub struct ProcessEvent {
    pub execution_id: String,
    pub receive_sequence: u64,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub stream: StreamKind,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamKind {
    Stdout,
    Stderr,
    System,
}

#[derive(Debug, Clone)]
pub struct ProcessControl {
    pub execution_id: String,
    pub cancel_token: tokio_util::sync::CancellationToken,
}

#[derive(Debug, Clone)]
pub struct ProcessLimits {
    pub max_output_bytes: usize,
    pub max_runtime: Duration,
}
