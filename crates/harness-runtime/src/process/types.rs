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
    /// Harness-owned artifact directory for spool files. Required when either
    /// capture policy is `Spool`. Must NEVER point into a user git worktree
    /// (create it via `crate::artifact::ArtifactRoot`).
    pub spool_dir: Option<PathBuf>,
    /// Known secret values of this execution (e.g. injected credential env
    /// values). Redacted from previews, errors, and tracing fields.
    pub known_secrets: Vec<String>,
    /// Environment variable NAMES that this profile is explicitly allowed to
    /// pass to the child process. Any override whose name is NOT in this set
    /// AND matches a sensitive pattern is rejected at spawn time.
    /// An empty list means "no overrides allowed" (defense-in-depth default).
    pub allowed_env_var_names: Vec<String>,
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
    /// Spool file reference (path) when the stream spilled to disk.
    pub stdout_ref: Option<String>,
    pub stderr_ref: Option<String>,
    /// True when the stream exceeded `output_byte_limit` (excess was drained
    /// but not stored).
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// Redacted, lossy-decoded head of the stream (bounded).
    pub stdout_preview: Option<String>,
    pub stderr_preview: Option<String>,
}

impl ProcessOutcome {
    /// Outcome skeleton before capture results are folded in.
    pub(crate) fn skeleton(
        termination: ProcessTermination,
        exit_code: Option<i32>,
        duration_ms: u64,
    ) -> Self {
        Self {
            termination,
            exit_code,
            stdout_bytes: 0,
            stderr_bytes: 0,
            duration_ms,
            stdout_ref: None,
            stderr_ref: None,
            stdout_truncated: false,
            stderr_truncated: false,
            stdout_preview: None,
            stderr_preview: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessTermination {
    Completed,
    NonZeroExit,
    Timeout,
    Cancelled,
    /// Process was killed (Job Object / taskkill) but the kill itself
    /// was NOT confirmed — the OS may still be running the process.
    /// Clean Cancelled/Timeout MUST NOT be written when termination
    /// is unconfirmed. Completion eligibility MUST be blocked.
    ProcessUnknown {
        reason: String,
    },
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
