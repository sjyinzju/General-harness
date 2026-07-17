//! Codex CLI Production Adapter — drives `codex` via ProcessManager.
//!
//! - spawns `codex exec --json`
//! - sends prompt via stdin or --prompt
//! - parses stdout JSONL → AgentEvent (see codex-cli-spike.md for mapping)
//! - handles config compatibility diagnostics (service_tier, etc.)
//! - synthesizes SessionEnded; preserves unknown events as RawVendorEvent

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use harness_core::contracts::agent_adapter::{
    AgentAdapter, AgentConfigInfo, AgentEventSink, AgentSession, AuthCheckResult, DetectionResult,
    SessionOptions,
};
use harness_core::contracts::agent_event::{AgentEvent, TerminationReason};
use harness_core::contracts::discovery::{
    AdapterCompatibility, CompatibilityDiagnostic, DiagnosticLevel,
};
use harness_core::contracts::runtime_profile::{
    ActiveProbeChecks, ActiveValidationResult, RuntimeProfile,
};
use harness_core::contracts::task_envelope::TaskEnvelope;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::types::{CapturePolicy, ProcessSpec, ProcessState, StdinMode};
use tokio::sync::Mutex;
use tracing;

const MAX_OUTPUT_BYTES: usize = 1_024 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct CodexCliAdapter {
    process_manager: Arc<ProcessManager>,
    executable_path: Option<PathBuf>,
}

impl CodexCliAdapter {
    pub fn new(process_manager: Arc<ProcessManager>) -> Self {
        Self {
            process_manager,
            executable_path: None,
        }
    }

    pub fn with_executable(mut self, path: PathBuf) -> Self {
        self.executable_path = Some(path);
        self
    }

    fn resolve_exe(&self, profile: &RuntimeProfile) -> PathBuf {
        if let Some(ref exe) = self.executable_path {
            exe.clone()
        } else if !profile.executable_path.is_empty() {
            PathBuf::from(&profile.executable_path)
        } else {
            PathBuf::from("codex")
        }
    }

    /// Build Codex CLI args from RuntimeProfile.
    /// Does NOT hardcode model, service_tier, or provider.
    fn build_args(profile: &RuntimeProfile, opts: &SessionOptions) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "exec".to_string(),
            "--json".to_string(),
            "--full-auto".to_string(),
        ];

        // Model override (from RuntimeProfile or SessionOptions, not hardcoded)
        if let Some(ref model) = profile.model {
            if !model.is_empty() {
                args.push("-m".to_string());
                args.push(model.clone());
            }
        } else if let Some(ref model) = opts.model_override {
            if !model.is_empty() {
                args.push("-m".to_string());
                args.push(model.clone());
            }
        }

        // Working directory
        args.push("--cd".to_string());
        args.push(opts.working_directory.to_string_lossy().to_string());

        // Extra args from user
        for extra in &opts.extra_args {
            args.push(extra.clone());
        }

        args
    }

    /// Check compatibility of the Codex version with this adapter.
    /// Returns structured diagnostic — does NOT auto-modify user config.
    pub async fn check_compatibility(&self, profile: &RuntimeProfile) -> AdapterCompatibility {
        let exe = self.resolve_exe(profile);
        let version = self.probe_version(&exe).await;
        let mut diagnostics: Vec<CompatibilityDiagnostic> = Vec::new();

        if version.is_none() {
            diagnostics.push(CompatibilityDiagnostic {
                level: DiagnosticLevel::Error,
                category: "version".to_string(),
                message: "Could not determine Codex CLI version".to_string(),
                suggestion: Some("Ensure codex is installed and accessible in PATH".to_string()),
            });
        }

        // Check for config compatibility
        // We do NOT read config.toml — we check via `codex exec --help` output
        let help_output = self.probe_command(&exe, &["exec", "--help"]).await;
        let has_json_flag = help_output
            .as_ref()
            .map(|o| o.contains("--json"))
            .unwrap_or(false);

        if !has_json_flag {
            diagnostics.push(CompatibilityDiagnostic {
                level: DiagnosticLevel::Fatal,
                category: "capability".to_string(),
                message: "Codex CLI does not support --json flag".to_string(),
                suggestion: Some(
                    "Upgrade Codex CLI to a version that supports `codex exec --json`".to_string(),
                ),
            });
        }

        AdapterCompatibility {
            compatible: diagnostics
                .iter()
                .all(|d| d.level != DiagnosticLevel::Fatal),
            adapter_kind: "codex-cli".to_string(),
            agent_version: version.unwrap_or_else(|| "unknown".to_string()),
            diagnostics,
        }
    }

    async fn probe_version(&self, exe: &Path) -> Option<String> {
        self.probe_command(exe, &["--version"]).await.ok()
    }

    async fn probe_command(&self, exe: &Path, args: &[&str]) -> Result<String, ()> {
        let exec_id = format!("codex-probe-{}", uuid::Uuid::new_v4());
        let spec = ProcessSpec {
            executable: exe.to_path_buf(),
            args: args.iter().map(|s| s.to_string()).collect(),
            working_directory: std::env::temp_dir(),
            env_overrides: HashMap::new(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: PROBE_TIMEOUT,
            graceful_shutdown_timeout: Duration::from_secs(3),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 4096,
            },
            stderr_capture: CapturePolicy::Discard,
            output_byte_limit: 4096,
            spool_dir: None,
            known_secrets: vec![],
            execution_id: exec_id.clone(),
            runtime_profile_id: String::new(),
        };

        let _handle = self.process_manager.spawn(&spec).await.map_err(|_| ())?;

        let mut waited = 0;
        loop {
            let state = self.process_manager.get_state(&exec_id).await;
            match state {
                Some(ProcessState::Completed { outcome }) => {
                    return outcome.stdout_preview.map(|s| s.to_string()).ok_or(());
                }
                Some(ProcessState::Running) => {
                    if waited > PROBE_TIMEOUT.as_millis() as u64 {
                        let _ = self.process_manager.cancel(&exec_id).await;
                        return Err(());
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    waited += 50;
                }
                _ => return Err(()),
            }
        }
    }

    /// Parse a single JSONL line from Codex stdout → Option<AgentEvent>.
    fn parse_line(line: &str, session_id: &str, profile_id: &str) -> Option<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return None;
        }

        let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                return Some(AgentEvent::RawVendorEvent {
                    raw_type: "malformed_json".to_string(),
                    payload: serde_json::json!({
                        "raw_line_preview": &trimmed[..trimmed.len().min(256)],
                        "error": "failed to parse JSONL line"
                    }),
                });
            }
        };

        let event_type = parsed["type"].as_str().unwrap_or("unknown");

        match event_type {
            "thread.started" => {
                let thread_id = parsed["thread_id"]
                    .as_str()
                    .unwrap_or(session_id)
                    .to_string();
                Some(AgentEvent::SessionStarted {
                    session_id: thread_id,
                    profile_id: profile_id.to_string(),
                })
            }
            "turn.started" => {
                // Internal turn marker — emit as progress
                Some(AgentEvent::Progress {
                    summary: "Turn started".to_string(),
                })
            }
            "item.completed" => {
                let item = &parsed["item"];
                let item_type = item["type"].as_str().unwrap_or("unknown");
                match item_type {
                    "message" => {
                        if let Some(content) = item["message"].as_str() {
                            return Some(AgentEvent::Message {
                                content: content.to_string(),
                                vendor_event_id: item["id"].as_str().map(|s| s.to_string()),
                            });
                        }
                        None
                    }
                    "tool_use" => {
                        let tool_name = item["name"].as_str().unwrap_or("unknown").to_string();
                        let tool_use_id = item["id"].as_str().unwrap_or("unknown").to_string();
                        let tool_input = item
                            .get("input")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        Some(AgentEvent::ToolCallStarted {
                            tool_name,
                            tool_use_id,
                            tool_input,
                            vendor_event_id: item["id"].as_str().map(|s| s.to_string()),
                        })
                    }
                    "tool_result" => {
                        let tool_use_id = item["tool_use_id"]
                            .as_str()
                            .unwrap_or("unknown")
                            .to_string();
                        let is_error = item["is_error"].as_bool().unwrap_or(false);
                        let content_preview = item["content"].as_str().unwrap_or("").to_string();
                        Some(AgentEvent::ToolCallCompleted {
                            tool_use_id,
                            is_error,
                            content_preview: content_preview[..content_preview.len().min(200)]
                                .to_string(),
                        })
                    }
                    "error" => {
                        let message = item["message"]
                            .as_str()
                            .unwrap_or("Unknown error")
                            .to_string();
                        Some(AgentEvent::Error {
                            message,
                            code: item["code"].as_str().map(|s| s.to_string()),
                        })
                    }
                    _ => {
                        // Unknown item type → RawVendorEvent
                        Some(AgentEvent::RawVendorEvent {
                            raw_type: format!("item.{}", item_type),
                            payload: item.clone(),
                        })
                    }
                }
            }
            "turn.completed" => {
                let content = parsed["result"]
                    .as_str()
                    .unwrap_or("Turn completed")
                    .to_string();
                Some(AgentEvent::Result {
                    content,
                    is_error: false,
                })
            }
            "turn.failed" => {
                let message = parsed["error"]
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Turn failed")
                    .to_string();
                Some(AgentEvent::Result {
                    content: message,
                    is_error: true,
                })
            }
            "error" => {
                let message = parsed["message"]
                    .as_str()
                    .unwrap_or("Unknown error")
                    .to_string();
                Some(AgentEvent::Error {
                    message,
                    code: parsed["code"].as_str().map(|s| s.to_string()),
                })
            }
            _ => {
                // Unknown event → RawVendorEvent (never silently dropped)
                Some(AgentEvent::RawVendorEvent {
                    raw_type: event_type.to_string(),
                    payload: parsed.clone(),
                })
            }
        }
    }
}

#[async_trait]
impl AgentAdapter for CodexCliAdapter {
    fn kind(&self) -> &'static str {
        "codex-cli"
    }

    async fn detect(&self, binary_path: Option<&Path>) -> Result<DetectionResult, CoreError> {
        let exe = match binary_path {
            Some(p) => p.to_path_buf(),
            None => self
                .executable_path
                .clone()
                .unwrap_or_else(|| PathBuf::from("codex")),
        };

        if !exe.is_file() && !exe.exists() {
            return Ok(DetectionResult {
                found: false,
                binary_path: Some(exe),
                error: Some("Executable not found".to_string()),
            });
        }

        let version = self.get_version().await.ok();

        Ok(DetectionResult {
            found: true,
            binary_path: Some(exe),
            error: if version.is_none() {
                Some("Could not determine version".to_string())
            } else {
                None
            },
        })
    }

    async fn get_version(&self) -> Result<String, CoreError> {
        let exe = self
            .executable_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("codex"));
        let exec_id = format!("codex-version-{}", uuid::Uuid::new_v4());

        let spec = ProcessSpec {
            executable: exe,
            args: vec!["--version".to_string()],
            working_directory: std::env::temp_dir(),
            env_overrides: HashMap::new(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: PROBE_TIMEOUT,
            graceful_shutdown_timeout: Duration::from_secs(3),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 4096,
            },
            stderr_capture: CapturePolicy::Discard,
            output_byte_limit: 4096,
            spool_dir: None,
            known_secrets: vec![],
            execution_id: exec_id.clone(),
            runtime_profile_id: String::new(),
        };

        let _handle = self.process_manager.spawn(&spec).await.map_err(|e| {
            CoreError::new(
                ErrorCode::ProcessSpawnFailed,
                format!("codex --version spawn: {e}"),
                ErrorSource::System,
            )
        })?;

        let mut waited = 0;
        loop {
            let state = self.process_manager.get_state(&exec_id).await;
            match state {
                Some(ProcessState::Completed { outcome }) => {
                    return outcome
                        .stdout_preview
                        .map(|s| s.trim().to_string())
                        .ok_or_else(|| {
                            CoreError::new(
                                ErrorCode::ProtocolError,
                                "codex --version: no output",
                                ErrorSource::Agent,
                            )
                        });
                }
                Some(ProcessState::Running) => {
                    if waited > PROBE_TIMEOUT.as_millis() as u64 {
                        let _ = self.process_manager.cancel(&exec_id).await;
                        return Err(CoreError::new(
                            ErrorCode::ProcessTimeout {
                                duration_ms: PROBE_TIMEOUT.as_millis() as u64,
                            },
                            "codex --version timed out",
                            ErrorSource::System,
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    waited += 50;
                }
                _ => {
                    return Err(CoreError::new(
                        ErrorCode::ProcessExited { exit_code: -1 },
                        "codex --version: unexpected state",
                        ErrorSource::Agent,
                    ));
                }
            }
        }
    }

    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, CoreError> {
        // Codex config: observe env var names only, never values.
        // Check env presence for OpenAI API key (ChatGPT login users have no env var)
        let has_api_key = std::env::var("OPENAI_API_KEY").is_ok();

        Ok(AgentConfigInfo {
            provider: Some("openai".to_string()),
            base_url: None,
            model: None, // Not hardcoded — Codex determines model from config
            auth_mode: if has_api_key {
                "api_key_env".to_string()
            } else {
                "login".to_string() // ChatGPT login users
            },
            config_file_path: None,
            extra: {
                let mut extra = HashMap::new();
                extra.insert(
                    "chatgpt_login_compatible".to_string(),
                    serde_json::Value::Bool(!has_api_key),
                );
                extra
            },
        })
    }

    async fn check_authentication(&self) -> Result<AuthCheckResult, CoreError> {
        let has_api_key = std::env::var("OPENAI_API_KEY").is_ok();

        Ok(AuthCheckResult {
            authenticated: has_api_key, // best-effort: presence ≠ valid
            method: Some(if has_api_key { "api_key_env" } else { "login" }.to_string()),
            provider: Some("openai".to_string()),
            error: if !has_api_key {
                Some("OPENAI_API_KEY not set — may be using ChatGPT login".to_string())
            } else {
                None
            },
        })
    }

    async fn probe(&self, _temp_dir: &Path) -> Result<ActiveValidationResult, CoreError> {
        Ok(ActiveValidationResult {
            validated_at: chrono::Utc::now(),
            smoke_test_passed: false,
            checks: ActiveProbeChecks {
                execute: false,
                stream_output: false,
                final_result: false,
                cancellation: false,
                exit_code_correct: false,
            },
            duration_ms: 0,
        })
    }

    async fn start_session(
        &self,
        profile: &RuntimeProfile,
        opts: &SessionOptions,
    ) -> Result<Box<dyn AgentSession>, CoreError> {
        let exe = self.resolve_exe(profile);
        let args = Self::build_args(profile, opts);
        let session_id = uuid::Uuid::new_v4().to_string();

        tracing::info!(
            session_id = %session_id,
            executable = %exe.display(),
            args = ?args,
            "CodexCliAdapter: starting session"
        );

        Ok(Box::new(CodexCliSession::new(
            session_id,
            profile.id.clone(),
            exe,
            args,
            opts.working_directory.clone(),
            opts.env.clone(),
            opts.timeout,
            self.process_manager.clone(),
        )))
    }
}

pub struct CodexCliSession {
    session_id: String,
    profile_id: String,
    executable: PathBuf,
    args: Vec<String>,
    working_directory: PathBuf,
    env: HashMap<String, String>,
    timeout: Duration,
    process_manager: Arc<ProcessManager>,
    active: Arc<Mutex<bool>>,
    execution_id: Option<String>,
}

impl CodexCliSession {
    #[allow(clippy::too_many_arguments)]
    fn new(
        session_id: String,
        profile_id: String,
        executable: PathBuf,
        args: Vec<String>,
        working_directory: PathBuf,
        env: HashMap<String, String>,
        timeout: Duration,
        process_manager: Arc<ProcessManager>,
    ) -> Self {
        Self {
            session_id,
            profile_id,
            executable,
            args,
            working_directory,
            env,
            timeout,
            process_manager,
            active: Arc::new(Mutex::new(true)),
            execution_id: None,
        }
    }
}

#[async_trait]
impl AgentSession for CodexCliSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn is_active(&self) -> bool {
        *self.active.blocking_lock()
    }

    async fn send_task(&mut self, envelope: &TaskEnvelope) -> Result<(), CoreError> {
        if !self.is_active() {
            return Err(CoreError::new(
                ErrorCode::SinkClosed,
                "Session not active",
                ErrorSource::Agent,
            ));
        }

        let exec_id = format!("codex-exec-{}", uuid::Uuid::new_v4());
        self.execution_id = Some(exec_id.clone());

        // Codex: prompt goes as positional argument or via stdin
        let mut args = self.args.clone();
        args.push(envelope.task_goal.clone());

        let spec = ProcessSpec {
            executable: self.executable.clone(),
            args,
            working_directory: self.working_directory.clone(),
            env_overrides: self.env.clone(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout: self.timeout,
            graceful_shutdown_timeout: Duration::from_secs(5),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 64 * 1024,
            },
            stderr_capture: CapturePolicy::Spool {
                max_memory_bytes: 64 * 1024,
            },
            output_byte_limit: MAX_OUTPUT_BYTES,
            spool_dir: None,
            known_secrets: vec![],
            execution_id: exec_id.clone(),
            runtime_profile_id: self.profile_id.clone(),
        };

        let _handle = self.process_manager.spawn(&spec).await.map_err(|e| {
            CoreError::new(
                ErrorCode::ProcessSpawnFailed,
                format!("codex spawn: {e}"),
                ErrorSource::System,
            )
        })?;

        tracing::debug!(
            session_id = %self.session_id,
            execution_id = %exec_id,
            "Codex process spawned"
        );

        Ok(())
    }

    async fn receive_events(&mut self, sink: &mut dyn AgentEventSink) -> Result<(), CoreError> {
        let exec_id = self.execution_id.clone().ok_or_else(|| {
            CoreError::new(
                ErrorCode::ConfigMissing,
                "No execution started",
                ErrorSource::Harness,
            )
        })?;

        let mut result_received = false;
        let mut process_exit_received = false;

        loop {
            if !self.is_active() {
                break;
            }

            let state = self.process_manager.get_state(&exec_id).await;
            match state {
                Some(ProcessState::Completed { outcome }) => {
                    if let Some(ref stdout_ref) = outcome.stdout_ref {
                        if let Ok(content) = tokio::fs::read_to_string(stdout_ref).await {
                            for line in content.lines() {
                                if let Some(event) = CodexCliAdapter::parse_line(
                                    line,
                                    &self.session_id,
                                    &self.profile_id,
                                ) {
                                    match &event {
                                        AgentEvent::Result { .. } => result_received = true,
                                        AgentEvent::ProcessExited { .. } => {
                                            process_exit_received = true;
                                        }
                                        _ => {}
                                    }
                                    if let Err(e) = sink.send(event).await {
                                        tracing::warn!(
                                            session_id = %self.session_id,
                                            error = %e,
                                            "Codex event sink error"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    } else if let Some(ref preview) = outcome.stdout_preview {
                        for line in preview.lines() {
                            if let Some(event) = CodexCliAdapter::parse_line(
                                line,
                                &self.session_id,
                                &self.profile_id,
                            ) {
                                match &event {
                                    AgentEvent::Result { .. } => result_received = true,
                                    AgentEvent::ProcessExited { .. } => {
                                        process_exit_received = true;
                                    }
                                    _ => {}
                                }
                                if let Err(e) = sink.send(event).await {
                                    tracing::warn!(
                                        session_id = %self.session_id,
                                        error = %e,
                                        "Codex event sink error"
                                    );
                                    break;
                                }
                            }
                        }
                    }

                    if !process_exit_received {
                        let _ = sink
                            .send(AgentEvent::ProcessExited {
                                exit_code: outcome.exit_code.unwrap_or(-1),
                                signal: None,
                            })
                            .await;
                    }

                    {
                        let termination_reason = match outcome.termination {
                            harness_runtime::process::types::ProcessTermination::Completed => {
                                TerminationReason::Completed
                            }
                            harness_runtime::process::types::ProcessTermination::NonZeroExit => {
                                TerminationReason::ProcessExited {
                                    exit_code: outcome.exit_code.unwrap_or(1),
                                    signal: None,
                                }
                            }
                            harness_runtime::process::types::ProcessTermination::Timeout => {
                                TerminationReason::Timeout
                            }
                            harness_runtime::process::types::ProcessTermination::Cancelled => {
                                TerminationReason::Cancelled
                            }
                            harness_runtime::process::types::ProcessTermination::Killed => {
                                TerminationReason::Cancelled
                            }
                            harness_runtime::process::types::ProcessTermination::Lost => {
                                TerminationReason::Lost
                            }
                            _ => TerminationReason::Unknown,
                        };

                        let _ = sink
                            .send(AgentEvent::SessionEnded {
                                session_id: self.session_id.clone(),
                                synthetic: true,
                                termination_reason,
                                result_received,
                                process_exit_received: true,
                            })
                            .await;
                    }

                    break;
                }
                Some(ProcessState::Running) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                None => break,
                _ => break,
            }
        }

        *self.active.lock().await = false;
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), CoreError> {
        if let Some(ref exec_id) = self.execution_id {
            self.process_manager.cancel(exec_id).await.map_err(|e| {
                CoreError::new(
                    ErrorCode::ProcessCancelled,
                    format!("interrupt: {e}"),
                    ErrorSource::System,
                )
            })?;
        }
        *self.active.lock().await = false;
        Ok(())
    }

    async fn cancel(&self) -> Result<(), CoreError> {
        self.interrupt().await
    }

    async fn dispose(&mut self) -> Result<(), CoreError> {
        if let Some(ref exec_id) = self.execution_id {
            let _ = self.process_manager.cancel(exec_id).await;
        }
        *self.active.lock().await = false;
        Ok(())
    }
}
