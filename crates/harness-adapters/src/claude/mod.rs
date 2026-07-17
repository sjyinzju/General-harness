//! Claude CLI Production Adapter — drives `claude` via ProcessManager.
//!
//! - spawns `claude -p --output-format stream-json --verbose`
//! - sends prompt to stdin as stream-json user message
//! - parses stdout JSONL → AgentEvent (see claude-cli-spike.md for mapping)
//! - synthesizes SessionEnded; preserves unknown events as RawVendorEvent
//! - all process lifecycle via ProcessManager (timeout, cancel, capture, redaction)

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
use harness_core::contracts::runtime_profile::{
    ActiveProbeChecks, ActiveValidationResult, RuntimeProfile,
};
use harness_core::contracts::task_envelope::TaskEnvelope;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::types::{CapturePolicy, ProcessSpec, ProcessState, StdinMode};
use tokio::sync::Mutex;
use tracing;

/// Max stdout bytes for stream parsing (1 MiB).
const MAX_OUTPUT_BYTES: usize = 1_024 * 1024;
/// Probe timeout for detect/version commands.
const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ClaudeCliAdapter {
    process_manager: Arc<ProcessManager>,
    /// Override executable path (from RuntimeProfile; otherwise PATH-discovered).
    executable_path: Option<PathBuf>,
}

impl ClaudeCliAdapter {
    pub fn new(process_manager: Arc<ProcessManager>) -> Self {
        Self {
            process_manager,
            executable_path: None,
        }
    }

    /// Set a specific executable path (from RuntimeProfile).
    pub fn with_executable(mut self, path: PathBuf) -> Self {
        self.executable_path = Some(path);
        self
    }

    /// Resolve the executable path: RuntimeProfile override > PATH discovery.
    fn resolve_exe(&self, profile: &RuntimeProfile) -> PathBuf {
        if let Some(ref exe) = self.executable_path {
            exe.clone()
        } else if !profile.executable_path.is_empty() {
            PathBuf::from(&profile.executable_path)
        } else {
            PathBuf::from("claude")
        }
    }

    /// Build the base Claude CLI args from a RuntimeProfile.
    /// Does NOT hardcode model, provider, or base URL.
    fn build_args(profile: &RuntimeProfile, opts: &SessionOptions) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "-p".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--verbose".to_string(),
            "--permission-mode".to_string(),
            "acceptEdits".to_string(),
        ];

        // Model override (from RuntimeProfile, not hardcoded)
        if let Some(ref model) = profile.model {
            if !model.is_empty() {
                args.push("--model".to_string());
                args.push(model.clone());
            }
        }
        // Or from SessionOptions model_override
        if let Some(ref model) = opts.model_override {
            if !model.is_empty() && profile.model.is_none() {
                args.push("--model".to_string());
                args.push(model.clone());
            }
        }

        // Resume session
        if let Some(ref resume_id) = opts.resume_session_id {
            if !resume_id.is_empty() {
                args.push("--resume".to_string());
                args.push(resume_id.clone());
            }
        }

        // Extra args (user-specified, not hardcoded)
        for extra in &opts.extra_args {
            args.push(extra.clone());
        }

        args
    }

    /// Build the stdin prompt JSON for Claude stream-json input.
    fn build_prompt_json(task_goal: &str) -> String {
        serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": task_goal
            }
        })
        .to_string()
    }

    /// Parse a single JSONL line from Claude stdout → Option<AgentEvent>.
    fn parse_line(line: &str, session_id: &str, profile_id: &str) -> Option<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return None;
        }

        let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                // Malformed JSON line → diagnostic, not silently dropped
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
            "system" => {
                let subtype = parsed["subtype"].as_str().unwrap_or("");
                if subtype == "init" {
                    let sid = parsed["session_id"]
                        .as_str()
                        .unwrap_or(session_id)
                        .to_string();
                    return Some(AgentEvent::SessionStarted {
                        session_id: sid,
                        profile_id: profile_id.to_string(),
                    });
                }
                // Other system events (thinking_tokens, etc.) → progress
                Some(AgentEvent::Progress {
                    summary: format!("system.{}", subtype),
                })
            }
            "assistant" => {
                let message = &parsed["message"];
                if let Some(content) = message["content"].as_array() {
                    for block in content {
                        match block["type"].as_str() {
                            Some("text") => {
                                if let Some(text) = block["text"].as_str() {
                                    return Some(AgentEvent::Message {
                                        content: text.to_string(),
                                        vendor_event_id: message["id"]
                                            .as_str()
                                            .map(|s| s.to_string()),
                                    });
                                }
                            }
                            Some("thinking") => {
                                if let Some(thinking) = block["thinking"].as_str() {
                                    return Some(AgentEvent::Progress {
                                        summary: thinking.to_string(),
                                    });
                                }
                            }
                            Some("tool_use") => {
                                let tool_name =
                                    block["name"].as_str().unwrap_or("unknown").to_string();
                                let tool_use_id =
                                    block["id"].as_str().unwrap_or("unknown").to_string();
                                let tool_input = block["input"].clone();
                                return Some(AgentEvent::ToolCallStarted {
                                    tool_name,
                                    tool_use_id,
                                    tool_input,
                                    vendor_event_id: message["id"].as_str().map(|s| s.to_string()),
                                });
                            }
                            _ => {
                                // Unknown content block within assistant → RawVendorEvent
                                return Some(AgentEvent::RawVendorEvent {
                                    raw_type: format!(
                                        "assistant.content.{}",
                                        block["type"].as_str().unwrap_or("unknown")
                                    ),
                                    payload: block.clone(),
                                });
                            }
                        }
                    }
                }
                // Assistant with no recognizable content
                Some(AgentEvent::RawVendorEvent {
                    raw_type: "assistant.unknown".to_string(),
                    payload: parsed.clone(),
                })
            }
            "user" => {
                // Tool result
                let message = &parsed["message"];
                if let Some(content) = message["content"].as_array() {
                    for block in content {
                        if block["type"].as_str() == Some("tool_result") {
                            let tool_use_id = block["tool_use_id"]
                                .as_str()
                                .unwrap_or("unknown")
                                .to_string();
                            let is_error = block["is_error"].as_bool().unwrap_or(false);
                            let content_preview =
                                block["content"].as_str().unwrap_or("").to_string();
                            return Some(AgentEvent::ToolCallCompleted {
                                tool_use_id,
                                is_error,
                                content_preview: content_preview[..content_preview.len().min(200)]
                                    .to_string(),
                            });
                        }
                    }
                }
                // User event without tool_result
                Some(AgentEvent::RawVendorEvent {
                    raw_type: "user.unknown".to_string(),
                    payload: parsed.clone(),
                })
            }
            "result" => {
                let content = parsed["content"].as_str().unwrap_or("").to_string();
                let is_error = parsed["is_error"].as_bool().unwrap_or(false);
                Some(AgentEvent::Result { content, is_error })
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
                // Unknown event type → RawVendorEvent (never silently dropped)
                Some(AgentEvent::RawVendorEvent {
                    raw_type: event_type.to_string(),
                    payload: parsed.clone(),
                })
            }
        }
    }
}

#[async_trait]
impl AgentAdapter for ClaudeCliAdapter {
    fn kind(&self) -> &'static str {
        "claude-cli"
    }

    async fn detect(&self, binary_path: Option<&Path>) -> Result<DetectionResult, CoreError> {
        let exe = match binary_path {
            Some(p) => p.to_path_buf(),
            None => self
                .executable_path
                .clone()
                .unwrap_or_else(|| PathBuf::from("claude")),
        };

        // Check if executable exists
        if !exe.is_file() && !exe.exists() {
            return Ok(DetectionResult {
                found: false,
                binary_path: Some(exe),
                error: Some("Executable not found".to_string()),
            });
        }

        // Probe: --version
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
            .unwrap_or_else(|| PathBuf::from("claude"));
        let exec_id = format!("claude-version-{}", uuid::Uuid::new_v4());

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
                format!("claude --version spawn: {e}"),
                ErrorSource::System,
            )
        })?;

        // Wait for completion
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
                                "claude --version: no output",
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
                            "claude --version timed out",
                            ErrorSource::System,
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    waited += 50;
                }
                _ => {
                    return Err(CoreError::new(
                        ErrorCode::ProcessExited { exit_code: -1 },
                        "claude --version: unexpected state",
                        ErrorSource::Agent,
                    ));
                }
            }
        }
    }

    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, CoreError> {
        // Claude CLI config: we only observe provider/model via env var names.
        // We do NOT read auth.json, API keys, or config files.
        let mut provider: Option<String> = None;
        let mut model: Option<String> = None;

        // Check env var names only (never values)
        if std::env::var("ANTHROPIC_BASE_URL").is_ok() {
            provider = Some("custom-anthropic-compatible".to_string());
        }
        if std::env::var("ANTHROPIC_MODEL").is_ok() {
            // Name only — do not read value
            model = Some("<from ANTHROPIC_MODEL>".to_string());
        }

        Ok(AgentConfigInfo {
            provider,
            base_url: None, // Never read the value
            model,
            auth_mode: if std::env::var("ANTHROPIC_API_KEY").is_ok() {
                "api_key_env".to_string()
            } else {
                "login".to_string()
            },
            config_file_path: None, // Do not expose config paths
            extra: HashMap::new(),
        })
    }

    async fn check_authentication(&self) -> Result<AuthCheckResult, CoreError> {
        // Check env var presence only (names, not values)
        let has_api_key = std::env::var("ANTHROPIC_API_KEY").is_ok();
        // We do NOT read auth.json or run login status commands
        Ok(AuthCheckResult {
            authenticated: has_api_key, // best-effort: key presence ≠ valid auth
            method: Some(
                if has_api_key {
                    "api_key_env"
                } else {
                    "unknown"
                }
                .to_string(),
            ),
            provider: Some("anthropic".to_string()),
            error: None,
        })
    }

    async fn probe(&self, _temp_dir: &Path) -> Result<ActiveValidationResult, CoreError> {
        // Active validation requires explicit user permission.
        // This is a placeholder — real active validation runs a minimal
        // smoke test after user approval.
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
            "ClaudeCliAdapter: starting session"
        );

        Ok(Box::new(ClaudeCliSession::new(
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

/// Active Claude CLI session wrapping a managed subprocess.
pub struct ClaudeCliSession {
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

impl ClaudeCliSession {
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
impl AgentSession for ClaudeCliSession {
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

        let exec_id = format!("claude-exec-{}", uuid::Uuid::new_v4());
        self.execution_id = Some(exec_id.clone());

        // Build stdin prompt
        let prompt_json = ClaudeCliAdapter::build_prompt_json(&envelope.task_goal);

        // Build ProcessSpec
        let spec = ProcessSpec {
            executable: self.executable.clone(),
            args: self.args.clone(),
            working_directory: self.working_directory.clone(),
            env_overrides: self.env.clone(),
            env_removals: vec![],
            stdin_mode: StdinMode::OneShot(prompt_json),
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
            known_secrets: vec![], // Caller can inject via env
            execution_id: exec_id.clone(),
            runtime_profile_id: self.profile_id.clone(),
        };

        let _handle = self.process_manager.spawn(&spec).await.map_err(|e| {
            CoreError::new(
                ErrorCode::ProcessSpawnFailed,
                format!("claude spawn: {e}"),
                ErrorSource::System,
            )
        })?;

        tracing::debug!(
            session_id = %self.session_id,
            execution_id = %exec_id,
            "Claude process spawned"
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

        let mut _seq: u64 = 0;
        let mut result_received = false;
        let mut process_exit_received = false;

        // Poll ProcessManager state
        loop {
            if !self.is_active() {
                break;
            }

            let state = self.process_manager.get_state(&exec_id).await;
            match state {
                Some(ProcessState::Completed { outcome }) => {
                    // Process completed — parse captured output
                    if let Some(ref stdout_ref) = outcome.stdout_ref {
                        // Read spool file and parse events
                        if let Ok(content) = tokio::fs::read_to_string(stdout_ref).await {
                            for line in content.lines() {
                                if let Some(event) = ClaudeCliAdapter::parse_line(
                                    line,
                                    &self.session_id,
                                    &self.profile_id,
                                ) {
                                    // Track terminal events
                                    match &event {
                                        AgentEvent::Result { .. } => {
                                            result_received = true;
                                        }
                                        AgentEvent::ProcessExited { .. } => {
                                            process_exit_received = true;
                                        }
                                        _ => {}
                                    }

                                    _seq += 1;
                                    if let Err(e) = sink.send(event).await {
                                        tracing::warn!(
                                            session_id = %self.session_id,
                                            error = %e,
                                            "Event sink error"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    } else if let Some(ref preview) = outcome.stdout_preview {
                        // Parse in-memory preview
                        for line in preview.lines() {
                            if let Some(event) = ClaudeCliAdapter::parse_line(
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
                                _seq += 1;
                                if let Err(e) = sink.send(event).await {
                                    tracing::warn!(
                                        session_id = %self.session_id,
                                        error = %e,
                                        "Event sink error"
                                    );
                                    break;
                                }
                            }
                        }
                    }

                    // Emit ProcessExited if not already in stream
                    if !process_exit_received {
                        let exit_code = outcome.exit_code.unwrap_or(-1);
                        _seq += 1;
                        let _ = sink
                            .send(AgentEvent::ProcessExited {
                                exit_code,
                                signal: None,
                            })
                            .await;
                    }

                    // Synthesize SessionEnded (exactly one terminal outcome)
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

                        _seq += 1;
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
                None => {
                    // Process registry entry gone
                    break;
                }
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
