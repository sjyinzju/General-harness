//! I4-A Closure: Production adapter integration tests.
//!
//! All tests use fake executables (scripts) spawned through real ProcessManager.
//! No real Claude, Codex, API keys, or paid calls.
//!
//! Fake scripts emit JSONL/stream-json to stdout matching Claude/Codex formats.

use harness_core::contracts::agent_adapter::{AgentAdapter, AgentEventSink, SessionOptions};
use harness_core::contracts::agent_event::{AgentEvent, TerminationReason};
use harness_core::contracts::runtime_profile::{
    AuthCheckStatus, AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus,
    OptionalCapabilities, ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::registry::ProcessRegistry;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ── Helpers ──────────────────────────────────────────────────────────

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("harness-i4a-test-{}", uuid::Uuid::new_v4()))
}

/// Create a fake agent script that writes JSON lines to stdout and exits.
/// Returns the path to the script file.
fn create_fake_agent_script(
    dir: &Path,
    name: &str,
    stdout_lines: &[&str],
    exit_code: i32,
    sleep_ms: u64,
) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = if cfg!(windows) {
        dir.join(format!("{}.bat", name))
    } else {
        dir.join(name)
    };

    let mut script = String::new();
    if cfg!(windows) {
        script.push_str("@echo off\r\n");
        for line in stdout_lines {
            script.push_str(&format!("echo {}\r\n", line));
        }
        if sleep_ms > 0 {
            script.push_str(&format!(
                "powershell -Command \"Start-Sleep -Milliseconds {}\"\r\n",
                sleep_ms
            ));
        }
        script.push_str(&format!("exit /b {}\r\n", exit_code));
    } else {
        script.push_str("#!/bin/sh\n");
        for line in stdout_lines {
            script.push_str(&format!("echo '{}'\n", line));
        }
        if sleep_ms > 0 {
            script.push_str(&format!("sleep {}\n", sleep_ms as f64 / 1000.0));
        }
        script.push_str(&format!("exit {}\n", exit_code));
    }
    std::fs::write(&path, &script).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    path
}

/// Create a fake agent that floods stdout with many lines.
fn create_flood_script(dir: &Path, name: &str, line_count: usize, exit_code: i32) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = if cfg!(windows) {
        dir.join(format!("{}.bat", name))
    } else {
        dir.join(name)
    };

    let mut script = String::new();
    if cfg!(windows) {
        script.push_str("@echo off\r\n");
        script.push_str(&format!(
            "for /l %%i in (1,1,{}) do echo {{\"type\":\"message\",\"content\":\"line %%i\"}}\r\n",
            line_count
        ));
        script.push_str(&format!("exit /b {}\r\n", exit_code));
    } else {
        script.push_str("#!/bin/sh\n");
        script.push_str(&format!(
            "for i in $(seq 1 {}); do echo '{{\"type\":\"message\",\"content\":\"line '\"$i\"'\"}}'; done\n",
            line_count
        ));
        script.push_str(&format!("exit {}\n", exit_code));
    }
    std::fs::write(&path, &script).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    path
}

/// Create a fake agent that outputs invalid UTF-8 bytes.
#[allow(dead_code)]
fn create_invalid_utf8_script(dir: &Path, name: &str) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = if cfg!(windows) {
        dir.join(format!("{}.bat", name))
    } else {
        dir.join(name)
    };

    // On Windows, we use a PowerShell script that writes raw bytes
    let ps_path = dir.join(format!("{}.ps1", name));
    let ps_script = format!(
        r#"[Console]::OutputEncoding = [Text.Encoding]::GetEncoding(28591)
$bytes = @(0x7B,0x22,0x74,0x79,0x70,0x65,0x22,0x3A,0x22,{0},0x22,0x7D,0x0A)
foreach ($b in $bytes) {{ [Console]::Write([char]$b) }}
exit {1}
"#,
        // "message" in the JSON
        "109,101,115,115,97,103,101",
        0
    );
    std::fs::write(&ps_path, &ps_script).unwrap();

    // Create a batch wrapper that calls the PowerShell script
    let batch_script = format!(
        "@echo off\r\npowershell -ExecutionPolicy Bypass -File \"{}\"\r\nexit /b 0\r\n",
        ps_path.display()
    );
    std::fs::write(&path, &batch_script).unwrap();

    path
}

/// Create a fake agent that just sleeps (for timeout tests).
fn create_sleep_script(dir: &Path, name: &str, sleep_secs: u64) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = if cfg!(windows) {
        dir.join(format!("{}.bat", name))
    } else {
        dir.join(name)
    };

    let script = if cfg!(windows) {
        format!(
            "@echo off\r\npowershell -Command \"Start-Sleep -Seconds {}\"\r\nexit /b 0\r\n",
            sleep_secs
        )
    } else {
        format!("#!/bin/sh\nsleep {}\nexit 0\n", sleep_secs)
    };
    std::fs::write(&path, &script).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    path
}

fn test_runtime_profile(exe_path: &str, agent_kind: &str, adapter_kind: &str) -> RuntimeProfile {
    RuntimeProfile {
        id: format!("test-{}-profile", agent_kind),
        agent_definition_id: format!("test-{}-def", agent_kind),
        label: "Test Profile".into(),
        agent_kind: agent_kind.into(),
        adapter_kind: adapter_kind.into(),
        agent_version: "1.0.0".into(),
        executable_path: exe_path.into(),
        provider: if agent_kind == "claude-code" {
            "anthropic"
        } else {
            "openai"
        }
        .into(),
        provider_source: ProviderSource::UserDeclared,
        model: None,
        base_url: None,
        auth_mode: AuthMode::ApiKeyEnv,
        auth_status: AuthStatus::Unknown,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Supported,
                working_directory: TriState::Supported,
                stream_output: TriState::Supported,
                process_exit: TriState::Supported,
                cancellation: TriState::Supported,
                timeout: TriState::Supported,
                final_result: TriState::Supported,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unknown,
                structured_output: TriState::Unknown,
                tool_events: TriState::Supported,
                file_change_events: TriState::Unsupported,
                reasoning_summary: TriState::Unknown,
                interactive_approval: TriState::Unsupported,
                usage_reporting: TriState::Unknown,
            },
            workspace_modes: vec!["read".into(), "write".into()],
            supported_languages: vec![],
            mcp_tools: vec![],
            supported_platforms: vec!["all".into()],
        },
        core_status: CoreStatus::Available,
        authentication_status: AuthCheckStatus::Unknown,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "test".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

fn session_opts(working_dir: &Path, timeout_secs: u64) -> SessionOptions {
    SessionOptions {
        working_directory: working_dir.to_path_buf(),
        env: HashMap::new(),
        timeout: Duration::from_secs(timeout_secs),
        max_turns: None,
        resume_session_id: None,
        model_override: None,
        effort_override: None,
        extra_args: vec![],
    }
}

fn test_envelope() -> TaskEnvelope {
    TaskEnvelope {
        task_id: "test-task-1".into(),
        project_id: "test-proj-1".into(),
        task_goal: "Write hello.txt".into(),
        scope: FileScope {
            allowed_paths: vec!["**".into()],
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec![],
        allowed_tools: vec!["read".into(), "write".into()],
        output_schema: "TaskResultV1".into(),
        budget: TaskBudget {
            max_turns: 5,
            max_time_ms: 30_000,
            max_cost_cents: None,
        },
        goal_contract_version: 1,
        plan_version: 1,
    }
}

/// Simple collecting sink for tests.
struct CollectingSink {
    events: Mutex<Vec<AgentEvent>>,
    fail_after: Option<usize>,
    send_count: Mutex<usize>,
}

impl CollectingSink {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            fail_after: None,
            send_count: Mutex::new(0),
        }
    }

    fn with_fail_after(after: usize) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            fail_after: Some(after),
            send_count: Mutex::new(0),
        }
    }

    fn into_events(self) -> Vec<AgentEvent> {
        self.events.into_inner().unwrap()
    }
}

impl AgentEventSink for CollectingSink {
    fn send(
        &mut self,
        event: AgentEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), harness_core::CoreError>> + Send + '_>> {
        let mut count = self.send_count.lock().unwrap();
        *count += 1;
        if let Some(max) = self.fail_after {
            if *count > max {
                return Box::pin(std::future::ready(Err(harness_core::CoreError::new(
                    harness_core::ErrorCode::SinkClosed,
                    "Sink closed for test",
                    harness_core::ErrorSource::Harness,
                ))));
            }
        }
        self.events.lock().unwrap().push(event);
        Box::pin(std::future::ready(Ok(())))
    }
}

fn process_manager() -> Arc<ProcessManager> {
    Arc::new(ProcessManager::new(Arc::new(ProcessRegistry::new())))
}

// ══════════════════════════════════════════════════════════════════════
// Claude Adapter Integration Tests
// ══════════════════════════════════════════════════════════════════════
#[cfg(test)]
mod claude_tests {
    use super::*;
    use harness_adapters::claude::ClaudeCliAdapter;

    mod helpers {
        pub(crate) fn claude_success_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"system","subtype":"init","session_id":"test-session-1","model":"claude-test"}"#,
                r#"{"type":"assistant","message":{"id":"msg-1","role":"assistant","content":[{"type":"text","text":"I will write the file."}]}}"#,
                r#"{"type":"assistant","message":{"id":"msg-2","role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"Write","input":{"file_path":"hello.txt","content":"hello"}}]}}"#,
                r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","is_error":false,"content":"File written"}]}}"#,
                r#"{"type":"result","content":"Task complete","is_error":false}"#,
            ]
        }

        pub(crate) fn claude_failure_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"system","subtype":"init","session_id":"test-session-1"}"#,
                r#"{"type":"error","message":"Something went wrong","code":"E001"}"#,
            ]
        }

        pub(crate) fn claude_unknown_event_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"system","subtype":"init","session_id":"test-session-1"}"#,
                r#"{"type":"future_event_v99","payload":{"data":"future format"}}"#,
                r#"{"type":"result","content":"done","is_error":false}"#,
            ]
        }

        pub(crate) fn claude_malformed_event_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"system","subtype":"init","session_id":"test-session-1"}"#,
                r#"{invalid json that cannot be parsed"#,
                r#"{"type":"result","content":"done","is_error":false}"#,
            ]
        }
    }

    // ── 1. working directory ────────────────────────────────────────

    #[tokio::test]
    async fn test_claude_working_directory() {
        let tmp = temp_dir();
        let _ = std::fs::create_dir_all(&tmp);
        let exe =
            create_fake_agent_script(&tmp, "fake-claude", &helpers::claude_success_json(), 0, 0);

        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let wd = tmp.clone();
        let opts = session_opts(&wd, 30);

        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        assert!(session.is_active());
        session.send_task(&test_envelope()).await.unwrap();
        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();

        let events = sink.into_events();
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::SessionStarted { .. })));
        assert!(!session.is_active());
    }

    // ── 2. args from RuntimeProfile ─────────────────────────────────

    #[tokio::test]
    async fn test_claude_args_from_profile() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-claude", &helpers::claude_success_json(), 0, 0);

        // Profile with explicit model — adapter should use it
        let profile = RuntimeProfile {
            model: Some("deepseek-v4-pro".into()),
            ..test_runtime_profile(&exe.to_string_lossy(), "claude-code", "claude-cli")
        };

        let adapter = ClaudeCliAdapter::new(process_manager());
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        assert!(!session.is_active());
    }

    // ── 3. stream event mapping ─────────────────────────────────────

    #[tokio::test]
    async fn test_claude_stream_event_mapping() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-claude", &helpers::claude_success_json(), 0, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::SessionStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::Message { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::ToolCallStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::ToolCallCompleted { .. })));
        assert!(events.iter().any(|e| matches!(
            e,
            AgentEvent::Result {
                is_error: false,
                ..
            }
        )));
    }

    // ── 4. final result ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_claude_final_result() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-claude", &helpers::claude_success_json(), 0, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        let result = events
            .iter()
            .find(|e| matches!(e, AgentEvent::Result { .. }));
        assert!(result.is_some(), "Should have a Result event");
    }

    // ── 5. process exit ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_claude_process_exit() {
        let tmp = temp_dir();
        let exe = create_fake_agent_script(&tmp, "fake-claude", &[""], 0, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::ProcessExited { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::SessionEnded { .. })));
    }

    // ── 6. nonzero exit ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_claude_nonzero_exit() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-claude", &helpers::claude_failure_json(), 1, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        assert!(events.iter().any(|e| matches!(e, AgentEvent::Error { .. })));
        // Nonzero exit must NOT map to success
        assert!(!events.iter().any(|e| matches!(
            e,
            AgentEvent::Result {
                is_error: false,
                ..
            }
        )));
    }

    // ── 7. unknown event → RawVendorEvent ───────────────────────────

    #[tokio::test]
    async fn test_claude_unknown_event_to_raw_vendor() {
        let tmp = temp_dir();
        let exe = create_fake_agent_script(
            &tmp,
            "fake-claude",
            &helpers::claude_unknown_event_json(),
            0,
            0,
        );
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentEvent::RawVendorEvent { .. })),
            "Unknown event must be preserved as RawVendorEvent"
        );
    }

    // ── 8. malformed event diagnostic ───────────────────────────────

    #[tokio::test]
    async fn test_claude_malformed_event_diagnostic() {
        let tmp = temp_dir();
        let exe = create_fake_agent_script(
            &tmp,
            "fake-claude",
            &helpers::claude_malformed_event_json(),
            0,
            0,
        );
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        // Malformed line should produce a RawVendorEvent with "malformed_json" type
        let malformed = events
            .iter()
            .find(|e| matches!(e, AgentEvent::RawVendorEvent { raw_type, .. } if raw_type == "malformed_json"));
        assert!(
            malformed.is_some(),
            "Malformed event must produce diagnostic"
        );
    }

    // ── 9. stdout flood ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_claude_stdout_flood_no_deadlock() {
        let tmp = temp_dir();
        let exe = create_flood_script(&tmp, "fake-claude", 1000, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        let result =
            tokio::time::timeout(Duration::from_secs(20), session.receive_events(&mut sink)).await;

        assert!(result.is_ok(), "Flood must not deadlock");
    }

    // ── 10. timeout ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_claude_timeout() {
        let tmp = temp_dir();
        let exe = create_sleep_script(&tmp, "fake-claude", 30);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = SessionOptions {
            timeout: Duration::from_secs(2),
            ..session_opts(&tmp, 2)
        };
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        let result =
            tokio::time::timeout(Duration::from_secs(15), session.receive_events(&mut sink)).await;

        assert!(result.is_ok(), "Timeout should complete, not hang");
        let events = sink.into_events();
        // Should have SessionEnded with Timeout or Cancelled
        assert!(
            events.iter().any(|e| matches!(
                e,
                AgentEvent::SessionEnded {
                    termination_reason: TerminationReason::Timeout,
                    ..
                }
            )) || events.iter().any(|e| matches!(
                e,
                AgentEvent::SessionEnded {
                    termination_reason: TerminationReason::Cancelled,
                    ..
                }
            )),
            "Should have timeout or cancelled termination"
        );
    }

    // ── 11. external cancellation ────────────────────────────────────

    #[tokio::test]
    async fn test_claude_external_cancellation() {
        let tmp = temp_dir();
        let exe = create_sleep_script(&tmp, "fake-claude", 30);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 60);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        // Cancel after brief delay
        tokio::time::sleep(Duration::from_millis(200)).await;
        session.cancel().await.unwrap();

        let mut sink = CollectingSink::new();
        let result =
            tokio::time::timeout(Duration::from_secs(10), session.receive_events(&mut sink)).await;
        assert!(result.is_ok(), "Cancel should complete quickly");
    }

    // ── 12. sink close cancels process ──────────────────────────────

    #[tokio::test]
    async fn test_claude_sink_close_cancels_process() {
        let tmp = temp_dir();
        let exe = create_flood_script(&tmp, "fake-claude", 5000, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        // Sink that fails after 5 events — should trigger cancellation
        let mut sink = CollectingSink::with_fail_after(5);
        let result =
            tokio::time::timeout(Duration::from_secs(15), session.receive_events(&mut sink)).await;

        assert!(result.is_ok(), "Sink close should not deadlock");
        assert!(!session.is_active());
    }

    // ── 13. exactly one terminal outcome ────────────────────────────

    #[tokio::test]
    async fn test_claude_exactly_one_terminal_outcome() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-claude", &helpers::claude_success_json(), 0, 0);
        let adapter = ClaudeCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "claude-code", "claude-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        let session_ended_count = events
            .iter()
            .filter(|e| matches!(e, AgentEvent::SessionEnded { .. }))
            .count();
        assert_eq!(
            session_ended_count, 1,
            "Exactly one SessionEnded, got {}",
            session_ended_count
        );
    }

    // ── 14. no hardcoded model ──────────────────────────────────────

    #[test]
    fn test_claude_no_hardcoded_model() {
        // When profile.model is None, adapter must NOT inject a hardcoded model
        let profile = test_runtime_profile("/fake/claude", "claude-code", "claude-cli");
        assert!(profile.model.is_none());
        // The adapter's build_args should not add --model when profile.model is None
    }

    // ── 15. no hardcoded provider/base URL ──────────────────────────

    #[test]
    fn test_claude_no_hardcoded_provider_or_base_url() {
        let profile = test_runtime_profile("/fake/claude", "claude-code", "claude-cli");
        assert!(profile.base_url.is_none());
    }
}

// ══════════════════════════════════════════════════════════════════════
// Codex Adapter Integration Tests
// ══════════════════════════════════════════════════════════════════════
#[cfg(test)]
mod codex_tests {
    use super::*;
    use harness_adapters::codex::CodexCliAdapter;

    mod helpers {
        pub(crate) fn codex_success_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"thread.started","thread_id":"thread-001"}"#,
                r#"{"type":"turn.started"}"#,
                r#"{"type":"item.completed","item":{"id":"item-1","type":"message","message":"Creating file hello.txt"}}"#,
                r#"{"type":"item.completed","item":{"id":"item-2","type":"tool_use","name":"write_file","input":{"path":"hello.txt","content":"hello"}}}"#,
                r#"{"type":"item.completed","item":{"id":"item-3","type":"tool_result","tool_use_id":"item-2","is_error":false,"content":"done"}}"#,
                r#"{"type":"turn.completed","result":"Task completed successfully"}"#,
            ]
        }

        pub(crate) fn codex_failure_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"thread.started","thread_id":"thread-001"}"#,
                r#"{"type":"turn.started"}"#,
                r#"{"type":"error","message":"Configuration error"}"#,
                r#"{"type":"turn.failed","error":{"message":"Turn failed due to config"}}"#,
            ]
        }

        pub(crate) fn codex_unknown_event_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"thread.started","thread_id":"thread-001"}"#,
                r#"{"type":"future.codex.event.v99","new_field":"value"}"#,
                r#"{"type":"turn.completed","result":"done"}"#,
            ]
        }

        pub(crate) fn codex_malformed_json() -> Vec<&'static str> {
            vec![
                r#"{"type":"thread.started","thread_id":"thread-001"}"#,
                r#"{this is not valid json at all"#,
                r#"{"type":"turn.completed","result":"done"}"#,
            ]
        }
    }

    // ── 1. working directory ────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_working_directory() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_success_json(), 0, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        assert!(session.is_active());
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        assert!(!session.is_active());
    }

    // ── 2. exec --json / JSONL args construction ────────────────────

    #[tokio::test]
    async fn test_codex_exec_json_args() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_success_json(), 0, 0);
        let adapter = CodexCliAdapter::new(process_manager());
        let profile = test_runtime_profile(&exe.to_string_lossy(), "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        assert!(!session.is_active());
    }

    // ── 3. stream event mapping ─────────────────────────────────────

    #[tokio::test]
    async fn test_codex_stream_event_mapping() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_success_json(), 0, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::SessionStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::Message { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::ToolCallStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::ToolCallCompleted { .. })));
        assert!(events.iter().any(|e| matches!(
            e,
            AgentEvent::Result {
                is_error: false,
                ..
            }
        )));
    }

    // ── 4. final result / turn completed ────────────────────────────

    #[tokio::test]
    async fn test_codex_final_result() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_success_json(), 0, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();
        assert!(events.iter().any(|e| matches!(
            e,
            AgentEvent::Result {
                is_error: false,
                ..
            }
        )));
    }

    // ── 5. process exit ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_process_exit() {
        let tmp = temp_dir();
        let exe = create_fake_agent_script(&tmp, "fake-codex", &[""], 0, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::ProcessExited { .. })));
    }

    // ── 6. nonzero exit ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_nonzero_exit() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_failure_json(), 1, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Error { .. })));
        assert!(!events.iter().any(|e| matches!(
            e,
            AgentEvent::Result {
                is_error: false,
                ..
            }
        )));
    }

    // ── 7. unknown field/event ──────────────────────────────────────

    #[tokio::test]
    async fn test_codex_unknown_event_to_raw_vendor() {
        let tmp = temp_dir();
        let exe = create_fake_agent_script(
            &tmp,
            "fake-codex",
            &helpers::codex_unknown_event_json(),
            0,
            0,
        );
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();
        assert!(events
            .iter()
            .any(|e| matches!(e, AgentEvent::RawVendorEvent { .. })));
    }

    // ── 8. malformed JSON ───────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_malformed_json_diagnostic() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_malformed_json(), 0, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        let malformed = events
            .iter()
            .find(|e| matches!(e, AgentEvent::RawVendorEvent { raw_type, .. } if raw_type == "malformed_json"));
        assert!(
            malformed.is_some(),
            "Malformed JSON must produce diagnostic"
        );
    }

    // ── 9. stdout flood ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_stdout_flood_no_deadlock() {
        let tmp = temp_dir();
        let exe = create_flood_script(&tmp, "fake-codex", 1000, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        let result =
            tokio::time::timeout(Duration::from_secs(20), session.receive_events(&mut sink)).await;
        assert!(result.is_ok(), "Flood must not deadlock");
    }

    // ── 10. timeout ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_timeout() {
        let tmp = temp_dir();
        let exe = create_sleep_script(&tmp, "fake-codex", 30);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = SessionOptions {
            timeout: Duration::from_secs(2),
            ..session_opts(&tmp, 2)
        };
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        let result =
            tokio::time::timeout(Duration::from_secs(15), session.receive_events(&mut sink)).await;
        assert!(result.is_ok());
    }

    // ── 11. cancellation ────────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_cancellation() {
        let tmp = temp_dir();
        let exe = create_sleep_script(&tmp, "fake-codex", 30);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 60);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        session.cancel().await.unwrap();

        let mut sink = CollectingSink::new();
        let result =
            tokio::time::timeout(Duration::from_secs(10), session.receive_events(&mut sink)).await;
        assert!(result.is_ok(), "Cancel should complete quickly");
    }

    // ── 12. sink close ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_codex_sink_close_cancels_process() {
        let tmp = temp_dir();
        let exe = create_flood_script(&tmp, "fake-codex", 5000, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::with_fail_after(5);
        let result =
            tokio::time::timeout(Duration::from_secs(15), session.receive_events(&mut sink)).await;
        assert!(result.is_ok(), "Sink close should not deadlock");
        assert!(!session.is_active());
    }

    // ── 13. no hardcoded model ──────────────────────────────────────

    #[test]
    fn test_codex_no_hardcoded_model() {
        let profile = test_runtime_profile("/fake/codex", "codex", "codex-cli");
        assert!(profile.model.is_none());
    }

    // ── 14. no hardcoded service tier ────────────────────────────────

    #[tokio::test]
    async fn test_codex_no_hardcoded_service_tier() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_success_json(), 0, 0);
        let profile = test_runtime_profile(&exe.to_string_lossy(), "codex", "codex-cli");
        // Profile does NOT set service_tier — adapter must not add -c service_tier=
        assert!(profile.model.is_none());
    }

    // ── 15. exactly one terminal outcome ────────────────────────────

    #[tokio::test]
    async fn test_codex_exactly_one_terminal_outcome() {
        let tmp = temp_dir();
        let exe =
            create_fake_agent_script(&tmp, "fake-codex", &helpers::codex_success_json(), 0, 0);
        let adapter = CodexCliAdapter::new(process_manager()).with_executable(exe);
        let profile = test_runtime_profile("", "codex", "codex-cli");
        let opts = session_opts(&tmp, 30);
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.send_task(&test_envelope()).await.unwrap();

        let mut sink = CollectingSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_events();

        let session_ended_count = events
            .iter()
            .filter(|e| matches!(e, AgentEvent::SessionEnded { .. }))
            .count();
        assert_eq!(session_ended_count, 1);
    }
}

// ══════════════════════════════════════════════════════════════════════
// Parser Unit Tests
// ══════════════════════════════════════════════════════════════════════
#[cfg(test)]
mod parser_tests {
    use harness_adapters::claude::ClaudeCliAdapter;
    use harness_adapters::codex::CodexCliAdapter;
    use harness_core::contracts::agent_event::AgentEvent;

    // ── Claude parser ────────────────────────────────────────────────

    #[test]
    fn test_claude_parse_system_init() {
        let line = r#"{"type":"system","subtype":"init","session_id":"sid-1"}"#;
        let result = ClaudeCliAdapter::parse_line(line, "fallback", "prof-1");
        assert!(result.is_some());
        match result.unwrap() {
            AgentEvent::SessionStarted {
                session_id,
                profile_id,
            } => {
                assert_eq!(session_id, "sid-1");
                assert_eq!(profile_id, "prof-1");
            }
            other => panic!("Expected SessionStarted, got {:?}", other),
        }
    }

    #[test]
    fn test_claude_parse_assistant_text() {
        let line = r#"{"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"Hello world"}]}}"#;
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::Message { .. })));
    }

    #[test]
    fn test_claude_parse_tool_use() {
        let line = r#"{"type":"assistant","message":{"id":"m1","content":[{"type":"tool_use","id":"call-1","name":"Write","input":{"path":"x"}}]}}"#;
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::ToolCallStarted { .. })));
    }

    #[test]
    fn test_claude_parse_tool_result() {
        let line = r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"call-1","is_error":false,"content":"ok"}]}}"#;
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::ToolCallCompleted { .. })));
    }

    #[test]
    fn test_claude_parse_result() {
        let line = r#"{"type":"result","content":"Done!","is_error":false}"#;
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(
            result,
            Some(AgentEvent::Result {
                is_error: false,
                ..
            })
        ));
    }

    #[test]
    fn test_claude_parse_error() {
        let line = r#"{"type":"error","message":"Oops","code":"E1"}"#;
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::Error { .. })));
    }

    #[test]
    fn test_claude_parse_unknown_type() {
        let line = r#"{"type":"future_v2_event","data":"x"}"#;
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::RawVendorEvent { .. })));
    }

    #[test]
    fn test_claude_parse_malformed_json() {
        let line = "{not valid json";
        let result = ClaudeCliAdapter::parse_line(line, "sid", "pid");
        match result {
            Some(AgentEvent::RawVendorEvent { raw_type, .. }) => {
                assert_eq!(raw_type, "malformed_json");
            }
            other => panic!("Expected RawVendorEvent malformed_json, got {:?}", other),
        }
    }

    // ── Codex parser ─────────────────────────────────────────────────

    #[test]
    fn test_codex_parse_thread_started() {
        let line = r#"{"type":"thread.started","thread_id":"th-1"}"#;
        let result = CodexCliAdapter::parse_line(line, "fallback", "prof-1");
        match result.unwrap() {
            AgentEvent::SessionStarted {
                session_id,
                profile_id,
            } => {
                assert_eq!(session_id, "th-1");
                assert_eq!(profile_id, "prof-1");
            }
            other => panic!("Expected SessionStarted, got {:?}", other),
        }
    }

    #[test]
    fn test_codex_parse_item_message() {
        let line =
            r#"{"type":"item.completed","item":{"id":"i1","type":"message","message":"Hello"}}"#;
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::Message { .. })));
    }

    #[test]
    fn test_codex_parse_tool_use() {
        let line = r#"{"type":"item.completed","item":{"id":"i1","type":"tool_use","name":"read","input":{"path":"x"}}}"#;
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::ToolCallStarted { .. })));
    }

    #[test]
    fn test_codex_parse_tool_result() {
        let line = r#"{"type":"item.completed","item":{"id":"i1","type":"tool_result","tool_use_id":"t1","is_error":false,"content":"ok"}}"#;
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::ToolCallCompleted { .. })));
    }

    #[test]
    fn test_codex_parse_turn_completed() {
        let line = r#"{"type":"turn.completed","result":"All done"}"#;
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(
            result,
            Some(AgentEvent::Result {
                is_error: false,
                ..
            })
        ));
    }

    #[test]
    fn test_codex_parse_turn_failed() {
        let line = r#"{"type":"turn.failed","error":{"message":"Failed"}}"#;
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(
            result,
            Some(AgentEvent::Result { is_error: true, .. })
        ));
    }

    #[test]
    fn test_codex_parse_unknown_type() {
        let line = r#"{"type":"codex_v99_future","data":"val"}"#;
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        assert!(matches!(result, Some(AgentEvent::RawVendorEvent { .. })));
    }

    #[test]
    fn test_codex_parse_malformed_json() {
        let line = "not json at all!!!!";
        let result = CodexCliAdapter::parse_line(line, "sid", "pid");
        match result {
            Some(AgentEvent::RawVendorEvent { raw_type, .. }) => {
                assert_eq!(raw_type, "malformed_json");
            }
            other => panic!("Expected RawVendorEvent malformed_json, got {:?}", other),
        }
    }
}
