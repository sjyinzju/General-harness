use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use harness_core::contracts::agent_adapter::{
    AgentAdapter, AgentConfigInfo, AgentSession, AuthCheckResult, DetectionResult, SessionOptions,
};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::{
    ActiveProbeChecks, ActiveValidationResult, RuntimeProfile, TriState,
};
use harness_core::contracts::task_envelope::TaskEnvelope;

use super::script::FakeExecutionScript;

pub struct FakeAgentAdapter {
    script: Arc<Mutex<Option<FakeExecutionScript>>>,
}

impl FakeAgentAdapter {
    pub fn new() -> Self {
        Self { script: Arc::new(Mutex::new(None)) }
    }
    pub fn set_script(&self, script: FakeExecutionScript) {
        *self.script.lock().unwrap() = Some(script);
    }
}

impl Default for FakeAgentAdapter {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl AgentAdapter for FakeAgentAdapter {
    fn kind(&self) -> &'static str { "fake" }

    async fn detect(&self, _binary_path: Option<&Path>) -> Result<DetectionResult, String> {
        Ok(DetectionResult { found: true, binary_path: Some(PathBuf::from("fake-adapter")), error: None })
    }

    async fn get_version(&self) -> Result<String, String> { Ok("fake-1.0.0".into()) }

    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, String> {
        Ok(AgentConfigInfo {
            provider: Some("fake".into()), base_url: None, model: Some("fake-model".into()),
            auth_mode: "none".into(), config_file_path: None, extra: HashMap::new(),
        })
    }

    async fn check_authentication(&self) -> Result<AuthCheckResult, String> {
        Ok(AuthCheckResult { authenticated: true, method: Some("none".into()), provider: Some("fake".into()), error: None })
    }

    async fn probe(&self, _temp_dir: &Path) -> Result<ActiveValidationResult, String> {
        Ok(ActiveValidationResult {
            validated_at: chrono::Utc::now(),
            smoke_test_passed: true,
            checks: ActiveProbeChecks {
                execute: true, stream_output: true, final_result: true,
                cancellation: true, exit_code_correct: true,
            },
            duration_ms: 5,
        })
    }

    async fn start_session(&self, profile: &RuntimeProfile, _opts: &SessionOptions) -> Result<Box<dyn AgentSession>, String> {
        let script = self.script.lock().unwrap().clone();
        Ok(Box::new(FakeAgentSession::new(profile.id.clone(), script)))
    }
}

struct FakeAgentSession {
    _profile_id: String,
    session_id: String,
    script: Option<FakeExecutionScript>,
    active: Arc<AtomicBool>,
}

impl FakeAgentSession {
    fn new(profile_id: String, script: Option<FakeExecutionScript>) -> Self {
        Self { session_id: uuid::Uuid::new_v4().to_string(), _profile_id: profile_id, script, active: Arc::new(AtomicBool::new(true)) }
    }
}

#[async_trait]
impl AgentSession for FakeAgentSession {
    fn session_id(&self) -> &str { &self.session_id }
    fn is_active(&self) -> bool { self.active.load(Ordering::SeqCst) }

    async fn send_task(&mut self, _envelope: &TaskEnvelope) -> Result<(), String> {
        if !self.is_active() { return Err("Session not active".into()); }
        Ok(())
    }

    async fn receive_events(&mut self, sink: &mut dyn harness_core::contracts::agent_adapter::AgentEventSink) -> Result<(), String> {
        let script = self.script.as_ref().ok_or("No script configured")?.clone();
        for file_op in &script.files_to_create {
            if let Some(parent) = file_op.path.parent() { let _ = std::fs::create_dir_all(parent); }
            std::fs::write(&file_op.path, &file_op.content).map_err(|e| format!("FakeAgent: {e}"))?;
        }
        for (i, event) in script.events.iter().enumerate() {
            if let Some(ref failure) = script.failure {
                if i > failure.after_event_index {
                    self.active.store(false, Ordering::SeqCst);
                    return Err(failure.error_message.clone());
                }
            }
            sink.send(event.clone()).await?;
        }
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), String> { self.active.store(false, Ordering::SeqCst); Ok(()) }
    async fn cancel(&self) -> Result<(), String> { self.active.store(false, Ordering::SeqCst); Ok(()) }
    async fn dispose(&mut self) -> Result<(), String> { self.active.store(false, Ordering::SeqCst); Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::runtime_profile::{
        AuthMode, AuthStatus, CapabilitySet, CoreStatus, CredentialReference,
        ExecutionStatus, OptionalCapabilities, ProviderSource, RequiredCapabilities,
    };
    use harness_core::contracts::agent_adapter::SessionOptions;
    use std::time::Duration;

    fn fake_profile() -> RuntimeProfile {
        RuntimeProfile {
            id: "fake-1".into(), agent_definition_id: "fake-def-1".into(),
            label: "Fake Agent".into(), agent_kind: "fake".into(),
            adapter_kind: "fake".into(), agent_version: "1.0.0".into(),
            executable_path: "fake".into(), provider: "fake".into(),
            provider_source: ProviderSource::UserDeclared, model: None, base_url: None,
            auth_mode: AuthMode::None, auth_status: AuthStatus::Authenticated,
            credential_ref: None,
            capabilities: CapabilitySet {
                required: RequiredCapabilities {
                    execute: TriState::Supported, working_directory: TriState::Supported,
                    stream_output: TriState::Supported, process_exit: TriState::Supported,
                    cancellation: TriState::Supported, timeout: TriState::Supported,
                    final_result: TriState::Supported,
                },
                optional: OptionalCapabilities {
                    native_session_resume: TriState::Unsupported,
                    structured_output: TriState::Supported,
                    tool_events: TriState::Supported,
                    file_change_events: TriState::Unsupported,
                    reasoning_summary: TriState::Supported,
                    interactive_approval: TriState::Unsupported,
                    usage_reporting: TriState::Unsupported,
                },
                workspace_modes: vec!["read".into(), "write".into(), "shell".into()],
                supported_languages: vec![], mcp_tools: vec![],
                supported_platforms: vec!["all".into()],
            },
            core_status: CoreStatus::Available,
            authentication_status: harness_core::contracts::runtime_profile::AuthCheckStatus::Authenticated,
            execution_status: ExecutionStatus::Untested,
            optional_integrations: vec![],
            discovery_source: "test".into(),
            passive_probe: None, active_validation: None,
            concurrency_max: 1,
            created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_fake_adapter_detect() {
        let adapter = FakeAgentAdapter::new();
        let result = adapter.detect(None).await.unwrap();
        assert!(result.found);
    }

    #[tokio::test]
    async fn test_fake_adapter_successful_execution() {
        let adapter = FakeAgentAdapter::new();
        adapter.set_script(FakeExecutionScript::success_with_file("output.txt", "hello world"));
        let mut session = adapter.start_session(&fake_profile(), &SessionOptions {
            working_directory: std::env::temp_dir(), env: HashMap::new(),
            timeout: Duration::from_secs(30), max_turns: None,
            resume_session_id: None, model_override: None,
            effort_override: None, extra_args: vec![],
        }).await.unwrap();

        let envelope = TaskEnvelope {
            task_id: "test-1".into(), project_id: "proj-1".into(),
            task_goal: "Create output.txt".into(),
            scope: harness_core::contracts::task_envelope::FileScope {
                allowed_paths: vec!["**".into()], forbidden_paths: vec![],
                readable_paths: vec![], scope_expansion_allowed: false,
            },
            resource_claims: vec![], dependencies: vec![], acceptance_checks: vec![],
            allowed_tools: vec!["write".into()], output_schema: "TaskResultV1".into(),
            budget: harness_core::contracts::task_envelope::TaskBudget {
                max_turns: 10, max_time_ms: 30_000, max_cost_cents: None,
            },
            goal_contract_version: 1, plan_version: 1,
        };
        session.send_task(&envelope).await.unwrap();
        let mut sink = crate::contract_test::TestSink::new();
        session.receive_events(&mut sink).await.unwrap();
        let events = sink.into_inner();
        assert!(events.iter().any(|e| matches!(e, AgentEvent::SessionStarted { .. })));
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Result { is_error: false, .. })));
        assert!(!session.is_active());
    }
}
