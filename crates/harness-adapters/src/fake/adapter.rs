use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use harness_core::contracts::agent_adapter::{
    AgentAdapter, AgentConfigInfo, AgentSession, AuthCheckResult, DetectionResult, SessionOptions,
};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::{ProbeChecks, ProbeResult, RuntimeProfile};
use harness_core::contracts::task_envelope::TaskEnvelope;

use super::script::FakeExecutionScript;

pub struct FakeAgentAdapter {
    script: Arc<Mutex<Option<FakeExecutionScript>>>,
}

impl FakeAgentAdapter {
    pub fn new() -> Self {
        Self {
            script: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_script(&self, script: FakeExecutionScript) {
        *self.script.lock().unwrap() = Some(script);
    }
}

impl Default for FakeAgentAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
#[async_trait]
impl AgentAdapter for FakeAgentAdapter {
    fn kind(&self) -> &'static str {
        "fake"
    }

    async fn detect(&self, _binary_path: Option<&Path>) -> Result<DetectionResult, String> {
        Ok(DetectionResult {
            found: true,
            binary_path: Some(PathBuf::from("fake-adapter")),
            error: None,
        })
    }

    async fn get_version(&self) -> Result<String, String> {
        Ok("fake-1.0.0".into())
    }

    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, String> {
        Ok(AgentConfigInfo {
            provider: Some("fake".into()),
            base_url: None,
            model: Some("fake-model".into()),
            auth_mode: "none".into(),
            config_file_path: None,
            extra: HashMap::new(),
        })
    }

    async fn check_authentication(&self) -> Result<AuthCheckResult, String> {
        Ok(AuthCheckResult {
            authenticated: true,
            method: Some("none".into()),
            provider: Some("fake".into()),
            error: None,
        })
    }

    async fn probe(&self, _temp_dir: &Path) -> Result<ProbeResult, String> {
        let now = chrono::Utc::now();
        Ok(ProbeResult {
            status: "passed".into(),
            tested_at: Some(now),
            checks: ProbeChecks {
                read_repo: true,
                create_file: true,
                execute_test: true,
                structured_output: true,
                interrupt_and_resume: true,
                budget_stop: true,
                accepts_task_envelope: true,
            },
            error_summary: None,
            duration_ms: 5,
        })
    }

    async fn start_session(
        &self,
        profile: &RuntimeProfile,
        _opts: &SessionOptions,
    ) -> Result<Box<dyn AgentSession>, String> {
        let script = self.script.lock().unwrap().clone();
        Ok(Box::new(FakeAgentSession::new(
            profile.id.clone(),
            script,
        )))
    }
}

struct FakeAgentSession {
    profile_id: String,
    session_id: String,
    script: Option<FakeExecutionScript>,
    active: Arc<AtomicBool>,
}

impl FakeAgentSession {
    fn new(profile_id: String, script: Option<FakeExecutionScript>) -> Self {
        Self {
            session_id: uuid::Uuid::new_v4().to_string(),
            profile_id,
            script,
            active: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
#[async_trait]
impl AgentSession for FakeAgentSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    async fn send_task(&mut self, _envelope: &TaskEnvelope) -> Result<(), String> {
        if !self.is_active() {
            return Err("Session not active".into());
        }
        Ok(())
    }

    async fn receive_events(
        &mut self,
        on_event: &(dyn Fn(AgentEvent) + Send + Sync),
    ) -> Result<(), String> {
        let script = self
            .script
            .as_ref()
            .ok_or("No script configured for FakeAgent")?
            .clone();

        // 1. Create files
        for file_op in &script.files_to_create {
            if let Some(parent) = file_op.path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            std::fs::write(&file_op.path, &file_op.content)
                .map_err(|e| format!("FakeAgent: failed to create file: {e}"))?;
        }

        // 2. Emit events
        for (i, event) in script.events.iter().enumerate() {
            // Check for scripted failure
            if let Some(ref failure) = script.failure {
                if i > failure.after_event_index {
                    self.active.store(false, Ordering::SeqCst);
                    return Err(failure.error_message.clone());
                }
            }

            on_event(event.clone());
            // Yield to the runtime so timeouts/cancellation can fire
            tokio::task::yield_now().await;
        }

        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), String> {
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn cancel(&self) -> Result<(), String> {
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&mut self) -> Result<(), String> {
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::runtime_profile::{
        CapabilitySet, OptionalCapabilities, RequiredCapabilities, RuntimeProfileStatus,
    };

    fn fake_profile() -> RuntimeProfile {
        RuntimeProfile {
            id: "fake-1".into(),
            agent_kind: "fake".into(),
            adapter_kind: "fake".into(),
            agent_version: "1.0.0".into(),
            binary_path: "fake".into(),
            provider: "fake".into(),
            model: "fake".into(),
            base_url: None,
            auth_mode: "none".into(),
            auth_state: "authenticated".into(),
            capabilities: CapabilitySet {
                required: RequiredCapabilities {
                    execute: true,
                    working_directory: true,
                    stream_output: true,
                    process_exit: true,
                    cancellation: true,
                    timeout: true,
                    final_result: true,
                },
                optional: OptionalCapabilities {
                    native_session_resume: false,
                    structured_output: true,
                    tool_events: true,
                    file_change_events: false,
                    reasoning_summary: true,
                    interactive_approval: false,
                    usage_reporting: false,
                },
                workspace_modes: vec!["read".into(), "write".into(), "shell".into()],
                supported_languages: vec![],
                mcp_tools: vec![],
                supported_platforms: vec!["all".into()],
            },
            probe: None,
            status: RuntimeProfileStatus::Available,
            concurrency_max: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
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
        adapter.set_script(FakeExecutionScript::success_with_file(
            "output.txt",
            "hello world",
        ));

        let mut session = adapter
            .start_session(&fake_profile(), &SessionOptions {
                working_directory: std::env::temp_dir(),
                env: HashMap::new(),
                timeout: Duration::from_secs(30),
                max_turns: None,
                resume_session_id: None,
                model_override: None,
                effort_override: None,
                extra_args: vec![],
            })
            .await
            .unwrap();

        let envelope = TaskEnvelope {
            task_id: "test-1".into(),
            project_id: "proj-1".into(),
            task_goal: "Create output.txt".into(),
            scope: harness_core::contracts::task_envelope::FileScope {
                allowed_paths: vec!["**".into()],
                forbidden_paths: vec![],
                readable_paths: vec![],
                scope_expansion_allowed: false,
            },
            resource_claims: vec![],
            dependencies: vec![],
            acceptance_checks: vec![],
            allowed_tools: vec!["write".into()],
            output_schema: "TaskResultV1".into(),
            budget: harness_core::contracts::task_envelope::TaskBudget {
                max_turns: 10,
                max_time_ms: 30_000,
                max_cost_cents: None,
            },
            goal_contract_version: 1,
            plan_version: 1,
        };

        session.send_task(&envelope).await.unwrap();

        let events = std::sync::Mutex::new(Vec::new());
        session
            .receive_events(&|e| {
                events.lock().unwrap().push(e);
            })
            .await
            .unwrap();

        let events = events.into_inner().unwrap();
        assert!(events.iter().any(|e| matches!(e, AgentEvent::SessionStarted { .. })));
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Result { is_error: false, .. })));
        assert!(events.iter().any(|e| matches!(e, AgentEvent::ProcessExited { exit_code: 0, .. })));
        assert!(!session.is_active());
    }
}
