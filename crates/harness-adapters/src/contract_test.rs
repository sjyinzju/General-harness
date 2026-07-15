//! Reusable Adapter Contract Test Suite.
use std::time::Duration;
use std::{collections::HashMap, sync::Mutex};

use harness_core::contracts::agent_adapter::{
    AgentAdapter, AgentEventSink, SessionOptions,
};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::{
    AuthCheckStatus, AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus,
    OptionalCapabilities, ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use std::future::Future;
use std::pin::Pin;

fn test_profile() -> RuntimeProfile {
    RuntimeProfile {
        id: "contract-test-profile".into(), agent_definition_id: "ct-def".into(),
        label: "Contract Test".into(), agent_kind: "test".into(), adapter_kind: "test".into(),
        agent_version: "0.0.0".into(), executable_path: "test".into(),
        provider: "test".into(), provider_source: ProviderSource::UserDeclared,
        model: None, base_url: None,
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
                structured_output: TriState::Supported, tool_events: TriState::Supported,
                file_change_events: TriState::Unsupported,
                reasoning_summary: TriState::Supported,
                interactive_approval: TriState::Unsupported,
                usage_reporting: TriState::Unsupported,
            },
            workspace_modes: vec!["read".into(), "write".into()],
            supported_languages: vec![], mcp_tools: vec![],
            supported_platforms: vec!["all".into()],
        },
        core_status: CoreStatus::Available,
        authentication_status: AuthCheckStatus::Authenticated,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "test".into(),
        passive_probe: None, active_validation: None,
        concurrency_max: 1,
        created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
    }
}

fn test_envelope() -> TaskEnvelope {
    TaskEnvelope {
        task_id: "CONTRACT-TEST-001".into(), project_id: "ct-proj".into(),
        task_goal: "Run contract test".into(),
        scope: FileScope { allowed_paths: vec!["**".into()], forbidden_paths: vec![], readable_paths: vec![], scope_expansion_allowed: false },
        resource_claims: vec![], dependencies: vec![], acceptance_checks: vec![],
        allowed_tools: vec!["read".into(), "write".into()],
        output_schema: "TaskResultV1".into(),
        budget: TaskBudget { max_turns: 5, max_time_ms: 30_000, max_cost_cents: None },
        goal_contract_version: 1, plan_version: 1,
    }
}

fn session_opts() -> SessionOptions {
    SessionOptions { working_directory: std::env::temp_dir(), env: HashMap::new(), timeout: Duration::from_secs(30), max_turns: Some(5), resume_session_id: None, model_override: None, effort_override: None, extra_args: vec![] }
}

// Simple sync sink for contract tests — also used by fake adapter tests
pub struct TestSink { events: Mutex<Vec<AgentEvent>> }
impl TestSink {
    pub fn new() -> Self { Self { events: Mutex::new(Vec::new()) } }
    pub fn into_inner(self) -> Vec<AgentEvent> { self.events.into_inner().unwrap() }
}
impl AgentEventSink for TestSink {
    fn send(&mut self, event: AgentEvent) -> Pin<Box<dyn Future<Output = Result<(), harness_core::CoreError>> + Send + '_>> {
        self.events.lock().unwrap().push(event);
        Box::pin(std::future::ready(Ok(())))
    }
}

pub struct AdapterContractTest;

impl AdapterContractTest {
    pub async fn run(adapter: &dyn AgentAdapter) -> Vec<ContractTestResult> {
        vec![
            Self::test_detect(adapter).await, Self::test_get_version(adapter).await,
            Self::test_inspect_config(adapter).await, Self::test_auth(adapter).await,
            Self::test_probe(adapter).await, Self::test_session_lifecycle(adapter).await,
            Self::test_dispose_idempotent(adapter).await,
        ]
    }

    async fn test_detect(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.detect(None).await {
            Ok(r) if r.found => ContractTestResult::pass("detect"),
            Ok(r) => ContractTestResult::fail("detect", format!("not found: {:?}", r.error)),
            Err(e) => ContractTestResult::fail("detect", e.to_string()),
        }
    }

    async fn test_get_version(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.get_version().await {
            Ok(v) if !v.is_empty() => ContractTestResult::pass("get_version"),
            Ok(_) => ContractTestResult::fail("get_version", "empty".into()),
            Err(e) => ContractTestResult::fail("get_version", e.to_string()),
        }
    }

    async fn test_inspect_config(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.inspect_configuration().await {
            Ok(_) => ContractTestResult::pass("inspect_configuration"),
            Err(e) => ContractTestResult::fail("inspect_configuration", e.to_string()),
        }
    }

    async fn test_auth(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.check_authentication().await {
            Ok(_) => ContractTestResult::pass("check_authentication"),
            Err(e) => ContractTestResult::fail("check_authentication", e.to_string()),
        }
    }

    async fn test_probe(adapter: &dyn AgentAdapter) -> ContractTestResult {
        let tmp = std::env::temp_dir().join("harness-contract-probe");
        let _ = std::fs::create_dir_all(&tmp);
        match adapter.probe(&tmp).await {
            Ok(r) if r.smoke_test_passed => ContractTestResult::pass("probe"),
            Ok(r) => ContractTestResult::fail("probe", format!("smoke_test_passed: {}", r.smoke_test_passed)),
            Err(e) => ContractTestResult::fail("probe", e.to_string()),
        }
    }

    async fn test_session_lifecycle(adapter: &dyn AgentAdapter) -> ContractTestResult {
        let mut session = match adapter.start_session(&test_profile(), &session_opts()).await {
            Ok(s) => s, Err(e) => return ContractTestResult::fail("session_lifecycle", format!("start_session: {e}")),
        };
        if session.session_id().is_empty() { return ContractTestResult::fail("session_lifecycle", "empty session_id".into()); }
        if !session.is_active() { return ContractTestResult::fail("session_lifecycle", "not active".into()); }
        if let Err(e) = session.send_task(&test_envelope()).await {
            return ContractTestResult::fail("session_lifecycle", format!("send_task: {e}"));
        }
        let mut sink = TestSink::new();
        let result = session.receive_events(&mut sink).await;
        let events = sink.into_inner();
        match result {
            Ok(()) if events.is_empty() => ContractTestResult::fail("session_lifecycle", "no events".into()),
            Ok(()) => ContractTestResult::pass("session_lifecycle"),
            Err(e) if events.is_empty() => ContractTestResult::fail("session_lifecycle", format!("failed no events: {e}")),
            Err(_) => ContractTestResult::pass("session_lifecycle"),
        }
    }

    async fn test_dispose_idempotent(adapter: &dyn AgentAdapter) -> ContractTestResult {
        let mut session = match adapter.start_session(&test_profile(), &session_opts()).await {
            Ok(s) => s, Err(e) => return ContractTestResult::fail("dispose", format!("start: {e}")),
        };
        session.dispose().await.unwrap();
        session.dispose().await.unwrap();
        ContractTestResult::pass("dispose_idempotent")
    }
}

#[derive(Debug, Clone)]
pub struct ContractTestResult { pub name: String, pub passed: bool, pub detail: Option<String> }
impl ContractTestResult {
    pub fn pass(name: &str) -> Self { Self { name: name.into(), passed: true, detail: None } }
    pub fn fail(name: &str, detail: String) -> Self { Self { name: name.into(), passed: false, detail: Some(detail) } }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::adapter::FakeAgentAdapter;
    use crate::fake::script::FakeExecutionScript;

    #[tokio::test]
    async fn fake_adapter_passes_all_contract_tests() {
        let adapter = FakeAgentAdapter::new();
        adapter.set_script(FakeExecutionScript::success_with_file("out.txt", "ok"));
        let results = AdapterContractTest::run(&adapter).await;
        for r in &results { assert!(r.passed, "{}: {:?}", r.name, r.detail); }
        assert_eq!(results.len(), 7);
    }

    #[tokio::test]
    async fn fake_adapter_dispose_idempotent() {
        let adapter = FakeAgentAdapter::new();
        adapter.set_script(FakeExecutionScript::success_with_file("out.txt", "ok"));
        let mut session = adapter.start_session(&test_profile(), &session_opts()).await.unwrap();
        session.dispose().await.unwrap();
        session.dispose().await.unwrap();
    }
}
