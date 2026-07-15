//! Reusable Adapter Contract Test Suite.
//! Every AgentAdapter MUST pass these tests.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use harness_core::contracts::agent_adapter::{AgentAdapter, SessionOptions};
use harness_core::contracts::runtime_profile::{
    CapabilitySet, OptionalCapabilities, RequiredCapabilities, RuntimeProfile, RuntimeProfileStatus,
};
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};

fn test_profile() -> RuntimeProfile {
    RuntimeProfile {
        id: "contract-test-profile".into(),
        agent_kind: "test".into(),
        adapter_kind: "test".into(),
        agent_version: "0.0.0".into(),
        binary_path: "test".into(),
        provider: "test".into(),
        model: "test".into(),
        base_url: None,
        auth_mode: "none".into(),
        auth_state: "authenticated".into(),
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: true, working_directory: true, stream_output: true,
                process_exit: true, cancellation: true, timeout: true, final_result: true,
            },
            optional: OptionalCapabilities {
                native_session_resume: false, structured_output: true,
                tool_events: true, file_change_events: false,
                reasoning_summary: true, interactive_approval: false,
                usage_reporting: false,
            },
            workspace_modes: vec!["read".into(), "write".into()],
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

fn test_envelope() -> TaskEnvelope {
    TaskEnvelope {
        task_id: "CONTRACT-TEST-001".into(),
        project_id: "contract-test-proj".into(),
        task_goal: "Run contract test".into(),
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
        budget: TaskBudget { max_turns: 5, max_time_ms: 30_000, max_cost_cents: None },
        goal_contract_version: 1,
        plan_version: 1,
    }
}

fn session_opts() -> SessionOptions {
    SessionOptions {
        working_directory: std::env::temp_dir(),
        env: HashMap::new(),
        timeout: Duration::from_secs(30),
        max_turns: Some(5),
        resume_session_id: None,
        model_override: None,
        effort_override: None,
        extra_args: vec![],
    }
}

/// Run all contract tests against the given adapter.
pub struct AdapterContractTest;

impl AdapterContractTest {
    pub async fn run(adapter: &dyn AgentAdapter) -> Vec<ContractTestResult> {
        let mut results = Vec::new();

        results.push(Self::test_detect(adapter).await);
        results.push(Self::test_get_version(adapter).await);
        results.push(Self::test_inspect_config(adapter).await);
        results.push(Self::test_auth(adapter).await);
        results.push(Self::test_probe(adapter).await);
        results.push(Self::test_session_lifecycle(adapter).await);
        results.push(Self::test_dispose_idempotent(adapter).await);

        results
    }

    async fn test_detect(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.detect(None).await {
            Ok(r) if r.found => ContractTestResult::pass("detect"),
            Ok(r) => ContractTestResult::fail("detect", format!("not found: {:?}", r.error)),
            Err(e) => ContractTestResult::fail("detect", e),
        }
    }

    async fn test_get_version(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.get_version().await {
            Ok(v) if !v.is_empty() => ContractTestResult::pass("get_version"),
            Ok(_) => ContractTestResult::fail("get_version", "empty version".into()),
            Err(e) => ContractTestResult::fail("get_version", e),
        }
    }

    async fn test_inspect_config(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.inspect_configuration().await {
            Ok(_) => ContractTestResult::pass("inspect_configuration"),
            Err(e) => ContractTestResult::fail("inspect_configuration", e),
        }
    }

    async fn test_auth(adapter: &dyn AgentAdapter) -> ContractTestResult {
        match adapter.check_authentication().await {
            Ok(r) => ContractTestResult::pass("check_authentication"),
            Err(e) => ContractTestResult::fail("check_authentication", e),
        }
    }

    async fn test_probe(adapter: &dyn AgentAdapter) -> ContractTestResult {
        let tmp = std::env::temp_dir().join("harness-contract-probe");
        let _ = std::fs::create_dir_all(&tmp);
        match adapter.probe(&tmp).await {
            Ok(r) if r.status == "passed" || r.status == "degraded" => {
                ContractTestResult::pass("probe")
            }
            Ok(r) => ContractTestResult::fail("probe", format!("status: {}", r.status)),
            Err(e) => ContractTestResult::fail("probe", e),
        }
    }

    async fn test_session_lifecycle(adapter: &dyn AgentAdapter) -> ContractTestResult {
        let profile = test_profile();
        let opts = session_opts();

        let mut session = match adapter.start_session(&profile, &opts).await {
            Ok(s) => s,
            Err(e) => return ContractTestResult::fail("session_lifecycle", format!("start_session: {e}")),
        };

        let sid = session.session_id().to_string();
        if sid.is_empty() {
            return ContractTestResult::fail("session_lifecycle", "empty session_id".into());
        }

        if !session.is_active() {
            return ContractTestResult::fail("session_lifecycle", "not active after start".into());
        }

        let envelope = test_envelope();
        if let Err(e) = session.send_task(&envelope).await {
            return ContractTestResult::fail("session_lifecycle", format!("send_task: {e}"));
        }

        let events = Mutex::new(Vec::new());
        let result = session.receive_events(&|e| {
            events.lock().unwrap().push(e);
        }).await;

        let events = events.into_inner().unwrap();

        match result {
            Ok(()) => {
                if events.is_empty() {
                    return ContractTestResult::fail("session_lifecycle", "no events received".into());
                }
                ContractTestResult::pass("session_lifecycle")
            }
            Err(e) => {
                // Some adapters may fail (scripted failure)
                // Accept as long as we got some events
                if events.is_empty() {
                    ContractTestResult::fail("session_lifecycle", format!("failed with no events: {e}"))
                } else {
                    ContractTestResult::pass("session_lifecycle")
                }
            }
        }
    }

    async fn test_dispose_idempotent(adapter: &dyn AgentAdapter) -> ContractTestResult {
        let profile = test_profile();
        let opts = session_opts();

        let mut session = match adapter.start_session(&profile, &opts).await {
            Ok(s) => s,
            Err(e) => return ContractTestResult::fail("dispose_idempotent", format!("start_session: {e}")),
        };

        // First dispose
        session.dispose().await.unwrap();
        // Second dispose — must not panic
        session.dispose().await.unwrap();

        ContractTestResult::pass("dispose_idempotent")
    }
}

#[derive(Debug, Clone)]
pub struct ContractTestResult {
    pub name: String,
    pub passed: bool,
    pub detail: Option<String>,
}

impl ContractTestResult {
    pub fn pass(name: &str) -> Self {
        Self { name: name.into(), passed: true, detail: None }
    }
    pub fn fail(name: &str, detail: String) -> Self {
        Self { name: name.into(), passed: false, detail: Some(detail) }
    }
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
        for r in &results {
            assert!(r.passed, "{}: {:?}", r.name, r.detail);
        }
        assert_eq!(results.len(), 7);
    }

    #[tokio::test]
    async fn fake_adapter_dispose_idempotent() {
        let adapter = FakeAgentAdapter::new();
        adapter.set_script(FakeExecutionScript::success_with_file("out.txt", "ok"));

        let profile = test_profile();
        let opts = session_opts();
        let mut session = adapter.start_session(&profile, &opts).await.unwrap();
        session.dispose().await.unwrap();
        session.dispose().await.unwrap(); // must not panic
    }
}
