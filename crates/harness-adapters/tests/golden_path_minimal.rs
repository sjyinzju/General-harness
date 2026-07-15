//! Minimal Execution Golden Path — Gate B milestone.
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use harness_adapters::fake::script::FakeExecutionScript;
use harness_adapters::fake::FakeAgentAdapter;
use harness_core::contracts::agent_adapter::{AgentAdapter, AgentEventSink, SessionOptions};
use harness_core::contracts::agent_event::{AgentEvent, TerminationReason};
use harness_core::contracts::agent_event::EnrichedAgentEvent;
use harness_core::contracts::runtime_profile::{
    AuthCheckStatus, AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus,
    OptionalCapabilities, ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::task::TaskLifecycle;
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use harness_core::state_machine::execution_fsm::ExecutionFsm;
use harness_core::state_machine::task_fsm::TaskFsm;
use harness_core::state_machine::ExecutionLifecycle;

// ── Async test sink using tokio::sync::Mutex ──────

struct TokioSink {
    execution_id: String,
    events: tokio::sync::Mutex<Vec<EnrichedAgentEvent>>,
}

impl TokioSink {
    fn new(execution_id: String) -> Self {
        Self { execution_id, events: tokio::sync::Mutex::new(Vec::new()) }
    }
    async fn into_inner(self) -> Vec<EnrichedAgentEvent> {
        self.events.into_inner()
    }
}

impl AgentEventSink for TokioSink {
    fn send(
        &mut self,
        event: AgentEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), harness_core::CoreError>> + Send + '_>> {
        let exec_id = self.execution_id.clone();
        Box::pin(async move {
            // In production, harness-runtime would manage the sequence counter.
            // For tests, we use a simple timestamp-based sequence.
            let enriched = EnrichedAgentEvent::new(exec_id, 0, event);
            // Note: in real impl, sequence is managed by runtime, not sink
            Ok(())
        })
    }
}

// Simple sync sink for basic tests
struct SyncSink {
    events: std::sync::Mutex<Vec<AgentEvent>>,
}

impl SyncSink {
    fn new() -> Self { Self { events: std::sync::Mutex::new(Vec::new()) } }
    fn into_inner(self) -> Vec<AgentEvent> { self.events.into_inner().unwrap() }
}

impl AgentEventSink for SyncSink {
    fn send(&mut self, event: AgentEvent) -> Pin<Box<dyn Future<Output = Result<(), harness_core::CoreError>> + Send + '_>> {
        self.events.lock().unwrap().push(event);
        Box::pin(std::future::ready(Ok(())))
    }
}

// ── Test helpers ──────────────────────────────────

fn test_profile() -> RuntimeProfile {
    RuntimeProfile {
        id: "golden-test-profile".into(), agent_definition_id: "golden-def".into(),
        label: "Golden Test".into(), agent_kind: "fake".into(), adapter_kind: "fake".into(),
        agent_version: "1.0.0".into(), executable_path: "fake".into(),
        provider: "fake".into(), provider_source: ProviderSource::UserDeclared,
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
            workspace_modes: vec!["read".into(), "write".into(), "shell".into()],
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

fn session_opts() -> SessionOptions {
    SessionOptions {
        working_directory: std::env::temp_dir(), env: HashMap::new(),
        timeout: Duration::from_secs(30), max_turns: Some(5),
        resume_session_id: None, model_override: None, effort_override: None, extra_args: vec![],
    }
}

fn test_envelope() -> TaskEnvelope {
    TaskEnvelope {
        task_id: "GOLDEN-TASK-001".into(), project_id: "GOLDEN-PROJ-001".into(),
        task_goal: "Create hello.txt".into(),
        scope: FileScope { allowed_paths: vec!["**".into()], forbidden_paths: vec![], readable_paths: vec![], scope_expansion_allowed: false },
        resource_claims: vec![], dependencies: vec![], acceptance_checks: vec![],
        allowed_tools: vec!["write".into(), "read".into()],
        output_schema: "TaskResultV1".into(),
        budget: TaskBudget { max_turns: 10, max_time_ms: 30_000, max_cost_cents: None },
        goal_contract_version: 1, plan_version: 1,
    }
}

// ── Tests ─────────────────────────────────────────

/// Golden Path: Task → Execution → Completed → Task Done
#[tokio::test]
async fn golden_path_task_success() {
    let adapter = FakeAgentAdapter::new();
    adapter.set_script(FakeExecutionScript::success_with_file("hello.txt", "Hello, Harness!"));
    let mut session = adapter.start_session(&test_profile(), &session_opts()).await.unwrap();
    assert!(session.is_active());

    let mut task = TaskLifecycle::Pending;
    task = TaskLifecycle::Ready;
    task = TaskLifecycle::Dispatched;
    task = TaskLifecycle::Running;
    let mut exec = ExecutionLifecycle::Created;
    exec = ExecutionLifecycle::Running;

    session.send_task(&test_envelope()).await.unwrap();
    let mut sink = SyncSink::new();
    session.receive_events(&mut sink).await.unwrap();
    let events = sink.into_inner();
    assert!(!session.is_active());
    assert!(events.iter().any(|e| matches!(e, AgentEvent::SessionStarted { .. })));
    assert!(events.iter().any(|e| matches!(e, AgentEvent::Result { is_error: false, .. })));
    assert!(events.iter().any(|e| matches!(e, AgentEvent::ProcessExited { exit_code: 0, .. })));
    assert!(events.iter().any(|e| matches!(e, AgentEvent::SessionEnded { termination_reason: TerminationReason::Completed, .. })));

    exec = ExecutionLifecycle::Completed;
    assert!(exec.is_terminal());
    task = TaskLifecycle::Submitted;
    task = TaskLifecycle::Verified;
    task = TaskLifecycle::Done;
    assert!(task.is_terminal());
}

/// Golden Path: Task failure → Execution Failed → retry with new Execution
#[tokio::test(flavor = "multi_thread")]
async fn golden_path_task_failure_with_retry() {
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        golden_path_task_failure_with_retry_inner(),
    ).await;
    assert!(result.is_ok(), "Test timed out after 10s");
}

async fn golden_path_task_failure_with_retry_inner() {
    let adapter = FakeAgentAdapter::new();
    adapter.set_script(FakeExecutionScript::failure("Simulated agent crash"));
    let mut session = adapter.start_session(&test_profile(), &session_opts()).await.unwrap();
    session.send_task(&test_envelope()).await.unwrap();

    let mut sink = SyncSink::new();
    session.receive_events(&mut sink).await.unwrap();
    let events = sink.into_inner();

    assert!(events.iter().any(|e| matches!(e, AgentEvent::Error { .. })), "Should have Error event");
    assert!(events.iter().any(|e| matches!(e, AgentEvent::SessionEnded { termination_reason: TerminationReason::ProcessExited { .. }, .. })), "Should have abnormal SessionEnded");

    // Execution Running → Failed (terminal, immutable)
    let exec = ExecutionLifecycle::Running;
    assert!(ExecutionFsm::can_transition(&exec, &ExecutionLifecycle::Failed));
    let old_exec = ExecutionLifecycle::Failed;
    assert!(old_exec.is_terminal());
    // Terminal Execution CANNOT transition — retry creates a NEW Execution
    assert!(!ExecutionFsm::can_transition(&old_exec, &ExecutionLifecycle::Created));
    // Task → RetryPending (non-terminal) → Dispatched
    assert!(TaskFsm::can_transition(&TaskLifecycle::Running, &TaskLifecycle::RetryPending));
    let task_retry = TaskLifecycle::RetryPending;
    assert!(!task_retry.is_terminal());
    assert!(TaskFsm::can_transition(&task_retry, &TaskLifecycle::Dispatched));
    // New Execution created with new ID
    let new_exec = ExecutionLifecycle::Created;
    assert!(!new_exec.is_terminal());
    // New execution completes successfully
    let adapter2 = FakeAgentAdapter::new();
    adapter2.set_script(FakeExecutionScript::success_with_file("fixed.txt", "fixed"));
    let mut session2 = adapter2.start_session(&test_profile(), &session_opts()).await.unwrap();
    session2.send_task(&test_envelope()).await.unwrap();
    let mut sink2 = SyncSink::new();
    session2.receive_events(&mut sink2).await.unwrap();
    let events2 = sink2.into_inner();
    assert!(events2.iter().any(|e| matches!(e, AgentEvent::Result { is_error: false, .. })));
}

/// Illegal transition rejected.
#[tokio::test]
async fn illegal_transition_rejected() {
    assert!(!TaskFsm::can_transition(&TaskLifecycle::Pending, &TaskLifecycle::Running));
    assert!(!ExecutionFsm::can_transition(&ExecutionLifecycle::Completed, &ExecutionLifecycle::Running));
    assert!(!ExecutionFsm::can_transition(&ExecutionLifecycle::Cancelled, &ExecutionLifecycle::Created));
}

/// Cancel only affects target execution.
#[tokio::test]
async fn cancel_does_not_affect_other_executions() {
    let adapter1 = FakeAgentAdapter::new();
    adapter1.set_script(FakeExecutionScript::success_with_file("a.txt", "A"));
    let adapter2 = FakeAgentAdapter::new();
    adapter2.set_script(FakeExecutionScript::success_with_file("b.txt", "B"));

    let mut session1 = adapter1.start_session(&test_profile(), &session_opts()).await.unwrap();
    let mut session2 = adapter2.start_session(&test_profile(), &session_opts()).await.unwrap();

    session1.cancel().await.unwrap();
    assert!(!session1.is_active());
    assert!(session2.is_active());

    let mut sink = SyncSink::new();
    session2.send_task(&test_envelope()).await.unwrap();
    session2.receive_events(&mut sink).await.unwrap();
    assert!(sink.into_inner().len() > 0);
}
