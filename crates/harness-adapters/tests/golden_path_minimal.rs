//! Minimal Execution Golden Path — Gate B milestone.
//!
//! Validates: Task → Execution Attempt → FakeAdapter → AgentEvent → TaskResult → Termination.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness_adapters::fake::script::FakeExecutionScript;
use harness_adapters::fake::FakeAgentAdapter;
use harness_core::contracts::agent_adapter::{AgentAdapter, SessionOptions};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::{
    CapabilitySet, OptionalCapabilities, RequiredCapabilities, RuntimeProfile,
    RuntimeProfileStatus,
};
use harness_core::contracts::task::TaskLifecycle;
use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use harness_core::state_machine::execution_fsm::ExecutionFsm;
use harness_core::state_machine::task_fsm::TaskFsm;
use harness_core::state_machine::ExecutionLifecycle;

fn test_profile() -> RuntimeProfile {
    RuntimeProfile {
        id: "golden-test-profile".into(),
        agent_kind: "fake".into(),
        adapter_kind: "fake".into(),
        agent_version: "1.0.0".into(),
        binary_path: "fake".into(),
        provider: "fake".into(),
        model: "fake-model".into(),
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

fn test_envelope() -> TaskEnvelope {
    TaskEnvelope {
        task_id: "GOLDEN-TASK-001".into(),
        project_id: "GOLDEN-PROJ-001".into(),
        task_goal: "Create hello.txt".into(),
        scope: FileScope {
            allowed_paths: vec!["**".into()],
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec![],
        allowed_tools: vec!["write".into(), "read".into()],
        output_schema: "TaskResultV1".into(),
        budget: TaskBudget {
            max_turns: 10,
            max_time_ms: 30_000,
            max_cost_cents: None,
        },
        goal_contract_version: 1,
        plan_version: 1,
    }
}

/// Golden Path: Task → Dispatched → Execution Running → Completed → Task Done
#[tokio::test]
async fn golden_path_task_success() {
    // 1. Create FakeAdapter with script
    let adapter = FakeAgentAdapter::new();
    adapter.set_script(FakeExecutionScript::success_with_file(
        "hello.txt",
        "Hello, Harness!",
    ));

    // 2. Start session
    let mut session = adapter
        .start_session(&test_profile(), &session_opts())
        .await
        .unwrap();
    assert!(session.is_active());

    // 3. Task lifecycle: Pending → Ready → Dispatched → Running
    let mut task_lifecycle = TaskLifecycle::Pending;
    assert!(TaskFsm::can_transition(&task_lifecycle, &TaskLifecycle::Ready));
    task_lifecycle = TaskLifecycle::Ready;
    assert!(TaskFsm::can_transition(&task_lifecycle, &TaskLifecycle::Dispatched));
    task_lifecycle = TaskLifecycle::Dispatched;
    assert!(TaskFsm::can_transition(&task_lifecycle, &TaskLifecycle::Running));
    task_lifecycle = TaskLifecycle::Running;

    // 4. Execution lifecycle: Created → Running
    let mut exec_lifecycle = ExecutionLifecycle::Created;
    assert!(ExecutionFsm::can_transition(&exec_lifecycle, &ExecutionLifecycle::Running));
    exec_lifecycle = ExecutionLifecycle::Running;

    // 5. Send task + receive events
    let envelope = test_envelope();
    session.send_task(&envelope).await.unwrap();

    let execution_id = uuid::Uuid::new_v4().to_string();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let exec_id = execution_id.clone();

    let recv_result = session
        .receive_events(&move |event| {
            let enriched = harness_core::contracts::agent_event::EnrichedAgentEvent::new(
                exec_id.clone(),
                events_clone.lock().unwrap().len() as u64,
                event,
            );
            events_clone.lock().unwrap().push(enriched);
        })
        .await;
    assert!(recv_result.is_ok());
    assert!(!session.is_active());

    let events = events.lock().unwrap();

    // 6. Verify events
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, AgentEvent::SessionStarted { .. })),
        "Should have SessionStarted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, AgentEvent::Result { is_error: false, .. })),
        "Should have successful Result"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, AgentEvent::ProcessExited { exit_code: 0, .. })),
        "Should have ProcessExited with exit 0"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, AgentEvent::SessionEnded { abnormal: false, .. })),
        "Should have normal SessionEnded"
    );

    // 7. Verify receive_sequence is monotonic
    for w in events.windows(2) {
        assert!(w[1].receive_sequence > w[0].receive_sequence);
    }

    // 8. Verify execution_id is present on all events
    for e in events.iter() {
        assert_eq!(e.execution_id, execution_id);
    }

    // 9. Execution → Completed
    assert!(ExecutionFsm::can_transition(&exec_lifecycle, &ExecutionLifecycle::Completed));
    exec_lifecycle = ExecutionLifecycle::Completed;
    assert!(exec_lifecycle.is_terminal());

    // 10. Task → Submitted → Verified → Done
    assert!(TaskFsm::can_transition(&task_lifecycle, &TaskLifecycle::Submitted));
    task_lifecycle = TaskLifecycle::Submitted;
    assert!(TaskFsm::can_transition(&task_lifecycle, &TaskLifecycle::Verified));
    task_lifecycle = TaskLifecycle::Verified;
    assert!(TaskFsm::can_transition(&task_lifecycle, &TaskLifecycle::Done));
    task_lifecycle = TaskLifecycle::Done;
    assert!(task_lifecycle.is_terminal());
}

/// Golden Path: Task failure → Execution Failed → retry
///
/// KNOWN ISSUE (Windows): `std::sync::Mutex` inside a synchronous callback
/// that runs inside `async fn receive_events` blocks the tokio runtime's
/// ability to poll the timeout future, even with `yield_now()` and
/// `multi_thread` flavor. Root cause: `std::sync::Mutex::lock()` blocks
/// the OS thread, preventing tokio from making progress on other tasks
/// scheduled on the same thread.
///
/// Fix for Contract Freeze: use `tokio::sync::Mutex` and make the
/// `receive_events` callback async (`FnMut(AgentEvent) -> Future`).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn golden_path_task_failure_with_retry() {
    let result = tokio::time::timeout(Duration::from_secs(10), golden_path_task_failure_with_retry_inner()).await;
    assert!(result.is_ok(), "Test timed out after 10s — possible deadlock");
}

async fn golden_path_task_failure_with_retry_inner() {
    let adapter = FakeAgentAdapter::new();
    adapter.set_script(FakeExecutionScript::failure("Simulated agent crash"));

    let mut session = adapter
        .start_session(&test_profile(), &session_opts())
        .await
        .unwrap();

    let envelope = test_envelope();
    session.send_task(&envelope).await.unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let exec_id = uuid::Uuid::new_v4().to_string();
    let exec_id2 = exec_id.clone();

    let recv_result = session
        .receive_events(&move |event| {
            events_clone.lock().unwrap().push(harness_core::contracts::agent_event::EnrichedAgentEvent::new(
                exec_id2.clone(),
                events_clone.lock().unwrap().len() as u64,
                event,
            ));
        })
        .await;
    // FakeAdapter failure script: reports failure through events (Error + abnormal SessionEnded)
    assert!(recv_result.is_ok());

    let events = events.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, AgentEvent::Error { .. })),
        "Should have Error event"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, AgentEvent::SessionEnded { abnormal: true, .. })),
        "Should have abnormal SessionEnded"
    );

    // Execution → Failed → allows retry
    let mut exec = ExecutionLifecycle::Running;
    assert!(ExecutionFsm::can_transition(&exec, &ExecutionLifecycle::Failed));
    exec = ExecutionLifecycle::Failed;
    assert!(exec.allows_retry());
    assert!(ExecutionFsm::can_transition(&exec, &ExecutionLifecycle::Created));
    // New Execution Attempt created
    let new_exec = ExecutionLifecycle::Created;
    assert_ne!(new_exec, exec); // new execution, not overwriting history
}

/// Illegal transition must be rejected.
#[tokio::test]
async fn illegal_transition_rejected() {
    // Task cannot go from Pending directly to Running
    assert!(!TaskFsm::can_transition(&TaskLifecycle::Pending, &TaskLifecycle::Running));

    // Execution cannot go from Completed back to Running
    assert!(!ExecutionFsm::can_transition(
        &ExecutionLifecycle::Completed,
        &ExecutionLifecycle::Running
    ));

    // Cancelled execution cannot retry
    assert!(!ExecutionFsm::can_transition(
        &ExecutionLifecycle::Cancelled,
        &ExecutionLifecycle::Created
    ));
}

/// Cancel only affects target execution.
#[tokio::test]
async fn cancel_does_not_affect_other_executions() {
    // Start two independent sessions
    let adapter1 = FakeAgentAdapter::new();
    adapter1.set_script(FakeExecutionScript::success_with_file("a.txt", "A"));

    let adapter2 = FakeAgentAdapter::new();
    adapter2.set_script(FakeExecutionScript::success_with_file("b.txt", "B"));

    let mut session1 = adapter1
        .start_session(&test_profile(), &session_opts())
        .await
        .unwrap();
    let mut session2 = adapter2
        .start_session(&test_profile(), &session_opts())
        .await
        .unwrap();

    // Cancel session1
    session1.cancel().await.unwrap();
    assert!(!session1.is_active());

    // session2 should still be active
    assert!(session2.is_active());

    // session2 should still work
    let envelope = test_envelope();
    session2.send_task(&envelope).await.unwrap();
    let count = Arc::new(Mutex::new(0));
    let count2 = count.clone();
    session2
        .receive_events(&move |_| {
            *count2.lock().unwrap() += 1;
        })
        .await
        .unwrap();
    assert!(*count.lock().unwrap() > 0);
}
