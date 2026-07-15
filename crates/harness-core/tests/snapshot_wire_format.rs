//! Wire format snapshot tests — v1 FROZEN.
//! Breaking changes to these snapshots = breaking wire protocol.

use harness_core::contracts::agent_event::{AgentEvent, TerminationReason};
use harness_core::contracts::project::ProjectLifecycle;
use harness_core::contracts::task::TaskLifecycle;
use harness_core::contracts::workspace::LeaseLifecycle;
use harness_core::state_machine::ExecutionLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};

#[test]
fn snapshot_agent_event_session_started() {
    let event = AgentEvent::SessionStarted {
        session_id: "s1".into(),
        profile_id: "p1".into(),
    };
    let json = serde_json::to_string(&event).unwrap();
    assert_eq!(
        json,
        r#"{"type":"session_started","session_id":"s1","profile_id":"p1"}"#
    );
}

#[test]
fn snapshot_agent_event_result() {
    let event = AgentEvent::Result {
        content: "done".into(),
        is_error: false,
    };
    let json = serde_json::to_string(&event).unwrap();
    assert_eq!(
        json,
        r#"{"type":"result","content":"done","is_error":false}"#
    );
}

#[test]
fn snapshot_agent_event_session_ended() {
    let event = AgentEvent::SessionEnded {
        session_id: "s1".into(),
        synthetic: true,
        termination_reason: TerminationReason::Completed,
        result_received: true,
        process_exit_received: true,
    };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"type\":\"session_ended\""));
    assert!(json.contains("\"synthetic\":true"));
    assert!(json.contains("\"termination_reason\":\"completed\""));
}

#[test]
fn snapshot_agent_event_raw_vendor() {
    let event = AgentEvent::RawVendorEvent {
        raw_type: "vendor.future".into(),
        payload: serde_json::json!({"x": 1}),
    };
    let json = serde_json::to_string(&event).unwrap();
    assert_eq!(
        json,
        r#"{"type":"raw_vendor_event","raw_type":"vendor.future","payload":{"x":1}}"#
    );
}

#[test]
fn snapshot_project_lifecycle_done_is_terminal() {
    assert!(ProjectLifecycle::Done.is_terminal());
    assert!(!ProjectLifecycle::Active.is_terminal());
}

#[test]
fn snapshot_task_lifecycle_terminal_count() {
    let terminals = [
        TaskLifecycle::Pending,
        TaskLifecycle::Ready,
        TaskLifecycle::Dispatched,
        TaskLifecycle::Running,
        TaskLifecycle::AwaitingInput,
        TaskLifecycle::RetryPending,
        TaskLifecycle::Submitted,
        TaskLifecycle::Verified,
        TaskLifecycle::Done,
        TaskLifecycle::Cancelled,
        TaskLifecycle::Superseded,
        TaskLifecycle::Failed,
    ]
    .iter()
    .filter(|s| s.is_terminal())
    .count();
    assert_eq!(
        terminals, 4,
        "Task: 4 terminal states (Done, Cancelled, Superseded, Failed)"
    );
}

#[test]
fn snapshot_execution_lifecycle_all_terminal() {
    for t in &[
        ExecutionLifecycle::Completed,
        ExecutionLifecycle::Failed,
        ExecutionLifecycle::Lost,
        ExecutionLifecycle::Cancelled,
    ] {
        assert!(t.is_terminal(), "{t:?} should be terminal");
    }
}

#[test]
fn snapshot_core_error_codes() {
    let err = CoreError::new(ErrorCode::SinkClosed, "closed", ErrorSource::Agent);
    let json = serde_json::to_string(&err).unwrap();
    assert!(json.contains("\"code\":\"sink_closed\""));
    assert!(json.contains("\"retryable\":true"));
}

#[test]
fn snapshot_taskresult_wire() {
    let result = harness_core::contracts::task_result::TaskResult {
        status: "completed".into(),
        summary: "Added docstring".into(),
        changed_files: vec!["src/lib.rs".into()],
        checks: vec![harness_core::contracts::task_result::TaskResultCheck {
            command: "cargo test".into(),
            exit_code: 0,
            output_ref: None,
        }],
        blockers: vec![],
        risks: vec![],
        proposed_followups: vec![],
    };
    let json = serde_json::to_string(&result).unwrap();
    assert!(json.contains("\"status\":\"completed\""));
    assert!(json.contains("\"changed_files\":[\"src/lib.rs\"]"));
}

#[test]
fn snapshot_taskenvelope_wire() {
    let envelope = harness_core::contracts::task_envelope::TaskEnvelope {
        task_id: "T1".into(),
        project_id: "P1".into(),
        task_goal: "Add doc".into(),
        scope: harness_core::contracts::task_envelope::FileScope {
            allowed_paths: vec!["src/**".into()],
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec!["cargo test".into()],
        allowed_tools: vec!["write".into()],
        output_schema: "TaskResultV1".into(),
        budget: harness_core::contracts::task_envelope::TaskBudget {
            max_turns: 10,
            max_time_ms: 30000,
            max_cost_cents: None,
        },
        goal_contract_version: 1,
        plan_version: 1,
    };
    let json = serde_json::to_string(&envelope).unwrap();
    assert!(json.contains("\"task_id\":\"T1\""));
    assert!(json.contains("\"acceptance_checks\":[\"cargo test\"]"));
}
