use std::path::PathBuf;
use std::time::Duration;

use harness_core::contracts::agent_event::{AgentEvent, TerminationReason};

/// Pre-scripted behavior for a fake execution.
#[derive(Debug, Clone)]
pub struct FakeExecutionScript {
    /// Events to emit in sequence
    pub events: Vec<AgentEvent>,
    /// Files to create in the worktree
    pub files_to_create: Vec<FakeFileOp>,
    /// Delay between events
    pub event_delay: Duration,
    /// If set, fail after emitting this many events
    pub failure: Option<FakeFailure>,
}

#[derive(Debug, Clone)]
pub struct FakeFileOp {
    pub path: PathBuf,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct FakeFailure {
    /// Fail after this event index (0-based)
    pub after_event_index: usize,
    /// Error message
    pub error_message: String,
}

impl FakeExecutionScript {
    /// Pre-built: successful execution with a simple file creation.
    pub fn success_with_file(path: &str, content: &str) -> Self {
        let session_id = uuid::Uuid::new_v4().to_string();
        Self {
            files_to_create: vec![FakeFileOp {
                path: PathBuf::from(path),
                content: content.to_string(),
            }],
            events: vec![
                AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                    profile_id: "fake-profile-1".into(),
                },
                AgentEvent::ToolCallStarted {
                    tool_name: "write".into(),
                    tool_use_id: "fake-tool-1".into(),
                    tool_input: serde_json::json!({"file_path": path, "content": content}),
                    vendor_event_id: None,
                },
                AgentEvent::ToolCallCompleted {
                    tool_use_id: "fake-tool-1".into(),
                    is_error: false,
                    content_preview: "File written successfully".into(),
                },
                AgentEvent::Result {
                    content: format!("Created {path}"),
                    is_error: false,
                },
                AgentEvent::ProcessExited {
                    exit_code: 0,
                    signal: None,
                },
                AgentEvent::SessionEnded {
                    session_id,
                    synthetic: true,
                    termination_reason: TerminationReason::Completed,
                    result_received: true,
                    process_exit_received: true,
                },
            ],
            event_delay: Duration::from_millis(10),
            failure: None,
        }
    }

    /// Pre-built: execution that fails with an error.
    pub fn failure(error_msg: &str) -> Self {
        let session_id = uuid::Uuid::new_v4().to_string();
        Self {
            files_to_create: vec![],
            events: vec![
                AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                    profile_id: "fake-profile-1".into(),
                },
                AgentEvent::Error {
                    message: error_msg.to_string(),
                    code: Some("FAKE_ERROR".into()),
                },
                AgentEvent::ProcessExited {
                    exit_code: 1,
                    signal: None,
                },
                AgentEvent::SessionEnded {
                    session_id,
                    synthetic: true,
                    termination_reason: TerminationReason::ProcessExited { exit_code: 1, signal: None },
                    result_received: false,
                    process_exit_received: true,
                },
            ],
            event_delay: Duration::from_millis(10),
            failure: None,
        }
    }

    /// Pre-built: execution that times out (no result).
    pub fn timeout() -> Self {
        let session_id = uuid::Uuid::new_v4().to_string();
        Self {
            files_to_create: vec![],
            events: vec![
                AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                    profile_id: "fake-profile-1".into(),
                },
                AgentEvent::Progress {
                    summary: "Working...".into(),
                },
                // No Result, no ProcessExited — simulates timeout
            ],
            event_delay: Duration::from_millis(10),
            failure: Some(FakeFailure {
                after_event_index: 1,
                error_message: "Simulated timeout".into(),
            }),
        }
    }

    /// Pre-built: execution with an unknown vendor event.
    pub fn with_unknown_event() -> Self {
        let session_id = uuid::Uuid::new_v4().to_string();
        Self {
            files_to_create: vec![],
            events: vec![
                AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                    profile_id: "fake-profile-1".into(),
                },
                AgentEvent::RawVendorEvent {
                    raw_type: "vendor.specific.future_event".into(),
                    payload: serde_json::json!({"data": "some future format"}),
                },
                AgentEvent::Result {
                    content: "done".into(),
                    is_error: false,
                },
                AgentEvent::ProcessExited {
                    exit_code: 0,
                    signal: None,
                },
                AgentEvent::SessionEnded {
                    session_id,
                    synthetic: true,
                    termination_reason: TerminationReason::Completed,
                    result_received: true,
                    process_exit_received: true,
                },
            ],
            event_delay: Duration::from_millis(10),
            failure: None,
        }
    }

    /// Pre-built: success with ReasoningSummary.
    pub fn with_reasoning() -> Self {
        let session_id = uuid::Uuid::new_v4().to_string();
        Self {
            files_to_create: vec![],
            events: vec![
                AgentEvent::SessionStarted {
                    session_id: session_id.clone(),
                    profile_id: "fake-profile-1".into(),
                },
                AgentEvent::Progress {
                    summary: "Analyzing the codebase...".into(),
                },
                AgentEvent::ReasoningSummary {
                    summary: "The function needs a docstring explaining its parameters and return value. I will use the Edit tool to add it.".into(),
                },
                AgentEvent::Result {
                    content: "Task completed successfully".into(),
                    is_error: false,
                },
                AgentEvent::ProcessExited {
                    exit_code: 0,
                    signal: None,
                },
                AgentEvent::SessionEnded {
                    session_id,
                    synthetic: true,
                    termination_reason: TerminationReason::Completed,
                    result_received: true,
                    process_exit_received: true,
                },
            ],
            event_delay: Duration::from_millis(10),
            failure: None,
        }
    }
}
