//! AgentEvent — v1 FROZEN (Gate C).
//! Wire schema version: 1.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// AgentEvent v1 — unified event type for all adapters.
/// Wire format: JSON, tagged by "type" field.
/// Unknown event types → RawVendorEvent (never silently dropped).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AgentEvent {
    #[serde(rename = "session_started")]
    SessionStarted {
        session_id: String,
        profile_id: String,
    },
    #[serde(rename = "message")]
    Message {
        content: String,
        vendor_event_id: Option<String>,
    },
    #[serde(rename = "progress")]
    Progress { summary: String },
    #[serde(rename = "reasoning_summary")]
    ReasoningSummary { summary: String },
    #[serde(rename = "tool_call_started")]
    ToolCallStarted {
        tool_name: String,
        tool_use_id: String,
        tool_input: serde_json::Value,
        vendor_event_id: Option<String>,
    },
    #[serde(rename = "tool_call_completed")]
    ToolCallCompleted {
        tool_use_id: String,
        is_error: bool,
        content_preview: String,
    },
    /// Agent logical result — may arrive before or after ProcessExited.
    #[serde(rename = "result")]
    Result { content: String, is_error: bool },
    #[serde(rename = "error")]
    Error {
        message: String,
        code: Option<String>,
    },
    /// OS process exit — distinct from Agent logical Result.
    #[serde(rename = "process_exited")]
    ProcessExited { exit_code: i32, signal: Option<i32> },
    /// Passthrough — unknown events are NOT silently dropped.
    #[serde(rename = "raw_vendor_event")]
    RawVendorEvent {
        raw_type: String,
        payload: serde_json::Value,
    },
    /// Harness-synthesized session summary. Always synthetic=true.
    /// Contains aggregated termination info.
    #[serde(rename = "session_ended")]
    SessionEnded {
        session_id: String,
        synthetic: bool,
        termination_reason: TerminationReason,
        result_received: bool,
        process_exit_received: bool,
    },
}

/// Why the session ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminationReason {
    /// Agent completed normally, Result received, exit_code=0
    Completed,
    /// Agent process exited with non-zero code
    ProcessExited { exit_code: i32, signal: Option<i32> },
    /// Agent was interrupted (SIGTERM)
    Interrupted,
    /// Agent was force-killed (SIGKILL)
    Cancelled,
    /// Agent timed out
    Timeout,
    /// Supervisor crashed, Agent process lost
    Lost,
    /// Unknown / unexpected
    Unknown,
}

/// Enriched event envelope — added by harness-runtime.
/// NOT part of the wire format between Harness and Agent.
#[derive(Debug, Clone)]
pub struct EnrichedAgentEvent {
    pub schema_version: u32,
    pub execution_id: String,
    pub receive_sequence: u64, // monotonic per execution
    pub received_at: DateTime<Utc>,
    pub vendor_event_id: Option<String>,
    pub synthetic: bool,
    pub raw_event_ref: Option<String>,
    pub payload: AgentEvent,
}

impl EnrichedAgentEvent {
    pub fn new(execution_id: String, sequence: u64, event: AgentEvent) -> Self {
        Self {
            schema_version: 1,
            execution_id,
            receive_sequence: sequence,
            received_at: Utc::now(),
            vendor_event_id: None,
            synthetic: false,
            raw_event_ref: None,
            payload: event,
        }
    }

    pub fn synthetic(execution_id: String, sequence: u64, event: AgentEvent) -> Self {
        Self {
            schema_version: 1,
            execution_id,
            receive_sequence: sequence,
            received_at: Utc::now(),
            vendor_event_id: None,
            synthetic: true,
            raw_event_ref: None,
            payload: event,
        }
    }
}
