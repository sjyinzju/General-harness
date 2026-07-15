//! AgentEvent — CANDIDATE v1, will be revised after CLI spikes.
//!
//! Every event is enriched with execution_id, receive_sequence, and received_at
//! by the harness-runtime layer (not by the Adapter).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Raw event from Agent adapter, before enrichment.
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
    #[serde(rename = "result")]
    Result { content: String, is_error: bool },
    #[serde(rename = "error")]
    Error {
        message: String,
        code: Option<String>,
    },
    #[serde(rename = "process_exited")]
    ProcessExited { exit_code: i32, signal: Option<i32> },
    /// Passthrough for vendor-specific events not covered above.
    /// Adapters MUST NOT silently drop unknown events.
    #[serde(rename = "raw_vendor_event")]
    RawVendorEvent {
        raw_type: String,
        payload: serde_json::Value,
    },
    #[serde(rename = "session_ended")]
    SessionEnded {
        session_id: String,
        /// true if this event was synthesized by the Adapter (not from Agent)
        synthetic: bool,
        /// true if the session ended abnormally (crash, timeout, kill)
        abnormal: bool,
    },
}

/// Enriched event with execution metadata, added by harness-runtime.
#[derive(Debug, Clone)]
pub struct EnrichedAgentEvent {
    pub execution_id: String,
    pub receive_sequence: u64,
    pub received_at: DateTime<Utc>,
    pub event: AgentEvent,
}

impl EnrichedAgentEvent {
    pub fn new(execution_id: String, sequence: u64, event: AgentEvent) -> Self {
        Self {
            execution_id,
            receive_sequence: sequence,
            received_at: Utc::now(),
            event,
        }
    }
}
