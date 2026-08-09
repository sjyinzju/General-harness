//! IPC protocol types — request/response envelopes, framing, and status codes.
//!
//! These are pure domain types with ZERO I/O dependencies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Maximum IPC frame size in bytes (16 MiB).
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Length-prefix size in bytes (4 bytes = u32).
pub const FRAME_LENGTH_PREFIX_BYTES: usize = 4;

/// Current IPC protocol version.
pub const IPC_PROTOCOL_VERSION: &str = "1.0";

/// IPC request envelope sent from client to server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcRequestEnvelope {
    /// Protocol version string (must match server).
    pub protocol_version: String,
    /// Unique request identifier (UUID v4).
    pub request_id: String,
    /// Idempotency key for retry-safe commands.
    pub idempotency_key: String,
    /// The command to execute (e.g., "task.start", "supervisor.status").
    pub command: String,
    /// Command payload as JSON value.
    pub payload: serde_json::Value,
    /// Client process ID.
    pub client_pid: u32,
    /// When this request was sent.
    pub sent_at: DateTime<Utc>,
}

/// IPC response envelope sent from server to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcResponseEnvelope {
    /// Protocol version string.
    pub protocol_version: String,
    /// Echoes the original request_id.
    pub request_id: String,
    /// Response status.
    pub status: IpcResponseStatus,
    /// Response payload (command-specific).
    pub payload: Option<serde_json::Value>,
    /// Structured error details (if status is Error).
    pub error: Option<StructuredIpcError>,
    /// When this response was completed.
    pub completed_at: DateTime<Utc>,
}

/// IPC response status codes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpcResponseStatus {
    /// Command completed successfully.
    Success,
    /// Command accepted but still processing (async).
    Accepted,
    /// Invalid request (bad format, unknown command).
    BadRequest,
    /// Request rejected (e.g., supervisor not accepting writes).
    Rejected,
    /// Command execution failed.
    Error,
    /// Request already completed (idempotent reply).
    Duplicate,
    /// Idempotency key conflict (same key, different payload).
    Conflict,
    /// Supervisor not ready (recovering, draining, etc.).
    NotReady,
}

/// Structured error returned in IPC responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredIpcError {
    /// Machine-readable error code.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Optional additional details.
    pub details: Option<serde_json::Value>,
}

/// IPC command whitelist — all recognized commands.
#[derive(Debug, Clone)]
pub enum IpcCommand {
    // Supervisor lifecycle
    SupervisorStatus,
    SupervisorStop,

    // Task loop
    TaskStart,
    TaskStatus,
    TaskResume,
    TaskCancel,
    TaskInspect,
    TaskDryRunDecision,

    // Review
    ReviewCreate,
    ReviewShow,
    ReviewRun,
    ReviewList,

    // Integration
    IntegrationEnqueue,
    IntegrationRunNext,
    IntegrationShow,
    IntegrationList,
    IntegrationCancel,
    IntegrationRecover,

    // Inspection
    Inspect,

    // Cancellation
    Cancel,

    // Event streaming
    Subscribe,
    Unsubscribe,

    // Goal loop
    GoalCreate,
    GoalStart,
    GoalShow,
    GoalList,
    GoalStatus,
    GoalPause,
    GoalResume,
    GoalCancel,
    GoalReplan,
    GoalApprovals,
    GoalApprove,
    GoalReject,
    GoalAnswer,
    GoalEvents,
    /// Read-only projection of the full goal state (I8A).
    GoalSnapshot,
    /// Reject a plan revision with feedback → replan (I8A).
    GoalRequestChanges,
    /// Free-text user intervention (I8A).
    GoalIntervene,

    // Health / Diagnostics
    Health,
    Diagnostics,
}

impl IpcCommand {
    /// Parse a command string into an IpcCommand.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "supervisor.status" => Some(IpcCommand::SupervisorStatus),
            "supervisor.stop" => Some(IpcCommand::SupervisorStop),

            "task.start" => Some(IpcCommand::TaskStart),
            "task.status" => Some(IpcCommand::TaskStatus),
            "task.resume" => Some(IpcCommand::TaskResume),
            "task.cancel" => Some(IpcCommand::TaskCancel),
            "task.inspect" => Some(IpcCommand::TaskInspect),
            "task.dry_run_decision" => Some(IpcCommand::TaskDryRunDecision),

            "review.create" => Some(IpcCommand::ReviewCreate),
            "review.show" => Some(IpcCommand::ReviewShow),
            "review.run" => Some(IpcCommand::ReviewRun),
            "review.list" => Some(IpcCommand::ReviewList),

            "integration.enqueue" => Some(IpcCommand::IntegrationEnqueue),
            "integration.run_next" => Some(IpcCommand::IntegrationRunNext),
            "integration.show" => Some(IpcCommand::IntegrationShow),
            "integration.list" => Some(IpcCommand::IntegrationList),
            "integration.cancel" => Some(IpcCommand::IntegrationCancel),
            "integration.recover" => Some(IpcCommand::IntegrationRecover),

            "inspect" => Some(IpcCommand::Inspect),
            "cancel" => Some(IpcCommand::Cancel),

            "subscribe" => Some(IpcCommand::Subscribe),
            "unsubscribe" => Some(IpcCommand::Unsubscribe),

            "health" => Some(IpcCommand::Health),
            "diagnostics" => Some(IpcCommand::Diagnostics),

            // Goal commands
            "goal.create" => Some(IpcCommand::GoalCreate),
            "goal.start" => Some(IpcCommand::GoalStart),
            "goal.show" => Some(IpcCommand::GoalShow),
            "goal.list" => Some(IpcCommand::GoalList),
            "goal.status" => Some(IpcCommand::GoalStatus),
            "goal.pause" => Some(IpcCommand::GoalPause),
            "goal.resume" => Some(IpcCommand::GoalResume),
            "goal.cancel" => Some(IpcCommand::GoalCancel),
            "goal.replan" => Some(IpcCommand::GoalReplan),
            "goal.approvals" => Some(IpcCommand::GoalApprovals),
            "goal.approve" => Some(IpcCommand::GoalApprove),
            "goal.reject" => Some(IpcCommand::GoalReject),
            "goal.answer" => Some(IpcCommand::GoalAnswer),
            "goal.events" => Some(IpcCommand::GoalEvents),
            "goal.snapshot" => Some(IpcCommand::GoalSnapshot),
            "goal.request_changes" => Some(IpcCommand::GoalRequestChanges),
            "goal.intervene" => Some(IpcCommand::GoalIntervene),

            _ => None,
        }
    }

    /// Returns true if this command produces side effects (mutations).
    pub fn has_side_effects(&self) -> bool {
        !matches!(
            self,
            IpcCommand::SupervisorStatus
                | IpcCommand::TaskStatus
                | IpcCommand::TaskInspect
                | IpcCommand::TaskDryRunDecision
                | IpcCommand::ReviewShow
                | IpcCommand::ReviewList
                | IpcCommand::IntegrationShow
                | IpcCommand::IntegrationList
                | IpcCommand::Inspect
                | IpcCommand::Health
                | IpcCommand::Diagnostics
                | IpcCommand::GoalShow
                | IpcCommand::GoalList
                | IpcCommand::GoalStatus
                | IpcCommand::GoalApprovals
                | IpcCommand::GoalEvents
                | IpcCommand::GoalSnapshot
        )
    }

    /// Returns the command string.
    pub fn as_str(&self) -> &'static str {
        match self {
            IpcCommand::SupervisorStatus => "supervisor.status",
            IpcCommand::SupervisorStop => "supervisor.stop",
            IpcCommand::TaskStart => "task.start",
            IpcCommand::TaskStatus => "task.status",
            IpcCommand::TaskResume => "task.resume",
            IpcCommand::TaskCancel => "task.cancel",
            IpcCommand::TaskInspect => "task.inspect",
            IpcCommand::TaskDryRunDecision => "task.dry_run_decision",
            IpcCommand::ReviewCreate => "review.create",
            IpcCommand::ReviewShow => "review.show",
            IpcCommand::ReviewRun => "review.run",
            IpcCommand::ReviewList => "review.list",
            IpcCommand::IntegrationEnqueue => "integration.enqueue",
            IpcCommand::IntegrationRunNext => "integration.run_next",
            IpcCommand::IntegrationShow => "integration.show",
            IpcCommand::IntegrationList => "integration.list",
            IpcCommand::IntegrationCancel => "integration.cancel",
            IpcCommand::IntegrationRecover => "integration.recover",
            IpcCommand::Inspect => "inspect",
            IpcCommand::Cancel => "cancel",
            IpcCommand::Subscribe => "subscribe",
            IpcCommand::Unsubscribe => "unsubscribe",
            IpcCommand::Health => "health",
            IpcCommand::Diagnostics => "diagnostics",
            // Goal commands
            IpcCommand::GoalCreate => "goal.create",
            IpcCommand::GoalStart => "goal.start",
            IpcCommand::GoalShow => "goal.show",
            IpcCommand::GoalList => "goal.list",
            IpcCommand::GoalStatus => "goal.status",
            IpcCommand::GoalPause => "goal.pause",
            IpcCommand::GoalResume => "goal.resume",
            IpcCommand::GoalCancel => "goal.cancel",
            IpcCommand::GoalReplan => "goal.replan",
            IpcCommand::GoalApprovals => "goal.approvals",
            IpcCommand::GoalApprove => "goal.approve",
            IpcCommand::GoalReject => "goal.reject",
            IpcCommand::GoalAnswer => "goal.answer",
            IpcCommand::GoalEvents => "goal.events",
            IpcCommand::GoalSnapshot => "goal.snapshot",
            IpcCommand::GoalRequestChanges => "goal.request_changes",
            IpcCommand::GoalIntervene => "goal.intervene",
        }
    }
}

/// Request state in the durable request ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpcRequestState {
    /// Request received but not yet persisted.
    Received,
    /// Request persisted in ledger.
    Persisted,
    /// Request dispatched for execution.
    Dispatching,
    /// Request completed successfully.
    Completed,
    /// Request rejected (invalid command, protocol mismatch, etc.).
    Rejected,
    /// Request execution failed.
    Failed,
    /// Request cancelled.
    Cancelled,
}

impl IpcRequestState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            IpcRequestState::Completed
                | IpcRequestState::Rejected
                | IpcRequestState::Failed
                | IpcRequestState::Cancelled
        )
    }
}

/// An event stream entry for IPC subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcEvent {
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// Unique event identifier.
    pub event_id: String,
    /// Aggregate type (e.g., "task", "execution", "review", "integration").
    pub aggregate_type: String,
    /// Aggregate identifier.
    pub aggregate_id: String,
    /// Event type string.
    pub event_type: String,
    /// When this event occurred.
    pub occurred_at: DateTime<Utc>,
    /// Event payload as JSON.
    pub payload: serde_json::Value,
}

/// Configuration for the IPC transport.
#[derive(Debug, Clone)]
pub struct IpcConfig {
    /// Maximum frame size in bytes.
    pub max_frame_bytes: usize,
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// Maximum inflight requests.
    pub max_inflight_requests: usize,
    /// Read timeout in seconds.
    pub read_timeout_secs: u64,
    /// Write timeout in seconds.
    pub write_timeout_secs: u64,
    /// Connection accept timeout in seconds.
    pub accept_timeout_secs: u64,
    /// Maximum event stream buffer size.
    pub max_event_stream_buffer: usize,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_connections: 32,
            max_inflight_requests: 64,
            read_timeout_secs: 30,
            write_timeout_secs: 30,
            accept_timeout_secs: 5,
            max_event_stream_buffer: 1024,
        }
    }
}
