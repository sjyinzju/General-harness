//! Structured error types for harness-core.
//! Zero I/O dependencies. Used by all contracts.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub source: ErrorSource,
    pub diagnostic_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    // Configuration
    ConfigInvalid,
    ConfigMissing,
    // Authentication
    AuthFailed,
    AuthExpired,
    // Version
    UnsupportedVersion,
    // Capability
    UnsupportedCapability,
    // Process
    ProcessSpawnFailed,
    ProcessExited { exit_code: i32 },
    ProcessTimeout { duration_ms: u64 },
    ProcessCancelled,
    // Protocol
    ProtocolError,
    ProtocolParseError,
    // Event sink
    SinkClosed,
    SinkConsumerFailed,
    SinkCancelled,
    SinkInvalidSequence { expected: u64, got: u64 },
    // State machine
    InvalidStateTransition { from: String, to: String },
    InvalidState,
    EntityTerminal { entity_id: String },
    // Resource
    ResourceConflict { resource: String },
    NotFound,
    Conflict,
    // Workspace
    WorkspaceError,
    WorkspaceLeaseExpired,
    // Persistence
    PersistenceError,
    // Serialization
    SerializationError,
    // Verification
    VerificationFailed { check: String },
    // Internal
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSource {
    Harness,
    Agent,
    User,
    System,
}

impl CoreError {
    pub fn new(code: ErrorCode, message: impl Into<String>, source: ErrorSource) -> Self {
        let retryable = code.is_retryable();
        Self {
            code,
            message: message.into(),
            retryable,
            source,
            diagnostic_ref: None,
        }
    }

    pub fn with_diagnostic(mut self, ref_path: impl Into<String>) -> Self {
        self.diagnostic_ref = Some(ref_path.into());
        self
    }
}

impl ErrorCode {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::ProcessTimeout { .. }
                | ErrorCode::ProcessCancelled
                | ErrorCode::ProcessSpawnFailed
                | ErrorCode::SinkClosed
                | ErrorCode::SinkConsumerFailed
                | ErrorCode::ResourceConflict { .. }
                | ErrorCode::Conflict
                | ErrorCode::WorkspaceLeaseExpired
                | ErrorCode::PersistenceError
                | ErrorCode::ProtocolError
        )
    }
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (retryable={}, source={:?})",
            serde_json::to_string(&self.code).unwrap_or_default(),
            self.message,
            self.retryable,
            self.source,
        )
    }
}

impl std::error::Error for CoreError {}
