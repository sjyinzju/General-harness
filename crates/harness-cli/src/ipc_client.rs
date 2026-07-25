//! Thin CLI IPC client — discovers and communicates with the Supervisor.
#![allow(dead_code)] // CliMode and ping are wired in status/stop commands; full activation in I6.5+
//!
//! Default production mode: all CLI commands route through IPC to the
//! running Supervisor. If no Supervisor is available, the CLI reports
//! a structured error rather than silently falling back to standalone mode.
//!
//! Standalone mode (`--standalone`) is available for explicit maintenance
//! and testing, but refuses write operations if a healthy Supervisor exists.

use chrono::Utc;
use harness_core::contracts::ipc::{
    IpcRequestEnvelope, IpcResponseEnvelope, IpcResponseStatus, IPC_PROTOCOL_VERSION,
};
use harness_runtime::ipc::framing::{read_frame, write_frame};
use harness_runtime::ipc::transport::IpcClient;

/// Default IPC endpoint name.
pub const DEFAULT_ENDPOINT: &str = "harness-supervisor";

/// An IPC client that connects to the Supervisor.
pub struct SupervisorClient {
    endpoint: String,
}

impl SupervisorClient {
    /// Create a new client for the given endpoint.
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
        }
    }

    /// Send a request and receive a response.
    pub async fn send_request(
        &self,
        command: &str,
        payload: serde_json::Value,
    ) -> Result<IpcResponseEnvelope, ClientError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let idempotency_key = uuid::Uuid::new_v4().to_string();

        let request = IpcRequestEnvelope {
            protocol_version: IPC_PROTOCOL_VERSION.to_string(),
            request_id,
            idempotency_key,
            command: command.to_string(),
            payload,
            client_pid: std::process::id(),
            sent_at: Utc::now(),
        };

        let request_json =
            serde_json::to_vec(&request).map_err(|e| ClientError::Serialization(e.to_string()))?;

        // Connect to the supervisor
        let mut conn = IpcClient::connect(&self.endpoint)
            .await
            .map_err(|e| ClientError::Connection(format!("Cannot connect to supervisor at {}: {}. Is the supervisor running? Use --standalone for direct mode.", self.endpoint, e)))?;

        // Write request frame
        write_frame(&mut conn, &request_json)
            .await
            .map_err(|e| ClientError::Connection(format!("Failed to send request: {e}")))?;

        // Read response frame
        let response_bytes = read_frame(&mut conn, 16 * 1024 * 1024)
            .await
            .map_err(|e| ClientError::Connection(format!("Failed to read response: {e}")))?;

        let response: IpcResponseEnvelope = serde_json::from_slice(&response_bytes)
            .map_err(|e| ClientError::Serialization(e.to_string()))?;

        Ok(response)
    }

    /// Check if the supervisor is reachable.
    pub async fn ping(&self) -> Result<bool, ClientError> {
        match self.send_request("health", serde_json::json!({})).await {
            Ok(resp) => Ok(resp.status == IpcResponseStatus::Success),
            Err(_) => Ok(false),
        }
    }
}

/// Errors from the IPC client.
#[derive(Debug)]
pub enum ClientError {
    Connection(String),
    Serialization(String),
    SupervisorError { code: String, message: String },
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Connection(msg) => write!(f, "connection error: {msg}"),
            ClientError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            ClientError::SupervisorError { code, message } => {
                write!(f, "supervisor error [{code}]: {message}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

/// Determine whether to use IPC or standalone mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliMode {
    /// Route commands through the IPC Supervisor.
    Ipc,
    /// Execute commands directly (--standalone).
    Standalone,
}

impl CliMode {
    /// Determine the CLI mode based on flags and supervisor availability.
    pub async fn determine(standalone_requested: bool, endpoint: &str) -> (Self, Option<String>) {
        if standalone_requested {
            // Check if a healthy supervisor exists — refuse writes in standalone
            let client = SupervisorClient::new(endpoint);
            if let Ok(true) = client.ping().await {
                return (
                    CliMode::Standalone,
                    Some(
                        "WARNING: A healthy Supervisor is running. Write operations in standalone mode may conflict. Use IPC mode for production."
                            .to_string(),
                    ),
                );
            }
            return (CliMode::Standalone, None);
        }

        // Default: try IPC
        let client = SupervisorClient::new(endpoint);
        match client.ping().await {
            Ok(true) => (CliMode::Ipc, None),
            Ok(false) => (
                CliMode::Standalone,
                Some("Supervisor not reachable. Use --standalone for direct mode.".to_string()),
            ),
            Err(e) => (
                CliMode::Standalone,
                Some(format!(
                    "Cannot reach supervisor: {e}. Use --standalone for direct mode."
                )),
            ),
        }
    }
}
