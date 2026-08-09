//! Local IPC — Windows Named Pipe transport, protocol framing, and server.
//!
//! This module provides:
//! - Platform-abstract transport (Windows: Named Pipe, Unix: Unix Domain Socket)
//! - Length-prefix protocol framing
//! - Durable request ledger with idempotency
//! - Event streaming with resume support

pub mod framing;
#[cfg(test)]
mod tests;
pub mod transport;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use harness_core::contracts::ipc::{
    IpcCommand, IpcConfig, IpcRequestEnvelope, IpcResponseEnvelope, IpcResponseStatus,
    StructuredIpcError,
};
use harness_core::CoreError;
use sqlx::SqlitePool;
use tokio::sync::{RwLock, Semaphore};
use tracing;

use self::framing::{read_frame, write_frame, FrameTooLarge};
use self::transport::{IpcConnection, IpcListener};

/// Per-request identity threaded from the IPC envelope to handlers.
///
/// This is what lets mutating interaction commands participate in the
/// durable request ledger without handlers re-parsing the envelope.
#[derive(Debug, Clone)]
pub struct IpcRequestContext {
    /// Unique request identifier from the envelope.
    pub request_id: String,
    /// Idempotency key from the envelope (may be empty for reads).
    pub idempotency_key: String,
    /// Client process ID.
    pub client_pid: u32,
}

/// Handler outcome: a payload plus the response status to report.
///
/// Lets ledgered handlers signal `Duplicate`/`Accepted` replays without
/// abusing the error channel.
#[derive(Debug, Clone)]
pub struct IpcHandlerOutcome {
    pub status: IpcResponseStatus,
    pub payload: serde_json::Value,
}

impl IpcHandlerOutcome {
    pub fn success(payload: serde_json::Value) -> Self {
        Self {
            status: IpcResponseStatus::Success,
            payload,
        }
    }

    pub fn duplicate(payload: serde_json::Value) -> Self {
        Self {
            status: IpcResponseStatus::Duplicate,
            payload,
        }
    }
}

/// Trait for command handlers that process IPC requests.
#[async_trait::async_trait]
pub trait IpcCommandHandler: Send + Sync {
    /// Handle an IPC command and return a response payload.
    async fn handle_command(
        &self,
        command: &IpcCommand,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError>;

    /// Handle a command with the request identity from the envelope.
    ///
    /// The default implementation delegates to [`handle_command`] so
    /// legacy handlers and tests keep compiling. Handlers that ledger
    /// mutations (I8A interaction commands) override this.
    ///
    /// [`handle_command`]: IpcCommandHandler::handle_command
    async fn handle_request(
        &self,
        _ctx: &IpcRequestContext,
        command: &IpcCommand,
        payload: &serde_json::Value,
    ) -> Result<IpcHandlerOutcome, CoreError> {
        let result = self.handle_command(command, payload).await?;
        Ok(IpcHandlerOutcome::success(result))
    }
}

/// IPC server that listens for connections and routes commands to handlers.
pub struct IpcServer {
    config: IpcConfig,
    handler: Arc<dyn IpcCommandHandler>,
    #[allow(dead_code)]
    pool: SqlitePool,
    /// Currently active connections.
    active_connections: Arc<RwLock<usize>>,
    /// Semaphore to limit concurrent connections.
    connection_semaphore: Arc<Semaphore>,
    /// Shutdown signal.
    shutdown: Arc<RwLock<bool>>,
}

impl IpcServer {
    /// Create a new IPC server.
    pub fn new(config: IpcConfig, handler: Arc<dyn IpcCommandHandler>, pool: SqlitePool) -> Self {
        let max = config.max_connections;
        Self {
            config,
            handler,
            pool,
            active_connections: Arc::new(RwLock::new(0)),
            connection_semaphore: Arc::new(Semaphore::new(max)),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Start listening for IPC connections. Returns when the listener
    /// encounters an error or shutdown is requested.
    pub async fn serve(&self, endpoint: &str) -> Result<(), CoreError> {
        let mut listener = IpcListener::bind(endpoint).await.map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::Internal,
                format!("IPC bind failed: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;

        tracing::info!(endpoint = %endpoint, "IPC server listening");

        loop {
            // Check shutdown
            if *self.shutdown.read().await {
                tracing::info!("IPC server shutdown requested");
                break;
            }

            // Acquire connection permit
            let permit = self
                .connection_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| {
                    CoreError::new(
                        harness_core::ErrorCode::Internal,
                        "connection semaphore closed",
                        harness_core::ErrorSource::System,
                    )
                })?;

            match tokio::time::timeout(
                Duration::from_secs(self.config.accept_timeout_secs),
                listener.accept(),
            )
            .await
            {
                Ok(Ok(connection)) => {
                    let handler = self.handler.clone();
                    let config = self.config.clone();
                    let active = self.active_connections.clone();

                    tokio::spawn(async move {
                        let _permit = permit; // hold until done
                        handle_connection(connection, handler, config, active).await;
                    });
                }
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "IPC accept error");
                }
                Err(_) => {
                    // Timeout — check shutdown again
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Signal the server to shut down.
    pub async fn shutdown(&self) {
        *self.shutdown.write().await = true;
    }

    /// Get the number of active connections.
    pub async fn active_connections(&self) -> usize {
        *self.active_connections.read().await
    }
}

/// Handle a single IPC connection.
async fn handle_connection(
    mut conn: IpcConnection,
    handler: Arc<dyn IpcCommandHandler>,
    config: IpcConfig,
    active_connections: Arc<RwLock<usize>>,
) {
    let conn_id = conn.id();
    tracing::debug!(conn_id, "IPC connection accepted");

    // Track active connection
    {
        let mut count = active_connections.write().await;
        *count += 1;
    }

    let result = process_connection_loop(&mut conn, handler, &config).await;

    if let Err(e) = result {
        tracing::warn!(conn_id, error = %e, "IPC connection error");
    }

    // Untrack
    {
        let mut count = active_connections.write().await;
        *count = count.saturating_sub(1);
    }

    tracing::debug!(conn_id, "IPC connection closed");
}

/// Main per-connection processing loop.
async fn process_connection_loop(
    conn: &mut IpcConnection,
    handler: Arc<dyn IpcCommandHandler>,
    config: &IpcConfig,
) -> Result<(), CoreError> {
    loop {
        // Read a frame
        let frame_bytes = match read_frame(conn, config.max_frame_bytes).await {
            Ok(bytes) => bytes,
            Err(FrameReadError::Eof) => break, // clean disconnect
            Err(FrameReadError::Error(e)) => return Err(e),
            Err(FrameReadError::TooLarge(size)) => {
                tracing::warn!(size, "oversized frame rejected");
                let error_resp = IpcResponseEnvelope {
                    protocol_version: "1.0".to_string(),
                    request_id: "unknown".to_string(),
                    status: IpcResponseStatus::BadRequest,
                    payload: None,
                    error: Some(StructuredIpcError {
                        code: "frame_too_large".to_string(),
                        message: format!(
                            "frame size {size} exceeds max {}",
                            config.max_frame_bytes
                        ),
                        details: None,
                    }),
                    completed_at: Utc::now(),
                };
                let error_json = serde_json::to_vec(&error_resp).unwrap_or_default();
                let _ = write_frame(conn, &error_json).await;
                break;
            }
        };

        // Parse request
        let request: IpcRequestEnvelope = match serde_json::from_slice(&frame_bytes) {
            Ok(req) => req,
            Err(e) => {
                tracing::warn!(error = %e, "invalid JSON request");
                let error_resp = IpcResponseEnvelope {
                    protocol_version: "1.0".to_string(),
                    request_id: "unknown".to_string(),
                    status: IpcResponseStatus::BadRequest,
                    payload: None,
                    error: Some(StructuredIpcError {
                        code: "invalid_json".to_string(),
                        message: format!("failed to parse request: {e}"),
                        details: None,
                    }),
                    completed_at: Utc::now(),
                };
                let error_json = serde_json::to_vec(&error_resp).unwrap_or_default();
                let _ = write_frame(conn, &error_json).await;
                continue;
            }
        };

        // Validate protocol version
        if request.protocol_version != "1.0" {
            let error_resp = IpcResponseEnvelope {
                protocol_version: "1.0".to_string(),
                request_id: request.request_id.clone(),
                status: IpcResponseStatus::BadRequest,
                payload: None,
                error: Some(StructuredIpcError {
                    code: "protocol_version_mismatch".to_string(),
                    message: format!(
                        "expected protocol version 1.0, got {}",
                        request.protocol_version
                    ),
                    details: None,
                }),
                completed_at: Utc::now(),
            };
            let error_json = serde_json::to_vec(&error_resp).unwrap_or_default();
            let _ = write_frame(conn, &error_json).await;
            continue;
        }

        // Parse command
        let command = match IpcCommand::parse(&request.command) {
            Some(cmd) => cmd,
            None => {
                let error_resp = IpcResponseEnvelope {
                    protocol_version: "1.0".to_string(),
                    request_id: request.request_id.clone(),
                    status: IpcResponseStatus::BadRequest,
                    payload: None,
                    error: Some(StructuredIpcError {
                        code: "unknown_command".to_string(),
                        message: format!("unknown command: {}", request.command),
                        details: None,
                    }),
                    completed_at: Utc::now(),
                };
                let error_json = serde_json::to_vec(&error_resp).unwrap_or_default();
                let _ = write_frame(conn, &error_json).await;
                continue;
            }
        };

        // Handle command with envelope identity (ledger participation).
        let ctx = IpcRequestContext {
            request_id: request.request_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            client_pid: request.client_pid,
        };
        match handler
            .handle_request(&ctx, &command, &request.payload)
            .await
        {
            Ok(outcome) => {
                let response = IpcResponseEnvelope {
                    protocol_version: "1.0".to_string(),
                    request_id: request.request_id,
                    status: outcome.status,
                    payload: Some(outcome.payload),
                    error: None,
                    completed_at: Utc::now(),
                };
                let response_json = serde_json::to_vec(&response).unwrap_or_default();
                if let Err(e) = write_frame(conn, &response_json).await {
                    tracing::warn!(error = %e, "failed to write IPC response");
                    break;
                }
            }
            Err(e) => {
                // Idempotency conflicts surface as a dedicated status so
                // clients can distinguish "retry with same payload" bugs.
                let status = if e.code == harness_core::ErrorCode::Conflict {
                    IpcResponseStatus::Conflict
                } else {
                    IpcResponseStatus::Error
                };
                let response = IpcResponseEnvelope {
                    protocol_version: "1.0".to_string(),
                    request_id: request.request_id,
                    status,
                    payload: None,
                    error: Some(StructuredIpcError {
                        code: format!("{:?}", e.code).to_lowercase(),
                        message: e.to_string(),
                        details: None,
                    }),
                    completed_at: Utc::now(),
                };
                let response_json = serde_json::to_vec(&response).unwrap_or_default();
                let _ = write_frame(conn, &response_json).await;
            }
        }
    }

    Ok(())
}

/// Errors from frame reading.
#[derive(Debug)]
pub enum FrameReadError {
    Eof,
    TooLarge(usize),
    Error(CoreError),
}

impl From<FrameTooLarge> for FrameReadError {
    fn from(f: FrameTooLarge) -> Self {
        FrameReadError::TooLarge(f.0)
    }
}

impl From<CoreError> for FrameReadError {
    fn from(e: CoreError) -> Self {
        FrameReadError::Error(e)
    }
}

impl std::fmt::Display for FrameReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameReadError::Eof => write!(f, "EOF"),
            FrameReadError::TooLarge(n) => write!(f, "frame too large ({n} bytes)"),
            FrameReadError::Error(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for FrameReadError {}
