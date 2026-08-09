//! IPC gateway abstraction for the TUI.
//!
//! `PipeGateway` is the production path: one Named Pipe connection per
//! request, length-prefixed frames, caller-supplied idempotency keys.
//! The trait lets integration tests substitute an in-process Supervisor.
//!
//! Retry rules (Request Ledger semantics):
//! * ONE key per user action — retries reuse key AND payload.
//! * `Duplicate` replies are success (replay of an already-applied command).
//! * `Conflict` is surfaced to the user, never silently retried.

use harness_core::contracts::ipc::{
    IpcRequestEnvelope, IpcResponseEnvelope, IpcResponseStatus, IPC_PROTOCOL_VERSION,
};
use harness_runtime::ipc::framing::{read_frame, write_frame};
use harness_runtime::ipc::transport::IpcClient;
use serde_json::Value;

/// Normalized reply for one IPC exchange.
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayReply {
    /// Success — carries the response payload.
    Success(Value),
    /// Idempotent replay — the original command already applied.
    Duplicate(Value),
    /// Same key, different payload — must be shown to the user.
    Conflict(String),
    /// Structured failure (bad request, rejected, runtime error...).
    Failure(String),
}

impl GatewayReply {
    /// Duplicate is a success for mutation purposes.
    pub fn is_ok(&self) -> bool {
        matches!(self, GatewayReply::Success(_) | GatewayReply::Duplicate(_))
    }

    pub fn payload(self) -> Value {
        match self {
            GatewayReply::Success(v) | GatewayReply::Duplicate(v) => v,
            _ => Value::Null,
        }
    }
}

/// Transport-level error (distinct from supervisor-reported failures).
#[derive(Debug, Clone, PartialEq)]
pub enum GatewayError {
    Transport(String),
    Serialization(String),
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayError::Transport(m) => write!(f, "transport: {m}"),
            GatewayError::Serialization(m) => write!(f, "serialization: {m}"),
        }
    }
}

impl std::error::Error for GatewayError {}

/// Gateway trait — async-fn-in-trait keeps us dependency-free; the runner
/// is generic over it (no dyn needed).
pub trait TuiGateway: Send + Clone + 'static {
    /// Send one request with a caller-owned idempotency key.
    fn send(
        &self,
        command: &str,
        payload: Value,
        idempotency_key: &str,
    ) -> impl std::future::Future<Output = Result<GatewayReply, GatewayError>> + Send;

    /// Cheap health probe.
    fn ping(&self) -> impl std::future::Future<Output = bool> + Send;
}

/// Production gateway over the Supervisor Named Pipe.
#[derive(Debug, Clone)]
pub struct PipeGateway {
    endpoint: String,
}

impl PipeGateway {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    async fn exchange(
        &self,
        command: &str,
        payload: Value,
        idempotency_key: String,
    ) -> Result<IpcResponseEnvelope, GatewayError> {
        let request = IpcRequestEnvelope {
            protocol_version: IPC_PROTOCOL_VERSION.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            idempotency_key,
            command: command.to_string(),
            payload,
            client_pid: std::process::id(),
            sent_at: chrono::Utc::now(),
        };
        let bytes =
            serde_json::to_vec(&request).map_err(|e| GatewayError::Serialization(e.to_string()))?;

        let mut conn = IpcClient::connect(&self.endpoint)
            .await
            .map_err(|e| GatewayError::Transport(format!("connect {}: {e}", self.endpoint)))?;
        write_frame(&mut conn, &bytes)
            .await
            .map_err(|e| GatewayError::Transport(format!("write: {e}")))?;
        let response_bytes = read_frame(&mut conn, 16 * 1024 * 1024)
            .await
            .map_err(|e| GatewayError::Transport(format!("read: {e}")))?;
        serde_json::from_slice(&response_bytes)
            .map_err(|e| GatewayError::Serialization(e.to_string()))
    }
}

fn normalize(response: IpcResponseEnvelope) -> GatewayReply {
    match response.status {
        IpcResponseStatus::Success | IpcResponseStatus::Accepted => {
            GatewayReply::Success(response.payload.unwrap_or(Value::Null))
        }
        IpcResponseStatus::Duplicate => {
            GatewayReply::Duplicate(response.payload.unwrap_or(Value::Null))
        }
        IpcResponseStatus::Conflict => GatewayReply::Conflict(
            response
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "idempotency conflict".into()),
        ),
        other => GatewayReply::Failure(format!(
            "[{other:?}] {}",
            response
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "no details".into())
        )),
    }
}

impl TuiGateway for PipeGateway {
    async fn send(
        &self,
        command: &str,
        payload: Value,
        idempotency_key: &str,
    ) -> Result<GatewayReply, GatewayError> {
        let response = self
            .exchange(command, payload, idempotency_key.to_string())
            .await?;
        Ok(normalize(response))
    }

    async fn ping(&self) -> bool {
        match self
            .exchange(
                "health",
                serde_json::json!({}),
                format!("tui-ping-{}", uuid::Uuid::new_v4()),
            )
            .await
        {
            Ok(resp) => matches!(
                resp.status,
                IpcResponseStatus::Success | IpcResponseStatus::Accepted
            ),
            Err(_) => false,
        }
    }
}

/// Parse a `goal.snapshot` reply payload into a GoalSnapshot.
pub fn parse_snapshot(
    reply: GatewayReply,
) -> Result<harness_core::contracts::presentation::GoalSnapshot, String> {
    if !reply.is_ok() {
        return Err(format!("snapshot failed: {reply:?}"));
    }
    serde_json::from_value(reply.payload()).map_err(|e| format!("snapshot parse: {e}"))
}

/// Parse a `goal.events` reply into (events, last_sequence).
pub fn parse_events(
    reply: GatewayReply,
) -> Result<
    (
        Vec<harness_core::contracts::presentation::PresentationEvent>,
        i64,
    ),
    String,
> {
    if !reply.is_ok() {
        return Err(format!("events failed: {reply:?}"));
    }
    let payload = reply.payload();
    let events = serde_json::from_value(
        payload
            .get("events")
            .cloned()
            .unwrap_or(Value::Array(Vec::new())),
    )
    .map_err(|e| format!("events parse: {e}"))?;
    let last = payload
        .get("last_sequence")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    Ok((events, last))
}

/// Parse a `goal.list` reply into list rows.
pub fn parse_goal_list(
    reply: GatewayReply,
) -> Result<Vec<crate::tui::state::GoalListItem>, String> {
    if !reply.is_ok() {
        return Err(format!("goal.list failed: {reply:?}"));
    }
    let payload = reply.payload();
    let goals = payload
        .get("goals")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(goals
        .iter()
        .map(|g| crate::tui::state::GoalListItem {
            goal_id: g
                .get("goal_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            title: g
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            state: g
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string(),
            created_at: g
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            updated_at: g
                .get("updated_at")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_counts_as_success_conflict_does_not() {
        assert!(GatewayReply::Duplicate(json!({})).is_ok());
        assert!(GatewayReply::Success(json!({})).is_ok());
        assert!(!GatewayReply::Conflict("x".into()).is_ok());
        assert!(!GatewayReply::Failure("x".into()).is_ok());
    }

    #[test]
    fn normalizes_response_statuses() {
        use chrono::Utc;
        let mk = |status: IpcResponseStatus, error: Option<&str>| IpcResponseEnvelope {
            protocol_version: "1.0".into(),
            request_id: "r".into(),
            status,
            payload: Some(json!({"ok": true})),
            error: error.map(|m| harness_core::contracts::ipc::StructuredIpcError {
                code: "E".into(),
                message: m.into(),
                details: None,
            }),
            completed_at: Utc::now(),
        };
        assert!(matches!(
            normalize(mk(IpcResponseStatus::Success, None)),
            GatewayReply::Success(_)
        ));
        assert!(matches!(
            normalize(mk(IpcResponseStatus::Duplicate, None)),
            GatewayReply::Duplicate(_)
        ));
        assert!(matches!(
            normalize(mk(IpcResponseStatus::Conflict, Some("boom"))),
            GatewayReply::Conflict(m) if m == "boom"
        ));
        assert!(matches!(
            normalize(mk(IpcResponseStatus::Error, Some("bad"))),
            GatewayReply::Failure(m) if m.contains("bad")
        ));
    }

    use serde_json::json;
}
