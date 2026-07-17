//! SchedulerEventSink — production AgentEvent persistence via AgentEventSink.
//! Sequences events per execution, enforces ordering, handles large payloads.
//! Applies structured secret redaction before serialization.

use harness_core::contracts::agent_adapter::AgentEventSink;
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::process::redactor::ProcessEventRedactor;

/// Persists AgentEvents to event_log with execution-scoped sequence numbers.
/// Applies structured secret redaction before serialization and persistence.
pub struct SchedulerEventSink {
    pool: SqlitePool,
    execution_id: String,
    sequence: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    /// Large payloads (>64KB) are written to files and referenced by path.
    artifact_dir: Option<std::path::PathBuf>,
    /// Track last persisted sequence for crash recovery.
    last_persisted_seq: Arc<Mutex<u64>>,
    /// Redacts known secret values from AgentEvent payloads before persistence.
    redactor: Arc<ProcessEventRedactor>,
}

impl SchedulerEventSink {
    pub fn new(
        pool: SqlitePool,
        execution_id: String,
        artifact_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            pool,
            execution_id,
            sequence: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
            artifact_dir,
            last_persisted_seq: Arc::new(Mutex::new(0)),
            redactor: Arc::new(ProcessEventRedactor::new()),
        }
    }

    /// Create a sink with a pre-configured redactor containing known secrets.
    pub fn with_redactor(
        pool: SqlitePool,
        execution_id: String,
        artifact_dir: Option<std::path::PathBuf>,
        redactor: ProcessEventRedactor,
    ) -> Self {
        Self {
            pool,
            execution_id,
            sequence: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
            artifact_dir,
            last_persisted_seq: Arc::new(Mutex::new(0)),
            redactor: Arc::new(redactor),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Get the last persisted sequence number (for crash recovery).
    pub fn last_persisted(&self) -> u64 {
        *self.last_persisted_seq.lock().unwrap()
    }

    /// Redact an AgentEvent by removing secret values from its string fields.
    fn redact_event(&self, event: &AgentEvent) -> AgentEvent {
        if self.redactor.is_empty() {
            return event.clone();
        }
        match event {
            AgentEvent::Message {
                content,
                vendor_event_id,
            } => AgentEvent::Message {
                content: self.redactor.redact_str(content),
                vendor_event_id: vendor_event_id.clone(),
            },
            AgentEvent::Progress { summary } => AgentEvent::Progress {
                summary: self.redactor.redact_str(summary),
            },
            AgentEvent::ReasoningSummary { summary } => AgentEvent::ReasoningSummary {
                summary: self.redactor.redact_str(summary),
            },
            AgentEvent::ToolCallStarted {
                tool_name,
                tool_use_id,
                tool_input,
                vendor_event_id,
            } => AgentEvent::ToolCallStarted {
                tool_name: tool_name.clone(),
                tool_use_id: tool_use_id.clone(),
                tool_input: self.redact_value(tool_input),
                vendor_event_id: vendor_event_id.clone(),
            },
            AgentEvent::ToolCallCompleted {
                tool_use_id,
                is_error,
                content_preview,
            } => AgentEvent::ToolCallCompleted {
                tool_use_id: tool_use_id.clone(),
                is_error: *is_error,
                content_preview: self.redactor.redact_str(content_preview),
            },
            AgentEvent::Result { content, is_error } => AgentEvent::Result {
                content: self.redactor.redact_str(content),
                is_error: *is_error,
            },
            AgentEvent::Error { message, code } => AgentEvent::Error {
                message: self.redactor.redact_str(message),
                code: code.clone(),
            },
            AgentEvent::RawVendorEvent { raw_type, payload } => AgentEvent::RawVendorEvent {
                raw_type: raw_type.clone(),
                payload: self.redact_value(payload),
            },
            // Events without sensitive text content pass through unchanged
            AgentEvent::SessionStarted { .. }
            | AgentEvent::ProcessExited { .. }
            | AgentEvent::SessionEnded { .. } => event.clone(),
        }
    }

    fn redact_value(&self, value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::String(s) => serde_json::Value::String(self.redactor.redact_str(s)),
            serde_json::Value::Object(map) => {
                let redacted: serde_json::Map<String, serde_json::Value> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), self.redact_value(v)))
                    .collect();
                serde_json::Value::Object(redacted)
            }
            serde_json::Value::Array(arr) => {
                let redacted: Vec<serde_json::Value> =
                    arr.iter().map(|v| self.redact_value(v)).collect();
                serde_json::Value::Array(redacted)
            }
            other => other.clone(),
        }
    }
}

impl AgentEventSink for SchedulerEventSink {
    fn send(
        &mut self,
        event: AgentEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send + '_>> {
        if self.closed.load(Ordering::SeqCst) {
            return Box::pin(std::future::ready(Err(CoreError::new(
                ErrorCode::SinkClosed,
                "event sink is closed",
                ErrorSource::Harness,
            ))));
        }

        // Redact secrets before serialization
        let redacted_event = self.redact_event(&event);

        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let exec_id = self.execution_id.clone();
        let pool = self.pool.clone();
        let closed = self.closed.clone();
        let last_seq = self.last_persisted_seq.clone();
        let artifact_dir = self.artifact_dir.clone();

        Box::pin(async move {
            let event_json = serde_json::to_string(&redacted_event).map_err(|e| {
                CoreError::new(
                    ErrorCode::ProtocolError,
                    format!("serialize AgentEvent: {e}"),
                    ErrorSource::Harness,
                )
            })?;

            let (payload, _artifact_ref) = if event_json.len() > 64 * 1024 {
                // Large payload — write to artifact file
                if let Some(ref dir) = artifact_dir {
                    let file_name = format!("event-{}-{}.json", exec_id, seq);
                    let path = dir.join(&file_name);
                    if let Err(e) = tokio::fs::write(&path, &event_json).await {
                        tracing::warn!(
                            execution_id = %exec_id,
                            sequence = seq,
                            error = %e,
                            "Failed to write large event to artifact"
                        );
                    }
                    (
                        serde_json::json!({"content_ref": path.to_string_lossy()}).to_string(),
                        Some(path.to_string_lossy().to_string()),
                    )
                } else {
                    (event_json, None)
                }
            } else {
                (event_json, None)
            };

            let event_id = Uuid::new_v4().to_string();
            let event_type = event_type_str(&redacted_event);

            let result = sqlx::query(
                "INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?)",
            )
            .bind(&event_id)
            .bind(&exec_id)
            .bind(seq as i64)
            .bind(event_type)
            .bind(&payload)
            .bind(1i64)
            .bind(&event_id)
            .bind(&event_id)
            .bind("agent")
            .execute(&pool)
            .await;

            match result {
                Ok(_) => {
                    *last_seq.lock().unwrap() = seq;
                    Ok(())
                }
                Err(e) => {
                    closed.store(true, Ordering::SeqCst);
                    tracing::error!(
                        execution_id = %exec_id,
                        sequence = seq,
                        error = %e,
                        "Failed to persist AgentEvent"
                    );
                    Err(CoreError::new(
                        ErrorCode::SinkClosed,
                        format!("persist AgentEvent: {e}"),
                        ErrorSource::Harness,
                    ))
                }
            }
        })
    }
}

fn event_type_str(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::SessionStarted { .. } => "session_started",
        AgentEvent::Message { .. } => "message",
        AgentEvent::Progress { .. } => "progress",
        AgentEvent::ReasoningSummary { .. } => "reasoning_summary",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallCompleted { .. } => "tool_call_completed",
        AgentEvent::Result { .. } => "result",
        AgentEvent::Error { .. } => "error",
        AgentEvent::ProcessExited { .. } => "process_exited",
        AgentEvent::RawVendorEvent { .. } => "raw_vendor_event",
        AgentEvent::SessionEnded { .. } => "session_ended",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    #[tokio::test]
    async fn test_event_sink_persists() {
        let db = Database::open_in_memory().await.unwrap();
        let mut sink = SchedulerEventSink::new(db.pool.clone(), "exec-1".into(), None);

        let event = AgentEvent::SessionStarted {
            session_id: "s1".into(),
            profile_id: "p1".into(),
        };
        sink.send(event).await.unwrap();

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM event_log WHERE stream_id = 'exec-1'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1);
    }

    #[tokio::test]
    async fn test_event_sink_sequence_ordered() {
        let db = Database::open_in_memory().await.unwrap();
        let mut sink = SchedulerEventSink::new(db.pool.clone(), "exec-2".into(), None);

        sink.send(AgentEvent::Progress {
            summary: "first".into(),
        })
        .await
        .unwrap();
        sink.send(AgentEvent::Result {
            content: "done".into(),
            is_error: false,
        })
        .await
        .unwrap();
        sink.send(AgentEvent::ProcessExited {
            exit_code: 0,
            signal: None,
        })
        .await
        .unwrap();

        let rows: Vec<(i64,)> = sqlx::query_as(
            "SELECT stream_version FROM event_log WHERE stream_id = 'exec-2' ORDER BY stream_version",
        )
        .fetch_all(&db.pool)
        .await
        .unwrap();
        assert_eq!(rows, vec![(1,), (2,), (3,)]);
    }

    #[tokio::test]
    async fn test_event_sink_closed_after_error() {
        let db = Database::open_in_memory().await.unwrap();
        let mut sink = SchedulerEventSink::new(db.pool.clone(), "exec-3".into(), None);
        sink.close();
        let result = sink
            .send(AgentEvent::Progress {
                summary: "test".into(),
            })
            .await;
        assert!(result.is_err());
        assert!(sink.is_closed());
    }

    #[tokio::test]
    async fn test_raw_vendor_event_persisted() {
        let db = Database::open_in_memory().await.unwrap();
        let mut sink = SchedulerEventSink::new(db.pool.clone(), "exec-4".into(), None);
        sink.send(AgentEvent::RawVendorEvent {
            raw_type: "test.event".into(),
            payload: serde_json::json!({"data": "val"}),
        })
        .await
        .unwrap();

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM event_log WHERE stream_id = 'exec-4'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1);
    }

    #[tokio::test]
    async fn test_sink_redacts_secrets_in_events() {
        let db = Database::open_in_memory().await.unwrap();
        let mut redactor = ProcessEventRedactor::new();
        redactor.register_secret("sk-ant-secret-key-12345");

        let mut sink =
            SchedulerEventSink::with_redactor(db.pool.clone(), "exec-5".into(), None, redactor);

        // Message with a secret embedded in content
        sink.send(AgentEvent::Message {
            content: "Using API key: sk-ant-secret-key-12345 for auth".into(),
            vendor_event_id: None,
        })
        .await
        .unwrap();

        // Check the persisted payload is redacted
        let payload: (String,) = sqlx::query_as(
            "SELECT payload_json FROM event_log WHERE stream_id = 'exec-5' ORDER BY stream_version DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(!payload.0.contains("sk-ant-secret-key-12345"));
        assert!(payload.0.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn test_sink_redacts_secrets_in_tool_result() {
        let db = Database::open_in_memory().await.unwrap();
        let mut redactor = ProcessEventRedactor::new();
        redactor.register_secret("my-token-value");

        let mut sink =
            SchedulerEventSink::with_redactor(db.pool.clone(), "exec-6".into(), None, redactor);

        sink.send(AgentEvent::ToolCallCompleted {
            tool_use_id: "tu-1".into(),
            is_error: false,
            content_preview: "token=my-token-value url=https://api.example.com".into(),
        })
        .await
        .unwrap();

        let payload: (String,) = sqlx::query_as(
            "SELECT payload_json FROM event_log WHERE stream_id = 'exec-6' ORDER BY stream_version DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(!payload.0.contains("my-token-value"));
        assert!(payload.0.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn test_sink_redacts_raw_vendor_event() {
        let db = Database::open_in_memory().await.unwrap();
        let mut redactor = ProcessEventRedactor::new();
        redactor.register_secret("provider-secret-xyz");

        let mut sink =
            SchedulerEventSink::with_redactor(db.pool.clone(), "exec-7".into(), None, redactor);

        sink.send(AgentEvent::RawVendorEvent {
            raw_type: "auth.token".into(),
            payload: serde_json::json!({"access_token": "provider-secret-xyz"}),
        })
        .await
        .unwrap();

        let payload: (String,) = sqlx::query_as(
            "SELECT payload_json FROM event_log WHERE stream_id = 'exec-7' ORDER BY stream_version DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(!payload.0.contains("provider-secret-xyz"));
        assert!(payload.0.contains("[REDACTED]"));
    }
}
