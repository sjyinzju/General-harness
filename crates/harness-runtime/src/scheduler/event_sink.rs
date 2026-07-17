//! SchedulerEventSink — production AgentEvent persistence via AgentEventSink.
//! Sequences events per execution, enforces ordering, handles large payloads.

use harness_core::contracts::agent_adapter::AgentEventSink;
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Persists AgentEvents to event_log with execution-scoped sequence numbers.
pub struct SchedulerEventSink {
    pool: SqlitePool,
    execution_id: String,
    sequence: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    /// Large payloads (>64KB) are written to files and referenced by path.
    artifact_dir: Option<std::path::PathBuf>,
    /// Track last persisted sequence for crash recovery.
    last_persisted_seq: Arc<Mutex<u64>>,
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

        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let exec_id = self.execution_id.clone();
        let pool = self.pool.clone();
        let closed = self.closed.clone();
        let last_seq = self.last_persisted_seq.clone();
        let artifact_dir = self.artifact_dir.clone();

        Box::pin(async move {
            let event_json = serde_json::to_string(&event).map_err(|e| {
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
            let event_type = event_type_str(&event);

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
}
