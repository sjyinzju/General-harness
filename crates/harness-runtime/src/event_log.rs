//! Append-only event log — no update/delete API. Query helpers.

use harness_core::contracts::repository::EventLogEntry;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use crate::repo::event_row;

/// Append events within an existing transaction.
pub async fn append_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    events: &[EventLogEntry],
) -> Result<(), CoreError> {
    for e in events {
        sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, causation_id, idempotency_key, source) VALUES (?,?,?,?,?,?,?,?,?,?)")
            .bind(&e.id).bind(&e.stream_id).bind(e.stream_version as i64).bind(&e.event_type)
            .bind(&e.payload_json).bind(e.schema_version as i64).bind(&e.correlation_id)
            .bind(&e.causation_id).bind(&e.idempotency_key).bind(&e.source)
            .execute(&mut **tx).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
    }
    Ok(())
}

/// Query events by stream.
pub async fn get_by_stream(
    pool: &SqlitePool,
    stream_id: &str,
    since_version: Option<u32>,
) -> Result<Vec<EventLogEntry>, CoreError> {
    let sv = since_version.unwrap_or(0) as i64;
    let rows: Vec<event_row::EventRow> = sqlx::query_as("SELECT id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, causation_id, idempotency_key, source, created_at FROM event_log WHERE stream_id = ? AND stream_version > ? ORDER BY stream_version")
        .bind(stream_id).bind(sv).fetch_all(pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
    Ok(rows
        .into_iter()
        .map(|r| EventLogEntry {
            id: r.id,
            stream_id: r.stream_id,
            stream_version: r.stream_version as u32,
            event_type: r.event_type,
            payload_json: r.payload_json,
            schema_version: r.schema_version as u32,
            correlation_id: r.correlation_id,
            causation_id: r.causation_id,
            idempotency_key: r.idempotency_key,
            source: r.source,
            timestamp: r.created_at,
        })
        .collect())
}
