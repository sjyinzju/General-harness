//! Idempotency with ownership model — PENDING → COMPLETED/FAILED.
//! Prevents concurrent execution of the same idempotency key.

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum ClaimStatus {
    Pending,
    Completed,
    FailedRetryable,
    FailedFinal,
}

/// Try to claim an idempotency key for execution.
/// Returns Ok(Some(token)) if claimed, Ok(None) if already completed,
/// Err if request_hash mismatches or lease is held by another owner.
pub async fn try_claim(
    pool: &SqlitePool, key: &str, request_hash: &str, lease_secs: u32,
) -> Result<Option<String>, CoreError> {
    let token = Uuid::new_v4().to_string();
    let now = now_sql();
    let expires = expires_sql(lease_secs);

    // Check if already completed
    let existing: Option<(String, String, String)> = sqlx::query_as(
        "SELECT status, request_hash, owner_token FROM idempotency_records WHERE key = ?"
    ).bind(key).fetch_optional(pool).await.map_err(db_err)?;

    if let Some((status, existing_hash, _owner)) = existing {
        match status.as_str() {
            "completed" | "failed_final" => return Ok(None), // Already terminal
            "failed_retryable" => {
                if existing_hash != request_hash {
                    return Err(CoreError::new(ErrorCode::PersistenceError,
                        format!("idempotency_request_mismatch: existing_hash={existing_hash}"),
                        ErrorSource::System));
                }
                // Allow retry with same hash
            }
            "pending" => {
                if existing_hash != request_hash {
                    return Err(CoreError::new(ErrorCode::PersistenceError,
                        "idempotency_request_mismatch: different request_hash for pending key",
                        ErrorSource::System));
                }
                // Check if lease expired → takeover
                let lease_exp: Option<(String,)> = sqlx::query_as(
                    "SELECT lease_expires_at FROM idempotency_records WHERE key = ? AND lease_expires_at < datetime('now')"
                ).bind(key).fetch_optional(pool).await.map_err(db_err)?;
                if lease_exp.is_none() {
                    return Err(CoreError::new(ErrorCode::PersistenceError,
                        "idempotency_in_progress",
                        ErrorSource::System));
                }
                // Lease expired — takeover allowed (fall through to INSERT OR REPLACE)
                // We use UPDATE instead
                sqlx::query("UPDATE idempotency_records SET status='pending', owner_token=?, lease_expires_at=?, attempt_count=attempt_count+1, request_hash=?, updated_at=? WHERE key=? AND lease_expires_at < datetime('now')")
                    .bind(&token).bind(&expires).bind(request_hash).bind(&now).bind(key)
                    .execute(pool).await.map_err(db_err)?;
                return Ok(Some(token));
            }
            _ => {}
        }
    }

    // Insert or take over expired/completed claim
    let result = sqlx::query("INSERT INTO idempotency_records (key, request_hash, status, owner_token, lease_expires_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET status='pending', owner_token=?, lease_expires_at=?, attempt_count=idempotency_records.attempt_count+1, request_hash=?, updated_at=? WHERE idempotency_records.lease_expires_at < datetime('now') OR idempotency_records.status IN ('completed','failed_retryable','failed_final')")
        .bind(key).bind(request_hash).bind("pending").bind(&token).bind(&expires).bind(&now).bind(&now)
        .bind(&token).bind(&expires).bind(request_hash).bind(&now)
        .execute(pool).await.map_err(db_err)?;

    if result.rows_affected() == 0 {
        return Ok(None); // Someone else holds the lease
    }
    Ok(Some(token))
}

/// Renew the claim lease.
pub async fn renew_claim(pool: &SqlitePool, key: &str, token: &str, lease_secs: u32) -> Result<(), CoreError> {
    let expires = expires_sql(lease_secs);
    let r = sqlx::query("UPDATE idempotency_records SET lease_expires_at=?, updated_at=datetime('now') WHERE key=? AND owner_token=? AND status='pending'")
        .bind(&expires).bind(key).bind(token).execute(pool).await.map_err(db_err)?;
    if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::PersistenceError, "claim not found or token mismatch", ErrorSource::System)); }
    Ok(())
}

/// Complete a claim with a successful result.
pub async fn complete_claim(pool: &SqlitePool, key: &str, token: &str, result_json: &str) -> Result<(), CoreError> {
    let r = sqlx::query("UPDATE idempotency_records SET status='completed', result_json=?, owner_token=NULL, lease_expires_at=NULL, completed_at=datetime('now'), updated_at=datetime('now') WHERE key=? AND owner_token=? AND status='pending'")
        .bind(result_json).bind(key).bind(token).execute(pool).await.map_err(db_err)?;
    if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::PersistenceError, "claim not found, token mismatch, or not pending", ErrorSource::System)); }
    Ok(())
}

/// Fail a claim (retryable or final).
pub async fn fail_claim(pool: &SqlitePool, key: &str, token: &str, error_json: &str, final_fail: bool) -> Result<(), CoreError> {
    let status = if final_fail { "failed_final" } else { "failed_retryable" };
    let r = sqlx::query("UPDATE idempotency_records SET status=?, error_json=?, owner_token=NULL, lease_expires_at=NULL, completed_at=datetime('now'), updated_at=datetime('now') WHERE key=? AND owner_token=? AND status='pending'")
        .bind(status).bind(error_json).bind(key).bind(token).execute(pool).await.map_err(db_err)?;
    if r.rows_affected() == 0 { return Err(CoreError::new(ErrorCode::PersistenceError, "claim not found or token mismatch", ErrorSource::System)); }
    Ok(())
}

/// Get result for a completed key.
pub async fn get_result(pool: &SqlitePool, key: &str) -> Result<Option<String>, CoreError> {
    let row: Option<(String, String)> = sqlx::query_as("SELECT status, result_json FROM idempotency_records WHERE key = ?")
        .bind(key).fetch_optional(pool).await.map_err(db_err)?;
    match row {
        Some((status, result)) if status == "completed" => Ok(Some(result)),
        _ => Ok(None),
    }
}

// Legacy helper (simpler API, used by transition service)
pub async fn is_duplicate(pool: &SqlitePool, key: &str) -> Result<bool, CoreError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT key FROM idempotency_records WHERE key = ? AND status = 'completed'")
        .bind(key).fetch_optional(pool).await.map_err(db_err)?;
    Ok(row.is_some())
}

pub async fn record_in_tx(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, key: &str, result: &str) -> Result<(), CoreError> {
    let now = now_sql();
    sqlx::query("INSERT OR IGNORE INTO idempotency_records (key, request_hash, status, result_json, created_at, updated_at) VALUES (?,?,'completed',?,?,?)")
        .bind(key).bind(key).bind(result).bind(&now).bind(&now)
        .execute(&mut **tx).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
    Ok(())
}

fn now_sql() -> String { chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string() }
fn expires_sql(secs: u32) -> String { (chrono::Utc::now() + chrono::Duration::seconds(secs as i64)).format("%Y-%m-%d %H:%M:%S").to_string() }
fn db_err(e: sqlx::Error) -> CoreError { CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System) }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database { Database::open_in_memory().await.unwrap() }

    #[tokio::test]
    async fn test_claim_complete_read() {
        let db = setup().await;
        let key = "claim-test-1";
        let hash = "hash-1";
        let token = try_claim(&db.pool, key, hash, 60).await.unwrap().unwrap();
        complete_claim(&db.pool, key, &token, r#""ok""#).await.unwrap();
        let result = get_result(&db.pool, key).await.unwrap();
        assert_eq!(result, Some(r#""ok""#.into()));
        // Second claim returns None (already completed)
        let claim2 = try_claim(&db.pool, key, hash, 60).await.unwrap();
        assert!(claim2.is_none());
    }

    #[tokio::test]
    async fn test_request_hash_mismatch_rejected() {
        let db = setup().await;
        let key = "mismatch-test";
        try_claim(&db.pool, key, "hash-A", 60).await.unwrap();
        let result = try_claim(&db.pool, key, "hash-B", 60).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_claim_only_one_owner() {
        let db = setup().await;
        let key = "concurrent-claim";
        let pool = std::sync::Arc::new(db.pool.clone());
        let pool2 = pool.clone();
        let (r1, r2) = tokio::join!(
            try_claim(&pool, key, "hash-1", 60),
            try_claim(&pool2, key, "hash-1", 60),
        );
        // Only one should succeed in claiming
        let ok = r1.as_ref().ok().and_then(|o| o.as_ref()).is_some() as u8
               + r2.as_ref().ok().and_then(|o| o.as_ref()).is_some() as u8;
        assert!(ok <= 1, "At most one concurrent claim should succeed");
    }

    #[tokio::test]
    async fn test_old_owner_cannot_complete_after_takeover() {
        let db = setup().await;
        let key = "takeover-test";
        let token1 = try_claim(&db.pool, key, "hash-1", 1).await.unwrap().unwrap();
        // Let lease expire (lease=1s, sleep=3s for safety margin)
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let token2 = try_claim(&db.pool, key, "hash-1", 60).await.unwrap().unwrap();
        assert_ne!(token1, token2);
        // Old owner tries to complete — must fail
        let r = complete_claim(&db.pool, key, &token1, r#""stale""#).await;
        assert!(r.is_err());
        // New owner can complete
        complete_claim(&db.pool, key, &token2, r#""fresh""#).await.unwrap();
    }
}
