//! Idempotency — ensure commands are executed at most once.

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Check if an idempotency key has already been processed.
pub async fn is_duplicate(pool: &SqlitePool, key: &str) -> Result<bool, CoreError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT key FROM idempotency_records WHERE key = ?")
        .bind(key).fetch_optional(pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
    Ok(row.is_some())
}

/// Record an idempotency key and result inside an existing transaction.
pub async fn record_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key: &str,
    result: &str,
) -> Result<(), CoreError> {
    // Use INSERT OR IGNORE to handle concurrent duplicates gracefully
    sqlx::query("INSERT OR IGNORE INTO idempotency_records (key, result_json) VALUES (?, ?)")
        .bind(key).bind(result)
        .execute(&mut **tx).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
    Ok(())
}

/// Execute a function exactly once for the given idempotency key.
/// Returns the cached result on duplicate.
pub async fn execute_once<F, Fut, T>(
    pool: &SqlitePool, key: &str, f: F,
) -> Result<T, CoreError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>>,
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    // Check if already done
    if let Some(row) = sqlx::query_as::<_, (String,)>("SELECT result_json FROM idempotency_records WHERE key = ?")
        .bind(key).fetch_optional(pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))? {
        return serde_json::from_str(&row.0).map_err(|e| CoreError::new(ErrorCode::Internal, e.to_string(), ErrorSource::System));
    }

    // Execute
    let result = f().await?;

    // Record
    let result_json = serde_json::to_string(&result).map_err(|e| CoreError::new(ErrorCode::Internal, e.to_string(), ErrorSource::System))?;
    sqlx::query("INSERT OR IGNORE INTO idempotency_records (key, result_json) VALUES (?, ?)")
        .bind(key).bind(&result_json).execute(pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    #[tokio::test]
    async fn test_idempotency_duplicate_returns_original() {
        let db = Database::open_in_memory().await.unwrap();
        let key = "test-key-1";

        let r1: Result<String, _> = execute_once(&db.pool, key, || async { Ok("first".to_string()) }).await;
        assert_eq!(r1.unwrap(), "first");

        let r2: Result<String, _> = execute_once(&db.pool, key, || async { Ok("second".to_string()) }).await;
        assert_eq!(r2.unwrap(), "first"); // returns cached, not "second"
    }
}
