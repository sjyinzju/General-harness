//! ConcurrencyManager — atomic reservation of execution slots.
//! Uses SQLite constraints for cross-connection arbitration.

use harness_core::contracts::scheduler::{ConcurrencyConfig, ReservationResult};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

pub struct ConcurrencyManager {
    pool: SqlitePool,
    config: ConcurrencyConfig,
}

impl ConcurrencyManager {
    pub fn new(pool: SqlitePool, config: ConcurrencyConfig) -> Self {
        Self { pool, config }
    }

    /// Atomically reserve a concurrency slot for a task execution.
    /// Checks: global limit, per-profile limit, per-repository limit.
    /// A single reservation row covers all limits.
    pub async fn reserve(
        &self,
        task_id: &str,
        profile_id: Option<&str>,
        repository_id: Option<&str>,
    ) -> Result<ReservationResult, CoreError> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System)
        })?;

        // Check global limit
        let global_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM scheduler_reservations WHERE status='active'",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        if global_count.0 >= self.config.global_max as i64 {
            return Ok(ReservationResult::GlobalLimitReached);
        }

        // Check per-profile limit
        if let Some(pid) = profile_id {
            let profile_count: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM scheduler_reservations WHERE profile_id=? AND status='active'",
            )
            .bind(pid)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

            if profile_count.0 >= self.config.per_profile_max as i64 {
                return Ok(ReservationResult::ProfileLimitReached {
                    profile_id: pid.to_string(),
                });
            }
        }

        // Check per-repository limit
        if let Some(rid) = repository_id {
            let repo_count: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM scheduler_reservations WHERE repository_id=? AND status='active'",
            )
            .bind(rid)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

            if repo_count.0 >= self.config.per_repository_max as i64 {
                return Ok(ReservationResult::RepositoryLimitReached {
                    repository_id: rid.to_string(),
                });
            }
        }

        // All limits satisfied — insert single reservation row
        let reservation_id = Uuid::new_v4().to_string();
        let expires = (chrono::Utc::now() + chrono::Duration::minutes(15))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        let result = sqlx::query(
            "INSERT INTO scheduler_reservations (id, task_id, profile_id, repository_id, status, expires_at) VALUES (?,?,?,?,'active',?)",
        )
        .bind(&reservation_id).bind(task_id)
        .bind(profile_id).bind(repository_id)
        .bind(&expires)
        .execute(&mut *tx).await;

        match result {
            Ok(_) => {
                tx.commit().await.map_err(|e| {
                    CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System)
                })?;
                Ok(ReservationResult::Reserved {
                    reservation_id,
                    profile_id: profile_id.map(|s| s.to_string()),
                    repository_id: repository_id.map(|s| s.to_string()),
                })
            }
            Err(e) => {
                let _ = tx.rollback().await;
                // UNIQUE constraint violation → task already reserved
                if e.to_string().contains("UNIQUE") {
                    Err(CoreError::new(
                        ErrorCode::ResourceConflict {
                            resource: format!("task:{task_id}"),
                        },
                        format!("task already has active reservation: {e}"),
                        ErrorSource::System,
                    ))
                } else {
                    Err(CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))
                }
            }
        }
    }

    /// Release all reservations for a task.
    pub async fn release(&self, task_id: &str) -> Result<(), CoreError> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        sqlx::query(
            "UPDATE scheduler_reservations SET status='released', released_at=? WHERE task_id=? AND status='active'",
        )
        .bind(&now).bind(task_id)
        .execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(())
    }

    /// Expire stale reservations.
    pub async fn expire_stale(&self) -> Result<usize, CoreError> {
        let result = sqlx::query(
            "UPDATE scheduler_reservations SET status='expired' WHERE status='active' AND expires_at < datetime('now')",
        )
        .execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(result.rows_affected() as usize)
    }

    /// Check if a task has an active reservation.
    pub async fn has_active(&self, task_id: &str) -> Result<bool, CoreError> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM scheduler_reservations WHERE task_id=? AND status='active'",
        )
        .bind(task_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(count.0 > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        Database::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn test_reserve_success() {
        let db = setup().await;
        create_project_and_task(&db, "t1").await;
        let mgr = ConcurrencyManager::new(db.pool.clone(), ConcurrencyConfig::default());
        let result = mgr.reserve("t1", Some("p1"), None).await.unwrap();
        assert!(matches!(result, ReservationResult::Reserved { .. }));
    }

    #[tokio::test]
    async fn test_one_active_per_task() {
        let db = setup().await;
        create_project_and_task(&db, "t1").await;
        let mgr = ConcurrencyManager::new(db.pool.clone(), ConcurrencyConfig::default());
        mgr.reserve("t1", None, None).await.unwrap();
        // Second reservation for same task should fail
        let result = mgr.reserve("t1", None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_global_limit() {
        let db = setup().await;
        let config = ConcurrencyConfig {
            global_max: 1,
            ..Default::default()
        };
        create_project_and_task(&db, "t1").await;
        create_project_and_task(&db, "t2").await;
        let mgr = ConcurrencyManager::new(db.pool.clone(), config);
        mgr.reserve("t1", None, None).await.unwrap();
        let result = mgr.reserve("t2", None, None).await.unwrap();
        assert!(matches!(result, ReservationResult::GlobalLimitReached));
    }

    #[tokio::test]
    async fn test_per_profile_limit() {
        let db = setup().await;
        let config = ConcurrencyConfig {
            per_profile_max: 1,
            ..Default::default()
        };
        create_project_and_task(&db, "t1").await;
        create_project_and_task(&db, "t2").await;
        let mgr = ConcurrencyManager::new(db.pool.clone(), config);
        mgr.reserve("t1", Some("p1"), None).await.unwrap();
        let result = mgr.reserve("t2", Some("p1"), None).await.unwrap();
        assert!(matches!(
            result,
            ReservationResult::ProfileLimitReached { .. }
        ));
    }

    #[tokio::test]
    async fn test_expired_reclaimed() {
        let db = setup().await;
        create_project_and_task(&db, "t1").await;
        let mgr = ConcurrencyManager::new(db.pool.clone(), ConcurrencyConfig::default());
        mgr.reserve("t1", None, None).await.unwrap();

        // Force expire by setting expires_at to past
        sqlx::query("UPDATE scheduler_reservations SET expires_at='2000-01-01' WHERE task_id='t1'")
            .execute(&db.pool).await.unwrap();

        let expired = mgr.expire_stale().await.unwrap();
        assert!(expired > 0);

        // Should be able to reserve again
        let result = mgr.reserve("t1", None, None).await.unwrap();
        assert!(matches!(result, ReservationResult::Reserved { .. }));
    }

    async fn create_project_and_task(db: &Database, task_id: &str) {
        let pid = format!("proj-{}", task_id);
        sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?,'test','active')")
            .bind(&pid).execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?,?,'test','pending')")
            .bind(task_id).bind(&pid).execute(&db.pool).await.unwrap();
    }
}
