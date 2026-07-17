//! Database connection, bootstrap, and migrations.

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub struct Database {
    pub pool: SqlitePool,
}

impl Database {
    /// Open or create a database at `path`. Runs migrations automatically.
    pub async fn open(path: &Path) -> Result<Self, sqlx::Error> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                sqlx::Error::Configuration(format!("Failed to create DB parent dir: {e}").into())
            })?;
        }

        let opts = SqliteConnectOptions::from_str(&path.to_string_lossy())?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5) // WAL mode allows concurrent reads, one writer
            .connect_with(opts)
            .await?;

        // Run migrations
        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    /// Open an in-memory database for testing.
    pub async fn open_in_memory() -> Result<Self, sqlx::Error> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")?
            .create_if_missing(true)
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    /// Database file path from a directory.
    pub fn db_path(dir: &Path) -> PathBuf {
        dir.join("harness.db")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_open_in_memory() {
        let db = Database::open_in_memory().await.unwrap();
        // Verify tables exist
        let row: (String,) =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' AND name='projects'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(row.0, "projects");
    }

    #[tokio::test]
    async fn test_open_temp_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = Database::db_path(tmp.path());
        let db = Database::open(&path).await.unwrap();
        let row: (String,) =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(row.0, "tasks");
    }

    #[tokio::test]
    async fn test_repeated_open() {
        let tmp = tempfile::tempdir().unwrap();
        let path = Database::db_path(tmp.path());
        // Open twice — second should be safe (migrations idempotent)
        let _db1 = Database::open(&path).await.unwrap();
        let _db2 = Database::open(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_table_count() {
        let db = Database::open_in_memory().await.unwrap();
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_sqlx_%' ORDER BY name"
        ).fetch_all(&db.pool).await.unwrap();
        let names: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "agent_definitions",
                "agent_provider_hints",
                "discovery_evidence",
                "dispatch_operations",
                "event_log",
                "execution_attempts",
                "idempotency_records",
                "operations",
                "policy_approvals",
                "policy_evaluations",
                "policy_findings",
                "projects",
                "resource_claim_groups",
                "resource_claims",
                "resource_handoffs",
                "runtime_profiles",
                "scheduler_reconciliations",
                "scheduler_reservations",
                "task_dependencies",
                "tasks",
                "verification_approvals",
                "verification_diagnostics",
                "verification_evidence",
                "verification_ownership_events",
                "verification_plans",
                "verification_runs",
                "verification_step_events",
                "verification_step_operations",
                "verification_step_processes",
                "verification_step_results",
                "workspace_leases",
                "worktrees"
            ],
            "32 business tables expected (001–015)"
        );
    }

    #[tokio::test]
    async fn test_pragma_foreign_keys() {
        let db = Database::open_in_memory().await.unwrap();
        let row: (i64,) = sqlx::query_as("SELECT foreign_keys FROM pragma_foreign_keys")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(row.0, 1);
    }

    #[tokio::test]
    async fn test_pragma_journal_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let path = Database::db_path(tmp.path());
        let db = Database::open(&path).await.unwrap();
        // WAL is enabled for file-backed DBs (in-memory defaults to "memory")
        let row: (String,) = sqlx::query_as("SELECT * FROM pragma_journal_mode")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(row.0.to_lowercase(), "wal");
    }

    #[tokio::test]
    async fn test_pragma_busy_timeout() {
        let db = Database::open_in_memory().await.unwrap();
        let row: (i64,) = sqlx::query_as("SELECT * FROM pragma_busy_timeout")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert!(
            row.0 >= 1000,
            "busy_timeout should be >= 1000ms, got {}",
            row.0
        );
    }

    #[tokio::test]
    async fn test_reopen_persistence() {
        let tmp = tempfile::tempdir().unwrap();
        let path = Database::db_path(tmp.path());
        // Create and insert
        {
            let db = Database::open(&path).await.unwrap();
            sqlx::query(
                "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1', 'test', 'created')",
            )
            .execute(&db.pool)
            .await
            .unwrap();
        }
        // Reopen — data persists
        {
            let db = Database::open(&path).await.unwrap();
            let row: (String,) = sqlx::query_as("SELECT objective FROM projects WHERE id = 'p1'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
            assert_eq!(row.0, "test");
        }
    }

    #[tokio::test]
    async fn test_migration_version() {
        let db = Database::open_in_memory().await.unwrap();
        // _sqlx_migrations exists and has at least v1
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations WHERE version >= 1")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert!(row.0 >= 1);
    }

    #[tokio::test]
    async fn test_foreign_keys_enforced() {
        let db = Database::open_in_memory().await.unwrap();
        // Try inserting a task with non-existent project
        let result = sqlx::query("INSERT INTO tasks (id, project_id, lifecycle) VALUES ('t1', 'no-such-project', 'pending')")
            .execute(&db.pool)
            .await;
        assert!(result.is_err());
    }
}
