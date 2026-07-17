//! HandoffRepository — persistent storage for resource handoff records.
//!
//! Each handoff record links a completed execution to its retained resources
//! (worktree, lease, claim_group) so that I4-C Verification can discover,
//! inspect, and take over ownership.
//!
//! Takeover uses optimistic locking (version column) for CAS semantics.
//! The repository never persists lease tokens, API keys, or env values.

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// Persisted handoff status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffStatus {
    SchedulerOwned,
    VerificationOwned,
    Released,
    Lost,
    ReconciliationRequired,
}

impl HandoffStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HandoffStatus::SchedulerOwned => "scheduler_owned",
            HandoffStatus::VerificationOwned => "verification_owned",
            HandoffStatus::Released => "released",
            HandoffStatus::Lost => "lost",
            HandoffStatus::ReconciliationRequired => "reconciliation_required",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "verification_owned" => HandoffStatus::VerificationOwned,
            "released" => HandoffStatus::Released,
            "lost" => HandoffStatus::Lost,
            "reconciliation_required" => HandoffStatus::ReconciliationRequired,
            _ => HandoffStatus::SchedulerOwned,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, HandoffStatus::Released | HandoffStatus::Lost)
    }
}

/// A persisted resource handoff record.
#[derive(Debug, Clone)]
pub struct HandoffRecord {
    pub handoff_id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub worktree_id: Option<String>,
    pub lease_id: Option<String>,
    pub claim_group_id: Option<String>,
    pub fencing_token: i64,
    pub owner_kind: String,
    pub owner_id: String,
    pub status: String,
    pub heartbeat_last_seen_at: Option<String>,
    pub detail_json: Option<String>,
    pub version: i64,
}

/// Row type for handoff queries.
type HandoffRow = (
    String,         // handoff_id
    String,         // project_id
    String,         // task_id
    String,         // execution_id
    Option<String>, // worktree_id
    Option<String>, // lease_id
    Option<String>, // claim_group_id
    i64,            // fencing_token
    String,         // owner_kind
    String,         // owner_id
    String,         // status
    Option<String>, // heartbeat_last_seen_at
    Option<String>, // detail_json
    i64,            // version
);

/// Result of a takeover CAS operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakeoverPersistResult {
    /// Takeover succeeded.
    Acquired,
    /// Same owner repeated (idempotent).
    AlreadyOwned,
    /// Another owner already took over.
    Contested { current_owner: String },
    /// Version mismatch (concurrent modification).
    VersionConflict,
    /// Handoff is in a terminal state.
    TerminalState,
    /// Handoff not found.
    NotFound,
}

/// Parameters for creating a handoff record.
pub struct CreateHandoffParams<'a> {
    pub execution_id: &'a str,
    pub worktree_id: Option<&'a str>,
    pub lease_id: Option<&'a str>,
    pub claim_group_id: Option<&'a str>,
    pub fencing_token: i64,
    pub owner_id: &'a str,
}

#[derive(Clone)]
pub struct HandoffRepository {
    pool: SqlitePool,
}

impl HandoffRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Create a new handoff record after successful dispatch.
    pub async fn create(
        &self,
        handoff_id: &str,
        project_id: &str,
        task_id: &str,
        params: CreateHandoffParams<'_>,
    ) -> Result<HandoffRecord, CoreError> {
        sqlx::query(
            "INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, claim_group_id, fencing_token, owner_kind, owner_id, status) VALUES (?,?,?,?,?,?,?,?,'scheduler',?,'scheduler_owned')",
        )
        .bind(handoff_id)
        .bind(project_id)
        .bind(task_id)
        .bind(params.execution_id)
        .bind(params.worktree_id)
        .bind(params.lease_id)
        .bind(params.claim_group_id)
        .bind(params.fencing_token)
        .bind(params.owner_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("create handoff: {e}"),
                ErrorSource::System,
            )
        })?;

        self.get_by_execution(params.execution_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    "handoff created but not found".to_string(),
                    ErrorSource::System,
                )
            })
    }

    /// Get a handoff by execution_id.
    pub async fn get_by_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<HandoffRecord>, CoreError> {
        let row: Option<HandoffRow> = sqlx::query_as(
            "SELECT handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, claim_group_id, fencing_token, owner_kind, owner_id, status, heartbeat_last_seen_at, detail_json, version FROM resource_handoffs WHERE execution_id = ?",
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get handoff by execution: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(into_record))
    }

    /// Get a handoff by lease_id.
    pub async fn get_by_lease(&self, lease_id: &str) -> Result<Option<HandoffRecord>, CoreError> {
        let row: Option<HandoffRow> = sqlx::query_as(
            "SELECT handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, claim_group_id, fencing_token, owner_kind, owner_id, status, heartbeat_last_seen_at, detail_json, version FROM resource_handoffs WHERE lease_id = ?",
        )
        .bind(lease_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get handoff by lease: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(into_record))
    }

    /// Get a handoff by handoff_id.
    pub async fn get(&self, handoff_id: &str) -> Result<Option<HandoffRecord>, CoreError> {
        let row: Option<HandoffRow> = sqlx::query_as(
            "SELECT handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, claim_group_id, fencing_token, owner_kind, owner_id, status, heartbeat_last_seen_at, detail_json, version FROM resource_handoffs WHERE handoff_id = ?",
        )
        .bind(handoff_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("get handoff: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(row.map(into_record))
    }

    /// Takeover a handoff — CAS with version for optimistic locking.
    ///
    /// Only succeeds if:
    /// - The handoff exists and is in scheduler_owned or verification_owned status
    /// - The current version matches `expected_version`
    /// - The handoff is not in a terminal state
    ///
    /// Idempotent: same owner repeating takeover with same version is Accepted.
    pub async fn takeover(
        &self,
        execution_id: &str,
        verification_owner_id: &str,
        expected_version: i64,
    ) -> Result<TakeoverPersistResult, CoreError> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("begin takeover tx: {e}"),
                ErrorSource::System,
            )
        })?;

        let current: Option<(String, String, i64, String)> = sqlx::query_as(
            "SELECT owner_kind, owner_id, version, status FROM resource_handoffs WHERE execution_id = ?",
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("takeover read: {e}"),
                ErrorSource::System,
            )
        })?;

        let (owner_kind, owner_id, version, status) = match current {
            Some(row) => row,
            None => return Ok(TakeoverPersistResult::NotFound),
        };

        // Terminal states cannot be taken over
        let hs = HandoffStatus::parse(&status);
        if hs.is_terminal() {
            return Ok(TakeoverPersistResult::TerminalState);
        }
        if hs == HandoffStatus::ReconciliationRequired {
            return Ok(TakeoverPersistResult::TerminalState);
        }

        // Idempotent: same owner already owns it
        if owner_kind == "verification" && owner_id == verification_owner_id {
            return Ok(TakeoverPersistResult::AlreadyOwned);
        }

        // Contested: different verification owner
        if owner_kind == "verification" && owner_id != verification_owner_id {
            return Ok(TakeoverPersistResult::Contested {
                current_owner: owner_id,
            });
        }

        // Version check (optimistic locking)
        if version != expected_version {
            return Ok(TakeoverPersistResult::VersionConflict);
        }

        // CAS update: only if version still matches
        let rows = sqlx::query(
            "UPDATE resource_handoffs SET owner_kind = 'verification', owner_id = ?, status = 'verification_owned', version = version + 1, updated_at = datetime('now') WHERE execution_id = ? AND version = ?",
        )
        .bind(verification_owner_id)
        .bind(execution_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("takeover update: {e}"),
                ErrorSource::System,
            )
        })?;

        if rows.rows_affected() == 0 {
            return Ok(TakeoverPersistResult::VersionConflict);
        }

        tx.commit().await.map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("takeover commit: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(TakeoverPersistResult::Acquired)
    }

    /// Update heartbeat_last_seen_at for a handoff.
    pub async fn update_heartbeat(&self, execution_id: &str) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE resource_handoffs SET heartbeat_last_seen_at = datetime('now'), updated_at = datetime('now') WHERE execution_id = ?",
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("update heartbeat: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Mark a handoff as released.
    pub async fn mark_released(&self, execution_id: &str, reason: &str) -> Result<(), CoreError> {
        let detail = serde_json::json!({"reason": reason, "released_at": chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()}).to_string();
        sqlx::query(
            "UPDATE resource_handoffs SET status = 'released', detail_json = ?, updated_at = datetime('now'), version = version + 1 WHERE execution_id = ?",
        )
        .bind(&detail)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("mark released: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Mark a handoff as lost (heartbeat missing).
    pub async fn mark_lost(&self, execution_id: &str) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE resource_handoffs SET status = 'lost', updated_at = datetime('now'), version = version + 1 WHERE execution_id = ?",
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("mark lost: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// Mark a handoff as requiring reconciliation.
    pub async fn mark_reconciliation_required(
        &self,
        execution_id: &str,
        detail: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE resource_handoffs SET status = 'reconciliation_required', detail_json = ?, updated_at = datetime('now'), version = version + 1 WHERE execution_id = ?",
        )
        .bind(detail)
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("mark reconciliation: {e}"),
                ErrorSource::System,
            )
        })?;
        Ok(())
    }

    /// List handoffs by status.
    pub async fn list_by_status(&self, status: &str) -> Result<Vec<HandoffRecord>, CoreError> {
        let rows: Vec<HandoffRow> = sqlx::query_as(
            "SELECT handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, claim_group_id, fencing_token, owner_kind, owner_id, status, heartbeat_last_seen_at, detail_json, version FROM resource_handoffs WHERE status = ?",
        )
        .bind(status)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("list by status: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(rows.into_iter().map(into_record).collect())
    }
}

fn into_record(
    (
        handoff_id,
        project_id,
        task_id,
        execution_id,
        worktree_id,
        lease_id,
        claim_group_id,
        fencing_token,
        owner_kind,
        owner_id,
        status,
        heartbeat_last_seen_at,
        detail_json,
        version,
    ): HandoffRow,
) -> HandoffRecord {
    HandoffRecord {
        handoff_id,
        project_id,
        task_id,
        execution_id,
        worktree_id,
        lease_id,
        claim_group_id,
        fencing_token,
        owner_kind,
        owner_id,
        status,
        heartbeat_last_seen_at,
        detail_json,
        version,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        let db = Database::open_in_memory().await.unwrap();
        // Create prerequisite records for FK constraints
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('proj-1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('task-1','proj-1','test','submitted')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('exec-1','task-1',1,'completed')")
            .execute(&db.pool)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        let record = repo
            .create(
                "ho-1",
                "proj-1",
                "task-1",
                CreateHandoffParams {
                    execution_id: "exec-1",
                    worktree_id: Some("wt-1"),
                    lease_id: Some("lease-1"),
                    claim_group_id: Some("cg-1"),
                    fencing_token: 5,
                    owner_id: "scheduler-main",
                },
            )
            .await
            .unwrap();

        assert_eq!(record.handoff_id, "ho-1");
        assert_eq!(record.status, "scheduler_owned");
        assert_eq!(record.fencing_token, 5);

        let found = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        assert_eq!(found.handoff_id, "ho-1");
    }

    #[tokio::test]
    async fn test_get_by_lease() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        repo.create(
            "ho-1",
            "proj-1",
            "task-1",
            CreateHandoffParams {
                execution_id: "exec-1",
                worktree_id: Some("wt-1"),
                lease_id: Some("lease-1"),
                claim_group_id: Some("cg-1"),
                fencing_token: 5,
                owner_id: "scheduler-main",
            },
        )
        .await
        .unwrap();

        let found = repo.get_by_lease("lease-1").await.unwrap().unwrap();
        assert_eq!(found.execution_id, "exec-1");
    }

    #[tokio::test]
    async fn test_takeover_success() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        let record = repo
            .create(
                "ho-1",
                "proj-1",
                "task-1",
                CreateHandoffParams {
                    execution_id: "exec-1",
                    worktree_id: Some("wt-1"),
                    lease_id: Some("lease-1"),
                    claim_group_id: Some("cg-1"),
                    fencing_token: 5,
                    owner_id: "scheduler-main",
                },
            )
            .await
            .unwrap();

        let result = repo
            .takeover("exec-1", "verify-run-1", record.version)
            .await
            .unwrap();
        assert_eq!(result, TakeoverPersistResult::Acquired);

        let updated = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        assert_eq!(updated.owner_kind, "verification");
        assert_eq!(updated.owner_id, "verify-run-1");
        assert_eq!(updated.status, "verification_owned");
    }

    #[tokio::test]
    async fn test_takeover_version_conflict() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        let record = repo
            .create(
                "ho-1",
                "proj-1",
                "task-1",
                CreateHandoffParams {
                    execution_id: "exec-1",
                    worktree_id: Some("wt-1"),
                    lease_id: Some("lease-1"),
                    claim_group_id: Some("cg-1"),
                    fencing_token: 5,
                    owner_id: "scheduler-main",
                },
            )
            .await
            .unwrap();

        // Use wrong version
        let result = repo
            .takeover("exec-1", "verify-run-1", record.version + 99)
            .await
            .unwrap();
        assert_eq!(result, TakeoverPersistResult::VersionConflict);
    }

    #[tokio::test]
    async fn test_takeover_idempotent_same_owner() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        let record = repo
            .create(
                "ho-1",
                "proj-1",
                "task-1",
                CreateHandoffParams {
                    execution_id: "exec-1",
                    worktree_id: Some("wt-1"),
                    lease_id: Some("lease-1"),
                    claim_group_id: Some("cg-1"),
                    fencing_token: 5,
                    owner_id: "scheduler-main",
                },
            )
            .await
            .unwrap();

        // First takeover
        let result = repo
            .takeover("exec-1", "verify-run-1", record.version)
            .await
            .unwrap();
        assert_eq!(result, TakeoverPersistResult::Acquired);

        // Second takeover by same owner
        let updated = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        let result2 = repo
            .takeover("exec-1", "verify-run-1", updated.version)
            .await
            .unwrap();
        assert_eq!(result2, TakeoverPersistResult::AlreadyOwned);
    }

    #[tokio::test]
    async fn test_takeover_contested() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        let record = repo
            .create(
                "ho-1",
                "proj-1",
                "task-1",
                CreateHandoffParams {
                    execution_id: "exec-1",
                    worktree_id: Some("wt-1"),
                    lease_id: Some("lease-1"),
                    claim_group_id: Some("cg-1"),
                    fencing_token: 5,
                    owner_id: "scheduler-main",
                },
            )
            .await
            .unwrap();

        // First owner takes over
        repo.takeover("exec-1", "verify-run-1", record.version)
            .await
            .unwrap();

        // Second owner tries
        let updated = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        let result = repo
            .takeover("exec-1", "verify-run-2", updated.version)
            .await
            .unwrap();
        assert!(matches!(result, TakeoverPersistResult::Contested { .. }));
    }

    #[tokio::test]
    async fn test_takeover_terminal_rejected() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        let record = repo
            .create(
                "ho-1",
                "proj-1",
                "task-1",
                CreateHandoffParams {
                    execution_id: "exec-1",
                    worktree_id: Some("wt-1"),
                    lease_id: Some("lease-1"),
                    claim_group_id: Some("cg-1"),
                    fencing_token: 5,
                    owner_id: "scheduler-main",
                },
            )
            .await
            .unwrap();

        repo.mark_released("exec-1", "test").await.unwrap();

        let result = repo
            .takeover("exec-1", "verify-run-1", record.version)
            .await
            .unwrap();
        assert_eq!(result, TakeoverPersistResult::TerminalState);
    }

    #[tokio::test]
    async fn test_update_heartbeat() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        repo.create(
            "ho-1",
            "proj-1",
            "task-1",
            CreateHandoffParams {
                execution_id: "exec-1",
                worktree_id: Some("wt-1"),
                lease_id: Some("lease-1"),
                claim_group_id: Some("cg-1"),
                fencing_token: 5,
                owner_id: "scheduler-main",
            },
        )
        .await
        .unwrap();

        repo.update_heartbeat("exec-1").await.unwrap();

        let record = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        assert!(record.heartbeat_last_seen_at.is_some());
    }

    #[tokio::test]
    async fn test_mark_lost_and_released() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        repo.create(
            "ho-1",
            "proj-1",
            "task-1",
            CreateHandoffParams {
                execution_id: "exec-1",
                worktree_id: Some("wt-1"),
                lease_id: Some("lease-1"),
                claim_group_id: Some("cg-1"),
                fencing_token: 5,
                owner_id: "scheduler-main",
            },
        )
        .await
        .unwrap();

        repo.mark_lost("exec-1").await.unwrap();
        let record = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        assert_eq!(record.status, "lost");

        // Lost is terminal, cannot takeover
        let result = repo
            .takeover("exec-1", "verify-run-1", record.version)
            .await
            .unwrap();
        assert_eq!(result, TakeoverPersistResult::TerminalState);

        repo.mark_released("exec-1", "done").await.unwrap();
        let final_record = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        assert_eq!(final_record.status, "released");
    }

    #[tokio::test]
    async fn test_lease_token_absent_from_db() {
        let db = setup().await;
        let repo = HandoffRepository::new(db.pool.clone());

        repo.create(
            "ho-1",
            "proj-1",
            "task-1",
            CreateHandoffParams {
                execution_id: "exec-1",
                worktree_id: Some("wt-1"),
                lease_id: Some("lease-1"),
                claim_group_id: Some("cg-1"),
                fencing_token: 5,
                owner_id: "scheduler-main",
            },
        )
        .await
        .unwrap();

        // Check that the handoff record does not contain any token-like data
        let record = repo.get_by_execution("exec-1").await.unwrap().unwrap();
        let detail = record.detail_json.unwrap_or_default();

        // Ensure no lease token patterns in stored data
        assert!(!detail.contains("lease_token"));
        assert!(!detail.contains("token"));
        assert!(!detail.contains("secret"));
        assert!(!detail.contains("api_key"));
    }
}
