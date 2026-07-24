//! Controlled Commit repository — persistence for commit requests, candidates, and attempts.

use harness_core::contracts::commit::{CommitCandidate, CommitRequest, CommitState};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}

pub struct CommitRepo {
    pool: SqlitePool,
}

impl CommitRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // ── Commit Request ───────────────────────────────────────────────

    /// Insert a new commit request. Returns false if idempotency key already exists.
    pub async fn insert_request(&self, req: &CommitRequest) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO commit_requests (commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_base_commit, author_name, author_email, committer_name, committer_email, commit_timestamp, message, state, idempotency_key, idempotency_digest, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&req.commit_request_id)
        .bind(&req.candidate_id)
        .bind(&req.review_id)
        .bind(&req.repository_id)
        .bind(&req.target_ref)
        .bind(&req.expected_base_commit)
        .bind(&req.author_identity.name)
        .bind(&req.author_identity.email)
        .bind(&req.committer_identity.name)
        .bind(&req.committer_identity.email)
        .bind(req.commit_timestamp.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(&req.message)
        .bind(CommitState::Requested.as_str())
        .bind(&req.idempotency_key)
        .bind(req.idempotency_digest())
        .bind(req.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(req.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Look up a request by its idempotency key.
    pub async fn find_by_idempotency_key(
        &self,
        ikey: &str,
    ) -> Result<Option<CommitRequest>, CoreError> {
        let row: Option<CommitRequestRow> = sqlx::query_as(
            "SELECT commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_base_commit, author_name, author_email, committer_name, committer_email, commit_timestamp, message, state, idempotency_key, idempotency_digest, created_at FROM commit_requests WHERE idempotency_key = ?",
        )
        .bind(ikey)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Get a commit request by ID.
    pub async fn get_request(&self, id: &str) -> Result<Option<CommitRequest>, CoreError> {
        let row: Option<CommitRequestRow> = sqlx::query_as(
            "SELECT commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_base_commit, author_name, author_email, committer_name, committer_email, commit_timestamp, message, state, idempotency_key, idempotency_digest, created_at FROM commit_requests WHERE commit_request_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Get just the state of a commit request.
    pub async fn get_state(&self, id: &str) -> Result<Option<String>, CoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state FROM commit_requests WHERE commit_request_id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?;
        Ok(row.map(|r| r.0))
    }

    /// Find an existing commit request for the same scope (candidate + review + target_ref).
    pub async fn find_by_scope(
        &self,
        candidate_id: &str,
        review_id: &str,
        target_ref: &str,
    ) -> Result<Option<CommitRequest>, CoreError> {
        let row: Option<CommitRequestRow> = sqlx::query_as(
            "SELECT commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_base_commit, author_name, author_email, committer_name, committer_email, commit_timestamp, message, state, idempotency_key, idempotency_digest, created_at FROM commit_requests WHERE candidate_id = ? AND review_id = ? AND target_ref = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(candidate_id)
        .bind(review_id)
        .bind(target_ref)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Transition commit request state (CAS).
    pub async fn transition_state(
        &self,
        commit_request_id: &str,
        from: &CommitState,
        to: &CommitState,
    ) -> Result<bool, CoreError> {
        let is_terminal = to.is_terminal();
        let rows = sqlx::query(
            "UPDATE commit_requests SET state = ?, updated_at = datetime('now'), completed_at = CASE WHEN ? THEN datetime('now') ELSE completed_at END WHERE commit_request_id = ? AND state = ?",
        )
        .bind(to.as_str())
        .bind(is_terminal)
        .bind(commit_request_id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    // ── Commit Candidate ──────────────────────────────────────────────

    /// Insert a commit candidate record. Returns false if already exists.
    pub async fn insert_candidate(&self, cc: &CommitCandidate) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO commit_candidates (commit_request_id, candidate_id, review_id, repository_id, commit_oid, parent_oid, tree_oid, diff_digest, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(&cc.commit_request_id)
        .bind(&cc.candidate_id)
        .bind(&cc.review_id)
        .bind(&cc.repository_id)
        .bind(&cc.commit_oid)
        .bind(&cc.parent_oid)
        .bind(&cc.tree_oid)
        .bind(&cc.diff_digest)
        .bind(cc.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Get a commit candidate by request ID.
    pub async fn get_candidate(
        &self,
        commit_request_id: &str,
    ) -> Result<Option<CommitCandidate>, CoreError> {
        let row: Option<CommitCandidateRow> = sqlx::query_as(
            "SELECT commit_request_id, candidate_id, review_id, repository_id, commit_oid, parent_oid, tree_oid, diff_digest, created_at FROM commit_candidates WHERE commit_request_id = ?",
        )
        .bind(commit_request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    // ── Commit Creation Attempt ────────────────────────────────────────

    /// Log a creation attempt.
    pub async fn log_attempt(
        &self,
        attempt_id: &str,
        commit_request_id: &str,
        attempt_number: u32,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT INTO commit_creation_attempts (attempt_id, commit_request_id, attempt_number, state) VALUES (?,?,?,'started')",
        )
        .bind(attempt_id)
        .bind(commit_request_id)
        .bind(attempt_number as i64)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Complete a creation attempt.
    pub async fn complete_attempt(
        &self,
        attempt_id: &str,
        state: &str,
        commit_oid: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE commit_creation_attempts SET state = ?, commit_oid = ?, error_message = ?, completed_at = datetime('now') WHERE attempt_id = ?",
        )
        .bind(state)
        .bind(commit_oid)
        .bind(error_message)
        .bind(attempt_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Events ─────────────────────────────────────────────────────────

    /// Write a commit event.
    pub async fn write_event(
        &self,
        event_id: &str,
        commit_request_id: &str,
        candidate_id: &str,
        event_type: &str,
        payload_json: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT OR IGNORE INTO commit_events (event_id, commit_request_id, candidate_id, event_type, payload_json) VALUES (?,?,?,?,?)",
        )
        .bind(event_id)
        .bind(commit_request_id)
        .bind(candidate_id)
        .bind(event_type)
        .bind(payload_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }
}

// ── Row types ──────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct CommitRequestRow {
    commit_request_id: String,
    candidate_id: String,
    review_id: String,
    repository_id: String,
    target_ref: String,
    expected_base_commit: String,
    author_name: String,
    author_email: String,
    committer_name: String,
    committer_email: String,
    commit_timestamp: String,
    message: String,
    #[allow(dead_code)]
    state: String,
    idempotency_key: String,
    #[allow(dead_code)]
    idempotency_digest: String,
    created_at: String,
}

impl From<CommitRequestRow> for CommitRequest {
    fn from(r: CommitRequestRow) -> Self {
        let ts = parse_dt(&r.commit_timestamp);
        let created = parse_dt(&r.created_at);
        Self {
            commit_request_id: r.commit_request_id,
            candidate_id: r.candidate_id,
            review_id: r.review_id,
            repository_id: r.repository_id,
            target_ref: r.target_ref,
            expected_base_commit: r.expected_base_commit,
            author_identity: harness_core::contracts::commit::GitIdentity::new(
                &r.author_name,
                &r.author_email,
            ),
            committer_identity: harness_core::contracts::commit::GitIdentity::new(
                &r.committer_name,
                &r.committer_email,
            ),
            commit_timestamp: ts,
            message: r.message,
            idempotency_key: r.idempotency_key,
            created_at: created,
        }
    }
}

#[derive(sqlx::FromRow)]
struct CommitCandidateRow {
    commit_request_id: String,
    candidate_id: String,
    review_id: String,
    repository_id: String,
    commit_oid: String,
    parent_oid: String,
    tree_oid: String,
    diff_digest: String,
    created_at: String,
}

impl From<CommitCandidateRow> for CommitCandidate {
    fn from(r: CommitCandidateRow) -> Self {
        Self {
            commit_request_id: r.commit_request_id,
            candidate_id: r.candidate_id,
            review_id: r.review_id,
            repository_id: r.repository_id,
            commit_oid: r.commit_oid,
            parent_oid: r.parent_oid,
            tree_oid: r.tree_oid,
            diff_digest: r.diff_digest,
            created_at: parse_dt(&r.created_at),
        }
    }
}

fn parse_dt(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().into())
        .unwrap_or_else(chrono::Utc::now)
}
