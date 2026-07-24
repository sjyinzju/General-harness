//! Candidate and Review persistence — append-only repository.
//!
//! All writes are idempotent (ON CONFLICT DO NOTHING for idempotency keys).
//! Candidate snapshots are immutable once created.

use harness_core::contracts::candidate::CandidateSnapshot;
use harness_core::contracts::review::{
    ReviewCacheKey, ReviewDossier, ReviewFinding, ReviewRequest, ReviewState,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}

// ── Candidate Snapshot ─────────────────────────────────────────────────

pub struct CandidateRepo {
    pool: SqlitePool,
}

impl CandidateRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert a new Candidate snapshot. Returns false if a candidate with
    /// this ID already exists (immutable — no overwrite).
    pub async fn insert(&self, c: &CandidateSnapshot) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO candidate_snapshots (candidate_id, task_id, execution_id, executor_profile_id, workspace_id, base_commit, candidate_tree_hash, diff_digest, task_spec_digest, evidence_digest, composite_digest, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&c.candidate_id)
        .bind(&c.task_id)
        .bind(&c.execution_id)
        .bind(&c.executor_profile_id)
        .bind(&c.workspace_id)
        .bind(&c.base_commit)
        .bind(&c.candidate_tree_hash)
        .bind(&c.diff_digest)
        .bind(&c.task_spec_digest)
        .bind(&c.evidence_digest)
        .bind(c.composite_digest())
        .bind(c.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Load a Candidate by ID.
    pub async fn get(&self, candidate_id: &str) -> Result<Option<CandidateSnapshot>, CoreError> {
        let row: Option<CandidateRow> = sqlx::query_as(
            "SELECT candidate_id, task_id, execution_id, executor_profile_id, workspace_id, base_commit, candidate_tree_hash, diff_digest, task_spec_digest, evidence_digest, composite_digest, created_at FROM candidate_snapshots WHERE candidate_id=?",
        )
        .bind(candidate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Check if a candidate with the given composite digest already exists.
    pub async fn find_by_composite_digest(
        &self,
        digest: &str,
    ) -> Result<Option<CandidateSnapshot>, CoreError> {
        let row: Option<CandidateRow> = sqlx::query_as(
            "SELECT candidate_id, task_id, execution_id, executor_profile_id, workspace_id, base_commit, candidate_tree_hash, diff_digest, task_spec_digest, evidence_digest, composite_digest, created_at FROM candidate_snapshots WHERE composite_digest=? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(digest)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct CandidateRow {
    candidate_id: String,
    task_id: String,
    execution_id: String,
    executor_profile_id: String,
    workspace_id: String,
    base_commit: String,
    candidate_tree_hash: String,
    diff_digest: String,
    task_spec_digest: String,
    evidence_digest: String,
    composite_digest: String,
    created_at: String,
}

impl From<CandidateRow> for CandidateSnapshot {
    fn from(r: CandidateRow) -> Self {
        Self {
            candidate_id: r.candidate_id,
            task_id: r.task_id,
            execution_id: r.execution_id,
            executor_profile_id: r.executor_profile_id,
            workspace_id: r.workspace_id,
            base_commit: r.base_commit,
            candidate_tree_hash: r.candidate_tree_hash,
            diff_digest: r.diff_digest,
            task_spec_digest: r.task_spec_digest,
            evidence_digest: r.evidence_digest,
            created_at: chrono::DateTime::parse_from_rfc3339(&r.created_at)
                .unwrap_or_else(|_| chrono::Utc::now().into())
                .with_timezone(&chrono::Utc),
        }
    }
}

// ── Review Repository ──────────────────────────────────────────────────

pub struct ReviewRepo {
    pool: SqlitePool,
}

impl ReviewRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert a new review request. Returns false if the idempotency key
    /// already exists.
    pub async fn insert_request(
        &self,
        req: &ReviewRequest,
        ikey: &str,
        hash: &str,
    ) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO review_requests (review_id, candidate_id, reviewer_profile_id, state, idempotency_key, request_hash, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        )
        .bind(&req.review_id)
        .bind(&req.candidate_id)
        .bind(&req.reviewer_profile_id)
        .bind(req.state.as_str())
        .bind(ikey)
        .bind(hash)
        .bind(req.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(req.updated_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Update review state with CAS (expected current state).
    pub async fn transition_state(
        &self,
        review_id: &str,
        from: &ReviewState,
        to: &ReviewState,
    ) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "UPDATE review_requests SET state=?, updated_at=datetime('now'), completed_at=CASE WHEN ? IN ('approved','rejected','blocked','cancelled','stale') THEN datetime('now') ELSE completed_at END, version=version+1 WHERE review_id=? AND state=?",
        )
        .bind(to.as_str())
        .bind(to.as_str())
        .bind(review_id)
        .bind(from.as_str())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Get a review request by ID.
    pub async fn get_request(&self, review_id: &str) -> Result<Option<ReviewRequest>, CoreError> {
        let row: Option<ReviewRow> = sqlx::query_as(
            "SELECT review_id, candidate_id, reviewer_profile_id, state, created_at, updated_at, completed_at FROM review_requests WHERE review_id=?",
        )
        .bind(review_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// Find an active review for a candidate (if any).
    pub async fn find_active_for_candidate(
        &self,
        candidate_id: &str,
    ) -> Result<Option<ReviewRequest>, CoreError> {
        let row: Option<ReviewRow> = sqlx::query_as(
            "SELECT review_id, candidate_id, reviewer_profile_id, state, created_at, updated_at, completed_at FROM review_requests WHERE candidate_id=? AND state NOT IN ('approved','rejected','blocked','cancelled','stale') ORDER BY created_at DESC LIMIT 1",
        )
        .bind(candidate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// List reviews, optionally filtered by state.
    pub async fn list_requests(
        &self,
        state_filter: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ReviewRequest>, CoreError> {
        let rows: Vec<ReviewRow> = if let Some(s) = state_filter {
            sqlx::query_as(
                "SELECT review_id, candidate_id, reviewer_profile_id, state, created_at, updated_at, completed_at FROM review_requests WHERE state=? ORDER BY created_at DESC LIMIT ?",
            )
            .bind(s)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?
        } else {
            sqlx::query_as(
                "SELECT review_id, candidate_id, reviewer_profile_id, state, created_at, updated_at, completed_at FROM review_requests ORDER BY created_at DESC LIMIT ?",
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?
        };
        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Insert findings.
    pub async fn insert_findings(&self, findings: &[ReviewFinding]) -> Result<(), CoreError> {
        for f in findings {
            sqlx::query(
                "INSERT OR IGNORE INTO review_findings (finding_id, review_id, severity, category, summary, details, source_location, evidence_reference, blocking) VALUES (?,?,?,?,?,?,?,?,?)",
            )
            .bind(&f.finding_id)
            .bind(&f.review_id)
            .bind(severity_str(&f.severity))
            .bind(category_str(&f.category))
            .bind(&f.summary)
            .bind(&f.details)
            .bind(&f.source_location)
            .bind(&f.evidence_reference)
            .bind(if f.blocking { 1 } else { 0 })
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        }
        Ok(())
    }

    /// Get findings for a review.
    pub async fn get_findings(&self, review_id: &str) -> Result<Vec<ReviewFinding>, CoreError> {
        let rows: Vec<FindingRow> = sqlx::query_as(
            "SELECT finding_id, review_id, severity, category, summary, details, source_location, evidence_reference, blocking FROM review_findings WHERE review_id=? ORDER BY severity, created_at",
        )
        .bind(review_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Insert a review decision.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_decision(
        &self,
        decision_id: &str,
        review_id: &str,
        candidate_id: &str,
        decision: &str,
        summary: &str,
        candidate_digest: &str,
        decision_digest: &str,
        findings_count: i64,
        reviewer_output_json: &str,
    ) -> Result<bool, CoreError> {
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO review_decisions (decision_id, review_id, candidate_id, decision, summary, candidate_digest_at_decision, decision_digest, findings_count, reviewer_output_json) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(decision_id)
        .bind(review_id)
        .bind(candidate_id)
        .bind(decision)
        .bind(summary)
        .bind(candidate_digest)
        .bind(decision_digest)
        .bind(findings_count)
        .bind(reviewer_output_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Get decision for a review.
    pub async fn get_decision(&self, review_id: &str) -> Result<Option<DecisionRow>, CoreError> {
        let row: Option<DecisionRow> = sqlx::query_as(
            "SELECT decision_id, review_id, candidate_id, decision, summary, candidate_digest_at_decision, decision_digest, findings_count, reviewer_output_json, created_at FROM review_decisions WHERE review_id=? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(review_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row)
    }

    /// Insert dossier ref.
    pub async fn insert_dossier(&self, dossier: &ReviewDossier) -> Result<bool, CoreError> {
        let json = serde_json::to_string(dossier).unwrap_or_default();
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO review_dossier_refs (dossier_id, review_id, candidate_id, dossier_json, dossier_digest) VALUES (?,?,?,?,?)",
        )
        .bind(&dossier.dossier_id)
        .bind(&dossier.review_id)
        .bind(&dossier.candidate_id)
        .bind(&json)
        .bind(&dossier.dossier_digest)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected() == 1)
    }

    /// Load dossier by ID.
    pub async fn get_dossier(&self, dossier_id: &str) -> Result<Option<ReviewDossier>, CoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT dossier_json FROM review_dossier_refs WHERE dossier_id=?")
                .bind(dossier_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?;
        match row {
            Some((json,)) => {
                let d: ReviewDossier = serde_json::from_str(&json).map_err(|e| {
                    CoreError::new(
                        ErrorCode::SerializationError,
                        e.to_string(),
                        ErrorSource::System,
                    )
                })?;
                Ok(Some(d))
            }
            None => Ok(None),
        }
    }

    /// Find dossier by review ID.
    pub async fn get_dossier_by_review(
        &self,
        review_id: &str,
    ) -> Result<Option<ReviewDossier>, CoreError> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT dossier_id, dossier_json FROM review_dossier_refs WHERE review_id=? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(review_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        match row {
            Some((_, json)) => {
                let d: ReviewDossier = serde_json::from_str(&json).map_err(|e| {
                    CoreError::new(
                        ErrorCode::SerializationError,
                        e.to_string(),
                        ErrorSource::System,
                    )
                })?;
                Ok(Some(d))
            }
            None => Ok(None),
        }
    }

    /// Mark all active reviews for a candidate as Stale.
    pub async fn mark_stale_for_candidate(&self, candidate_id: &str) -> Result<u64, CoreError> {
        let rows = sqlx::query(
            "UPDATE review_requests SET state='stale', updated_at=datetime('now'), completed_at=datetime('now') WHERE candidate_id=? AND state NOT IN ('approved','rejected','blocked','cancelled','stale')",
        )
        .bind(candidate_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.rows_affected())
    }

    // ── Review Cache ──────────────────────────────────────────────

    /// Look up a cached review decision by composite cache key.
    pub async fn find_cache_entry(
        &self,
        cache_key: &ReviewCacheKey,
    ) -> Result<Option<CacheEntry>, CoreError> {
        let digest = cache_key.compute_digest();
        let row: Option<CacheRow> = sqlx::query_as(
            "SELECT cache_key_digest, candidate_id, review_id, decision, reviewer_output_json, invocation_count FROM review_cache WHERE cache_key_digest=?",
        )
        .bind(&digest)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| CacheEntry {
            cache_key_digest: r.cache_key_digest,
            candidate_id: r.candidate_id,
            review_id: r.review_id,
            decision: r.decision,
            reviewer_output_json: r.reviewer_output_json,
            invocation_count: r.invocation_count,
        }))
    }

    /// Insert a cache entry (idempotent — ON CONFLICT IGNORE).
    pub async fn insert_cache_entry(
        &self,
        cache_key: &ReviewCacheKey,
        candidate_id: &str,
        review_id: &str,
        decision: &str,
        reviewer_output_json: &str,
    ) -> Result<(), CoreError> {
        let digest = cache_key.compute_digest();
        sqlx::query(
            "INSERT OR IGNORE INTO review_cache (cache_key_digest, candidate_id, review_id, decision, reviewer_output_json) VALUES (?,?,?,?,?)",
        )
        .bind(&digest)
        .bind(candidate_id)
        .bind(review_id)
        .bind(decision)
        .bind(reviewer_output_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Invocation Counter ────────────────────────────────────────

    /// Log a reviewer invocation. Returns true if this is the first invocation
    /// for this review_id (not a cache hit).
    pub async fn log_invocation(
        &self,
        invocation_id: &str,
        review_id: &str,
        candidate_id: &str,
        reviewer_profile_id: &str,
        cache_hit: bool,
        dossier_digest: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT INTO review_invocation_log (invocation_id, review_id, candidate_id, reviewer_profile_id, cache_hit, dossier_digest, started_at) VALUES (?,?,?,?,?,?,datetime('now'))",
        )
        .bind(invocation_id)
        .bind(review_id)
        .bind(candidate_id)
        .bind(reviewer_profile_id)
        .bind(if cache_hit { 1 } else { 0 })
        .bind(dossier_digest)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Count invocations (excluding cache hits) for a review.
    pub async fn count_real_invocations(&self, review_id: &str) -> Result<i64, CoreError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM review_invocation_log WHERE review_id=? AND cache_hit=0",
        )
        .bind(review_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.0)
    }

    /// Update invocation outcome.
    pub async fn complete_invocation(
        &self,
        invocation_id: &str,
        outcome: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE review_invocation_log SET completed_at=datetime('now'), outcome=? WHERE invocation_id=?",
        )
        .bind(outcome)
        .bind(invocation_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Events ────────────────────────────────────────────────────

    /// Append a review event (idempotent).
    pub async fn write_event(
        &self,
        event_id: &str,
        review_id: &str,
        candidate_id: &str,
        event_type: &str,
        payload_json: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT OR IGNORE INTO review_events (event_id, review_id, candidate_id, event_type, payload_json) VALUES (?,?,?,?,?)",
        )
        .bind(event_id)
        .bind(review_id)
        .bind(candidate_id)
        .bind(event_type)
        .bind(payload_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }
}

pub struct CacheEntry {
    pub cache_key_digest: String,
    pub candidate_id: String,
    pub review_id: String,
    pub decision: String,
    pub reviewer_output_json: String,
    pub invocation_count: i64,
}

#[derive(sqlx::FromRow)]
struct CacheRow {
    cache_key_digest: String,
    candidate_id: String,
    review_id: String,
    decision: String,
    reviewer_output_json: String,
    invocation_count: i64,
}

// ── Row types ──────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct ReviewRow {
    review_id: String,
    candidate_id: String,
    reviewer_profile_id: String,
    state: String,
    created_at: String,
    updated_at: String,
    completed_at: Option<String>,
}

impl From<ReviewRow> for ReviewRequest {
    fn from(r: ReviewRow) -> Self {
        let state = match r.state.as_str() {
            "requested" => ReviewState::Requested,
            "preparing" => ReviewState::Preparing,
            "prechecking" => ReviewState::Prechecking,
            "reviewing" => ReviewState::Reviewing,
            "approved" => ReviewState::Approved,
            "rejected" => ReviewState::Rejected,
            "blocked" => ReviewState::Blocked,
            "cancelled" => ReviewState::Cancelled,
            "stale" => ReviewState::Stale,
            _ => ReviewState::Requested,
        };
        Self {
            review_id: r.review_id,
            candidate_id: r.candidate_id,
            reviewer_profile_id: r.reviewer_profile_id,
            state,
            created_at: parse_dt(&r.created_at),
            updated_at: parse_dt(&r.updated_at),
            completed_at: r.completed_at.as_deref().map(parse_dt),
        }
    }
}

#[derive(sqlx::FromRow)]
struct FindingRow {
    finding_id: String,
    review_id: String,
    severity: String,
    category: String,
    summary: String,
    details: String,
    source_location: Option<String>,
    evidence_reference: Option<String>,
    blocking: i64,
}

impl From<FindingRow> for ReviewFinding {
    fn from(r: FindingRow) -> Self {
        Self {
            finding_id: r.finding_id,
            review_id: r.review_id,
            severity: parse_sev(&r.severity),
            category: parse_cat(&r.category),
            summary: r.summary,
            details: r.details,
            source_location: r.source_location,
            evidence_reference: r.evidence_reference,
            blocking: r.blocking != 0,
        }
    }
}

#[derive(sqlx::FromRow)]
pub struct DecisionRow {
    pub decision_id: String,
    pub review_id: String,
    pub candidate_id: String,
    pub decision: String,
    pub summary: String,
    pub candidate_digest_at_decision: String,
    pub decision_digest: String,
    pub findings_count: i64,
    pub reviewer_output_json: String,
    pub created_at: String,
}

fn parse_dt(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap_or_else(|_| chrono::Utc::now().into())
        .with_timezone(&chrono::Utc)
}

fn severity_str(s: &harness_core::contracts::review::FindingSeverity) -> &'static str {
    use harness_core::contracts::review::FindingSeverity;
    match s {
        FindingSeverity::Critical => "critical",
        FindingSeverity::High => "high",
        FindingSeverity::Medium => "medium",
        FindingSeverity::Low => "low",
    }
}

fn category_str(c: &harness_core::contracts::review::FindingCategory) -> &'static str {
    use harness_core::contracts::review::FindingCategory;
    match c {
        FindingCategory::RequirementMismatch => "requirement_mismatch",
        FindingCategory::ScopeViolation => "scope_violation",
        FindingCategory::Correctness => "correctness",
        FindingCategory::Safety => "safety",
        FindingCategory::Security => "security",
        FindingCategory::EvidenceGap => "evidence_gap",
        FindingCategory::TestGap => "test_gap",
        FindingCategory::ArchitectureViolation => "architecture_violation",
        FindingCategory::Maintainability => "maintainability",
    }
}

fn parse_sev(s: &str) -> harness_core::contracts::review::FindingSeverity {
    use harness_core::contracts::review::FindingSeverity;
    match s {
        "critical" => FindingSeverity::Critical,
        "high" => FindingSeverity::High,
        "medium" => FindingSeverity::Medium,
        "low" => FindingSeverity::Low,
        _ => FindingSeverity::Medium,
    }
}

fn parse_cat(s: &str) -> harness_core::contracts::review::FindingCategory {
    use harness_core::contracts::review::FindingCategory;
    match s {
        "requirement_mismatch" => FindingCategory::RequirementMismatch,
        "scope_violation" => FindingCategory::ScopeViolation,
        "correctness" => FindingCategory::Correctness,
        "safety" => FindingCategory::Safety,
        "security" => FindingCategory::Security,
        "evidence_gap" => FindingCategory::EvidenceGap,
        "test_gap" => FindingCategory::TestGap,
        "architecture_violation" => FindingCategory::ArchitectureViolation,
        "maintainability" => FindingCategory::Maintainability,
        _ => FindingCategory::Correctness,
    }
}
