//! Policy Evidence — immutable record of policy decisions. Evidence is
//! stored in SQLite (migration v6); large diffs reference artifact spool
//! files. Lease tokens and raw secrets are NEVER persisted in evidence.

use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

/// A single policy evaluation record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PolicyEvaluationRecord {
    pub id: String,
    pub evaluation_type: String, // "command" | "file_scope" | "diff" | "secret_scan"
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub worktree_id: Option<String>,
    pub fencing_token: Option<i64>, // epoch, not the raw lease token
    pub policy_version: u32,
    pub input_fingerprint: Option<String>,
    pub decision: String,     // "allowed" | "denied" | "require_approval"
    pub reasons_json: String, // JSON array of reason strings
    pub changed_path_count: Option<i64>,
    pub finding_count: Option<i64>,
    pub artifact_reference: Option<String>, // spool path for large diff
    pub evaluator_identity: String,
    pub created_at: String,
}

/// A single secret/scope/violation finding.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PolicyFinding {
    pub id: String,
    pub evaluation_id: String,
    pub finding_type: String,
    pub file_path: Option<String>,
    pub line_number: Option<i64>,
    pub byte_range_start: Option<i64>,
    pub byte_range_end: Option<i64>,
    pub redacted_preview: String,
    pub fingerprint: Option<String>, // hash of the rule/value, NOT raw secret
}

pub struct PolicyEvidenceStore {
    pool: SqlitePool,
}

impl PolicyEvidenceStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn insert_evaluation(
        &self,
        record: &PolicyEvaluationRecord,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT INTO policy_evaluations (id, evaluation_type, project_id, task_id, execution_id, worktree_id, fencing_token, policy_version, input_fingerprint, decision, reasons_json, changed_path_count, finding_count, artifact_reference, evaluator_identity, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))",
        )
        .bind(&record.id)
        .bind(&record.evaluation_type)
        .bind(&record.project_id)
        .bind(&record.task_id)
        .bind(&record.execution_id)
        .bind(&record.worktree_id)
        .bind(record.fencing_token)
        .bind(record.policy_version)
        .bind(&record.input_fingerprint)
        .bind(&record.decision)
        .bind(&record.reasons_json)
        .bind(record.changed_path_count)
        .bind(record.finding_count)
        .bind(&record.artifact_reference)
        .bind(&record.evaluator_identity)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn insert_finding(&self, finding: &PolicyFinding) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT INTO policy_findings (id, evaluation_id, finding_type, file_path, line_number, byte_range_start, byte_range_end, redacted_preview, fingerprint) VALUES (?,?,?,?,?,?,?,?,?)",
        )
        .bind(&finding.id)
        .bind(&finding.evaluation_id)
        .bind(&finding.finding_type)
        .bind(&finding.file_path)
        .bind(finding.line_number)
        .bind(finding.byte_range_start)
        .bind(finding.byte_range_end)
        .bind(&finding.redacted_preview)
        .bind(&finding.fingerprint)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Find stale evidence: created with an old fencing token (another
    /// owner now holds the lease).
    pub async fn find_stale_evidence(
        &self,
        worktree_id: &str,
        current_fencing_token: i64,
    ) -> Result<Vec<PolicyEvaluationRecord>, CoreError> {
        let rows: Vec<EvalRow> = sqlx::query_as(
            "SELECT id, evaluation_type, project_id, task_id, execution_id, worktree_id, fencing_token, policy_version, input_fingerprint, decision, reasons_json, changed_path_count, finding_count, artifact_reference, evaluator_identity, created_at FROM policy_evaluations WHERE worktree_id = ? AND fencing_token IS NOT NULL AND fencing_token != ?",
        )
        .bind(worktree_id)
        .bind(current_fencing_token)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Mark evidence as stale (another lease owner took over).
    pub async fn invalidate_stale(&self, evaluation_id: &str) -> Result<(), CoreError> {
        sqlx::query("UPDATE policy_evaluations SET decision = decision || ' (stale)' WHERE id = ?")
            .bind(evaluation_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    /// Return an existing evaluation for the same input fingerprint
    /// (idempotency).
    pub async fn find_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<Option<PolicyEvaluationRecord>, CoreError> {
        let row: Option<EvalRow> = sqlx::query_as(
            "SELECT id, evaluation_type, project_id, task_id, execution_id, worktree_id, fencing_token, policy_version, input_fingerprint, decision, reasons_json, changed_path_count, finding_count, artifact_reference, evaluator_identity, created_at FROM policy_evaluations WHERE input_fingerprint = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(fingerprint)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }

    /// All evidence rows for a worktree (used by the reconciler).
    pub async fn find_all_for_worktree(
        &self,
        worktree_id: &str,
    ) -> Result<Vec<PolicyEvaluationRecord>, CoreError> {
        let rows: Vec<EvalRow> = sqlx::query_as(
            "SELECT id, evaluation_type, project_id, task_id, execution_id, worktree_id, fencing_token, policy_version, input_fingerprint, decision, reasons_json, changed_path_count, finding_count, artifact_reference, evaluator_identity, created_at FROM policy_evaluations WHERE worktree_id = ? ORDER BY created_at ASC",
        )
        .bind(worktree_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Mark an evaluation invalid so it can no longer serve as a basis for
    /// commit/verification. The decision is set to a sentinel `invalid` and
    /// the reason is recorded in `reasons_json`.
    pub async fn mark_invalid(&self, evaluation_id: &str, reason: &str) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE policy_evaluations SET decision = 'invalid', reasons_json = ? WHERE id = ?",
        )
        .bind(reason)
        .bind(evaluation_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Approval persistence boundary ──────────────────────────────

    pub async fn insert_approval(&self, rec: &ApprovalRecord) -> Result<(), CoreError> {
        sqlx::query(
            "INSERT INTO policy_approvals (id, project_id, task_id, execution_id, command_fingerprint, decision, expiry, fencing_token, evaluator_identity, created_at) VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))",
        )
        .bind(&rec.id)
        .bind(&rec.project_id)
        .bind(&rec.task_id)
        .bind(&rec.execution_id)
        .bind(&rec.command_fingerprint)
        .bind(&rec.decision)
        .bind(&rec.expiry)
        .bind(rec.fencing_token)
        .bind(&rec.evaluator_identity)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Latest approval for a composite fingerprint under a given fencing
    /// epoch. Approvals recorded under a different (stale) epoch are not
    /// returned.
    pub async fn find_approval(
        &self,
        command_fingerprint: &str,
        fencing_token: i64,
    ) -> Result<Option<ApprovalRecord>, CoreError> {
        let row: Option<ApprovalRow> = sqlx::query_as(
            "SELECT id, project_id, task_id, execution_id, command_fingerprint, decision, expiry, fencing_token, evaluator_identity, created_at FROM policy_approvals WHERE command_fingerprint = ? AND fencing_token = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(command_fingerprint)
        .bind(fencing_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into()))
    }
}

/// A persisted command approval decision.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ApprovalRecord {
    pub id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub command_fingerprint: String,
    pub decision: String,
    pub expiry: Option<String>,
    pub fencing_token: Option<i64>,
    pub evaluator_identity: String,
    pub created_at: String,
}

#[derive(sqlx::FromRow)]
struct ApprovalRow {
    id: String,
    project_id: String,
    task_id: String,
    execution_id: String,
    command_fingerprint: String,
    decision: String,
    expiry: Option<String>,
    fencing_token: Option<i64>,
    evaluator_identity: String,
    created_at: String,
}

impl From<ApprovalRow> for ApprovalRecord {
    fn from(r: ApprovalRow) -> Self {
        Self {
            id: r.id,
            project_id: r.project_id,
            task_id: r.task_id,
            execution_id: r.execution_id,
            command_fingerprint: r.command_fingerprint,
            decision: r.decision,
            expiry: r.expiry,
            fencing_token: r.fencing_token,
            evaluator_identity: r.evaluator_identity,
            created_at: r.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct EvalRow {
    id: String,
    evaluation_type: String,
    project_id: String,
    task_id: String,
    execution_id: String,
    worktree_id: Option<String>,
    fencing_token: Option<i64>,
    policy_version: i64,
    input_fingerprint: Option<String>,
    decision: String,
    reasons_json: String,
    changed_path_count: Option<i64>,
    finding_count: Option<i64>,
    artifact_reference: Option<String>,
    evaluator_identity: String,
    created_at: String,
}

impl From<EvalRow> for PolicyEvaluationRecord {
    fn from(r: EvalRow) -> Self {
        Self {
            id: r.id,
            evaluation_type: r.evaluation_type,
            project_id: r.project_id,
            task_id: r.task_id,
            execution_id: r.execution_id,
            worktree_id: r.worktree_id,
            fencing_token: r.fencing_token,
            policy_version: r.policy_version as u32,
            input_fingerprint: r.input_fingerprint,
            decision: r.decision,
            reasons_json: r.reasons_json,
            changed_path_count: r.changed_path_count,
            finding_count: r.finding_count,
            artifact_reference: r.artifact_reference,
            evaluator_identity: r.evaluator_identity,
            created_at: r.created_at,
        }
    }
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}

/// Human-redacted PolicyEvidence summary (safe to log/display).
#[derive(Debug, Clone)]
pub struct PolicyEvidence {
    pub evaluation: PolicyEvaluationRecord,
    pub findings: Vec<PolicyFinding>,
}
