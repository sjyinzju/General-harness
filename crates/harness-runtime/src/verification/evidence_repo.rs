//! VerificationEvidenceRepo — persistent storage for verification evidence.
//!
//! Never persists: lease tokens, credentials, API keys, environment variable
//! values, raw secrets, full log dumps, or large diffs.

use harness_core::contracts::verification::{
    VerificationDiagnostic, VerificationDiagnosticLevel, VerificationEvidence,
    VerificationEvidenceKind, VerificationStepResult, VerificationStepStatus,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::content_validator::VerificationContentValidator;

type StepResultRow = (
    String,         // result_id
    String,         // run_id
    String,         // step_id
    String,         // plan_id
    String,         // status
    Option<String>, // detail_json
    Option<String>, // started_at
    Option<String>, // completed_at
    Option<i64>,    // duration_ms
    Option<String>, // error_message
    String,         // created_at
);

type EvidenceRow = (
    String,         // evidence_id
    String,         // run_id
    String,         // step_id
    String,         // evidence_kind
    String,         // summary
    Option<String>, // detail_json
    Option<String>, // artifact_ref
    String,         // collected_at
);

pub struct VerificationEvidenceRepo {
    pool: SqlitePool,
}

impl VerificationEvidenceRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // ── Step Results ───────────────────────────────────────────────

    /// Persist a step result.
    pub async fn insert_step_result(
        &self,
        result: &VerificationStepResult,
    ) -> Result<(), CoreError> {
        // Validate before any SQL — fail-closed security boundary.
        if let Some(ref detail) = result.detail_json {
            VerificationContentValidator::validate_detail_json(detail)?;
        }
        if let Some(ref err_msg) = result.error_message {
            VerificationContentValidator::validate_text(err_msg)?;
        }

        sqlx::query(
            "INSERT INTO verification_step_results (result_id, run_id, step_id, plan_id, status, detail_json, started_at, completed_at, duration_ms, error_message) VALUES (?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&result.result_id).bind(&result.run_id).bind(&result.step_id)
        .bind(&result.plan_id).bind(step_status_str(&result.status))
        .bind(result.detail_json.as_deref()).bind(result.started_at.as_deref())
        .bind(result.completed_at.as_deref()).bind(result.duration_ms.map(|d| d as i64))
        .bind(result.error_message.as_deref())
        .execute(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert step result: {e}"), ErrorSource::System))?;
        Ok(())
    }

    /// Load all step results for a run, ordered by creation.
    pub async fn get_step_results(
        &self,
        run_id: &str,
    ) -> Result<Vec<VerificationStepResult>, CoreError> {
        let rows: Vec<StepResultRow> = sqlx::query_as(
            "SELECT result_id, run_id, step_id, plan_id, status, detail_json, started_at, completed_at, duration_ms, error_message, created_at FROM verification_step_results WHERE run_id = ? ORDER BY created_at",
        )
        .bind(run_id).fetch_all(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("get step results: {e}"), ErrorSource::System))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    rid,
                    run_id,
                    sid,
                    pid,
                    status,
                    detail,
                    started,
                    completed,
                    dur,
                    err,
                    _created,
                )| {
                    VerificationStepResult {
                        result_id: rid,
                        run_id,
                        step_id: sid,
                        plan_id: pid,
                        status: parse_step_status(&status),
                        detail_json: detail,
                        started_at: started,
                        completed_at: completed,
                        duration_ms: dur.map(|d| d as u64),
                        error_message: err,
                    }
                },
            )
            .collect())
    }

    // ── Evidence ───────────────────────────────────────────────────

    /// Persist a piece of verification evidence.
    /// Caller must ensure `detail_json` never contains raw secrets.
    pub async fn insert_evidence(&self, evidence: &VerificationEvidence) -> Result<(), CoreError> {
        // Validate before any SQL — fail-closed security boundary.
        VerificationContentValidator::validate_text(&evidence.summary)?;
        if let Some(ref detail) = evidence.detail_json {
            VerificationContentValidator::validate_detail_json(detail)?;
        }

        sqlx::query(
            "INSERT INTO verification_evidence (evidence_id, run_id, step_id, evidence_kind, summary, detail_json, artifact_ref) VALUES (?,?,?,?,?,?,?)",
        )
        .bind(&evidence.evidence_id).bind(&evidence.run_id).bind(&evidence.step_id)
        .bind(evidence_kind_str(&evidence.evidence_kind)).bind(&evidence.summary)
        .bind(evidence.detail_json.as_deref()).bind(evidence.artifact_ref.as_deref())
        .execute(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert evidence: {e}"), ErrorSource::System))?;
        Ok(())
    }

    /// Load all evidence for a run.
    pub async fn get_evidence(&self, run_id: &str) -> Result<Vec<VerificationEvidence>, CoreError> {
        let rows: Vec<EvidenceRow> = sqlx::query_as(
            "SELECT evidence_id, run_id, step_id, evidence_kind, summary, detail_json, artifact_ref, collected_at FROM verification_evidence WHERE run_id = ? ORDER BY collected_at",
        )
        .bind(run_id).fetch_all(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("get evidence: {e}"), ErrorSource::System))?;

        Ok(rows
            .into_iter()
            .map(
                |(eid, rid, sid, kind, summary, detail, artifact, collected)| {
                    VerificationEvidence {
                        evidence_id: eid,
                        run_id: rid,
                        step_id: sid,
                        evidence_kind: parse_evidence_kind(&kind),
                        summary,
                        detail_json: detail,
                        artifact_ref: artifact,
                        collected_at: collected,
                    }
                },
            )
            .collect())
    }

    // ── Diagnostics ────────────────────────────────────────────────

    /// Persist a diagnostic entry.
    pub async fn insert_diagnostic(&self, diag: &VerificationDiagnostic) -> Result<(), CoreError> {
        // Validate before any SQL — fail-closed security boundary.
        VerificationContentValidator::validate_text(&diag.message)?;
        if let Some(ref ctx) = diag.context_json {
            VerificationContentValidator::validate_detail_json(ctx)?;
        }

        sqlx::query(
            "INSERT INTO verification_diagnostics (diagnostic_id, run_id, level, message, context_json) VALUES (?,?,?,?,?)",
        )
        .bind(&diag.diagnostic_id).bind(&diag.run_id).bind(diag_level_str(&diag.level))
        .bind(&diag.message).bind(diag.context_json.as_deref())
        .execute(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert diagnostic: {e}"), ErrorSource::System))?;
        Ok(())
    }

    /// Load diagnostics for a run.
    pub async fn get_diagnostics(
        &self,
        run_id: &str,
    ) -> Result<Vec<VerificationDiagnostic>, CoreError> {
        let rows: Vec<(String, String, String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT diagnostic_id, run_id, level, message, context_json, created_at FROM verification_diagnostics WHERE run_id = ? ORDER BY created_at",
        )
        .bind(run_id).fetch_all(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("get diagnostics: {e}"), ErrorSource::System))?;

        Ok(rows
            .into_iter()
            .map(
                |(did, rid, level, msg, ctx, created)| VerificationDiagnostic {
                    diagnostic_id: did,
                    run_id: rid,
                    level: parse_diag_level(&level),
                    message: msg,
                    context_json: ctx,
                    created_at: created,
                },
            )
            .collect())
    }
}

fn step_status_str(s: &VerificationStepStatus) -> &'static str {
    match s {
        VerificationStepStatus::Passed => "passed",
        VerificationStepStatus::Failed => "failed",
        VerificationStepStatus::Blocked => "blocked",
        VerificationStepStatus::Skipped => "skipped",
        VerificationStepStatus::Error => "error",
    }
}
fn parse_step_status(s: &str) -> VerificationStepStatus {
    match s {
        "failed" => VerificationStepStatus::Failed,
        "blocked" => VerificationStepStatus::Blocked,
        "skipped" => VerificationStepStatus::Skipped,
        "error" => VerificationStepStatus::Error,
        _ => VerificationStepStatus::Passed,
    }
}
fn evidence_kind_str(k: &VerificationEvidenceKind) -> &'static str {
    match k {
        VerificationEvidenceKind::FileDiffSummary => "file_diff_summary",
        VerificationEvidenceKind::SecretFinding => "secret_finding",
        VerificationEvidenceKind::PolicyViolation => "policy_violation",
        VerificationEvidenceKind::TestOutput => "test_output",
        VerificationEvidenceKind::ArtifactRef => "artifact_ref",
        VerificationEvidenceKind::WorktreeState => "worktree_state",
        VerificationEvidenceKind::ResourceOwnership => "resource_ownership",
        VerificationEvidenceKind::Custom => "custom",
    }
}
fn parse_evidence_kind(s: &str) -> VerificationEvidenceKind {
    match s {
        "secret_finding" => VerificationEvidenceKind::SecretFinding,
        "policy_violation" => VerificationEvidenceKind::PolicyViolation,
        "test_output" => VerificationEvidenceKind::TestOutput,
        "artifact_ref" => VerificationEvidenceKind::ArtifactRef,
        "worktree_state" => VerificationEvidenceKind::WorktreeState,
        "resource_ownership" => VerificationEvidenceKind::ResourceOwnership,
        "custom" => VerificationEvidenceKind::Custom,
        _ => VerificationEvidenceKind::FileDiffSummary,
    }
}
fn diag_level_str(l: &VerificationDiagnosticLevel) -> &'static str {
    match l {
        VerificationDiagnosticLevel::Info => "info",
        VerificationDiagnosticLevel::Warning => "warning",
        VerificationDiagnosticLevel::Error => "error",
    }
}
fn parse_diag_level(s: &str) -> VerificationDiagnosticLevel {
    match s {
        "warning" => VerificationDiagnosticLevel::Warning,
        "error" => VerificationDiagnosticLevel::Error,
        _ => VerificationDiagnosticLevel::Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        let db = Database::open_in_memory().await.unwrap();
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-1','t1','e1','p1','hash-aaa',1,'[]')").execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, idempotency_key, request_hash) VALUES ('run-1','plan-1','hash-aaa',1,'e1','t1','p1','created','ikey-1','hash-aaa')").execute(&db.pool).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_insert_and_get_step_results() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());

        let result = VerificationStepResult {
            result_id: "sr-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            status: VerificationStepStatus::Passed,
            detail_json: Some(r#"{"files":3}"#.into()),
            started_at: Some("2026-01-01".into()),
            completed_at: Some("2026-01-01".into()),
            duration_ms: Some(150),
            error_message: None,
        };
        repo.insert_step_result(&result).await.unwrap();

        let results = repo.get_step_results("run-1").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, VerificationStepStatus::Passed);
        assert_eq!(results[0].duration_ms, Some(150));
    }

    #[tokio::test]
    async fn test_insert_and_get_evidence() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());

        let evidence = VerificationEvidence {
            evidence_id: "ev-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::FileDiffSummary,
            summary: "3 files changed".into(),
            detail_json: Some(r#"{"added":1,"modified":2}"#.into()),
            artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        repo.insert_evidence(&evidence).await.unwrap();

        let items = repo.get_evidence("run-1").await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].summary, "3 files changed");
    }

    #[tokio::test]
    async fn test_no_secret_in_evidence_db() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());

        // Insert evidence — detail must not contain lease_token or secrets
        let evidence = VerificationEvidence {
            evidence_id: "ev-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::SecretFinding,
            summary: "pattern found".into(),
            detail_json: Some(r#"{"pattern":"AWS_KEY","file":"src/main.rs","line":10}"#.into()),
            artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        repo.insert_evidence(&evidence).await.unwrap();

        let items = repo.get_evidence("run-1").await.unwrap();
        let detail = items[0].detail_json.as_deref().unwrap_or("");
        assert!(!detail.contains("lease_token"));
        assert!(!detail.contains("sk-"));
        assert!(!detail.contains("api_key"));
    }

    #[tokio::test]
    async fn test_insert_diagnostic() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());

        let diag = VerificationDiagnostic {
            diagnostic_id: "d-1".into(),
            run_id: "run-1".into(),
            level: VerificationDiagnosticLevel::Warning,
            message: "step took longer than expected".into(),
            context_json: Some(r#"{"step_id":"step-1","threshold_ms":5000}"#.into()),
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        repo.insert_diagnostic(&diag).await.unwrap();

        let diags = repo.get_diagnostics("run-1").await.unwrap();
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].level, VerificationDiagnosticLevel::Warning);
    }

    // ── Repository-level validator enforcement tests ─────────────────

    #[tokio::test]
    async fn test_reject_secret_in_evidence_detail() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let evidence = VerificationEvidence {
            evidence_id: "ev-bad".into(), run_id: "run-1".into(), step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::FileDiffSummary,
            summary: "safe summary".into(),
            detail_json: Some(r#"{"key": "sk-live-secret-12345"}"#.into()),
            artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        let result = repo.insert_evidence(&evidence).await;
        assert!(result.is_err(), "secret in detail must be rejected by repo");
        let items = repo.get_evidence("run-1").await.unwrap();
        assert!(!items.iter().any(|e| e.evidence_id == "ev-bad"), "rejected evidence must leave zero rows");
    }

    #[tokio::test]
    async fn test_reject_secret_in_summary() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let evidence = VerificationEvidence {
            evidence_id: "ev-sum".into(), run_id: "run-1".into(), step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::FileDiffSummary,
            summary: "Bearer eyJhbGciOiJIUzI1NiJ9.token found".into(),
            detail_json: None, artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        assert!(repo.insert_evidence(&evidence).await.is_err());
    }

    #[tokio::test]
    async fn test_reject_private_key_in_diagnostic() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let diag = VerificationDiagnostic {
            diagnostic_id: "d-bad".into(), run_id: "run-1".into(),
            level: VerificationDiagnosticLevel::Error,
            message: "-----BEGIN RSA PRIVATE KEY----- found".into(),
            context_json: None,
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        assert!(repo.insert_diagnostic(&diag).await.is_err());
        let diags = repo.get_diagnostics("run-1").await.unwrap();
        assert!(!diags.iter().any(|d| d.diagnostic_id == "d-bad"));
    }

    #[tokio::test]
    async fn test_reject_credential_in_step_result() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let result = VerificationStepResult {
            result_id: "sr-bad".into(), run_id: "run-1".into(), step_id: "step-1".into(),
            plan_id: "plan-1".into(), status: VerificationStepStatus::Failed,
            detail_json: Some(r#"{"password": "super-secret"}"#.into()),
            started_at: None, completed_at: None, duration_ms: None, error_message: None,
        };
        assert!(repo.insert_step_result(&result).await.is_err());
        let results = repo.get_step_results("run-1").await.unwrap();
        assert!(!results.iter().any(|sr| sr.result_id == "sr-bad"));
    }

    #[tokio::test]
    async fn test_reject_oversized_evidence_detail() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let evidence = VerificationEvidence {
            evidence_id: "ev-big".into(), run_id: "run-1".into(), step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::FileDiffSummary,
            summary: "safe".into(), detail_json: Some("x".repeat(300_000)),
            artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        assert!(repo.insert_evidence(&evidence).await.is_err());
    }

    #[tokio::test]
    async fn test_safe_artifact_ref_accepted() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let evidence = VerificationEvidence {
            evidence_id: "ev-art".into(), run_id: "run-1".into(), step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::ArtifactRef,
            summary: "artifact captured".into(),
            detail_json: Some(r#"{"artifact_id":"art-abc"}"#.into()),
            artifact_ref: Some("artifacts/art-abc.diff".into()),
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        repo.insert_evidence(&evidence).await.unwrap();
        assert!(repo.get_evidence("run-1").await.unwrap().iter().any(|e| e.evidence_id == "ev-art"));
    }

    #[tokio::test]
    async fn test_error_does_not_contain_raw_secret() {
        let db = setup().await;
        let repo = VerificationEvidenceRepo::new(db.pool.clone());
        let evidence = VerificationEvidence {
            evidence_id: "ev-err".into(), run_id: "run-1".into(), step_id: "step-1".into(),
            evidence_kind: VerificationEvidenceKind::FileDiffSummary,
            summary: "safe".into(),
            detail_json: Some(r#"{"token":"sk-abc-secret"}"#.into()),
            artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        let err = repo.insert_evidence(&evidence).await.unwrap_err();
        let err_str = format!("{:?}", err);
        assert!(!err_str.contains("sk-abc-secret"), "error must not leak raw secret");
    }
}
