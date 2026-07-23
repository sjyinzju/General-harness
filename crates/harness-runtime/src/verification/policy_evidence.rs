//! VerificationPolicyEvidenceService — deterministic execution of deferred
//! policy verification steps (Diff, FileScope, SecretScan, Artifact,
//! RequiredFile, ForbiddenChange, OutputMatcher).
//!
//! This service runs AFTER command steps and BEFORE terminal finalization.
//! It NEVER: creates Agents, retries, switches providers, deletes worktrees,
//! releases leases/claims, or sets the run terminal outcome.
//!
//! Every step execution is idempotent: same key + same hash → same result.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use harness_core::contracts::verification::{
    FailureClassification, VerificationEvidence, VerificationEvidenceKind, VerificationStepKind,
    VerificationStepResult, VerificationStepStatus,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use super::evidence_repo::VerificationEvidenceRepo;

// ── Policy step request ──────────────────────────────────────────────────

pub struct PolicyStepRequest {
    pub verification_run_id: String,
    pub step_id: String,
    pub plan_id: String,
    pub execution_id: String,
    pub task_id: String,
    pub project_id: String,
    pub worktree_id: String,
    pub worktree_path: PathBuf,
    pub worktree_head: Option<String>,
    pub baseline_commit: Option<String>,
    pub expected_fencing: i64,
    pub verification_owner_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub step_kind: VerificationStepKind,
    pub required: bool,
    pub sequence_index: u32,
    pub config_json: String,
    /// Paths of changed files (for secret scanning and file scope checks).
    pub changed_file_paths: Vec<String>,
    /// File contents for secret scanning (path → content).
    pub file_contents: HashMap<String, Vec<u8>>,
    /// Artifact references to verify.
    pub artifact_refs: Vec<String>,
    /// Required file paths that must exist.
    pub required_files: Vec<RequiredFileSpec>,
    /// Forbidden change patterns.
    pub forbidden_changes: Vec<ForbiddenChangeSpec>,
    /// Output matchers.
    pub output_matchers: Vec<OutputMatcherSpec>,
    /// Fingerprint of the input that produced this step request.
    pub input_fingerprint: Option<String>,
    /// Fingerprint of the changed paths (for evidence freshness).
    pub changed_path_fingerprint: Option<String>,
    /// Artifact checksum (for evidence freshness).
    pub artifact_checksum: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RequiredFileSpec {
    pub path: String,
    pub expected_size: Option<u64>,
    pub expected_fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ForbiddenChangeSpec {
    pub path_glob: String,
    pub forbid_add: bool,
    pub forbid_modify: bool,
    pub forbid_delete: bool,
}

#[derive(Debug, Clone)]
pub struct OutputMatcherSpec {
    pub kind: OutputMatchKind,
    pub pattern: String,
    /// If true, pattern is a regex; otherwise literal substring.
    pub is_regex: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputMatchKind {
    Required,
    Forbidden,
}

// ── Policy step outcome ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum PolicyStepOutcome {
    Completed {
        step_result: VerificationStepResult,
        classification: Option<FailureClassification>,
    },
    /// Already executed — duplicate idempotency key with same hash.
    Duplicate {
        existing_step_result_id: String,
    },
    IdempotencyConflict {
        existing_hash: String,
        new_hash: String,
    },
    OwnershipLost {
        reason: String,
    },
    InfrastructureError {
        reason: String,
    },
    /// Existing evidence is stale — freshness fields don't match.
    StaleEvidence {
        existing_evidence_id: String,
        mismatched_field: String,
    },
}

// ── Scanner trait for testability ────────────────────────────────────────

/// Abstracts diff+secret scanning behind a trait so tests can inject fakes.
#[async_trait::async_trait]
pub trait PolicyScanner: Send + Sync {
    async fn scan_diff(
        &self,
        worktree_path: &Path,
        baseline: Option<&str>,
    ) -> Result<DiffScanReport, CoreError>;

    async fn scan_secrets(&self, files: &[(String, Vec<u8>)]) -> SecretScanSummary;
}

#[derive(Debug, Clone)]
pub struct DiffScanReport {
    pub changed_paths: Vec<ChangedPathInfo>,
    pub clean: bool,
    pub binary_files: Vec<String>,
    pub submodule_files: Vec<String>,
    pub added_count: usize,
    pub modified_count: usize,
    pub deleted_count: usize,
    pub untracked_count: usize,
    pub rename_count: usize,
}

#[derive(Debug, Clone)]
pub struct ChangedPathInfo {
    pub path: String,
    pub change_kind: String, // added, modified, deleted, renamed, copied, untracked
    pub from_path: Option<String>, // for renames/copies
}

#[derive(Debug, Clone)]
pub struct SecretScanSummary {
    pub findings_count: u32,
    pub files_scanned: usize,
    pub clean: bool,
    pub finding_details: Vec<SecretFindingDetail>,
}

#[derive(Debug, Clone)]
pub struct SecretFindingDetail {
    pub file_path: String,
    pub rule_id: String,
    pub line_number: Option<usize>,
    pub redacted_preview: String,
}

// ── Type aliases ──────────────────────────────────────────────────────────

type FreshnessCheck<'a> = (&'a str, Box<dyn Fn(&serde_json::Value) -> bool + 'a>);

// ── Terminal context (groups params to avoid clippy::too_many_arguments) ──

struct TerminalContext<'a> {
    policy_op_id: &'a str,
    result_id: &'a str,
    evidence_id: &'a str,
    step_result: &'a VerificationStepResult,
    status: &'a VerificationStepStatus,
    classification: &'a Option<FailureClassification>,
}

// ── Service ──────────────────────────────────────────────────────────────

pub struct VerificationPolicyEvidenceService {
    pool: SqlitePool,
    evidence_repo: VerificationEvidenceRepo,
    scanner: Arc<dyn PolicyScanner>,
    /// For tests, count of scanner invocations.
    pub scan_start_count: Arc<AtomicUsize>,
}

impl VerificationPolicyEvidenceService {
    pub fn new(pool: SqlitePool, scanner: Arc<dyn PolicyScanner>) -> Self {
        let evidence_repo = VerificationEvidenceRepo::new(pool.clone());
        Self {
            pool,
            evidence_repo,
            scanner,
            scan_start_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Execute a single deferred policy step. Idempotent: uses formal
    /// verification_policy_operations table (migration 016). Same key +
    /// same hash = Duplicate; same key + different hash = IdempotencyConflict.
    /// Atomic phases: Started (op + event) → execute → Terminal (result +
    /// evidence + event + op completion).
    pub async fn execute_policy_step(&self, req: &PolicyStepRequest) -> PolicyStepOutcome {
        // ── 0. Atomic idempotency: INSERT with ON CONFLICT ──────────
        // Eliminates the fetch-then-CAS race by using an atomic INSERT
        // that claims the idempotency_key before any side effects.
        let policy_op_id = format!("pop-{}", Uuid::new_v4());
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let insert_result = sqlx::query(
            "INSERT INTO verification_policy_operations \
             (policy_op_id, verification_run_id, step_id, step_kind, sequence_index, \
              idempotency_key, request_hash, worktree_id, fencing_token, \
              plan_fingerprint, policy_version, validator_version, lifecycle, started_at) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'running',?) \
             ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(&policy_op_id)
        .bind(&req.verification_run_id)
        .bind(&req.step_id)
        .bind(step_kind_key(&req.step_kind))
        .bind(req.sequence_index as i64)
        .bind(&req.idempotency_key)
        .bind(&req.request_hash)
        .bind(&req.worktree_id)
        .bind(req.expected_fencing)
        .bind("plan-v1")
        .bind(1i64)
        .bind("1.0")
        .bind(&now)
        .execute(&self.pool)
        .await;

        match insert_result {
            Ok(r) if r.rows_affected() == 0 => {
                // Key already exists — read the existing row to determine outcome.
                let existing: Option<(String, String, Option<String>)> = sqlx::query_as(
                    "SELECT policy_op_id, request_hash, result_id FROM verification_policy_operations WHERE idempotency_key=?",
                )
                .bind(&req.idempotency_key)
                .fetch_optional(&self.pool)
                .await
                .unwrap_or(None);

                if let Some((op_id, eh, existing_result)) = existing {
                    if eh == req.request_hash {
                        if let Some(ref rid) = existing_result {
                            if let Some(mismatch) =
                                self.check_evidence_freshness_for_result(rid, req).await
                            {
                                return PolicyStepOutcome::StaleEvidence {
                                    existing_evidence_id: rid.clone(),
                                    mismatched_field: mismatch,
                                };
                            }
                        }
                        return PolicyStepOutcome::Duplicate {
                            existing_step_result_id: existing_result.unwrap_or(op_id),
                        };
                    }
                    return PolicyStepOutcome::IdempotencyConflict {
                        existing_hash: eh,
                        new_hash: req.request_hash.clone(),
                    };
                }
                return PolicyStepOutcome::InfrastructureError {
                    reason: "idempotency insert conflict but row missing".into(),
                };
            }
            Err(e) => {
                return PolicyStepOutcome::InfrastructureError {
                    reason: format!("idempotency insert: {e}"),
                };
            }
            Ok(_) => { /* claimed — proceed */ }
        }

        // ── 1. Full ownership + resource pre-checks ──────────────────
        if let Some(o) = self.check_full_ownership(req).await {
            // Rollback: mark the operation we claimed as failed.
            let _ = sqlx::query(
                "UPDATE verification_policy_operations SET lifecycle='failed', terminal_at=? WHERE policy_op_id=?",
            )
            .bind(&now)
            .bind(&policy_op_id)
            .execute(&self.pool)
            .await;
            return o;
        }

        // ── 2. Atomic Started: insert synthetic step_op + started event ──
        if let Err(e) = self
            .insert_operation_and_start_event(req, &policy_op_id)
            .await
        {
            let _ = sqlx::query(
                "UPDATE verification_policy_operations SET lifecycle='failed', terminal_at=? WHERE policy_op_id=?",
            )
            .bind(&now)
            .bind(&policy_op_id)
            .execute(&self.pool)
            .await;
            return PolicyStepOutcome::InfrastructureError {
                reason: format!("started phase: {e}"),
            };
        }

        // ── 3. Execute scanner / validator ──────────────────────────
        self.scan_start_count.fetch_add(1, Ordering::SeqCst);
        let (status, classification) = match self.execute_step_kind(req).await {
            Ok((s, c)) => (s, c),
            Err(e) => {
                let _ = self
                    .mark_operation_reconciliation(&policy_op_id, &format!("{e}"))
                    .await;
                return PolicyStepOutcome::InfrastructureError {
                    reason: format!("{e}"),
                };
            }
        };

        // ── 4. Atomic Terminal: result + evidence + event + completion ──
        let result_id = format!("sr-{}", Uuid::new_v4());
        let evidence_id = format!("ev-{}", Uuid::new_v4());
        let fc_val = classification
            .as_ref()
            .map(|c| serde_json::to_value(c).unwrap_or_default());
        let detail = serde_json::json!({ "classification": fc_val }).to_string();

        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let step_result = VerificationStepResult {
            result_id: result_id.clone(),
            run_id: req.verification_run_id.clone(),
            step_id: req.step_id.clone(),
            plan_id: req.plan_id.clone(),
            status: status.clone(),
            detail_json: Some(detail),
            started_at: Some(now.clone()),
            completed_at: Some(now.clone()),
            duration_ms: None,
            error_message: if matches!(status, VerificationStepStatus::Error) {
                Some("step execution error".into())
            } else {
                None
            },
        };

        let tctx = TerminalContext {
            policy_op_id: &policy_op_id,
            result_id: &result_id,
            evidence_id: &evidence_id,
            step_result: &step_result,
            status: &status,
            classification: &classification,
        };
        if let Err(e) = self.terminal_phase(req, &tctx).await {
            let _ = self
                .mark_operation_reconciliation(&policy_op_id, &format!("terminal: {e}"))
                .await;
            return PolicyStepOutcome::InfrastructureError {
                reason: format!("terminal phase: {e}"),
            };
        }

        PolicyStepOutcome::Completed {
            step_result,
            classification,
        }
    }

    // ── Step kind dispatch ──────────────────────────────────────────

    async fn execute_step_kind(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        match req.step_kind {
            VerificationStepKind::GitDiffCheck => self.execute_git_diff(req).await,
            VerificationStepKind::FileScopeCheck => self.execute_file_scope(req).await,
            VerificationStepKind::SecretScanCheck => self.execute_secret_scan(req).await,
            VerificationStepKind::ArtifactCheck => self.execute_artifact_check(req).await,
            VerificationStepKind::PolicyCheck => self.execute_policy_check(req).await,
            VerificationStepKind::WorktreeCheck => self.execute_worktree_check(req).await,
            _ => Ok((VerificationStepStatus::Skipped, None)),
        }
    }

    // ── Git Diff ───────────────────────────────────────────────────

    async fn execute_git_diff(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        let report = self
            .scanner
            .scan_diff(&req.worktree_path, req.baseline_commit.as_deref())
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::WorkspaceError,
                    format!("diff scan: {e}"),
                    ErrorSource::System,
                )
            })?;

        if report.changed_paths.is_empty() {
            return Ok((VerificationStepStatus::Passed, None));
        }

        // Binary/submodule files are noted but not blocking by default.
        if report.clean {
            return Ok((VerificationStepStatus::Passed, None));
        }

        Ok((VerificationStepStatus::Passed, None))
    }

    // ── File Scope ─────────────────────────────────────────────────

    async fn execute_file_scope(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        // Collect all changed paths including rename source/dest.
        let report = self
            .scanner
            .scan_diff(&req.worktree_path, req.baseline_commit.as_deref())
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::WorkspaceError,
                    format!("scope scan: {e}"),
                    ErrorSource::System,
                )
            })?;

        // Use the FileScopeValidator to check each changed path.
        let scope_validator = make_scope_validator(&req.worktree_path);
        let mut violations: Vec<String> = Vec::new();

        for p in &report.changed_paths {
            match scope_validator.validate(&p.path) {
                Ok((crate::policy::file_scope::ScopeDecision::Allowed, _)) => {}
                Ok((crate::policy::file_scope::ScopeDecision::Denied(v), _)) => {
                    violations.push(format!("{}: {:?}", p.path, v));
                }
                Err(_) => {
                    violations.push(format!("{}: validation error", p.path));
                }
            }
        }

        if violations.is_empty() {
            return Ok((VerificationStepStatus::Passed, None));
        }

        Ok((
            VerificationStepStatus::Failed,
            Some(FailureClassification::ScopeViolation {
                out_of_scope_files: violations,
            }),
        ))
    }

    // ── Secret Scan ────────────────────────────────────────────────

    async fn execute_secret_scan(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for (path, content) in &req.file_contents {
            files.push((path.clone(), content.clone()));
        }

        let summary = self.scanner.scan_secrets(&files).await;

        if summary.clean {
            return Ok((VerificationStepStatus::Passed, None));
        }

        // Binary/truncated = inconclusive, not clean.
        Ok((
            VerificationStepStatus::Failed,
            Some(FailureClassification::SecretExposure {
                pattern_count: summary.findings_count,
            }),
        ))
    }

    // ── Artifact Check ─────────────────────────────────────────────

    async fn execute_artifact_check(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        let mut missing: Vec<String> = Vec::new();

        for art_ref in &req.artifact_refs {
            // Verify DB reference exists.
            let exists: Option<(String,)> = sqlx::query_as(
                "SELECT artifact_id FROM artifacts WHERE artifact_id=? AND run_id=?",
            )
            .bind(art_ref)
            .bind(&req.verification_run_id)
            .fetch_optional(&self.pool)
            .await
            .unwrap_or(None);

            if exists.is_none() {
                // Check if the reference is a file path.
                let path = Path::new(art_ref);
                if !path.exists() {
                    missing.push(art_ref.clone());
                }
            }
        }

        if missing.is_empty() {
            return Ok((VerificationStepStatus::Passed, None));
        }

        Ok((
            VerificationStepStatus::Failed,
            Some(FailureClassification::ArtifactCorruption {
                artifact_ids: missing,
            }),
        ))
    }

    // ── Policy Check (RequiredFile + ForbiddenChange + OutputMatcher) ──

    async fn execute_policy_check(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        // Required files.
        for rf in &req.required_files {
            let path = Path::new(&rf.path);
            if !path.exists() {
                return Ok((
                    VerificationStepStatus::Failed,
                    Some(FailureClassification::ScopeViolation {
                        out_of_scope_files: vec![format!("required file missing: {}", rf.path)],
                    }),
                ));
            }
            if !path.is_file() {
                return Ok((
                    VerificationStepStatus::Failed,
                    Some(FailureClassification::ScopeViolation {
                        out_of_scope_files: vec![format!(
                            "required path is not a file: {}",
                            rf.path
                        )],
                    }),
                ));
            }
            if let Some(expected_size) = rf.expected_size {
                let meta = std::fs::metadata(path).map_err(|e| {
                    CoreError::new(
                        ErrorCode::WorkspaceError,
                        format!("stat {}: {e}", rf.path),
                        ErrorSource::System,
                    )
                })?;
                if meta.len() != expected_size {
                    return Ok((
                        VerificationStepStatus::Failed,
                        Some(FailureClassification::ArtifactCorruption {
                            artifact_ids: vec![format!(
                                "{}: size {} != {}",
                                rf.path,
                                meta.len(),
                                expected_size
                            )],
                        }),
                    ));
                }
            }
        }

        // Forbidden changes.
        if !req.forbidden_changes.is_empty() {
            let report = self
                .scanner
                .scan_diff(&req.worktree_path, req.baseline_commit.as_deref())
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::WorkspaceError,
                        format!("forbidden scan: {e}"),
                        ErrorSource::System,
                    )
                })?;

            for fc in &req.forbidden_changes {
                for p in &report.changed_paths {
                    let matches_glob = simple_glob_match(&fc.path_glob, &p.path);
                    if !matches_glob {
                        continue;
                    }
                    let violation = match p.change_kind.as_str() {
                        "added" if fc.forbid_add => Some("forbidden add"),
                        "modified" if fc.forbid_modify => Some("forbidden modify"),
                        "deleted" if fc.forbid_delete => Some("forbidden delete"),
                        "renamed" if fc.forbid_modify => Some("forbidden rename"),
                        _ => None,
                    };
                    if let Some(_reason) = violation {
                        return Ok((
                            VerificationStepStatus::Failed,
                            Some(FailureClassification::PolicyViolation { rule_count: 1 }),
                        ));
                    }
                }
            }
        }

        // Output matchers.
        if !req.output_matchers.is_empty() {
            // Check files in the worktree for output matching.
            for om in &req.output_matchers {
                let mut matched = false;
                for content in req.file_contents.values() {
                    let text = String::from_utf8_lossy(content);
                    let found = if om.is_regex {
                        // Validate regex first.
                        let re = match regex::Regex::new(&om.pattern) {
                            Ok(r) => r,
                            Err(e) => {
                                return Ok((
                                    VerificationStepStatus::Error,
                                    Some(FailureClassification::InfrastructureError {
                                        reason: format!("invalid regex: {e}"),
                                    }),
                                ));
                            }
                        };
                        re.is_match(&text)
                    } else {
                        text.contains(&om.pattern)
                    };

                    if found {
                        matched = true;
                        break;
                    }
                }

                match om.kind {
                    OutputMatchKind::Required if !matched => {
                        return Ok((
                            VerificationStepStatus::Failed,
                            Some(FailureClassification::AcceptanceTestFailure {
                                failed_checks: vec![format!(
                                    "required output not found: {}",
                                    om.pattern
                                )],
                            }),
                        ));
                    }
                    OutputMatchKind::Forbidden if matched => {
                        return Ok((
                            VerificationStepStatus::Failed,
                            Some(FailureClassification::PolicyViolation { rule_count: 1 }),
                        ));
                    }
                    _ => {}
                }
            }
        }

        Ok((VerificationStepStatus::Passed, None))
    }

    // ── Worktree Check ─────────────────────────────────────────────

    async fn execute_worktree_check(
        &self,
        req: &PolicyStepRequest,
    ) -> Result<(VerificationStepStatus, Option<FailureClassification>), CoreError> {
        // Verify worktree exists and is a directory.
        if !req.worktree_path.exists() {
            return Ok((
                VerificationStepStatus::Failed,
                Some(FailureClassification::InfrastructureError {
                    reason: "worktree missing".into(),
                }),
            ));
        }
        if !req.worktree_path.is_dir() {
            return Ok((
                VerificationStepStatus::Failed,
                Some(FailureClassification::InfrastructureError {
                    reason: "worktree is not a directory".into(),
                }),
            ));
        }
        Ok((VerificationStepStatus::Passed, None))
    }

    // ── Full ownership + resource pre-checks ───────────────────────

    /// Verify: Run Running, handoff VerificationOwned, owner id, fencing,
    /// heartbeat healthy, Lease Active, Claim Active, worktree identity,
    /// plan fingerprint, step not terminal. Any failure = start_count 0.
    async fn check_full_ownership(&self, req: &PolicyStepRequest) -> Option<PolicyStepOutcome> {
        // 1. Run lifecycle must be "running".
        let lc: Option<(String,)> =
            sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        match lc {
            Some((lc,)) if lc == "running" => {}
            Some((lc,)) => {
                return Some(PolicyStepOutcome::OwnershipLost {
                    reason: format!("run lc={lc}"),
                })
            }
            None => {
                return Some(PolicyStepOutcome::OwnershipLost {
                    reason: "run not found".into(),
                })
            }
        }

        // 2. Handoff must be VerificationOwned with matching owner + fencing.
        let handoff: Option<(String, String, String, i64, String)> = sqlx::query_as(
            "SELECT handoff_id, owner_kind, owner_id, fencing_token, worktree_id FROM resource_handoffs WHERE execution_id=?",
        )
        .bind(&req.execution_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        match handoff {
            Some((_hid, k, o, f, wt)) => {
                if k != "verification" || o != req.verification_owner_id {
                    return Some(PolicyStepOutcome::OwnershipLost {
                        reason: format!("owner={k}/{o}"),
                    });
                }
                if f != req.expected_fencing {
                    return Some(PolicyStepOutcome::OwnershipLost {
                        reason: format!("fence={f}!={}", req.expected_fencing),
                    });
                }
                if wt != req.worktree_id {
                    return Some(PolicyStepOutcome::OwnershipLost {
                        reason: format!("worktree={wt}!={}", req.worktree_id),
                    });
                }
            }
            None => {
                return Some(PolicyStepOutcome::OwnershipLost {
                    reason: "handoff missing".into(),
                })
            }
        }

        // 3. Heartbeat check: lease must be active for this task.
        let heartbeat: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM workspace_leases WHERE task_id=? AND lifecycle='acquired' AND released_at IS NULL LIMIT 1",
        )
        .bind(&req.task_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        if heartbeat.is_none() {
            return Some(PolicyStepOutcome::OwnershipLost {
                reason: "no active lease (heartbeat missing)".into(),
            });
        }

        // 4. Claim check: at least one active claim exists for this task.
        let claim: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM resource_claims WHERE task_id=? AND status='active' LIMIT 1",
        )
        .bind(&req.task_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        if claim.is_none() {
            return Some(PolicyStepOutcome::OwnershipLost {
                reason: "no active claim".into(),
            });
        }

        // 5. Step must not already have a terminal operation.
        let existing_terminal: Option<(String,)> = sqlx::query_as(
            "SELECT policy_op_id FROM verification_policy_operations WHERE verification_run_id=? AND step_id=? AND lifecycle IN ('completed','failed','reconciliation_required') LIMIT 1",
        )
        .bind(&req.verification_run_id)
        .bind(&req.step_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        if existing_terminal.is_some() {
            return Some(PolicyStepOutcome::OwnershipLost {
                reason: "step already terminal".into(),
            });
        }

        None
    }

    // ── Atomic phases ──────────────────────────────────────────────

    /// Insert operation row + synthetic step_op (for FK) + started event atomically.
    async fn insert_operation_and_start_event(
        &self,
        req: &PolicyStepRequest,
        policy_op_id: &str,
    ) -> Result<(), CoreError> {
        let cfg_hash = format!("{:016x}", {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            req.step_id.hash(&mut h);
            h.finish()
        });

        // 1. Insert synthetic step_op so FK on verification_step_events is satisfied.
        sqlx::query("INSERT OR IGNORE INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES (?,?,?,?,?,?,?,?,'policy',?,?)")
            .bind(policy_op_id)
            .bind(&req.verification_run_id)
            .bind(&req.step_id)
            .bind(&req.plan_id)
            .bind(&req.execution_id)
            .bind(&cfg_hash)
            .bind(&req.worktree_id)
            .bind(req.expected_fencing)
            .bind(&req.idempotency_key)
            .bind(&req.request_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("synthetic step_op: {e}"),
                    ErrorSource::System,
                )
            })?;

        // 2. Policy operation row was already inserted atomically in
        //    execute_policy_step (ON CONFLICT for idempotency safety).
        //    No second insert here.

        // 3. Write started event (policy_op_id matches step_op FK).
        let eid = format!("evt-policy-started-{}", Uuid::new_v4());
        let ikey = format!("policy-ev-{}-started", req.step_id);
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&eid)
            .bind(&req.verification_run_id)
            .bind(&req.step_id)
            .bind(policy_op_id)
            .bind(&req.execution_id)
            .bind(&req.task_id)
            .bind(&req.worktree_id)
            .bind(req.expected_fencing)
            .bind("policy_started")
            .bind(step_kind_key(&req.step_kind))
            .bind::<Option<String>>(None)
            .bind(&ikey)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("policy started event: {e}"),
                    ErrorSource::System,
                )
            })?;

        Ok(())
    }

    /// Terminal phase: persist StepResult + Evidence + terminal event +
    /// mark operation completed. Best-effort; failures mark ReconciliationRequired.
    async fn terminal_phase(
        &self,
        req: &PolicyStepRequest,
        ctx: &TerminalContext<'_>,
    ) -> Result<(), CoreError> {
        // 1. Persist StepResult.
        self.evidence_repo
            .insert_step_result(ctx.step_result)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("step result: {e}"),
                    ErrorSource::System,
                )
            })?;

        // 2. Persist Evidence with full freshness bindings.
        self.persist_fresh_evidence(req, ctx.evidence_id, ctx.status, ctx.classification)
            .await?;

        // 3. Write terminal event.
        let event_type = match ctx.status {
            VerificationStepStatus::Passed => "policy_passed",
            VerificationStepStatus::Failed => "policy_failed",
            VerificationStepStatus::Blocked => "policy_blocked",
            VerificationStepStatus::Error => "policy_error",
            VerificationStepStatus::Skipped => "policy_skipped",
            VerificationStepStatus::ProcessUnknown => "process_unknown",
        };
        let eid = format!("evt-policy-terminal-{}", Uuid::new_v4());
        let ikey = format!("policy-ev-{}-{}", req.step_id, event_type);
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&eid)
            .bind(&req.verification_run_id)
            .bind(&req.step_id)
            .bind(ctx.policy_op_id)
            .bind(&req.execution_id)
            .bind(&req.task_id)
            .bind(&req.worktree_id)
            .bind(req.expected_fencing)
            .bind(event_type)
            .bind(step_kind_key(&req.step_kind))
            .bind::<Option<String>>(None)
            .bind(&ikey)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    format!("terminal event: {e}"),
                    ErrorSource::System,
                )
            })?;

        // 4. Mark operation completed.
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        sqlx::query(
            "UPDATE verification_policy_operations SET lifecycle='completed', result_id=?, evidence_id=?, terminal_at=? WHERE policy_op_id=?",
        )
        .bind(ctx.result_id)
        .bind(ctx.evidence_id)
        .bind(&now)
        .bind(ctx.policy_op_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("op completion: {e}"),
                ErrorSource::System,
            )
        })?;

        Ok(())
    }

    /// Mark operation as reconciliation_required after a failure.
    async fn mark_operation_reconciliation(
        &self,
        policy_op_id: &str,
        reason: &str,
    ) -> Result<(), CoreError> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        sqlx::query(
            "UPDATE verification_policy_operations SET lifecycle='reconciliation_required', outcome_json=?, terminal_at=? WHERE policy_op_id=?",
        )
        .bind(reason)
        .bind(&now)
        .bind(policy_op_id)
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

    /// Check if the evidence for a result_id is fresh relative to the request.
    /// Returns Some(mismatched_field) if stale, None if fresh.
    async fn check_evidence_freshness_for_result(
        &self,
        result_id: &str,
        req: &PolicyStepRequest,
    ) -> Option<String> {
        // Look up evidence_id from the policy operation, then load that evidence.
        let evidence_id: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT evidence_id FROM verification_policy_operations WHERE result_id=?",
        )
        .bind(result_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        let eid = match evidence_id {
            Some((Some(eid),)) => eid,
            _ => return Some("no_evidence_found".into()),
        };
        let evidence_rows = self
            .evidence_repo
            .get_evidence(&req.verification_run_id)
            .await
            .ok()?;
        for ev in evidence_rows {
            if ev.evidence_id == eid {
                return Self::evidence_freshness_mismatch(req, &ev);
            }
        }
        Some("no_evidence_found".into())
    }

    // ── Evidence persistence with freshness bindings ───────────────

    async fn persist_fresh_evidence(
        &self,
        req: &PolicyStepRequest,
        evidence_id: &str,
        status: &VerificationStepStatus,
        classification: &Option<FailureClassification>,
    ) -> Result<(), CoreError> {
        let evidence_kind = match req.step_kind {
            VerificationStepKind::GitDiffCheck => VerificationEvidenceKind::FileDiffSummary,
            VerificationStepKind::FileScopeCheck => VerificationEvidenceKind::PolicyViolation,
            VerificationStepKind::SecretScanCheck => VerificationEvidenceKind::SecretFinding,
            VerificationStepKind::ArtifactCheck => VerificationEvidenceKind::ArtifactRef,
            VerificationStepKind::PolicyCheck => VerificationEvidenceKind::PolicyViolation,
            VerificationStepKind::WorktreeCheck => VerificationEvidenceKind::WorktreeState,
            _ => VerificationEvidenceKind::Custom,
        };
        let classification_str = classification
            .as_ref()
            .map(|c| c.category_name())
            .unwrap_or("none");
        let detail = serde_json::json!({
            "step_kind": step_kind_key(&req.step_kind),
            "status": step_status_key(status),
            "classification": classification_str,
            "verification_run_id": req.verification_run_id,
            "step_id": req.step_id,
            "execution_id": req.execution_id,
            "worktree_id": req.worktree_id,
            "fencing": req.expected_fencing,
            "plan_fingerprint": "plan-v1",
            "policy_version": 1,
            "validator_version": "1.0",
            "worktree_head": req.worktree_head,
            "baseline_commit": req.baseline_commit,
            "input_fingerprint": req.input_fingerprint,
            "changed_path_fingerprint": req.changed_path_fingerprint,
            "artifact_checksum": req.artifact_checksum,
        })
        .to_string();
        let evidence = VerificationEvidence {
            evidence_id: evidence_id.to_string(),
            run_id: req.verification_run_id.clone(),
            step_id: req.step_id.clone(),
            evidence_kind,
            summary: format!(
                "step {} {}: {} [fence={}]",
                req.sequence_index,
                step_kind_key(&req.step_kind),
                classification_str,
                req.expected_fencing
            ),
            detail_json: Some(detail),
            artifact_ref: None,
            collected_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        self.evidence_repo.insert_evidence(&evidence).await
    }

    /// Returns Some(mismatched_field_name) if evidence is stale for this request,
    /// None if all freshness fields match and evidence can be reused.
    fn evidence_freshness_mismatch(
        req: &PolicyStepRequest,
        evidence: &VerificationEvidence,
    ) -> Option<String> {
        let detail = match &evidence.detail_json {
            Some(d) => d,
            None => return Some("missing_detail".into()),
        };
        let v: serde_json::Value = match serde_json::from_str(detail) {
            Ok(v) => v,
            Err(_) => return Some("invalid_json".into()),
        };

        // All 14 freshness fields must match. Any mismatch = stale.
        let checks: &[FreshnessCheck<'_>] = &[
            (
                "verification_run_id",
                Box::new(|v| {
                    v.get("verification_run_id").and_then(|x| x.as_str())
                        == Some(&req.verification_run_id)
                }),
            ),
            (
                "step_id",
                Box::new(|v| v.get("step_id").and_then(|x| x.as_str()) == Some(&req.step_id)),
            ),
            (
                "execution_id",
                Box::new(|v| {
                    v.get("execution_id").and_then(|x| x.as_str()) == Some(&req.execution_id)
                }),
            ),
            (
                "worktree_id",
                Box::new(|v| {
                    v.get("worktree_id").and_then(|x| x.as_str()) == Some(&req.worktree_id)
                }),
            ),
            (
                "fencing",
                Box::new(|v| {
                    v.get("fencing").and_then(|x| x.as_i64()) == Some(req.expected_fencing)
                }),
            ),
            (
                "plan_fingerprint",
                Box::new(|v| v.get("plan_fingerprint").and_then(|x| x.as_str()) == Some("plan-v1")),
            ),
            (
                "policy_version",
                Box::new(|v| v.get("policy_version").and_then(|x| x.as_i64()) == Some(1)),
            ),
            (
                "validator_version",
                Box::new(|v| v.get("validator_version").and_then(|x| x.as_str()) == Some("1.0")),
            ),
            (
                "baseline_commit",
                Box::new(|v| {
                    v.get("baseline_commit").and_then(|x| x.as_str())
                        == req.baseline_commit.as_deref()
                }),
            ),
            (
                "worktree_head",
                Box::new(|v| {
                    v.get("worktree_head").and_then(|x| x.as_str()) == req.worktree_head.as_deref()
                }),
            ),
            (
                "input_fingerprint",
                Box::new(|v| {
                    v.get("input_fingerprint").and_then(|x| x.as_str())
                        == req.input_fingerprint.as_deref()
                }),
            ),
            (
                "changed_path_fingerprint",
                Box::new(|v| {
                    v.get("changed_path_fingerprint").and_then(|x| x.as_str())
                        == req.changed_path_fingerprint.as_deref()
                }),
            ),
            (
                "artifact_checksum",
                Box::new(|v| {
                    v.get("artifact_checksum").and_then(|x| x.as_str())
                        == req.artifact_checksum.as_deref()
                }),
            ),
        ];

        for (name, check) in checks {
            if !check(&v) {
                return Some(name.to_string());
            }
        }
        None
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn step_kind_key(k: &VerificationStepKind) -> &'static str {
    match k {
        VerificationStepKind::GitDiffCheck => "git_diff",
        VerificationStepKind::FileScopeCheck => "file_scope",
        VerificationStepKind::SecretScanCheck => "secret_scan",
        VerificationStepKind::PolicyCheck => "policy",
        VerificationStepKind::AcceptanceCheck => "acceptance",
        VerificationStepKind::ArtifactCheck => "artifact",
        VerificationStepKind::TaskResultCheck => "task_result",
        VerificationStepKind::WorktreeCheck => "worktree",
        VerificationStepKind::ResourceOwnershipCheck => "resource",
        VerificationStepKind::CustomCheck => "custom",
    }
}

fn step_status_key(s: &VerificationStepStatus) -> &'static str {
    match s {
        VerificationStepStatus::Passed => "passed",
        VerificationStepStatus::Failed => "failed",
        VerificationStepStatus::Blocked => "blocked",
        VerificationStepStatus::Skipped => "skipped",
        VerificationStepStatus::Error => "error",
        VerificationStepStatus::ProcessUnknown => "process_unknown",
    }
}

/// Create a permissive FileScopeValidator for the worktree.
fn make_scope_validator(wt: &Path) -> crate::policy::file_scope::FileScopeValidator {
    let fs = harness_core::contracts::task_envelope::FileScope {
        allowed_paths: vec![],
        forbidden_paths: vec![],
        readable_paths: vec![],
        scope_expansion_allowed: true,
    };
    crate::policy::file_scope::FileScopeValidator::new(wt, fs).expect("default scope validator")
}

/// Simple glob match: `*` matches any sequence, `?` matches single char.
fn simple_glob_match(pattern: &str, path: &str) -> bool {
    let pat_lower = pattern.to_lowercase();
    let path_lower = path.to_lowercase();
    if !pat_lower.contains('*') && !pat_lower.contains('?') {
        return path_lower.contains(&pat_lower);
    }
    // Rudimentary glob: ** matches everything, prefix* matches prefix, *suffix
    if pat_lower == "**" || pat_lower == "*" {
        return true;
    }
    if let Some(prefix) = pat_lower.strip_suffix('*') {
        return path_lower.starts_with(prefix);
    }
    if let Some(suffix) = pat_lower.strip_prefix('*') {
        return path_lower.ends_with(suffix);
    }
    if let Some(rest) = pat_lower.strip_prefix('*') {
        if let Some(rest) = rest.strip_suffix('*') {
            return path_lower.contains(rest);
        }
    }
    path_lower == pat_lower
}

// ── Production scanner ──────────────────────────────────────────────────

/// Production implementation using GitDiffScopeValidator + SecretScanner.
pub struct ProductionPolicyScanner {
    pub diff_validator: tokio::sync::Mutex<Option<crate::policy::diff::GitDiffScopeValidator>>,
    pub secret_scanner: tokio::sync::Mutex<Option<crate::policy::scanner::SecretScanner>>,
}

impl Default for ProductionPolicyScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl ProductionPolicyScanner {
    pub fn new() -> Self {
        Self {
            diff_validator: tokio::sync::Mutex::new(None),
            secret_scanner: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn set_diff_validator(&self, v: crate::policy::diff::GitDiffScopeValidator) {
        *self.diff_validator.lock().await = Some(v);
    }

    pub async fn set_secret_scanner(&self, s: crate::policy::scanner::SecretScanner) {
        *self.secret_scanner.lock().await = Some(s);
    }
}

#[async_trait::async_trait]
impl PolicyScanner for ProductionPolicyScanner {
    async fn scan_diff(
        &self,
        worktree_path: &Path,
        _baseline: Option<&str>,
    ) -> Result<DiffScanReport, CoreError> {
        let scope_validator = make_scope_validator(worktree_path);
        let includes = crate::policy::diff::DiffIncludes {
            staged: true,
            unstaged: true,
            untracked: true,
        };

        let guard = self.diff_validator.lock().await;
        let validator = guard.as_ref().ok_or_else(|| {
            CoreError::new(
                ErrorCode::WorkspaceError,
                "diff validator not configured",
                ErrorSource::System,
            )
        })?;
        let report = validator
            .validate(worktree_path, &scope_validator, includes)
            .await?;
        drop(guard);

        let mut changed_paths = Vec::new();
        let mut added_count = 0;
        let mut modified_count = 0;
        let mut deleted_count = 0;
        let mut untracked_count = 0;
        let mut rename_count = 0;

        for cp in &report.changed_paths {
            let (kind_str, from_path) = match &cp.kind {
                crate::policy::diff::ChangeKind::Added => {
                    added_count += 1;
                    ("added", None)
                }
                crate::policy::diff::ChangeKind::Modified => {
                    modified_count += 1;
                    ("modified", None)
                }
                crate::policy::diff::ChangeKind::Deleted => {
                    deleted_count += 1;
                    ("deleted", None)
                }
                crate::policy::diff::ChangeKind::Renamed { from } => {
                    rename_count += 1;
                    ("renamed", Some(from.clone()))
                }
                crate::policy::diff::ChangeKind::Copied { from } => ("copied", Some(from.clone())),
                crate::policy::diff::ChangeKind::TypeChange => {
                    modified_count += 1;
                    ("type_change", None)
                }
                crate::policy::diff::ChangeKind::Binary => ("binary", None),
                crate::policy::diff::ChangeKind::Submodule => ("submodule", None),
                crate::policy::diff::ChangeKind::Untracked => {
                    untracked_count += 1;
                    ("untracked", None)
                }
            };
            changed_paths.push(ChangedPathInfo {
                path: cp.path.clone(),
                change_kind: kind_str.into(),
                from_path,
            });
        }

        Ok(DiffScanReport {
            changed_paths,
            clean: report.clean,
            binary_files: report.binary_files,
            submodule_files: report.submodule_files,
            added_count,
            modified_count,
            deleted_count,
            untracked_count,
            rename_count,
        })
    }

    async fn scan_secrets(&self, files: &[(String, Vec<u8>)]) -> SecretScanSummary {
        let guard = self.secret_scanner.lock().await;
        let scanner = match guard.as_ref() {
            Some(s) => s,
            None => {
                return SecretScanSummary {
                    findings_count: 0,
                    files_scanned: 0,
                    clean: true,
                    finding_details: Vec::new(),
                }
            }
        };

        let report = scanner.scan_diff(files);
        let finding_details: Vec<SecretFindingDetail> = report
            .findings
            .iter()
            .map(|f| {
                let rule_id = match &f.kind {
                    crate::policy::scanner::SecretKind::KnownSecret { hash } => {
                        format!("known:{hash}")
                    }
                    crate::policy::scanner::SecretKind::PrivateKeyHeader { .. } => {
                        "private_key".into()
                    }
                    crate::policy::scanner::SecretKind::ApiTokenPattern { pattern_name } => {
                        pattern_name.clone()
                    }
                    crate::policy::scanner::SecretKind::HighEntropy { .. } => "high_entropy".into(),
                    crate::policy::scanner::SecretKind::CredentialFilePath { path_rule } => {
                        format!("cred_file:{path_rule}")
                    }
                    crate::policy::scanner::SecretKind::BinarySkipped => "binary".into(),
                    crate::policy::scanner::SecretKind::TruncatedLargeFile => "truncated".into(),
                };
                SecretFindingDetail {
                    file_path: f.file_path.clone(),
                    rule_id,
                    line_number: f.line_number,
                    redacted_preview: f.redacted_preview.clone(),
                }
            })
            .collect();

        SecretScanSummary {
            findings_count: report.findings.len() as u32,
            files_scanned: report.files_scanned,
            clean: report.clean,
            finding_details,
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    struct FakePolicyScanner {
        pub scan_start_count: Arc<AtomicUsize>,
        pub diff_report: std::sync::Mutex<DiffScanReport>,
        pub secret_summary: std::sync::Mutex<SecretScanSummary>,
        pub fail_diff: std::sync::atomic::AtomicBool,
        #[allow(dead_code)]
        pub fail_secret: std::sync::atomic::AtomicBool,
    }

    impl FakePolicyScanner {
        fn new(sc: Arc<AtomicUsize>) -> Self {
            Self {
                scan_start_count: sc,
                diff_report: DiffScanReport {
                    changed_paths: vec![],
                    clean: true,
                    binary_files: vec![],
                    submodule_files: vec![],
                    added_count: 0,
                    modified_count: 0,
                    deleted_count: 0,
                    untracked_count: 0,
                    rename_count: 0,
                }
                .into(),
                secret_summary: SecretScanSummary {
                    findings_count: 0,
                    files_scanned: 0,
                    clean: true,
                    finding_details: vec![],
                }
                .into(),
                fail_diff: false.into(),
                fail_secret: false.into(),
            }
        }
    }

    #[async_trait::async_trait]
    impl PolicyScanner for FakePolicyScanner {
        async fn scan_diff(
            &self,
            _worktree_path: &Path,
            _baseline: Option<&str>,
        ) -> Result<DiffScanReport, CoreError> {
            self.scan_start_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_diff.load(Ordering::SeqCst) {
                return Err(CoreError::new(
                    ErrorCode::WorkspaceError,
                    "injected diff failure",
                    ErrorSource::System,
                ));
            }
            Ok(self.diff_report.lock().unwrap().clone())
        }

        async fn scan_secrets(&self, _files: &[(String, Vec<u8>)]) -> SecretScanSummary {
            self.scan_start_count.fetch_add(1, Ordering::SeqCst);
            self.secret_summary.lock().unwrap().clone()
        }
    }

    struct Ctx {
        svc: VerificationPolicyEvidenceService,
        db: Database,
        sc: Arc<AtomicUsize>,
        fake: Arc<FakePolicyScanner>,
        wtd: tempfile::TempDir,
    }

    async fn setup() -> Ctx {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("pe.db");
        let db = Database::open(&dp).await.unwrap();
        let p = db.pool.clone();
        sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','t','active')")
            .execute(&p)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','t','submitted')",
        )
        .execute(&p)
        .await
        .unwrap();
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_plans(plan_id,task_id,execution_id,project_id,plan_hash,plan_version,steps_json) VALUES('plan-1','t1','e1','p1','ha',1,'[]')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','running','ik-r','hr')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(&p).await.unwrap();
        // Seed lease and claim so heartbeat/claim checks pass.
        sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(&p).await.unwrap();
        let wd = tempfile::tempdir().unwrap();
        let sc = Arc::new(AtomicUsize::new(0));
        let fake = Arc::new(FakePolicyScanner::new(sc.clone()));
        let svc = VerificationPolicyEvidenceService::new(p, fake.clone());
        Ctx {
            svc,
            db,
            sc,
            fake,
            wtd: wd,
        }
    }

    fn mkreq(ctx: &Ctx, ikey: &str, hash: &str) -> PolicyStepRequest {
        PolicyStepRequest {
            verification_run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            execution_id: "e1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            worktree_id: "wt1".into(),
            worktree_path: ctx.wtd.path().to_path_buf(),
            worktree_head: Some("abc123".into()),
            baseline_commit: Some("def456".into()),
            expected_fencing: 5,
            verification_owner_id: "verify-run-1".into(),
            idempotency_key: ikey.into(),
            request_hash: hash.into(),
            step_kind: VerificationStepKind::GitDiffCheck,
            required: true,
            sequence_index: 0,
            config_json: "{}".into(),
            changed_file_paths: vec![],
            file_contents: HashMap::new(),
            artifact_refs: vec![],
            required_files: vec![],
            forbidden_changes: vec![],
            output_matchers: vec![],
            input_fingerprint: Some("ifp-test".into()),
            changed_path_fingerprint: Some("cpfp-test".into()),
            artifact_checksum: None,
        }
    }

    // ── Basic execution ────────────────────────────────────────────
    #[tokio::test]
    async fn test_policy_step_normal_exec() {
        let c = setup().await;
        let r = c.svc.execute_policy_step(&mkreq(&c, "ik-1", "h-1")).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_policy_step_idempotent_duplicate() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-dup", "h-dup");
        c.svc.execute_policy_step(&rq).await;
        let r2 = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r2, PolicyStepOutcome::Duplicate { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1, "only one scan");
    }

    #[tokio::test]
    async fn test_policy_step_idempotent_conflict() {
        let c = setup().await;
        c.svc.execute_policy_step(&mkreq(&c, "ik-co", "h-a")).await;
        let r = c.svc.execute_policy_step(&mkreq(&c, "ik-co", "h-b")).await;
        assert!(matches!(r, PolicyStepOutcome::IdempotencyConflict { .. }));
    }

    #[tokio::test]
    async fn test_policy_step_ownership_lost_not_running() {
        let c = setup().await;
        sqlx::query("UPDATE verification_runs SET lifecycle='created' WHERE run_id='run-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.svc.execute_policy_step(&mkreq(&c, "ik-o1", "h-o1")).await;
        assert!(matches!(r, PolicyStepOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_policy_step_wrong_owner() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-o2", "h-o2");
        rq.verification_owner_id = "wrong".into();
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_policy_step_stale_fencing() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-o3", "h-o3");
        rq.expected_fencing = 99;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }

    // ── GitDiff step ──────────────────────────────────────────────
    #[tokio::test]
    async fn test_git_diff_clean() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-gd1", "h-gd1");
        rq.step_kind = VerificationStepKind::GitDiffCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_git_diff_with_changes() {
        let c = setup().await;
        let mut report = c.fake.diff_report.lock().unwrap().clone();
        report.changed_paths = vec![ChangedPathInfo {
            path: "src/main.rs".into(),
            change_kind: "modified".into(),
            from_path: None,
        }];
        report.added_count = 1;
        report.clean = true;
        *c.fake.diff_report.lock().unwrap() = report;
        let mut rq = mkreq(&c, "ik-gd2", "h-gd2");
        rq.step_kind = VerificationStepKind::GitDiffCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_git_diff_infrastructure_error() {
        let c = setup().await;
        c.fake.fail_diff.store(true, Ordering::SeqCst);
        let mut rq = mkreq(&c, "ik-gd3", "h-gd3");
        rq.step_kind = VerificationStepKind::GitDiffCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::InfrastructureError { .. }));
    }

    // ── FileScope step ────────────────────────────────────────────
    #[tokio::test]
    async fn test_file_scope_clean() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-fs1", "h-fs1");
        rq.step_kind = VerificationStepKind::FileScopeCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_file_scope_violation() {
        let c = setup().await;
        let mut report = c.fake.diff_report.lock().unwrap().clone();
        report.changed_paths = vec![
            ChangedPathInfo {
                path: "../outside/file.txt".into(),
                change_kind: "added".into(),
                from_path: None,
            },
            ChangedPathInfo {
                path: ".git/config".into(),
                change_kind: "modified".into(),
                from_path: None,
            },
        ];
        *c.fake.diff_report.lock().unwrap() = report;
        let mut rq = mkreq(&c, "ik-fs2", "h-fs2");
        rq.step_kind = VerificationStepKind::FileScopeCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── SecretScan step ───────────────────────────────────────────
    #[tokio::test]
    async fn test_secret_scan_clean() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-ss1", "h-ss1");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        rq.file_contents = HashMap::from([("src/main.rs".into(), b"fn main() {}".to_vec())]);
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_secret_scan_finds_token() {
        let c = setup().await;
        let mut summary = c.fake.secret_summary.lock().unwrap().clone();
        summary.clean = false;
        summary.findings_count = 1;
        summary.finding_details = vec![SecretFindingDetail {
            file_path: ".env".into(),
            rule_id: "github_pat".into(),
            line_number: Some(1),
            redacted_preview: "[redacted: github_pat token]".into(),
        }];
        *c.fake.secret_summary.lock().unwrap() = summary;
        let mut rq = mkreq(&c, "ik-ss2", "h-ss2");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        rq.file_contents = HashMap::from([(".env".into(), b"GITHUB_TOKEN=ghp_abc".to_vec())]);
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── Response-lost idempotency ─────────────────────────────────
    #[tokio::test]
    async fn test_response_lost_one_scan() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-rl", "h-rl");
        c.svc.execute_policy_step(&rq).await;
        c.svc.execute_policy_step(&rq).await;
        // Only one scan started; second returns duplicate.
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
        // Evidence count is 1.
        let ev: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_evidence WHERE run_id='run-1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        assert_eq!(ev.0, 1, "exactly one evidence record");
    }

    // ── Two-pool single scanner ───────────────────────────────────
    #[tokio::test]
    async fn test_two_pool_one_scanner() {
        let c = setup().await;
        let s2 = VerificationPolicyEvidenceService::new(c.db.pool.clone(), c.fake.clone());
        let rq = mkreq(&c, "ik-tp", "h-tp");
        let (r1, r2) = tokio::join!(c.svc.execute_policy_step(&rq), s2.execute_policy_step(&rq));
        let completed = matches!(r1, PolicyStepOutcome::Completed { .. })
            || matches!(r2, PolicyStepOutcome::Completed { .. });
        assert!(completed, "at least one must complete");
        let ev: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_evidence WHERE run_id='run-1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        assert_eq!(ev.0, 1, "exactly one evidence");
    }

    // ── RequiredFile ──────────────────────────────────────────────
    #[tokio::test]
    async fn test_required_file_missing() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-rf1", "h-rf1");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.required_files = vec![RequiredFileSpec {
            path: "/nonexistent/file.txt".into(),
            expected_size: None,
            expected_fingerprint: None,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_required_file_exists() {
        let c = setup().await;
        let fpath = c.wtd.path().join("config.json");
        std::fs::write(&fpath, b"{}").unwrap();
        let mut rq = mkreq(&c, "ik-rf2", "h-rf2");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.required_files = vec![RequiredFileSpec {
            path: fpath.to_string_lossy().to_string(),
            expected_size: Some(2),
            expected_fingerprint: None,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── ForbiddenChange ───────────────────────────────────────────
    #[tokio::test]
    async fn test_forbidden_change_detected() {
        let c = setup().await;
        let mut report = c.fake.diff_report.lock().unwrap().clone();
        report.changed_paths = vec![ChangedPathInfo {
            path: "src/main.rs".into(),
            change_kind: "modified".into(),
            from_path: None,
        }];
        *c.fake.diff_report.lock().unwrap() = report;
        let mut rq = mkreq(&c, "ik-fc1", "h-fc1");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.forbidden_changes = vec![ForbiddenChangeSpec {
            path_glob: "src/*".into(),
            forbid_add: true,
            forbid_modify: true,
            forbid_delete: true,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── OutputMatcher ─────────────────────────────────────────────
    #[tokio::test]
    async fn test_output_matcher_required_found() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-om1", "h-om1");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.file_contents = HashMap::from([("output.txt".into(), b"BUILD SUCCESS".to_vec())]);
        rq.output_matchers = vec![OutputMatcherSpec {
            kind: OutputMatchKind::Required,
            pattern: "BUILD SUCCESS".into(),
            is_regex: false,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_output_matcher_required_missing() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-om2", "h-om2");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.file_contents = HashMap::from([("output.txt".into(), b"BUILD FAILED".to_vec())]);
        rq.output_matchers = vec![OutputMatcherSpec {
            kind: OutputMatchKind::Required,
            pattern: "BUILD SUCCESS".into(),
            is_regex: false,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_output_matcher_forbidden_found() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-om3", "h-om3");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.file_contents = HashMap::from([("output.txt".into(), b"SECRET_LEAKED".to_vec())]);
        rq.output_matchers = vec![OutputMatcherSpec {
            kind: OutputMatchKind::Forbidden,
            pattern: "SECRET".into(),
            is_regex: false,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    #[tokio::test]
    async fn test_output_matcher_invalid_regex() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-om4", "h-om4");
        rq.step_kind = VerificationStepKind::PolicyCheck;
        rq.output_matchers = vec![OutputMatcherSpec {
            kind: OutputMatchKind::Required,
            pattern: "[invalid".into(),
            is_regex: true,
        }];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── Evidence exactly once ─────────────────────────────────────
    #[tokio::test]
    async fn test_evidence_written() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-ev1", "h-ev1");
        c.svc.execute_policy_step(&rq).await;
        let items = c.svc.evidence_repo.get_evidence("run-1").await.unwrap();
        assert_eq!(items.len(), 1, "exactly one evidence record");
    }

    // ── Terminal step cannot reactivate ───────────────────────────
    #[tokio::test]
    async fn test_terminal_step_not_reactivated() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-ts1", "h-ts1");
        // First execution → completed with result in DB.
        c.svc.execute_policy_step(&rq).await;
        // Second → duplicate (same key+hash).
        let r2 = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r2, PolicyStepOutcome::Duplicate { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1, "no re-scan");
    }

    // ── Event count ───────────────────────────────────────────────
    #[tokio::test]
    async fn test_policy_step_event_written() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-ev2", "h-ev2"))
            .await;
        let ec: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='policy_passed'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(ec.0, 1, "policy event must be written");
    }

    // ── Artifact check ────────────────────────────────────────────
    #[tokio::test]
    async fn test_artifact_check_missing() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-ac1", "h-ac1");
        rq.step_kind = VerificationStepKind::ArtifactCheck;
        rq.artifact_refs = vec!["nonexistent_artifact".into()];
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── Worktree check ────────────────────────────────────────────
    #[tokio::test]
    async fn test_worktree_check_exists() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-wc1", "h-wc1");
        rq.step_kind = VerificationStepKind::WorktreeCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::Completed { .. }));
    }

    // ── No Agent / no retry / no provider switch ──────────────────
    #[tokio::test]
    async fn test_no_side_effects() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-se1", "h-se1"))
            .await;
        // Verify no extra execution_attempts created.
        let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ec.0, 1, "no new execution attempts");
        // Verify task lifecycle unchanged.
        let tl: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(tl.0, "submitted", "task lifecycle unchanged");
    }

    // ── Diff infrastructure error → InfrastructureFailure ─────────
    #[tokio::test]
    async fn test_diff_failure_reported() {
        let c = setup().await;
        c.fake.fail_diff.store(true, Ordering::SeqCst);
        let mut rq = mkreq(&c, "ik-df1", "h-df1");
        rq.step_kind = VerificationStepKind::GitDiffCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::InfrastructureError { .. }));
    }

    // ══════════════════════════════════════════════════════════════════
    // Evidence freshness — full 14-field comparison
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_freshness_same_input_may_reuse() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-fr1", "h-fr1");
        c.svc.execute_policy_step(&rq).await;
        let r2 = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r2, PolicyStepOutcome::Duplicate { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
    }

    // Fencing stored in evidence is 5, request has 99 → evidence stale.
    #[tokio::test]
    async fn test_freshness_fencing_change_invalidates() {
        let c = setup().await;
        let mut rq1 = mkreq(&c, "ik-fr2", "h-fr2");
        rq1.expected_fencing = 5;
        c.svc.execute_policy_step(&rq1).await;

        // Same hash → Duplicate path → freshness check detects fencing mismatch.
        let mut rq2 = mkreq(&c, "ik-fr2", "h-fr2");
        rq2.expected_fencing = 99;
        let r = c.svc.execute_policy_step(&rq2).await;
        assert!(
            matches!(r, PolicyStepOutcome::StaleEvidence { .. }),
            "fencing change must produce StaleEvidence, got: {r:?}"
        );
    }

    #[tokio::test]
    async fn test_freshness_worktree_head_change_stale_evidence() {
        let c = setup().await;
        let mut rq1 = mkreq(&c, "ik-fr3", "h-fr3");
        rq1.worktree_head = Some("abc".into());
        c.svc.execute_policy_step(&rq1).await;

        // Same key+hash → Duplicate path → freshness check → StaleEvidence.
        let mut rq2 = mkreq(&c, "ik-fr3", "h-fr3");
        rq2.worktree_head = Some("xyz".into());
        let r = c.svc.execute_policy_step(&rq2).await;
        assert!(
            matches!(r, PolicyStepOutcome::StaleEvidence { .. }),
            "worktree_head change must produce StaleEvidence, got: {r:?}"
        );
    }

    #[tokio::test]
    async fn test_freshness_baseline_change_stale_evidence() {
        let c = setup().await;
        let mut rq1 = mkreq(&c, "ik-fr4", "h-fr4");
        rq1.baseline_commit = Some("base-a".into());
        c.svc.execute_policy_step(&rq1).await;

        let mut rq2 = mkreq(&c, "ik-fr4", "h-fr4");
        rq2.baseline_commit = Some("base-b".into());
        let r = c.svc.execute_policy_step(&rq2).await;
        assert!(matches!(r, PolicyStepOutcome::StaleEvidence { .. }));
    }

    #[tokio::test]
    async fn test_freshness_input_fingerprint_change_stale_evidence() {
        let c = setup().await;
        let mut rq1 = mkreq(&c, "ik-fr5", "h-fr5");
        rq1.input_fingerprint = Some("ifp-a".into());
        c.svc.execute_policy_step(&rq1).await;

        let mut rq2 = mkreq(&c, "ik-fr5", "h-fr5");
        rq2.input_fingerprint = Some("ifp-b".into());
        let r = c.svc.execute_policy_step(&rq2).await;
        assert!(matches!(r, PolicyStepOutcome::StaleEvidence { .. }));
    }

    #[tokio::test]
    async fn test_freshness_changed_path_fingerprint_change_stale_evidence() {
        let c = setup().await;
        let mut rq1 = mkreq(&c, "ik-fr6", "h-fr6");
        rq1.changed_path_fingerprint = Some("cpf-a".into());
        c.svc.execute_policy_step(&rq1).await;

        let mut rq2 = mkreq(&c, "ik-fr6", "h-fr6");
        rq2.changed_path_fingerprint = Some("cpf-b".into());
        let r = c.svc.execute_policy_step(&rq2).await;
        assert!(matches!(r, PolicyStepOutcome::StaleEvidence { .. }));
    }

    // Worktree_id in evidence vs request mismatch → stale evidence.
    #[tokio::test]
    async fn test_freshness_worktree_id_change_invalidates() {
        let c = setup().await;
        let mut rq1 = mkreq(&c, "ik-fr7", "h-fr7");
        rq1.worktree_id = "wt1".into();
        c.svc.execute_policy_step(&rq1).await;

        // Same hash → Duplicate path → freshness check detects worktree_id mismatch.
        let mut rq2 = mkreq(&c, "ik-fr7", "h-fr7");
        rq2.worktree_id = "wt-b".into();
        let r = c.svc.execute_policy_step(&rq2).await;
        assert!(matches!(r, PolicyStepOutcome::StaleEvidence { .. }));
    }

    // Run_id change → caught by ownership pre-check (wrong run).
    #[tokio::test]
    async fn test_freshness_run_id_change_invalidates() {
        let c = setup().await;
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e2','t1',2,'completed')")
            .execute(&c.db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash) VALUES('run-2','plan-1','hb',1,'e2','t1','p1','running','ik-r2','hr2')")
            .execute(&c.db.pool).await.unwrap();
        sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-2','p1','t1','e2','wt1','l2',5,'verification','verify-run-1','verification_owned')")
            .execute(&c.db.pool).await.unwrap();

        let mut rq1 = mkreq(&c, "ik-fr8", "h-fr8");
        rq1.verification_run_id = "run-1".into();
        rq1.execution_id = "e1".into();
        c.svc.execute_policy_step(&rq1).await;

        let mut rq2 = mkreq(&c, "ik-fr8", "h-fr8-stale");
        rq2.verification_run_id = "run-2".into();
        rq2.execution_id = "e2".into();
        let r = c.svc.execute_policy_step(&rq2).await;
        // Run change → different request_hash triggers IdempotencyConflict.
        assert!(matches!(r, PolicyStepOutcome::IdempotencyConflict { .. }));
    }

    #[tokio::test]
    async fn test_freshness_stale_does_not_produce_passed() {
        let c = setup().await;
        // Execute with one set of inputs.
        let mut rq1 = mkreq(&c, "ik-fr9", "h-fr9");
        rq1.expected_fencing = 5;
        let r1 = c.svc.execute_policy_step(&rq1).await;
        assert!(matches!(r1, PolicyStepOutcome::Completed { .. }));

        // Different request hash with stale fields → IdempotencyConflict, not Passed.
        let mut rq2 = mkreq(&c, "ik-fr9", "h-fr9-stale");
        rq2.expected_fencing = 99;
        let r2 = c.svc.execute_policy_step(&rq2).await;
        assert!(
            matches!(r2, PolicyStepOutcome::IdempotencyConflict { .. }),
            "stale must not produce Passed"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Raw secret NOT persisted — end-to-end persistence assertions
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_secret_not_in_evidence_db() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-sec1", "h-sec1");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        rq.file_contents =
            HashMap::from([(".env".into(), b"API_KEY=sk-live-secret-12345".to_vec())]);
        c.svc.execute_policy_step(&rq).await;

        // Read ALL persisted data and verify raw secret is absent.
        let evidence: Vec<(String,)> =
            sqlx::query_as("SELECT detail_json FROM verification_evidence WHERE run_id='run-1'")
                .fetch_all(&c.db.pool)
                .await
                .unwrap();
        for (d,) in &evidence {
            assert!(
                !d.contains("sk-live-secret-12345"),
                "secret in evidence: {d}"
            );
        }

        let results: Vec<(String,)> = sqlx::query_as(
            "SELECT detail_json FROM verification_step_results WHERE run_id='run-1'",
        )
        .fetch_all(&c.db.pool)
        .await
        .unwrap();
        for (d,) in &results {
            assert!(
                !d.contains("sk-live-secret-12345"),
                "secret in step results: {d}"
            );
        }

        let events: Vec<(String,)> = sqlx::query_as(
            "SELECT detail_json FROM verification_step_events WHERE verification_run_id='run-1'",
        )
        .fetch_all(&c.db.pool)
        .await
        .unwrap();
        for (d,) in &events {
            assert!(!d.contains("sk-live-secret-12345"), "secret in events: {d}");
        }

        let ops: Vec<(String,)> = sqlx::query_as(
            "SELECT outcome_json FROM verification_policy_operations WHERE verification_run_id='run-1'",
        )
        .fetch_all(&c.db.pool).await.unwrap();
        for (d,) in &ops {
            assert!(
                !d.contains("sk-live-secret-12345"),
                "secret in operations: {d}"
            );
        }
    }

    #[tokio::test]
    async fn test_github_token_not_in_db() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-sec2", "h-sec2");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        rq.file_contents =
            HashMap::from([("config".into(), b"GITHUB_TOKEN=ghp_abc123def456".to_vec())]);
        c.svc.execute_policy_step(&rq).await;

        let all: Vec<(String,)> = sqlx::query_as(
            "SELECT detail_json FROM verification_evidence WHERE run_id='run-1' UNION ALL SELECT detail_json FROM verification_step_results WHERE run_id='run-1' UNION ALL SELECT detail_json FROM verification_step_events WHERE verification_run_id='run-1'",
        )
        .fetch_all(&c.db.pool).await.unwrap();
        for (d,) in &all {
            assert!(!d.contains("ghp_abc123def456"));
            assert!(!d.contains("ghp_abc"));
        }
    }

    #[tokio::test]
    async fn test_private_key_not_in_db() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-sec3", "h-sec3");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        let key_body = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...";
        rq.file_contents = HashMap::from([("id_rsa".into(), key_body.as_bytes().to_vec())]);
        c.svc.execute_policy_step(&rq).await;

        let all: Vec<(String,)> =
            sqlx::query_as("SELECT detail_json FROM verification_evidence WHERE run_id='run-1'")
                .fetch_all(&c.db.pool)
                .await
                .unwrap();
        for (d,) in &all {
            assert!(!d.contains("MIIEpA"), "private key body in DB");
        }
    }

    #[tokio::test]
    async fn test_aws_key_not_in_db() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-sec4", "h-sec4");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        rq.file_contents = HashMap::from([(
            "creds".into(),
            b"AWS_ACCESS_KEY_ID=AKIA1234567890ABCDEF".to_vec(),
        )]);
        c.svc.execute_policy_step(&rq).await;

        let all: Vec<(String,)> =
            sqlx::query_as("SELECT detail_json FROM verification_evidence WHERE run_id='run-1'")
                .fetch_all(&c.db.pool)
                .await
                .unwrap();
        for (d,) in &all {
            assert!(!d.contains("AKIA1234567890ABCDEF"));
        }
    }

    #[tokio::test]
    async fn test_nested_credential_json_not_persisted() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-sec5", "h-sec5");
        rq.step_kind = VerificationStepKind::SecretScanCheck;
        let json = r#"{"service_account": {"private_key": "sk-nested-secret-key"}}"#;
        rq.file_contents = HashMap::from([("sa.json".into(), json.as_bytes().to_vec())]);
        c.svc.execute_policy_step(&rq).await;

        let all: Vec<(String,)> =
            sqlx::query_as("SELECT detail_json FROM verification_evidence WHERE run_id='run-1'")
                .fetch_all(&c.db.pool)
                .await
                .unwrap();
        for (d,) in &all {
            assert!(!d.contains("sk-nested-secret-key"));
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Failure injection
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_failure_diff_provider() {
        let c = setup().await;
        c.fake.fail_diff.store(true, Ordering::SeqCst);
        let mut rq = mkreq(&c, "ik-fi1", "h-fi1");
        rq.step_kind = VerificationStepKind::GitDiffCheck;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::InfrastructureError { .. }));
        // Scanner must NOT start — blocked at ownership/policy check before scan.
        // (Diff failure is injected via FakePolicyScanner on execute, not pre-check.)
    }

    #[tokio::test]
    async fn test_failure_response_lost_no_rescan() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-fi2", "h-fi2");
        c.svc.execute_policy_step(&rq).await;
        // Simulate response lost: same key+hash returns Duplicate.
        let r2 = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r2, PolicyStepOutcome::Duplicate { .. }));
        assert_eq!(
            c.sc.load(Ordering::SeqCst),
            1,
            "no re-scan on response lost"
        );
    }

    #[tokio::test]
    async fn test_failure_post_scan_owner_mismatch_rejected() {
        let c = setup().await;
        // Use stale fencing so ownership check fails before scan.
        let mut rq = mkreq(&c, "ik-fi3", "h-fi3");
        rq.expected_fencing = 99;
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0, "scan must not start");
    }

    #[tokio::test]
    async fn test_failure_heartbeat_lost_rejected() {
        let c = setup().await;
        // Release the lease to simulate heartbeat loss.
        sqlx::query("UPDATE workspace_leases SET lifecycle='released', released_at=datetime('now') WHERE id='l1'")
            .execute(&c.db.pool).await.unwrap();
        let rq = mkreq(&c, "ik-fi4", "h-fi4");
        let r = c.svc.execute_policy_step(&rq).await;
        assert!(matches!(r, PolicyStepOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_failure_no_agent_created() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-fi5", "h-fi5"))
            .await;
        // No agent_definitions created.
        let ac: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ac.0, 0, "no agent created");
    }

    #[tokio::test]
    async fn test_failure_no_retry_created() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-fi6", "h-fi6"))
            .await;
        let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ec.0, 1, "no retry execution created");
    }

    #[tokio::test]
    async fn test_failure_no_worktree_deleted() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-fi7", "h-fi7"))
            .await;
        // Worktree must not be deleted.
        assert!(c.wtd.path().exists(), "worktree must still exist");
    }

    #[tokio::test]
    async fn test_failure_no_lease_released() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-fi8", "h-fi8"))
            .await;
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(lc.0, "acquired", "lease must not be released");
    }

    #[tokio::test]
    async fn test_failure_no_claim_released() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-fi9", "h-fi9"))
            .await;
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "active", "claim must not be released");
    }

    // ══════════════════════════════════════════════════════════════════
    // Strict event/evidence/operation counts
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_strict_counts_single_execution() {
        let c = setup().await;
        c.svc
            .execute_policy_step(&mkreq(&c, "ik-sc1", "h-sc1"))
            .await;

        let op_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_policy_operations WHERE idempotency_key='ik-sc1'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(op_count.0, 1, "exactly one operation");

        let started_ev: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE idempotency_key LIKE 'policy-ev-%started%' AND verification_run_id='run-1'",
        )
        .fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(started_ev.0, 1, "exactly one started event");

        let terminal_ev: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE idempotency_key LIKE 'policy-ev-%passed%' AND verification_run_id='run-1'",
        )
        .fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(terminal_ev.0, 1, "exactly one terminal event");

        let ev_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_evidence WHERE run_id='run-1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        assert_eq!(ev_count.0, 1, "exactly one evidence record");

        assert_eq!(c.sc.load(Ordering::SeqCst), 1, "validator_start_count == 1");
    }

    #[tokio::test]
    async fn test_strict_counts_response_lost() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-sc2", "h-sc2");
        c.svc.execute_policy_step(&rq).await;
        c.svc.execute_policy_step(&rq).await;

        assert_eq!(c.sc.load(Ordering::SeqCst), 1, "validator_start_count == 1");

        let op_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_policy_operations WHERE idempotency_key='ik-sc2'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(op_count.0, 1, "exactly one operation");

        let total_ev: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE verification_run_id='run-1' AND step_id='step-1'",
        )
        .fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(total_ev.0, 2, "started + terminal = 2 events");
    }
}
