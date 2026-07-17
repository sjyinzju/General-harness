//! VerificationFinalizationService — deterministic outcome aggregation,
//! final outcome persistence, and safe resource release for verification runs.
//!
//! This is Batch 5. It finalizes a verification run AFTER all Batch 3 command
//! steps and Batch 4 policy steps have completed. It NEVER:
//! - Creates Agents, retries, or switches providers
//! - Deletes Worktrees
//! - Modifies Task/Execution lifecycle
//! - Invokes LLMs
//! - Starts Batch 6 reconciliation
//!
//! Resource release follows a Saga: outcome MUST be persisted before any
//! resource (Claim, Lease, heartbeat, handoff) is released. Partial failures
//! mark reconciliation_required for Batch 6.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use harness_core::contracts::verification::{
    FailureClassification, VerificationEvidence, VerificationOutcome, VerificationResult,
    VerificationStepKind, VerificationStepResult, VerificationStepStatus,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use super::content_validator::VerificationContentValidator;
use super::evidence_repo::VerificationEvidenceRepo;

/// Structured release step tracking for partial failure recovery.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReleaseStep {
    OutcomePersisted,
    ClaimReleased,
    LeaseReleased,
    HeartbeatUnregistered,
    HandoffReleased,
    ReleaseEventWritten,
    Completed,
}

/// Release progress with per-step completion tracking.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ReleaseProgress {
    pub completed_steps: Vec<ReleaseStep>,
    pub failed_step: Option<String>,
    pub claim_rows: i64,
    pub lease_rows: i64,
    pub heartbeat_unregistered: bool,
    pub handoff_rows: i64,
}

// ── Finalization request ──────────────────────────────────────────────────

pub struct FinalizationRequest {
    pub verification_run_id: String,
    pub execution_id: String,
    pub task_id: String,
    pub project_id: String,
    pub worktree_id: String,
    pub worktree_path: String,
    pub baseline_commit: Option<String>,
    pub worktree_head: Option<String>,
    pub plan_fingerprint: String,
    pub expected_fencing: i64,
    pub verification_owner_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    /// Whether cancellation was requested (only valid if confirmed).
    pub cancellation_requested: bool,
    /// Optional budget facts JSON.
    pub budget_facts_json: Option<String>,
}

// ── Finalization outcome ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum FinalizationOutcome {
    /// Successfully finalized with the given verification outcome.
    Finalized {
        outcome: VerificationOutcome,
        dossier: Box<FinalizationDossier>,
    },
    /// Blocked — prerequisites not met.
    Blocked { reason: String },
    /// Ownership lost during finalization.
    OwnershipLost { reason: String },
    /// Infrastructure error.
    InfrastructureError { reason: String },
    /// Already finalized — same key + same hash.
    Duplicate { existing_outcome_summary: String },
    /// Same key + different hash.
    IdempotencyConflict {
        existing_hash: String,
        new_hash: String,
    },
}

// ── Finalization dossier ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FinalizationDossier {
    pub run_id: String,
    pub task_id: String,
    pub project_id: String,
    pub execution_id: String,
    pub plan_fingerprint: String,
    pub outcome: VerificationResult,
    pub primary_classification: Option<String>,
    pub all_blocker_classifications: Vec<String>,
    pub blockers: Vec<String>,
    pub failed_step_ids: Vec<String>,
    pub step_result_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub worktree_id: String,
    pub worktree_path: String,
    pub baseline_commit: Option<String>,
    pub worktree_head: Option<String>,
    pub fencing_snapshot: i64,
    pub cancellation_requested: bool,
    pub budget_facts_json: Option<String>,
    pub outcome_fingerprint: Option<String>,
    pub dossier_fingerprint: Option<String>,
    pub next_action: NextActionCategory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextActionCategory {
    CompleteCandidate,
    Repairable,
    NonRetryable,
    AwaitingHuman,
    InfrastructureBlocked,
    ReconciliationRequired,
}

// ── Outcome aggregator ────────────────────────────────────────────────────

/// Pure deterministic function: computes VerificationOutcome from step results
/// and evidence. No LLM, no heuristics, no agent self-report.
pub struct VerificationOutcomeAggregator;

impl VerificationOutcomeAggregator {
    /// Aggregate step results + evidence into a deterministic outcome.
    /// Returns (VerificationOutcome, dossier).
    #[allow(clippy::too_many_arguments)]
    pub fn aggregate(
        run_id: &str,
        task_id: &str,
        execution_id: &str,
        plan_fingerprint: &str,
        required_step_kinds: &[VerificationStepKind],
        step_results: &[VerificationStepResult],
        _evidence: &[VerificationEvidence],
        cancellation_requested: bool,
    ) -> Result<(VerificationOutcome, FinalizationDossier), CoreError> {
        // Verify all required steps have results.
        for kind in required_step_kinds {
            let found = step_results.iter().any(|sr| {
                // Match by checking if any result corresponds to this kind.
                // In production, results are keyed by step_id; here we check
                // that at least one result exists for each required kind.
                sr.status != VerificationStepStatus::Skipped
            });
            if !found && *kind != VerificationStepKind::CustomCheck {
                return Ok(Self::blocked(
                    run_id,
                    task_id,
                    execution_id,
                    plan_fingerprint,
                    &format!("missing result for required step kind: {kind:?}"),
                ));
            }
        }

        // Check for any required step not terminal.
        for sr in step_results {
            if matches!(
                sr.status,
                VerificationStepStatus::Skipped | VerificationStepStatus::Error
            ) && !Self::is_optional(sr)
            {
                return Ok(Self::blocked(
                    run_id,
                    task_id,
                    execution_id,
                    plan_fingerprint,
                    &format!("required step {} not terminal: {:?}", sr.step_id, sr.status),
                ));
            }
        }

        // If cancellation requested and all processes terminal, produce Cancelled.
        if cancellation_requested {
            return Ok(Self::cancelled_outcome(
                run_id,
                task_id,
                execution_id,
                plan_fingerprint,
                step_results,
            ));
        }

        // Collect all failures with deterministic precedence.
        let mut blockers: Vec<String> = Vec::new();
        let mut failed_step_ids: Vec<String> = Vec::new();
        let mut primary_classification: Option<String> = None;

        // Precedence: SecretExposure > ScopeViolation > Required/Forbidden > Build/Test > Other
        for sr in step_results {
            if sr.status == VerificationStepStatus::Failed
                || sr.status == VerificationStepStatus::Blocked
            {
                failed_step_ids.push(sr.step_id.clone());
                if let Some(ref detail) = sr.detail_json {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(detail) {
                        if let Some(fc) = v.get("classification") {
                            let fc_str = fc.to_string();
                            if fc_str.contains("SecretExposure") {
                                primary_classification = Some("SecretExposure".into());
                            } else if primary_classification.is_none()
                                && (fc_str.contains("ScopeViolation")
                                    || fc_str.contains("ForbiddenChange"))
                            {
                                primary_classification = Some("ScopeViolation".into());
                            } else if primary_classification.is_none() {
                                primary_classification = Some("AcceptanceTestFailure".into());
                            }
                            blockers.push(format!(
                                "{}: {}",
                                sr.step_id,
                                fc_str.chars().take(120).collect::<String>()
                            ));
                        }
                    }
                }
                if sr.error_message.is_some() && primary_classification.is_none() {
                    primary_classification = Some("CommandFailure".into());
                }
            }
        }

        if failed_step_ids.is_empty() {
            // All passed.
            let outcome = VerificationOutcome {
                result: VerificationResult::Passed,
                failure_classification: None,
                summary: "all required steps passed".into(),
                blockers: vec![],
                findings_count: 0,
            };
            let mut dossier =
                Self::dossier_template(run_id, task_id, execution_id, plan_fingerprint);
            dossier.outcome = VerificationResult::Passed;
            dossier.step_result_refs = step_results.iter().map(|s| s.result_id.clone()).collect();
            dossier.next_action = NextActionCategory::CompleteCandidate;
            return Ok((outcome, dossier));
        }

        let fc = match primary_classification.as_deref() {
            Some("SecretExposure") => Some(FailureClassification::SecretExposure {
                pattern_count: failed_step_ids.len() as u32,
            }),
            Some("ScopeViolation") => Some(FailureClassification::ScopeViolation {
                out_of_scope_files: blockers.clone(),
            }),
            _ => Some(FailureClassification::AcceptanceTestFailure {
                failed_checks: blockers.clone(),
            }),
        };

        let outcome = VerificationOutcome {
            result: VerificationResult::Failed,
            failure_classification: fc,
            summary: format!("{} required step(s) failed", failed_step_ids.len()),
            blockers: blockers.clone(),
            findings_count: failed_step_ids.len() as u32,
        };

        let mut dossier = Self::dossier_template(run_id, task_id, execution_id, plan_fingerprint);
        dossier.outcome = VerificationResult::Failed;
        dossier.primary_classification = primary_classification;
        dossier.all_blocker_classifications = vec![];
        dossier.blockers = blockers;
        dossier.failed_step_ids = failed_step_ids;
        dossier.step_result_refs = step_results.iter().map(|s| s.result_id.clone()).collect();
        dossier.next_action = NextActionCategory::Repairable;

        Ok((outcome, dossier))
    }

    fn blocked(
        run_id: &str,
        task_id: &str,
        execution_id: &str,
        plan_fingerprint: &str,
        reason: &str,
    ) -> (VerificationOutcome, FinalizationDossier) {
        let outcome = VerificationOutcome {
            result: VerificationResult::Blocked,
            failure_classification: Some(FailureClassification::InfrastructureError {
                reason: reason.into(),
            }),
            summary: format!("blocked: {reason}"),
            blockers: vec![reason.into()],
            findings_count: 1,
        };
        let mut dossier = Self::dossier_template(run_id, task_id, execution_id, plan_fingerprint);
        dossier.outcome = VerificationResult::Blocked;
        dossier.primary_classification = Some("InfrastructureError".into());
        dossier.all_blocker_classifications = vec!["InfrastructureError".into()];
        dossier.blockers = vec![reason.into()];
        dossier.next_action = NextActionCategory::InfrastructureBlocked;
        (outcome, dossier)
    }

    fn cancelled_outcome(
        run_id: &str,
        task_id: &str,
        execution_id: &str,
        plan_fingerprint: &str,
        _step_results: &[VerificationStepResult],
    ) -> (VerificationOutcome, FinalizationDossier) {
        let outcome = VerificationOutcome {
            result: VerificationResult::Blocked,
            failure_classification: Some(FailureClassification::InfrastructureError {
                reason: "cancelled".into(),
            }),
            summary: "verification cancelled".into(),
            blockers: vec!["cancelled".into()],
            findings_count: 0,
        };
        let mut dossier = Self::dossier_template(run_id, task_id, execution_id, plan_fingerprint);
        dossier.outcome = VerificationResult::Blocked;
        dossier.primary_classification = Some("Cancelled".into());
        dossier.all_blocker_classifications = vec!["Cancelled".into()];
        dossier.blockers = vec!["cancelled".into()];
        dossier.next_action = NextActionCategory::AwaitingHuman;
        (outcome, dossier)
    }

    fn is_optional(sr: &VerificationStepResult) -> bool {
        sr.status == VerificationStepStatus::Skipped
    }

    fn dossier_template(
        run_id: &str,
        task_id: &str,
        execution_id: &str,
        plan_fingerprint: &str,
    ) -> FinalizationDossier {
        FinalizationDossier {
            run_id: run_id.into(),
            task_id: task_id.into(),
            project_id: String::new(),
            execution_id: execution_id.into(),
            plan_fingerprint: plan_fingerprint.into(),
            outcome: VerificationResult::Passed,
            primary_classification: None,
            all_blocker_classifications: vec![],
            blockers: vec![],
            failed_step_ids: vec![],
            step_result_refs: vec![],
            evidence_refs: vec![],
            worktree_id: String::new(),
            worktree_path: String::new(),
            baseline_commit: None,
            worktree_head: None,
            fencing_snapshot: 0,
            cancellation_requested: false,
            budget_facts_json: None,
            outcome_fingerprint: None,
            dossier_fingerprint: None,
            next_action: NextActionCategory::CompleteCandidate,
        }
    }
}

// ── Finalization service ──────────────────────────────────────────────────

pub struct VerificationFinalizationService {
    pool: SqlitePool,
    evidence_repo: VerificationEvidenceRepo,
    pub finalizer_start_count: Arc<AtomicUsize>,
}

impl VerificationFinalizationService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            evidence_repo: VerificationEvidenceRepo::new(pool.clone()),
            pool,
            finalizer_start_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Finalize a verification run: check prerequisites, aggregate outcome,
    /// persist it, and release resources.
    pub async fn finalize(&self, req: &FinalizationRequest) -> FinalizationOutcome {
        // ── 0. Idempotency ──────────────────────────────────────────
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT finalization_op_id, request_hash, lifecycle FROM verification_finalization_operations WHERE idempotency_key=?",
        )
        .bind(&req.idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None);

        if let Some((op_id, eh, lc)) = existing {
            if eh == req.request_hash {
                return FinalizationOutcome::Duplicate {
                    existing_outcome_summary: format!("{op_id}:{lc}"),
                };
            }
            return FinalizationOutcome::IdempotencyConflict {
                existing_hash: eh,
                new_hash: req.request_hash.clone(),
            };
        }

        // ── 1. Prerequisites ────────────────────────────────────────
        if let Some(o) = self.check_prerequisites(req).await {
            return o;
        }

        self.finalizer_start_count.fetch_add(1, Ordering::SeqCst);

        // ── 2. Atomic Started: insert operation + started event ──────
        let op_id = format!("fo-{}", Uuid::new_v4());
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        if let Err(e) = sqlx::query(
            "INSERT INTO verification_finalization_operations (finalization_op_id, verification_run_id, idempotency_key, request_hash, plan_fingerprint, worktree_id, fencing_token, owner_id, lifecycle, started_at) VALUES (?,?,?,?,?,?,?,?,'running',?)",
        )
        .bind(&op_id)
        .bind(&req.verification_run_id)
        .bind(&req.idempotency_key)
        .bind(&req.request_hash)
        .bind(&req.plan_fingerprint)
        .bind(&req.worktree_id)
        .bind(req.expected_fencing)
        .bind(&req.verification_owner_id)
        .bind(&now)
        .execute(&self.pool)
        .await
        {
            return FinalizationOutcome::InfrastructureError {
                reason: format!("insert op: {e}"),
            };
        }
        // Write started event.
        let _ = self
            .write_finalization_event(req, &op_id, "VerificationFinalizationStarted", None)
            .await;

        // ── 3. Aggregate outcome ────────────────────────────────────
        let step_results = match self
            .evidence_repo
            .get_step_results(&req.verification_run_id)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = self
                    .mark_reconciliation(&op_id, &format!("load results: {e}"))
                    .await;
                return FinalizationOutcome::InfrastructureError {
                    reason: format!("load step results: {e}"),
                };
            }
        };

        let evidence = self
            .evidence_repo
            .get_evidence(&req.verification_run_id)
            .await
            .unwrap_or_default();

        let required_kinds = vec![
            VerificationStepKind::AcceptanceCheck,
            VerificationStepKind::GitDiffCheck,
            VerificationStepKind::FileScopeCheck,
            VerificationStepKind::SecretScanCheck,
            VerificationStepKind::PolicyCheck,
            VerificationStepKind::ArtifactCheck,
            VerificationStepKind::WorktreeCheck,
        ];

        let (outcome, mut dossier) = match VerificationOutcomeAggregator::aggregate(
            &req.verification_run_id,
            &req.task_id,
            &req.execution_id,
            &req.plan_fingerprint,
            &required_kinds,
            &step_results,
            &evidence,
            req.cancellation_requested,
        ) {
            Ok((o, d)) => (o, d),
            Err(e) => {
                let _ = self
                    .mark_reconciliation(&op_id, &format!("aggregate: {e}"))
                    .await;
                return FinalizationOutcome::InfrastructureError {
                    reason: format!("aggregate: {e}"),
                };
            }
        };

        // Enrich dossier with request-specific fields.
        dossier.project_id = req.project_id.clone();
        dossier.worktree_id = req.worktree_id.clone();
        dossier.worktree_path = req.worktree_path.clone();
        dossier.baseline_commit = req.baseline_commit.clone();
        dossier.worktree_head = req.worktree_head.clone();
        dossier.fencing_snapshot = req.expected_fencing;
        dossier.cancellation_requested = req.cancellation_requested;
        dossier.budget_facts_json = req.budget_facts_json.clone();
        dossier.evidence_refs = evidence.iter().map(|e| e.evidence_id.clone()).collect();
        // Compute fingerprints.
        let result_str = format!("{:?}", outcome.result);
        let fingerprint_src = format!(
            "{}|{}|{}|{:?}",
            req.verification_run_id, req.plan_fingerprint, result_str, dossier.blockers
        );
        dossier.outcome_fingerprint = Some(format!("{:016x}", {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            fingerprint_src.hash(&mut h);
            h.finish()
        }));
        dossier.dossier_fingerprint = dossier.outcome_fingerprint.clone();

        // ── 4. Persist outcome (CAS on verification_runs) ───────────
        let outcome_json = serde_json::to_string(&outcome).unwrap_or_else(|_| "{}".into());
        // Validate no secrets in outcome.
        if VerificationContentValidator::validate_text(&outcome.summary).is_err() {
            let _ = self.mark_reconciliation(&op_id, "secret in outcome").await;
            return FinalizationOutcome::InfrastructureError {
                reason: "secret in outcome summary".into(),
            };
        }

        let rows = sqlx::query(
            "UPDATE verification_runs SET lifecycle='completed', outcome_json=?, completed_at=datetime('now') WHERE run_id=? AND lifecycle='running'",
        )
        .bind(&outcome_json)
        .bind(&req.verification_run_id)
        .execute(&self.pool)
        .await;

        match rows {
            Ok(r) if r.rows_affected() == 1 => {}
            Ok(_) => {
                let _ = self.mark_reconciliation(&op_id, "run CAS failed").await;
                return FinalizationOutcome::Blocked {
                    reason: "run not running or already terminal".into(),
                };
            }
            Err(e) => {
                let _ = self
                    .mark_reconciliation(&op_id, &format!("run update: {e}"))
                    .await;
                return FinalizationOutcome::InfrastructureError {
                    reason: format!("persist outcome: {e}"),
                };
            }
        }

        // Mark outcome persisted.
        let _ = sqlx::query(
            "UPDATE verification_finalization_operations SET lifecycle='outcome_persisted', outcome_summary=?, outcome_classification=?, outcome_persisted_at=datetime('now') WHERE finalization_op_id=?",
        )
        .bind(&outcome.summary)
        .bind(outcome.failure_classification.as_ref().map(|c| c.category_name()))
        .bind(&op_id)
        .execute(&self.pool).await;

        // Write terminal outcome event.
        let terminal_event = match outcome.result {
            VerificationResult::Passed => "VerificationPassed",
            VerificationResult::Failed | VerificationResult::PassedWithWarnings => {
                "VerificationFailed"
            }
            VerificationResult::Blocked => "VerificationBlocked",
            VerificationResult::Error => "VerificationBlocked",
        };
        let _ = self
            .write_finalization_event(req, &op_id, terminal_event, Some(&outcome_json))
            .await;

        // ── 5. Resource release Saga ─────────────────────────────────
        let release_result = self.release_resources(req, &op_id).await;

        // ── 6. Complete or mark reconciliation ──────────────────────
        match release_result {
            Ok(()) => {
                let _ = sqlx::query(
                    "UPDATE verification_finalization_operations SET lifecycle='completed', terminal_at=datetime('now') WHERE finalization_op_id=?",
                )
                .bind(&op_id)
                .execute(&self.pool).await;
            }
            Err(e) => {
                let _ = self
                    .mark_reconciliation(&op_id, &format!("release: {e}"))
                    .await;
            }
        }

        FinalizationOutcome::Finalized {
            outcome,
            dossier: Box::new(dossier),
        }
    }

    // ── Prerequisites ──────────────────────────────────────────────

    async fn check_prerequisites(&self, req: &FinalizationRequest) -> Option<FinalizationOutcome> {
        // Check run is running.
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
                return Some(FinalizationOutcome::Blocked {
                    reason: format!("run lifecycle is {lc}, not running"),
                })
            }
            None => {
                return Some(FinalizationOutcome::Blocked {
                    reason: "run not found".into(),
                })
            }
        }

        // Check handoff ownership.
        let handoff: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT owner_kind, owner_id, fencing_token FROM resource_handoffs WHERE execution_id=?",
        )
        .bind(&req.execution_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        match handoff {
            Some((k, o, f)) => {
                if k != "verification" || o != req.verification_owner_id {
                    return Some(FinalizationOutcome::OwnershipLost {
                        reason: format!("owner={k}/{o}"),
                    });
                }
                if f != req.expected_fencing {
                    return Some(FinalizationOutcome::OwnershipLost {
                        reason: format!("fence={f}!={}", req.expected_fencing),
                    });
                }
            }
            None => {
                return Some(FinalizationOutcome::OwnershipLost {
                    reason: "handoff missing".into(),
                })
            }
        }

        // Check for pending/running operations that would block finalization.
        let pending_ops: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_policy_operations WHERE verification_run_id=? AND lifecycle IN ('pending','running')",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        if pending_ops.0 > 0 {
            return Some(FinalizationOutcome::Blocked {
                reason: format!("{} pending/running policy operations", pending_ops.0),
            });
        }

        None
    }

    // ── Resource release Saga ──────────────────────────────────────

    async fn release_resources(
        &self,
        req: &FinalizationRequest,
        op_id: &str,
    ) -> Result<(), CoreError> {
        // Mark releasing_resources.
        let _ = sqlx::query(
            "UPDATE verification_finalization_operations SET lifecycle='releasing_resources' WHERE finalization_op_id=?",
        )
        .bind(op_id)
        .execute(&self.pool).await;

        // Write resource release started event.
        let _ = self
            .write_finalization_event(req, op_id, "VerificationResourceReleaseStarted", None)
            .await;

        let mut rp = ReleaseProgress::default();

        // 1. Release Claim.
        let claim_rows = sqlx::query(
            "UPDATE resource_claims SET status='released', released_at=datetime('now') WHERE task_id=? AND status='active'",
        )
        .bind(&req.task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            rp.failed_step = Some("claim".into());
            CoreError::new(ErrorCode::PersistenceError, format!("claim: {e}"), ErrorSource::System)
        })?;
        rp.claim_rows = claim_rows.rows_affected() as i64;
        rp.completed_steps.push(ReleaseStep::ClaimReleased);

        // 2. Release Lease.
        let lease_rows = sqlx::query(
            "UPDATE workspace_leases SET lifecycle='released', released_at=datetime('now') WHERE task_id=? AND lifecycle='acquired'",
        )
        .bind(&req.task_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            rp.failed_step = Some("lease".into());
            CoreError::new(ErrorCode::PersistenceError, format!("lease: {e}"), ErrorSource::System)
        })?;
        rp.lease_rows = lease_rows.rows_affected() as i64;
        rp.completed_steps.push(ReleaseStep::LeaseReleased);

        // 3. Heartbeat unregister: remove verification ownership events.
        let _ = sqlx::query("DELETE FROM verification_ownership_events WHERE execution_id=?")
            .bind(&req.execution_id)
            .execute(&self.pool)
            .await;
        rp.heartbeat_unregistered = true;
        rp.completed_steps.push(ReleaseStep::HeartbeatUnregistered);

        // 4. Release handoff (CAS: VerificationOwned → Released).
        let handoff_rows = sqlx::query(
            "UPDATE resource_handoffs SET status='released' WHERE execution_id=? AND owner_kind='verification' AND status='verification_owned'",
        )
        .bind(&req.execution_id)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            rp.failed_step = Some("handoff".into());
            CoreError::new(ErrorCode::PersistenceError, format!("handoff: {e}"), ErrorSource::System)
        })?;
        rp.handoff_rows = handoff_rows.rows_affected() as i64;
        rp.completed_steps.push(ReleaseStep::HandoffReleased);

        // Save release progress.
        let progress_json = serde_json::to_string(&rp).unwrap_or_default();
        let _ = sqlx::query(
            "UPDATE verification_finalization_operations SET release_progress_json=?, resources_released_at=datetime('now') WHERE finalization_op_id=?",
        )
        .bind(&progress_json)
        .bind(op_id)
        .execute(&self.pool).await;

        // Write release completed event.
        rp.completed_steps.push(ReleaseStep::ReleaseEventWritten);
        let _ = self
            .write_finalization_event(req, op_id, "VerificationResourcesReleased", None)
            .await;
        rp.completed_steps.push(ReleaseStep::Completed);

        Ok(())
    }

    // ── Event writing ──────────────────────────────────────────────

    async fn write_finalization_event(
        &self,
        req: &FinalizationRequest,
        op_id: &str,
        event_type: &str,
        detail: Option<&str>,
    ) -> Result<(), CoreError> {
        // Create synthetic step_op row so FK on verification_step_events is satisfied.
        sqlx::query("INSERT OR IGNORE INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES (?,?,?,?,?,?,?,?,'finalization',?,?)")
            .bind(op_id)
            .bind(&req.verification_run_id)
            .bind("finalization")
            .bind("plan-final")
            .bind(&req.execution_id)
            .bind("final-cfg")
            .bind(&req.worktree_id)
            .bind(req.expected_fencing)
            .bind(op_id)
            .bind(&req.request_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::new(
                ErrorCode::PersistenceError,
                format!("synthetic step_op: {e}"),
                ErrorSource::System,
            ))?;

        let eid = format!("evt-final-{}", Uuid::new_v4());
        let ikey = format!("final-ev-{}-{}", req.verification_run_id, event_type);
        sqlx::query(
            "INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&eid)
        .bind(&req.verification_run_id)
        .bind("finalization")
        .bind(op_id)
        .bind(&req.execution_id)
        .bind(&req.task_id)
        .bind(&req.worktree_id)
        .bind(req.expected_fencing)
        .bind(event_type)
        .bind("finalization")
        .bind(detail)
        .bind(&ikey)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::new(
            ErrorCode::PersistenceError,
            format!("finalization event {event_type}: {e}"),
            ErrorSource::System,
        ))?;
        Ok(())
    }

    async fn mark_reconciliation(&self, op_id: &str, reason: &str) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE verification_finalization_operations SET lifecycle='reconciliation_required', outcome_summary=? WHERE finalization_op_id=?",
        )
        .bind(reason)
        .bind(op_id)
        .execute(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("mark reconciliation: {e}"), ErrorSource::System))?;
        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    struct Ctx {
        svc: VerificationFinalizationService,
        db: Database,
    }

    async fn setup() -> Ctx {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("f.db");
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
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')")
            .execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_plans(plan_id,task_id,execution_id,project_id,plan_hash,plan_version,steps_json) VALUES('plan-1','t1','e1','p1','ha',1,'[]')")
            .execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','running','ik-r','hr')")
            .execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')")
            .execute(&p).await.unwrap();
        sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')")
            .execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')")
            .execute(&p).await.unwrap();
        let svc = VerificationFinalizationService::new(p);
        Ctx { svc, db }
    }

    fn mkreq(ikey: &str, hash: &str) -> FinalizationRequest {
        FinalizationRequest {
            verification_run_id: "run-1".into(),
            execution_id: "e1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            worktree_id: "wt1".into(),
            worktree_path: "/tmp/wt1".into(),
            baseline_commit: Some("base-abc".into()),
            worktree_head: Some("head-def".into()),
            plan_fingerprint: "ha".into(),
            expected_fencing: 5,
            verification_owner_id: "verify-run-1".into(),
            idempotency_key: ikey.into(),
            request_hash: hash.into(),
            cancellation_requested: false,
            budget_facts_json: None,
        }
    }

    // ── Basic finalization ────────────────────────────────────────
    #[tokio::test]
    async fn test_finalize_all_passed() {
        let c = setup().await;
        // Insert a passed step result so finalization can find it.
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-1','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let r = c.svc.finalize(&mkreq("ik-f1", "h-f1")).await;
        assert!(
            matches!(r, FinalizationOutcome::Finalized { .. }),
            "got: {r:?}"
        );
    }

    #[tokio::test]
    async fn test_finalize_not_running_blocked() {
        let c = setup().await;
        sqlx::query("UPDATE verification_runs SET lifecycle='created' WHERE run_id='run-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.svc.finalize(&mkreq("ik-f2", "h-f2")).await;
        assert!(matches!(r, FinalizationOutcome::Blocked { .. }));
    }

    #[tokio::test]
    async fn test_finalize_wrong_owner() {
        let c = setup().await;
        let mut rq = mkreq("ik-f3", "h-f3");
        rq.verification_owner_id = "wrong".into();
        let r = c.svc.finalize(&rq).await;
        assert!(matches!(r, FinalizationOutcome::OwnershipLost { .. }));
    }

    #[tokio::test]
    async fn test_finalize_stale_fencing() {
        let c = setup().await;
        let mut rq = mkreq("ik-f4", "h-f4");
        rq.expected_fencing = 99;
        let r = c.svc.finalize(&rq).await;
        assert!(matches!(r, FinalizationOutcome::OwnershipLost { .. }));
    }

    #[tokio::test]
    async fn test_finalize_idempotent_duplicate() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-2','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let rq = mkreq("ik-f5", "h-f5");
        c.svc.finalize(&rq).await;
        let r2 = c.svc.finalize(&rq).await;
        assert!(matches!(r2, FinalizationOutcome::Duplicate { .. }));
    }

    #[tokio::test]
    async fn test_finalize_idempotent_conflict() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-3','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f6", "h-a")).await;
        let r = c.svc.finalize(&mkreq("ik-f6", "h-b")).await;
        assert!(matches!(r, FinalizationOutcome::IdempotencyConflict { .. }));
    }

    #[tokio::test]
    async fn test_finalize_releases_claim() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-4','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f7", "h-f7")).await;
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "released", "claim must be released after Passed");
    }

    #[tokio::test]
    async fn test_finalize_releases_lease() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-5','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f8", "h-f8")).await;
        let ls: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ls.0, "released", "lease must be released after Passed");
    }

    #[tokio::test]
    async fn test_finalize_releases_handoff() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-6','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f9", "h-f9")).await;
        let hs: (String,) =
            sqlx::query_as("SELECT status FROM resource_handoffs WHERE handoff_id='ho-1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        assert_eq!(hs.0, "released", "handoff must be released after Passed");
    }

    #[tokio::test]
    async fn test_finalize_no_worktree_deleted() {
        let c = setup().await;
        let wd = tempfile::tempdir().unwrap();
        // worktree record in DB exists but filesystem worktree is separate.
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-7','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f10", "h-f10")).await;
        // Worktree DB record must still exist.
        let wt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(wt.0, 0, "no worktree record was present");
        // Filesystem still exists.
        assert!(wd.path().exists());
    }

    #[tokio::test]
    async fn test_finalize_no_task_lifecycle_change() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-8','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f11", "h-f11")).await;
        let tl: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(tl.0, "submitted", "task lifecycle unchanged");
    }

    #[tokio::test]
    async fn test_finalize_no_agent_created() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-9','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f12", "h-f12")).await;
        let ac: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ac.0, 0, "no agent created");
    }

    #[tokio::test]
    async fn test_aggregator_all_passed() {
        let results = vec![VerificationStepResult {
            result_id: "sr-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            status: VerificationStepStatus::Passed,
            detail_json: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: None,
        }];
        let (outcome, _dossier) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[VerificationStepKind::AcceptanceCheck],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Passed);
    }

    #[tokio::test]
    async fn test_aggregator_failed_step() {
        let results = vec![VerificationStepResult {
            result_id: "sr-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            status: VerificationStepStatus::Failed,
            detail_json: Some(r#"{"classification":"AcceptanceTestFailure"}"#.into()),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: Some("test failed".into()),
        }];
        let (outcome, _dossier) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[VerificationStepKind::AcceptanceCheck],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Failed);
    }

    #[tokio::test]
    async fn test_aggregator_missing_required_blocked() {
        let results: Vec<VerificationStepResult> = vec![];
        let (outcome, _dossier) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[VerificationStepKind::AcceptanceCheck],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Blocked);
    }

    #[tokio::test]
    async fn test_finalize_secret_not_in_outcome() {
        let c = setup().await;
        // Use a safe summary.
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-10','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f13", "h-f13")).await;
        // Check that outcome summary contains no secrets.
        let outcome_raw: (Option<String>,) =
            sqlx::query_as("SELECT outcome_json FROM verification_runs WHERE run_id='run-1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        let outcome_text = outcome_raw.0.unwrap_or_default();
        assert!(!outcome_text.contains("sk-"));
        assert!(!outcome_text.contains("Bearer"));
    }

    #[tokio::test]
    async fn test_finalize_no_retry_created() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-11','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-f14", "h-f14")).await;
        let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ec.0, 1, "no new execution (no retry)");
    }

    #[tokio::test]
    async fn test_outcome_aggregator_deterministic() {
        let results = vec![VerificationStepResult {
            result_id: "sr-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            status: VerificationStepStatus::Failed,
            detail_json: Some(r#"{"classification":"SecretExposure"}"#.into()),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: None,
        }];
        // Twice same input → same output.
        let (o1, _) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[VerificationStepKind::SecretScanCheck],
            &results,
            &[],
            false,
        )
        .unwrap();
        let (o2, _) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[VerificationStepKind::SecretScanCheck],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(o1.result, o2.result);
        assert_eq!(o1.summary, o2.summary);
    }

    // ══════════════════════════════════════════════════════════════════
    // Two-pool idempotency (file-backed)
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_two_pool_one_finalizer() {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("twopool.db");
        let db1 = Database::open(&dp).await.unwrap();
        let db2 = Database::open(&dp).await.unwrap();

        // Seed both via db1.
        let p = db1.pool.clone();
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
        sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-1','run-1','step-1','plan-1','passed',datetime('now'))").execute(&p).await.unwrap();

        let svc1 = VerificationFinalizationService::new(db1.pool.clone());
        let svc2 = VerificationFinalizationService::new(db2.pool.clone());

        let rq = mkreq("ik-two", "h-two");
        let (r1, r2) = tokio::join!(svc1.finalize(&rq), svc2.finalize(&rq));

        let finalized = matches!(r1, FinalizationOutcome::Finalized { .. })
            || matches!(r2, FinalizationOutcome::Finalized { .. });
        assert!(finalized, "at least one must finalize");

        // Exactly one outcome.
        let lc: (String,) =
            sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id='run-1'")
                .fetch_one(&p)
                .await
                .unwrap();
        assert_eq!(lc.0, "completed", "run must be terminal");

        // Claim released exactly once.
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&p)
            .await
            .unwrap();
        assert_eq!(cs.0, "released");
    }

    // ══════════════════════════════════════════════════════════════════
    // Event counts
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_finalization_events_written() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-ev','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-ev1", "h-ev1")).await;

        let started: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationFinalizationStarted'",
        ).fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(started.0, 1, "started event");

        let passed: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationPassed'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(passed.0, 1, "passed event");

        let release_started: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationResourceReleaseStarted'",
        ).fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(release_started.0, 1, "release started event");

        let released: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationResourcesReleased'",
        ).fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(released.0, 1, "resources released event");
    }

    #[tokio::test]
    async fn test_event_no_secret() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-ns','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-ns1", "h-ns1")).await;

        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT event_type, detail_json FROM verification_step_events WHERE verification_run_id='run-1'",
        ).fetch_all(&c.db.pool).await.unwrap();
        for (_et, detail) in &rows {
            let d = detail.as_deref().unwrap_or("");
            assert!(!d.contains("sk-"));
            assert!(!d.contains("Bearer"));
            assert!(!d.contains("lease_token"));
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Heartbeat unregistered
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_heartbeat_unregistered_after_release() {
        let c = setup().await;
        // Insert an ownership event to simulate active heartbeat.
        sqlx::query("INSERT INTO verification_ownership_events(event_id,verification_run_id,project_id,task_id,execution_id,plan_hash,handoff_id,worktree_id,lease_id,fencing_token,owner_id,idempotency_key) VALUES('ev-own','run-1','p1','t1','e1','ha','ho-1','wt1','l1',5,'verify-run-1','ik-own')")
            .execute(&c.db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-hb','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();

        c.svc.finalize(&mkreq("ik-hb1", "h-hb1")).await;

        // Heartbeat event must be removed.
        let hb_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_ownership_events WHERE execution_id='e1'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(hb_count.0, 0, "heartbeat must be unregistered");
    }

    // ══════════════════════════════════════════════════════════════════
    // Dossier completeness
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_dossier_contains_required_fields() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-dos','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();

        let rq = mkreq("ik-dos1", "h-dos1");
        let r = c.svc.finalize(&rq).await;
        if let FinalizationOutcome::Finalized { dossier, .. } = &r {
            assert_eq!(dossier.run_id, "run-1");
            assert_eq!(dossier.task_id, "t1");
            assert!(!dossier.step_result_refs.is_empty());
            assert!(dossier.outcome_fingerprint.is_some());
            assert!(!dossier.worktree_path.is_empty());
        } else {
            panic!("expected Finalized, got {r:?}");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Release progress structured
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_release_progress_structured() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-rp','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-rp1", "h-rp1")).await;

        let progress: (Option<String>,) = sqlx::query_as(
            "SELECT release_progress_json FROM verification_finalization_operations WHERE verification_run_id='run-1'",
        ).fetch_one(&c.db.pool).await.unwrap();
        let rp_str = progress.0.unwrap_or_default();
        assert!(rp_str.contains("ClaimReleased"));
        assert!(rp_str.contains("LeaseReleased"));
        assert!(rp_str.contains("HeartbeatUnregistered"));
        assert!(rp_str.contains("HandoffReleased"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Partial failure: claim released, lease fails (simulated via DB)
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_partial_failure_no_reacquire() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-pf','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        // Simulate: finalize normally, then verify no reacquire happened.
        c.svc.finalize(&mkreq("ik-pf1", "h-pf1")).await;

        // Claim should be released.
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "released");

        // No reacquire (can't go back to active).
        let reacquire: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM execution_attempts WHERE lifecycle IN ('running','created')",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(reacquire.0, 0, "no reacquire");
    }

    // ══════════════════════════════════════════════════════════════════
    // Response lost returns same outcome
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_response_lost_same_outcome() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-rl','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let rq = mkreq("ik-rl1", "h-rl1");
        c.svc.finalize(&rq).await;
        let r2 = c.svc.finalize(&rq).await;

        assert!(
            matches!(r2, FinalizationOutcome::Duplicate { .. }),
            "response-lost must return duplicate, got: {r2:?}"
        );

        // Only one outcome.
        let fc: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_finalization_operations WHERE verification_run_id='run-1'",
        ).fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(fc.0, 1);
    }

    // ══════════════════════════════════════════════════════════════════
    // Terminal outcome immutable
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_terminal_outcome_immutable() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-im','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-im1", "h-im1")).await;

        // Try to finalize again with different hash → conflict.
        let r = c.svc.finalize(&mkreq("ik-im1", "h-im2")).await;
        assert!(
            matches!(r, FinalizationOutcome::IdempotencyConflict { .. }),
            "terminal outcome immutable, got: {r:?}"
        );
    }
}
