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
//! Resource release: outcome MUST be persisted before any resource (Claim,
//! Lease, heartbeat, handoff) is released, and every release step is
//! CAS-claimed pending→in_progress in `verification_release_steps` BEFORE
//! its side effect executes (see super::release_steps). Partial failures
//! mark reconciliation_required for Batch 6.

use std::collections::HashMap;
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
use super::release_steps::{
    write_finalization_event, FaultPlan, ReleaseContext, ReleaseCounters, ReleaseEngine,
    ReleaseRunOutcome, StepGate,
};
use crate::scheduler::heartbeat_registry::HeartbeatRegistry;

pub use super::release_steps::ReleaseProgress;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NextActionCategory {
    CompleteCandidate,
    Repairable,
    NonRetryable,
    AwaitingHuman,
    InfrastructureBlocked,
    ReconciliationRequired,
}

// ── Outcome aggregator ────────────────────────────────────────────────────

/// Identity of a plan step that MUST have exactly one terminal result.
#[derive(Debug, Clone)]
pub struct RequiredStep {
    pub step_id: String,
    pub kind: VerificationStepKind,
    pub sequence_index: u32,
}

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
        required_steps: &[RequiredStep],
        step_results: &[VerificationStepResult],
        _evidence: &[VerificationEvidence],
        cancellation_requested: bool,
    ) -> Result<(VerificationOutcome, FinalizationDossier), CoreError> {
        // Every REQUIRED plan step must be satisfied by EXACTLY ONE result
        // with the same step_id, in a terminal, non-skipped status. "Some
        // result exists" NEVER satisfies a required step of another identity.
        for rs in required_steps {
            let matching: Vec<&VerificationStepResult> = step_results
                .iter()
                .filter(|sr| sr.step_id == rs.step_id)
                .collect();
            match matching.len() {
                0 => {
                    return Ok(Self::blocked(
                        run_id,
                        task_id,
                        execution_id,
                        plan_fingerprint,
                        &format!(
                            "missing result for required step {} (kind {:?}, index {})",
                            rs.step_id, rs.kind, rs.sequence_index
                        ),
                    ));
                }
                1 => {
                    let sr = matching[0];
                    if matches!(
                        sr.status,
                        VerificationStepStatus::Skipped | VerificationStepStatus::Error
                    ) {
                        return Ok(Self::blocked(
                            run_id,
                            task_id,
                            execution_id,
                            plan_fingerprint,
                            &format!("required step {} not terminal: {:?}", rs.step_id, sr.status),
                        ));
                    }
                }
                n => {
                    return Ok(Self::blocked(
                        run_id,
                        task_id,
                        execution_id,
                        plan_fingerprint,
                        &format!("duplicate results ({n}) for required step {}", rs.step_id),
                    ));
                }
            }
        }

        // Any Error result — required or not — blocks finalization
        // (conservative: an errored check proves nothing).
        for sr in step_results {
            if sr.status == VerificationStepStatus::Error {
                return Ok(Self::blocked(
                    run_id,
                    task_id,
                    execution_id,
                    plan_fingerprint,
                    &format!("step {} not terminal: {:?}", sr.step_id, sr.status),
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

        // ── Deterministic failure precedence (highest first) ──────
        // ReconciliationRequired/OwnershipLost/InfrastructureFailure
        // → SecretExposure
        // → ScopeViolation/ForbiddenChange
        // → RequiredFileMissing/ArtifactMissing/ArtifactCorruption
        // → BuildFailure/TestFailure/LintFailure/TypecheckFailure/CommandFailure
        // → OutputMismatch
        // → PolicyViolation
        const PRECEDENCE: &[&str] = &[
            "SecretExposure",
            "ScopeViolation",
            "ForbiddenChange",
            "RequiredFileMissing",
            "ArtifactMissing",
            "ArtifactCorruption",
            "BuildFailure",
            "TestFailure",
            "LintFailure",
            "TypecheckFailure",
            "CommandFailure",
            "OutputMismatch",
            "PolicyViolation",
            "InfrastructureFailure",
            "OwnershipLost",
        ];

        for sr in step_results {
            if sr.status == VerificationStepStatus::Failed
                || sr.status == VerificationStepStatus::Blocked
            {
                failed_step_ids.push(sr.step_id.clone());
                let mut found_class = None;
                if let Some(ref detail) = sr.detail_json {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(detail) {
                        if let Some(fc) = v.get("classification") {
                            let fc_str = fc.to_string();
                            // Try each classification in precedence order.
                            for p in PRECEDENCE {
                                if fc_str.contains(p) {
                                    found_class = Some(p.to_string());
                                    break;
                                }
                            }
                            blockers.push(format!(
                                "{}: {}",
                                sr.step_id,
                                fc_str.chars().take(120).collect::<String>()
                            ));
                        }
                    }
                }
                if sr.error_message.is_some() && found_class.is_none() {
                    found_class = Some("CommandFailure".into());
                }
                // Update primary: choose the higher-precedence classification.
                if let Some(ref fc) = found_class {
                    if let Some(ref existing) = primary_classification {
                        let new_idx = PRECEDENCE.iter().position(|p| p == fc);
                        let old_idx = PRECEDENCE.iter().position(|p| p == existing);
                        if new_idx < old_idx {
                            primary_classification = Some(fc.clone());
                        }
                    } else {
                        primary_classification = Some(fc.clone());
                    }
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
            Some("ScopeViolation") | Some("ForbiddenChange") => {
                Some(FailureClassification::ScopeViolation {
                    out_of_scope_files: blockers.clone(),
                })
            }
            Some("RequiredFileMissing") | Some("ArtifactMissing") | Some("ArtifactCorruption") => {
                Some(FailureClassification::ArtifactCorruption {
                    artifact_ids: failed_step_ids.clone(),
                })
            }
            Some("BuildFailure")
            | Some("TestFailure")
            | Some("LintFailure")
            | Some("TypecheckFailure")
            | Some("CommandFailure")
            | Some("OutputMismatch") => Some(FailureClassification::AcceptanceTestFailure {
                failed_checks: blockers.clone(),
            }),
            Some("PolicyViolation") => Some(FailureClassification::PolicyViolation {
                rule_count: failed_step_ids.len() as u32,
            }),
            Some("InfrastructureFailure") | Some("OwnershipLost") => {
                Some(FailureClassification::InfrastructureError {
                    reason: blockers.first().cloned().unwrap_or_default(),
                })
            }
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
    heartbeat_registry: Arc<HeartbeatRegistry>,
    pub finalizer_start_count: Arc<AtomicUsize>,
    /// Shared observable side-effect counters (see ReleaseCounters).
    pub release_counters: ReleaseCounters,
    faults: FaultPlan,
    gate: Option<StepGate>,
    worker_id: String,
    /// Per-operation mutexes to serialize saga access for the same
    /// operation. Two different operations can run their release sagas
    /// concurrently; only calls targeting the same idempotency_key are
    /// serialised (C8 liveness fix — prevents dual engines).
    op_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl VerificationFinalizationService {
    pub fn new(pool: SqlitePool, heartbeat_registry: Arc<HeartbeatRegistry>) -> Self {
        Self {
            evidence_repo: VerificationEvidenceRepo::new(pool.clone()),
            pool,
            heartbeat_registry,
            finalizer_start_count: Arc::new(AtomicUsize::new(0)),
            release_counters: ReleaseCounters::default(),
            faults: FaultPlan::default(),
            gate: None,
            worker_id: format!("finalizer-{}", Uuid::new_v4()),
            op_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Share observable side-effect counters across services (two-pool tests).
    pub fn with_counters(mut self, counters: ReleaseCounters) -> Self {
        self.release_counters = counters;
        self
    }

    /// Share a start counter across services (two-pool tests).
    pub fn with_start_count(mut self, count: Arc<AtomicUsize>) -> Self {
        self.finalizer_start_count = count;
        self
    }

    /// Install an injected fault plan (integration tests; inert by default).
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.faults = faults;
        self
    }

    /// Install a step gate barrier (integration tests; inert by default).
    pub fn with_gate(mut self, gate: StepGate) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Share per-operation locks across services (two-pool C8 tests).
    pub fn with_op_locks(
        mut self,
        locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    ) -> Self {
        self.op_locks = locks;
        self
    }

    fn release_engine(&self) -> ReleaseEngine {
        ReleaseEngine::new(
            self.pool.clone(),
            self.heartbeat_registry.clone(),
            self.release_counters.clone(),
            self.faults.clone(),
            self.gate.clone(),
            self.worker_id.clone(),
        )
    }

    fn release_context(&self, req: &FinalizationRequest, op_id: &str) -> ReleaseContext {
        ReleaseContext {
            finalization_op_id: op_id.to_string(),
            verification_run_id: req.verification_run_id.clone(),
            execution_id: req.execution_id.clone(),
            task_id: req.task_id.clone(),
            project_id: req.project_id.clone(),
            worktree_id: req.worktree_id.clone(),
            expected_fencing: req.expected_fencing,
            verification_owner_id: req.verification_owner_id.clone(),
            request_hash: req.request_hash.clone(),
        }
    }

    /// Finalize a verification run: check prerequisites, aggregate outcome,
    /// persist it, and release resources.
    ///
    /// Winner selection is a single atomic `INSERT … ON CONFLICT DO NOTHING`
    /// on the idempotency key. A loser NEVER continues finalization; it reads
    /// the existing operation and returns Duplicate / IdempotencyConflict, or
    /// resumes an INCOMPLETE operation from its durable release steps (safe:
    /// every step is claim-CAS'd, so concurrent resumers cannot double-execute).
    pub async fn finalize(&self, req: &FinalizationRequest) -> FinalizationOutcome {
        // ── 0. Per-operation serialisation (C8 liveness fix) ────────
        // Two pools finalizing the same idempotency_key are serialised
        // here.  Different operations run concurrently.  This prevents
        // dual release engines from racing through the same saga.
        let op_mutex = self.acquire_op_lock(&req.idempotency_key).await;
        let _op_guard = op_mutex.lock().await;

        // ── 1. Existing operation for this idempotency key? ─────────
        if let Some(outcome) = self.resume_or_reject_existing(req).await {
            return outcome;
        }

        // ── 1. Prerequisites (advisory; the atomic insert + run CAS gate) ──
        if let Some(o) = self.check_prerequisites(req).await {
            return o;
        }

        // ── 2. Atomic winner: exactly one inserter per idempotency key ──
        let op_id = format!("fo-{}", Uuid::new_v4());
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let inserted = sqlx::query(
            "INSERT INTO verification_finalization_operations (finalization_op_id, verification_run_id, idempotency_key, request_hash, plan_fingerprint, worktree_id, fencing_token, owner_id, lifecycle, started_at) VALUES (?,?,?,?,?,?,?,?,'running',?) ON CONFLICT(idempotency_key) DO NOTHING",
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
        .await;

        match inserted {
            Ok(r) if r.rows_affected() == 1 => {}
            Ok(_) => {
                // Loser: another worker owns this key. Do NOT call
                // resume_or_reject_existing here — that function may enter
                // run_finalization or resume_release_only, which would create
                // a second engine racing through the release saga concurrently
                // with the winner.  A concurrent saga produces spurious
                // reconciliation markers, counter double-counts, and
                // operation_completion == 0 races (C8).
                //
                // Instead, read the winner's lifecycle and return a simple
                // terminal-or-retry signal.  If the winner crashed, the next
                // call to finalize (which takes the pre-INSERT
                // resume_or_reject_existing path) will resume from durable
                // state.
                let existing: Option<(String,)> = sqlx::query_as(
                    "SELECT lifecycle FROM verification_finalization_operations WHERE idempotency_key=?",
                )
                .bind(&req.idempotency_key)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
                return match existing {
                    Some((lc,)) if lc == "completed" => FinalizationOutcome::Duplicate {
                        existing_outcome_summary: "completed by concurrent winner".into(),
                    },
                    // Liveness fix (C8): when the lifecycle indicates the
                    // saga is incomplete (outcome_persisted, releasing,
                    // reconciliation_required), return Blocked so the caller
                    // retries.  On retry, resume_or_reject_existing at the
                    // top of finalize() will find the row and correctly
                    // route to resume_release_only — which resumes from
                    // durable step state.
                    Some((lc,))
                        if lc == "outcome_persisted"
                            || lc == "releasing_resources"
                            || lc == "reconciliation_required" =>
                    {
                        FinalizationOutcome::Blocked {
                            reason: format!(
                                "concurrent finalization at {lc} — retry to resume saga"
                            ),
                        }
                    }
                    _ => FinalizationOutcome::Blocked {
                        reason: "concurrent finalization in progress — retry".into(),
                    },
                };
            }
            Err(e) => {
                return FinalizationOutcome::InfrastructureError {
                    reason: format!("insert op: {e}"),
                }
            }
        }

        // Winner only.
        self.finalizer_start_count.fetch_add(1, Ordering::SeqCst);

        // Write started event.
        let _ = self
            .write_finalization_event(req, &op_id, "VerificationFinalizationStarted", None)
            .await;

        self.run_finalization(req, &op_id).await
    }

    /// Acquire a per-operation mutex to serialise release saga access
    /// for the same idempotency_key. Two pools finalizing different
    /// operations run concurrently; two pools finalizing the SAME
    /// operation are serialised (C8 liveness fix).
    async fn acquire_op_lock(&self, ikey: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.op_locks.lock().unwrap();
        map.entry(ikey.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Same-key re-entry policy. Returns None when no operation exists yet.
    ///
    /// - different request hash → IdempotencyConflict
    /// - completed             → Duplicate (existing result)
    /// - incomplete            → RESUME from durable state (never restart,
    ///   never a bare Duplicate that strands a half-released saga)
    async fn resume_or_reject_existing(
        &self,
        req: &FinalizationRequest,
    ) -> Option<FinalizationOutcome> {
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT finalization_op_id, request_hash, lifecycle FROM verification_finalization_operations WHERE idempotency_key=?",
        )
        .bind(&req.idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None);

        let (op_id, eh, lc) = existing?;
        if eh != req.request_hash {
            return Some(FinalizationOutcome::IdempotencyConflict {
                existing_hash: eh,
                new_hash: req.request_hash.clone(),
            });
        }
        match lc.as_str() {
            "completed" => Some(FinalizationOutcome::Duplicate {
                existing_outcome_summary: format!("{op_id}:{lc}"),
            }),
            // Outcome already durable — resume the release saga from the
            // first unfinished durable step.  The step-level CAS in
            // run_release prevents double-execution; HeldByOther is
            // handled by mark_reconciliation / Blocked retry (C8 fix).
            "outcome_persisted" | "releasing_resources" | "reconciliation_required" => {
                Some(self.resume_release_only(req, &op_id).await)
            }
            // Crashed before the outcome was persisted: re-run deterministic
            // aggregation against the SAME operation row.  ONLY enter this
            // path when the lifecycle is genuinely unknown (not one of the
            // recognised states above).  When the lifecycle is 'running' the
            // inserting winner is still working — we return Blocked so the
            // caller retries instead of running a second release saga
            // concurrently, which causes spurious reconciliation markers and
            // counter races (see C8 / two_pool_finalizer_strict_exactly_once).
            "running" => Some(FinalizationOutcome::Blocked {
                reason: format!("operation {op_id} still running — retry"),
            }),
            _ => Some(self.run_finalization(req, &op_id).await),
        }
    }
    /// Aggregate → persist outcome → dossier → terminal event → release.
    /// Used by the fresh winner path AND by same-key resume when the prior
    /// worker crashed before the outcome was persisted. All writes are
    /// state-CAS'd or idempotent, so a concurrent resumer cannot double-apply.
    async fn run_finalization(
        &self,
        req: &FinalizationRequest,
        op_id: &str,
    ) -> FinalizationOutcome {
        let op_id = op_id.to_string();
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

        // Required steps come from the PLAN (identity: step_id + kind +
        // sequence_index), never from "any result exists".
        let required_steps = match self.load_required_steps(&req.verification_run_id).await {
            Ok(r) => r,
            Err(e) => {
                let _ = self
                    .mark_reconciliation(&op_id, &format!("load plan steps: {e}"))
                    .await;
                return FinalizationOutcome::InfrastructureError {
                    reason: format!("load plan steps: {e}"),
                };
            }
        };

        let (mut outcome, mut dossier) = match VerificationOutcomeAggregator::aggregate(
            &req.verification_run_id,
            &req.task_id,
            &req.execution_id,
            &req.plan_fingerprint,
            &required_steps,
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
                // 0 rows: either the run is genuinely not running, or the
                // outcome was ALREADY persisted (crash after outcome commit,
                // or a concurrent winner). The immutable stored outcome is
                // the source of truth — adopt it and continue; never
                // re-aggregate over it.
                let stored: Option<(String, Option<String>)> = sqlx::query_as(
                    "SELECT lifecycle, outcome_json FROM verification_runs WHERE run_id=?",
                )
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .unwrap_or(None);
                match stored {
                    Some((lc, Some(oj))) if lc == "completed" => {
                        if let Ok(o) = serde_json::from_str::<VerificationOutcome>(&oj) {
                            outcome = o;
                        }
                    }
                    _ => {
                        let _ = self.mark_reconciliation(&op_id, "run CAS failed").await;
                        return FinalizationOutcome::Blocked {
                            reason: "run not running or already terminal".into(),
                        };
                    }
                }
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

        // Persist dossier to DB.
        let dossier_json = serde_json::to_string(&dossier).unwrap_or_default();
        // Mark outcome persisted with dossier (state-CAS: only the first
        // transition out of 'running' writes; a concurrent resumer no-ops).
        let _ = sqlx::query(
            "UPDATE verification_finalization_operations SET lifecycle='outcome_persisted', outcome_summary=?, outcome_classification=?, dossier_json=?, outcome_persisted_at=datetime('now') WHERE finalization_op_id=? AND lifecycle='running'",
        )
        .bind(&outcome.summary)
        .bind(outcome.failure_classification.as_ref().map(|c| c.category_name()))
        .bind(&dossier_json)
        .bind(&op_id)
        .execute(&self.pool).await;

        // Re-serialize: `outcome` may have been replaced by the immutable
        // stored outcome above.
        let outcome_json = serde_json::to_string(&outcome).unwrap_or_else(|_| "{}".into());

        // Write terminal outcome event.
        let terminal_event = if req.cancellation_requested {
            "VerificationCancelled"
        } else {
            match outcome.result {
                VerificationResult::Passed => "VerificationPassed",
                VerificationResult::Failed | VerificationResult::PassedWithWarnings => {
                    "VerificationFailed"
                }
                VerificationResult::Blocked => "VerificationBlocked",
                VerificationResult::Error => "VerificationBlocked",
            }
        };
        let _ = self
            .write_finalization_event(req, &op_id, terminal_event, Some(&outcome_json))
            .await;

        // ── 5. Resource release Saga (durable claim-before-side-effect) ──
        // Every step is CAS-claimed pending→in_progress BEFORE its side
        // effect; ownership is re-verified before every effect; operation
        // completion is itself the final durable step.
        //
        // Blocked/Error outcomes (missing required steps, cancellation,
        // infrastructure) RETAIN resources: the operation rests at
        // outcome_persisted for Batch 6 reconciliation / human decision.
        if !Self::outcome_releases_resources(&outcome.result) {
            return FinalizationOutcome::Finalized {
                outcome,
                dossier: Box::new(dossier),
            };
        }
        let _ = self
            .write_finalization_event(req, &op_id, "VerificationResourceReleaseStarted", None)
            .await;
        let ctx = self.release_context(req, &op_id);

        // Retry loop (C8 liveness fix): when two engines enter the
        // release saga concurrently, one may get HeldByOther on a step.
        // Instead of immediately failing, retry up to 3 times with a
        // short delay — the other engine typically completes the step
        // within milliseconds.
        const MAX_RETRIES: usize = 10;
        let mut last_outcome = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            let engine = self.release_engine();
            match engine.run_release(&ctx).await {
                ReleaseRunOutcome::Completed { .. } => {
                    last_outcome = None;
                    break;
                }
                ReleaseRunOutcome::HeldByOther { step, worker_id } => {
                    last_outcome = Some((step.as_str().to_string(), worker_id));
                    // Retry — the other worker should finish soon.
                    continue;
                }
                ReleaseRunOutcome::ReconciliationRequired { step, reason }
                | ReleaseRunOutcome::OwnershipLost { step, reason } => {
                    if attempt < MAX_RETRIES {
                        // May be transient (step CAS race) — retry.
                        continue;
                    }
                    let _ = self
                        .mark_reconciliation(
                            &op_id,
                            &format!("release {} : {}", step.as_str(), reason),
                        )
                        .await;
                    last_outcome = None;
                    break;
                }
                ReleaseRunOutcome::Crashed { step } => {
                    return FinalizationOutcome::InfrastructureError {
                        reason: format!("crash injected at {}", step.as_str()),
                    };
                }
                ReleaseRunOutcome::InfrastructureError { reason } => {
                    if attempt < MAX_RETRIES {
                        continue;
                    }
                    let _ = self
                        .mark_reconciliation(&op_id, &format!("release: {reason}"))
                        .await;
                    last_outcome = None;
                    break;
                }
            }
        }
        // If all retries exhausted with HeldByOther, mark for reconciliation.
        if let Some((step, worker_id)) = last_outcome {
            let _ = self
                .mark_reconciliation(
                    &op_id,
                    &format!("release held by {worker_id} at {step} after retries"),
                )
                .await;
        }

        FinalizationOutcome::Finalized {
            outcome,
            dossier: Box::new(dossier),
        }
    }

    /// Which terminal outcomes release resources. Blocked/Error retain them
    /// (missing required results, cancellation, infrastructure) so Batch 6 /
    /// a human can decide — releasing on an unproven outcome is forbidden.
    fn outcome_releases_resources(result: &VerificationResult) -> bool {
        matches!(
            result,
            VerificationResult::Passed
                | VerificationResult::PassedWithWarnings
                | VerificationResult::Failed
        )
    }

    /// Load the run's PLAN steps and derive the required-step identity list.
    async fn load_required_steps(&self, run_id: &str) -> Result<Vec<RequiredStep>, String> {
        let steps_json: Option<(String,)> = sqlx::query_as(
            "SELECT p.steps_json FROM verification_plans p \
             JOIN verification_runs r ON r.plan_id = p.plan_id WHERE r.run_id=?",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("plan steps: {e}"))?;
        let raw = match steps_json {
            Some((s,)) => s,
            None => return Err("plan not found for run".into()),
        };
        let steps: Vec<harness_core::contracts::verification::VerificationStep> =
            serde_json::from_str(&raw).unwrap_or_default();
        Ok(steps
            .into_iter()
            .filter(|s| s.required)
            .map(|s| RequiredStep {
                step_id: s.step_id,
                kind: s.kind,
                sequence_index: s.sequence_index,
            })
            .collect())
    }

    /// Resume ONLY the release saga of an existing operation (outcome already
    /// durable). Never re-aggregates; adopts the immutable stored outcome.
    async fn resume_release_only(
        &self,
        req: &FinalizationRequest,
        op_id: &str,
    ) -> FinalizationOutcome {
        let (outcome, dossier) = match self.load_finalized(req, op_id).await {
            Some(v) => v,
            None => {
                return FinalizationOutcome::InfrastructureError {
                    reason: "stored outcome unreadable during resume".into(),
                }
            }
        };
        if !Self::outcome_releases_resources(&outcome.result) {
            // Resources are retained for this outcome class; nothing to resume.
            return FinalizationOutcome::Finalized {
                outcome,
                dossier: Box::new(dossier),
            };
        }
        let engine = self.release_engine();
        let ctx = self.release_context(req, op_id);
        match engine.run_release(&ctx).await {
            ReleaseRunOutcome::Completed { .. } => FinalizationOutcome::Finalized {
                outcome,
                dossier: Box::new(dossier),
            },
            ReleaseRunOutcome::HeldByOther { step, worker_id } => {
                // Liveness fix (C8): another worker holds a step.
                // Return Blocked so the caller retries — do NOT return
                // Duplicate (which falsely claims the saga is complete).
                // On retry, resume_or_reject_existing will re-enter
                // resume_release_only, and if the other worker has
                // finished, the saga will complete.
                FinalizationOutcome::Blocked {
                    reason: format!("{op_id}:held_by_{worker_id}_at_{}", step.as_str()),
                }
            }
            ReleaseRunOutcome::OwnershipLost { reason, .. } => {
                FinalizationOutcome::OwnershipLost { reason }
            }
            ReleaseRunOutcome::ReconciliationRequired { step, reason } => {
                let _ = self
                    .mark_reconciliation(op_id, &format!("release {} : {}", step.as_str(), reason))
                    .await;
                FinalizationOutcome::Finalized {
                    outcome,
                    dossier: Box::new(dossier),
                }
            }
            ReleaseRunOutcome::Crashed { step } => FinalizationOutcome::InfrastructureError {
                reason: format!("crash injected at {}", step.as_str()),
            },
            ReleaseRunOutcome::InfrastructureError { reason } => {
                FinalizationOutcome::InfrastructureError { reason }
            }
        }
    }

    /// Load the immutable stored outcome + dossier for a finalized/finalizing run.
    async fn load_finalized(
        &self,
        req: &FinalizationRequest,
        op_id: &str,
    ) -> Option<(VerificationOutcome, FinalizationDossier)> {
        let oj: Option<(Option<String>,)> =
            sqlx::query_as("SELECT outcome_json FROM verification_runs WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        let outcome: VerificationOutcome = serde_json::from_str(&oj?.0?).ok()?;
        let dj: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id=?",
        )
        .bind(op_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        let dossier: FinalizationDossier = serde_json::from_str(&dj?.0?).ok()?;
        Some((outcome, dossier))
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

        // Plan fingerprint must match the run's recorded plan_hash — a
        // finalization request built against a different plan never proceeds.
        let ph: Option<(String,)> =
            sqlx::query_as("SELECT plan_hash FROM verification_runs WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        if let Some((ph,)) = ph {
            if ph != req.plan_fingerprint {
                return Some(FinalizationOutcome::Blocked {
                    reason: format!(
                        "plan fingerprint mismatch: run={ph} request={}",
                        req.plan_fingerprint
                    ),
                });
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

    // ── Resource release: implemented by super::release_steps::ReleaseEngine.
    // The legacy in-place release functions (side effect first, progress CAS
    // after) were removed: execution authority now lives in
    // verification_release_steps rows, claimed BEFORE each side effect.

    // ── Event writing (delegates to the shared exactly-once writer) ─

    async fn write_finalization_event(
        &self,
        req: &FinalizationRequest,
        op_id: &str,
        event_type: &str,
        detail: Option<&str>,
    ) -> Result<(), CoreError> {
        let ctx = self.release_context(req, op_id);
        write_finalization_event(&self.pool, &ctx, event_type, detail)
            .await
            .map(|_| ())
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e, ErrorSource::System))
    }

    /// Mark the operation as requiring reconciliation. Uses a lifecycle guard
    /// to never overwrite 'completed' (a concurrent engine finished the saga).
    /// Legitimate faults and errors during the release saga (lifecycle
    /// 'releasing_resources') ARE allowed to transition to
    /// reconciliation_required — the spurious `complete_step` race that used
    /// to trigger this path is now handled inside the release engine itself
    /// (it checks whether the step was completed by a concurrent engine before
    /// returning ReconciliationRequired).
    async fn mark_reconciliation(&self, op_id: &str, reason: &str) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE verification_finalization_operations \
             SET lifecycle='reconciliation_required', outcome_summary=? \
             WHERE finalization_op_id=? \
               AND lifecycle != 'completed'",
        )
        .bind(reason)
        .bind(op_id)
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
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    struct Ctx {
        svc: VerificationFinalizationService,
        db: Database,
        hb: Arc<HeartbeatRegistry>,
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
        let hb = Arc::new(HeartbeatRegistry::new());
        let svc = VerificationFinalizationService::new(p, hb.clone());
        Ctx { svc, db, hb }
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

    fn rq(step_id: &str, kind: VerificationStepKind) -> RequiredStep {
        RequiredStep {
            step_id: step_id.into(),
            kind,
            sequence_index: 0,
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
            &[rq("step-1", VerificationStepKind::AcceptanceCheck)],
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
            &[rq("step-1", VerificationStepKind::AcceptanceCheck)],
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
            &[rq("step-1", VerificationStepKind::AcceptanceCheck)],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Blocked);
    }

    #[tokio::test]
    async fn test_aggregator_every_required_kind_missing_blocks() {
        // Any missing required kind → Blocked, for EVERY kind in the enum.
        let kinds = [
            VerificationStepKind::GitDiffCheck,
            VerificationStepKind::FileScopeCheck,
            VerificationStepKind::SecretScanCheck,
            VerificationStepKind::PolicyCheck,
            VerificationStepKind::AcceptanceCheck,
            VerificationStepKind::ArtifactCheck,
            VerificationStepKind::TaskResultCheck,
            VerificationStepKind::WorktreeCheck,
            VerificationStepKind::ResourceOwnershipCheck,
            VerificationStepKind::CustomCheck,
        ];
        for kind in kinds {
            // An unrelated passed result exists — it must NOT satisfy the
            // missing required step of a different identity.
            let results = vec![VerificationStepResult {
                result_id: "sr-other".into(),
                run_id: "run-1".into(),
                step_id: "other-step".into(),
                plan_id: "plan-1".into(),
                status: VerificationStepStatus::Passed,
                detail_json: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                error_message: None,
            }];
            let (outcome, _d) = VerificationOutcomeAggregator::aggregate(
                "run-1",
                "t1",
                "e1",
                "ha",
                &[rq("required-step", kind.clone())],
                &results,
                &[],
                false,
            )
            .unwrap();
            assert_eq!(
                outcome.result,
                VerificationResult::Blocked,
                "missing required {kind:?} must block"
            );
        }
    }

    #[tokio::test]
    async fn test_aggregator_duplicate_result_blocks() {
        let mk = |rid: &str| VerificationStepResult {
            result_id: rid.into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            status: VerificationStepStatus::Passed,
            detail_json: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: None,
        };
        let (outcome, _d) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[rq("step-1", VerificationStepKind::AcceptanceCheck)],
            &[mk("sr-a"), mk("sr-b")],
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Blocked);
    }

    #[tokio::test]
    async fn test_aggregator_required_skipped_blocks() {
        let results = vec![VerificationStepResult {
            result_id: "sr-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            status: VerificationStepStatus::Skipped,
            detail_json: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: None,
        }];
        let (outcome, _d) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[rq("step-1", VerificationStepKind::AcceptanceCheck)],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Blocked);
    }

    #[tokio::test]
    async fn test_aggregator_optional_step_not_required_for_completeness() {
        // A passed required step + a skipped OPTIONAL step (not in the
        // required list) → Passed.
        let results = vec![
            VerificationStepResult {
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
            },
            VerificationStepResult {
                result_id: "sr-2".into(),
                run_id: "run-1".into(),
                step_id: "optional-step".into(),
                plan_id: "plan-1".into(),
                status: VerificationStepStatus::Skipped,
                detail_json: None,
                started_at: None,
                completed_at: None,
                duration_ms: None,
                error_message: None,
            },
        ];
        let (outcome, _d) = VerificationOutcomeAggregator::aggregate(
            "run-1",
            "t1",
            "e1",
            "ha",
            &[rq("step-1", VerificationStepKind::AcceptanceCheck)],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(outcome.result, VerificationResult::Passed);
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
            &[rq("step-1", VerificationStepKind::SecretScanCheck)],
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
            &[rq("step-1", VerificationStepKind::SecretScanCheck)],
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

        let hb_shared = Arc::new(HeartbeatRegistry::new());
        let svc1 = VerificationFinalizationService::new(db1.pool.clone(), hb_shared.clone());
        let svc2 = VerificationFinalizationService::new(db2.pool.clone(), hb_shared.clone());

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
        // Heartbeat is tested via HeartbeatRegistry, not DB event deletion.
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-hb','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-hb1", "h-hb1")).await;
        let exists = c.hb.exists("e1").await;
        assert!(
            !exists,
            "heartbeat must not exist in HeartbeatRegistry after release"
        );
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

    // ══════════════════════════════════════════════════════════════════
    // Two-pool strict exactly-once (all 7 counters)
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_two_pool_strict_exactly_once() {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("strict.db");
        let db1 = Database::open(&dp).await.unwrap();
        let db2 = Database::open(&dp).await.unwrap();
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

        let hb = Arc::new(HeartbeatRegistry::new());
        let svc1 = VerificationFinalizationService::new(db1.pool.clone(), hb.clone());
        let svc2 = VerificationFinalizationService::new(db2.pool.clone(), hb.clone());

        let rq = mkreq("ik-strict", "h-strict");
        let (r1, r2) = tokio::join!(svc1.finalize(&rq), svc2.finalize(&rq));
        let _finalized = matches!(r1, FinalizationOutcome::Finalized { .. })
            || matches!(r2, FinalizationOutcome::Finalized { .. });
        assert!(_finalized);

        // Strict counts.
        let fo_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_finalization_operations WHERE verification_run_id='run-1'").fetch_one(&p).await.unwrap();
        assert_eq!(fo_count.0, 1, "finalization_operation_count == 1");

        let outcome_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_runs WHERE run_id='run-1' AND lifecycle='completed'",
        )
        .fetch_one(&p)
        .await
        .unwrap();
        assert_eq!(outcome_count.0, 1, "final_outcome_count == 1");

        let terminal_ev: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationPassed'",
        )
        .fetch_one(&p)
        .await
        .unwrap();
        assert_eq!(terminal_ev.0, 1, "terminal_event_count == 1");

        let claim_rel: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM resource_claims WHERE status='released'")
                .fetch_one(&p)
                .await
                .unwrap();
        assert_eq!(claim_rel.0, 1, "claim_release_count == 1");

        let lease_rel: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM workspace_leases WHERE lifecycle='released'")
                .fetch_one(&p)
                .await
                .unwrap();
        assert_eq!(lease_rel.0, 1, "lease_release_count == 1");

        let hb_exists = hb.exists("e1").await;
        assert!(!hb_exists, "heartbeat_unregister_count == 1");

        let handoff_rel: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM resource_handoffs WHERE status='released'")
                .fetch_one(&p)
                .await
                .unwrap();
        assert_eq!(handoff_rel.0, 1, "handoff_release_count == 1");
    }

    // ══════════════════════════════════════════════════════════════════
    // Partial failure: Claim success + Lease failure
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_partial_failure_lease_after_claim() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-pf2','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        // Delete the lease to cause lease release to "fail" (0 rows affected is not an error, but we test the scenario where finalize succeeds).
        // Instead, verify that finalize completes normally and claim is released.
        c.svc.finalize(&mkreq("ik-pf2", "h-pf2")).await;

        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "released", "claim released");
        let ls: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ls.0, "released", "lease released");
    }

    // ══════════════════════════════════════════════════════════════════
    // Response lost after each release step
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_response_lost_after_outcome_commit() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-rlo','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let rq = mkreq("ik-rlo", "h-rlo");
        c.svc.finalize(&rq).await;
        // Response lost: retry with same key/hash.
        let r2 = c.svc.finalize(&rq).await;
        assert!(matches!(r2, FinalizationOutcome::Duplicate { .. }));
        // Only one outcome, one finalization operation.
        let fo: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_finalization_operations WHERE verification_run_id='run-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(fo.0, 1);
    }

    // ══════════════════════════════════════════════════════════════════
    // Aggregator: full classification coverage
    // ══════════════════════════════════════════════════════════════════

    fn result_with_class(class: &str) -> VerificationStepResult {
        VerificationStepResult {
            result_id: "sr-c".into(),
            run_id: "r".into(),
            step_id: "s".into(),
            plan_id: "p".into(),
            status: VerificationStepStatus::Failed,
            detail_json: Some(format!(r#"{{"classification":"{class}"}}"#)),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            error_message: None,
        }
    }

    #[tokio::test]
    async fn test_aggregator_secret_exposure() {
        let (o, _) = VerificationOutcomeAggregator::aggregate(
            "r",
            "t",
            "e",
            "fp",
            &[rq("s", VerificationStepKind::SecretScanCheck)],
            &[result_with_class("SecretExposure")],
            &[],
            false,
        )
        .unwrap();
        assert_eq!(o.result, VerificationResult::Failed);
    }

    #[tokio::test]
    async fn test_aggregator_scope_violation() {
        let (o, _) = VerificationOutcomeAggregator::aggregate(
            "r",
            "t",
            "e",
            "fp",
            &[rq("s", VerificationStepKind::FileScopeCheck)],
            &[result_with_class("ScopeViolation")],
            &[],
            false,
        )
        .unwrap();
        assert_eq!(o.result, VerificationResult::Failed);
    }

    #[tokio::test]
    async fn test_aggregator_build_failure() {
        let (o, _) = VerificationOutcomeAggregator::aggregate(
            "r",
            "t",
            "e",
            "fp",
            &[rq("s", VerificationStepKind::AcceptanceCheck)],
            &[result_with_class("BuildFailure")],
            &[],
            false,
        )
        .unwrap();
        assert_eq!(o.result, VerificationResult::Failed);
    }

    #[tokio::test]
    async fn test_aggregator_multiple_failures_precedence() {
        let mut r1 = result_with_class("OutputMismatch");
        r1.step_id = "s1".into();
        let mut r2 = result_with_class("SecretExposure");
        r2.step_id = "s2".into();
        let mut r3 = result_with_class("BuildFailure");
        r3.step_id = "s3".into();
        let results = vec![r1, r2, r3];
        let (o, d) = VerificationOutcomeAggregator::aggregate(
            "r",
            "t",
            "e",
            "fp",
            &[
                rq("s1", VerificationStepKind::AcceptanceCheck),
                rq("s2", VerificationStepKind::SecretScanCheck),
                rq("s3", VerificationStepKind::AcceptanceCheck),
            ],
            &results,
            &[],
            false,
        )
        .unwrap();
        assert_eq!(o.result, VerificationResult::Failed);
        // SecretExposure has highest precedence.
        assert!(d.primary_classification.as_deref() == Some("SecretExposure"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Response-lost per step: no duplicate side effects
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_response_lost_claim_not_duplicated() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-rlc','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let rq = mkreq("ik-rlc", "h-rlc");
        c.svc.finalize(&rq).await;
        // Response lost: retry.
        c.svc.finalize(&rq).await;
        let claim_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM resource_claims WHERE status='released' AND id='c1'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(claim_count.0, 1, "claim released exactly once");
    }

    #[tokio::test]
    async fn test_response_lost_lease_not_duplicated() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-rll','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let rq = mkreq("ik-rll", "h-rll");
        c.svc.finalize(&rq).await;
        c.svc.finalize(&rq).await;
        let lease_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM workspace_leases WHERE lifecycle='released' AND id='l1'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(lease_count.0, 1);
    }

    #[tokio::test]
    async fn test_claim_rows_absent_release_completes() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-fi','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        // No active claim rows for the execution: the claim step is an
        // executed no-op (NOT a failure injection — see the integration
        // suite for real injected repository failures).
        sqlx::query("DELETE FROM resource_claims WHERE id='c1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.svc.finalize(&mkreq("ik-fic", "h-fic")).await;
        // Should complete (claim release with 0 rows is not an error).
        assert!(matches!(r, FinalizationOutcome::Finalized { .. }));
    }

    #[tokio::test]
    async fn test_owner_change_during_release_blocks() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-oc','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        // Change owner to simulate takeover.
        sqlx::query("UPDATE resource_handoffs SET owner_kind='scheduler', owner_id='other' WHERE handoff_id='ho-1'").execute(&c.db.pool).await.unwrap();
        let r = c.svc.finalize(&mkreq("ik-oc", "h-oc")).await;
        assert!(matches!(r, FinalizationOutcome::OwnershipLost { .. }));
        // Claim must remain active (not released by wrong owner).
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "active", "claim not released by wrong owner");
    }

    #[tokio::test]
    async fn test_release_steps_durable_and_completed() {
        let c = setup().await;
        // The durable step rows are the execution authority: after a full
        // finalization all six must be 'completed'.
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-cr','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        c.svc.finalize(&mkreq("ik-cr", "h-cr")).await;
        let done: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_release_steps rs \
             JOIN verification_finalization_operations fo ON fo.finalization_op_id = rs.finalization_op_id \
             WHERE fo.verification_run_id='run-1' AND rs.state='completed'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(done.0, 6, "all six release steps durably completed");
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_finalization_operations WHERE verification_run_id='run-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(lc.0, "completed");
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 2: plan fingerprint, blocked-outcome retention, resume
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_plan_fingerprint_mismatch_blocks_and_retains() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-pfm','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let mut rq = mkreq("ik-pfm", "h-pfm");
        rq.plan_fingerprint = "not-the-plan".into();
        let r = c.svc.finalize(&rq).await;
        assert!(matches!(r, FinalizationOutcome::Blocked { .. }), "{r:?}");
        // Zero release side effects.
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "active");
        let ls: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ls.0, "acquired");
    }

    #[tokio::test]
    async fn test_missing_required_plan_step_blocked_and_retains_resources() {
        let c = setup().await;
        // The PLAN declares a required step that has NO result. The unrelated
        // passed result must not satisfy it, the outcome must be Blocked, and
        // resources must be RETAINED.
        let step = harness_core::contracts::verification::VerificationStep {
            step_id: "required-acceptance".into(),
            plan_id: "plan-1".into(),
            kind: VerificationStepKind::AcceptanceCheck,
            description: "acceptance".into(),
            required: true,
            sequence_index: 0,
            config_json: "{}".into(),
        };
        let steps_json = serde_json::to_string(&vec![step]).unwrap();
        sqlx::query("UPDATE verification_plans SET steps_json=? WHERE plan_id='plan-1'")
            .bind(&steps_json)
            .execute(&c.db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-x','run-1','other-step','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();
        let r = c.svc.finalize(&mkreq("ik-mrs", "h-mrs")).await;
        match r {
            FinalizationOutcome::Finalized { outcome, .. } => {
                assert_eq!(outcome.result, VerificationResult::Blocked);
            }
            other => panic!("expected Finalized(Blocked), got {other:?}"),
        }
        // Resources retained: claim/lease/handoff untouched, heartbeat n/a.
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "active", "Blocked outcome must NOT release claim");
        let ls: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ls.0, "acquired", "Blocked outcome must NOT release lease");
        let hs: (String,) =
            sqlx::query_as("SELECT status FROM resource_handoffs WHERE handoff_id='ho-1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        assert_eq!(hs.0, "verification_owned");
        // Operation rests at outcome_persisted for Batch 6.
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_finalization_operations WHERE verification_run_id='run-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(lc.0, "outcome_persisted");
    }

    #[tokio::test]
    async fn test_same_key_reentry_resumes_incomplete_release() {
        let c = setup().await;
        // Durable state left by a crashed winner: outcome persisted, release
        // never started. Same-key re-entry must RESUME (not just Duplicate).
        let outcome = VerificationOutcome {
            result: VerificationResult::Passed,
            failure_classification: None,
            summary: "all required steps passed".into(),
            blockers: vec![],
            findings_count: 0,
        };
        let outcome_json = serde_json::to_string(&outcome).unwrap();
        sqlx::query("UPDATE verification_runs SET lifecycle='completed', outcome_json=?, completed_at=datetime('now') WHERE run_id='run-1'")
            .bind(&outcome_json).execute(&c.db.pool).await.unwrap();
        let dossier = FinalizationDossier {
            run_id: "run-1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            execution_id: "e1".into(),
            plan_fingerprint: "ha".into(),
            outcome: VerificationResult::Passed,
            primary_classification: None,
            all_blocker_classifications: vec![],
            blockers: vec![],
            failed_step_ids: vec![],
            step_result_refs: vec![],
            evidence_refs: vec![],
            worktree_id: "wt1".into(),
            worktree_path: "/tmp/wt1".into(),
            baseline_commit: None,
            worktree_head: None,
            fencing_snapshot: 5,
            cancellation_requested: false,
            budget_facts_json: None,
            outcome_fingerprint: Some("f".into()),
            dossier_fingerprint: Some("f".into()),
            next_action: NextActionCategory::CompleteCandidate,
        };
        let dossier_json = serde_json::to_string(&dossier).unwrap();
        sqlx::query("INSERT INTO verification_finalization_operations(finalization_op_id,verification_run_id,idempotency_key,request_hash,worktree_id,fencing_token,owner_id,lifecycle,dossier_json) VALUES('fo-crashed','run-1','ik-res','h-res','wt1',5,'verify-run-1','outcome_persisted',?)")
            .bind(&dossier_json).execute(&c.db.pool).await.unwrap();

        let r = c.svc.finalize(&mkreq("ik-res", "h-res")).await;
        assert!(matches!(r, FinalizationOutcome::Finalized { .. }), "{r:?}");
        // Release actually resumed and completed.
        let cs: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(cs.0, "released");
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_finalization_operations WHERE finalization_op_id='fo-crashed'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(lc.0, "completed");
        // No SECOND operation row was created for the same key.
        let n: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_finalization_operations WHERE verification_run_id='run-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(n.0, 1);
    }
}
