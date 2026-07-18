//! VerificationReconciler — deterministic recovery of verification finalization
//! and resource release after crashes, restarts, or partial completions.
//!
//! Batch 6. Observes durable state AND actual runtime state through the real
//! production repositories/registries, produces a ReconciliationClassification
//! (every variant is production-reachable from `observe_state`), and executes
//! one safe recovery action. Before ANY automatic side effect the state is
//! re-observed and its canonical fingerprint compared with the one recorded at
//! plan time — a change invalidates the plan (ProgressConflict) and nothing
//! executes. Resource release goes through the shared
//! `release_steps::ReleaseEngine` (claim-before-side-effect protocol).
//!
//! NEVER: creates Agents, retries, switches providers, deletes Worktrees,
//! reacquires resources, or modifies Task lifecycle.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sqlx::SqlitePool;
use uuid::Uuid;

use super::release_steps::{
    write_finalization_event, FaultPlan, ReleaseContext, ReleaseCounters, ReleaseEngine,
    ReleaseRunOutcome, ReleaseStepKind, StepGate,
};
use crate::process::{ProcessManager, ProcessState};
use crate::scheduler::heartbeat_registry::{HeartbeatRegistry, HeartbeatRemoveOutcome};

// ── Process probe ─────────────────────────────────────────────────────────

/// Runtime child-process state as seen by the ProcessManager registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProcessChildState {
    /// A child for this execution is starting or running.
    Active,
    /// A child ran and exited.
    Exited,
    /// No child registered for this execution.
    NotFound,
    /// No probe wired (state unknowable) — treated conservatively.
    #[default]
    Unavailable,
}

impl ProcessChildState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Exited => "exited",
            Self::NotFound => "not_found",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Formal ProcessManager state query interface for the reconciler.
#[async_trait::async_trait]
pub trait ProcessStateProbe: Send + Sync {
    async fn child_state(&self, execution_id: &str) -> ProcessChildState;
}

#[async_trait::async_trait]
impl ProcessStateProbe for ProcessManager {
    async fn child_state(&self, execution_id: &str) -> ProcessChildState {
        match self.get_state(execution_id).await {
            Some(ProcessState::Starting) | Some(ProcessState::Running) => ProcessChildState::Active,
            Some(ProcessState::Completed { .. }) => ProcessChildState::Exited,
            None => ProcessChildState::NotFound,
        }
    }
}

// ── Reconciliation classification ────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationClassification {
    NoOpAlreadyConsistent,
    ResumeResourceRelease,
    CompleteOperationRecord,
    RepairMissingEvent,
    RepairMissingDossierLink,
    RuntimeHeartbeatStale,
    DurableHeartbeatMissing,
    ResourceStateMismatch,
    HandoffStateMismatch,
    OwnershipLost,
    StaleFencing,
    ActiveProcessUnknown,
    ActiveScannerUnknown,
    WorktreeMissing,
    OutcomeMissing,
    OutcomeConflict,
    ProgressConflict,
    IrrecoverableAmbiguity,
    AwaitingHuman,
}

impl ReconciliationClassification {
    pub fn is_auto_recoverable(&self) -> bool {
        matches!(
            self,
            Self::NoOpAlreadyConsistent
                | Self::ResumeResourceRelease
                | Self::CompleteOperationRecord
                | Self::RepairMissingEvent
                | Self::RepairMissingDossierLink
                | Self::RuntimeHeartbeatStale
        )
    }

    /// Non-auto classifications execute ZERO resource side effects.
    pub fn should_retain_resources(&self) -> bool {
        !self.is_auto_recoverable()
    }

    pub fn requires_human(&self) -> bool {
        matches!(self, Self::AwaitingHuman | Self::IrrecoverableAmbiguity)
    }

    /// Formal planned action persisted on the operation row.
    pub fn planned_action(&self) -> &'static str {
        if self.is_auto_recoverable() {
            "auto_recover"
        } else {
            "none"
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoOpAlreadyConsistent => "NoOpAlreadyConsistent",
            Self::ResumeResourceRelease => "ResumeResourceRelease",
            Self::CompleteOperationRecord => "CompleteOperationRecord",
            Self::RepairMissingEvent => "RepairMissingEvent",
            Self::RepairMissingDossierLink => "RepairMissingDossierLink",
            Self::RuntimeHeartbeatStale => "RuntimeHeartbeatStale",
            Self::DurableHeartbeatMissing => "DurableHeartbeatMissing",
            Self::ResourceStateMismatch => "ResourceStateMismatch",
            Self::HandoffStateMismatch => "HandoffStateMismatch",
            Self::OwnershipLost => "OwnershipLost",
            Self::StaleFencing => "StaleFencing",
            Self::ActiveProcessUnknown => "ActiveProcessUnknown",
            Self::ActiveScannerUnknown => "ActiveScannerUnknown",
            Self::WorktreeMissing => "WorktreeMissing",
            Self::OutcomeMissing => "OutcomeMissing",
            Self::OutcomeConflict => "OutcomeConflict",
            Self::ProgressConflict => "ProgressConflict",
            Self::IrrecoverableAmbiguity => "IrrecoverableAmbiguity",
            Self::AwaitingHuman => "AwaitingHuman",
        }
    }
}

// ── Reconciler request ───────────────────────────────────────────────────

pub struct ReconciliationRequest {
    pub verification_run_id: String,
    pub execution_id: String,
    pub task_id: String,
    pub project_id: String,
    pub worktree_id: String,
    pub expected_fencing: i64,
    pub verification_owner_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
}

// ── Reconciler outcome ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ReconciliationOutcome {
    Consistent,
    Resumed {
        completed_steps: Vec<String>,
    },
    Blocked {
        classification: ReconciliationClassification,
        reason: String,
    },
    AwaitingHuman {
        classification: ReconciliationClassification,
        reason: String,
    },
    InfrastructureError {
        reason: String,
    },
    Duplicate {
        existing_op_id: String,
    },
    IdempotencyConflict {
        existing_hash: String,
        new_hash: String,
    },
}

// ── Observed state (all fields filled by production queries) ─────────────

#[derive(Debug, Clone, Default)]
pub struct ObservedState {
    // Run + immutable outcome.
    pub run_lifecycle: Option<String>,
    pub outcome_present: bool,
    pub outcome_result: Option<String>,
    // Finalization operation.
    pub finalization_op_id: Option<String>,
    pub finalization_op_lifecycle: Option<String>,
    pub finalization_op_version: i64,
    pub dossier_present: bool,
    pub dossier_fingerprint: Option<String>,
    pub dossier_conflict: bool,
    pub cancellation_requested: bool,
    // Durable release steps: (kind, state, version) sorted by step order.
    pub steps: Vec<(String, String, i64)>,
    // Claims.
    pub claim_active_count: i64,
    pub claim_group_active_count: i64,
    // Lease.
    pub lease_active: bool,
    // Heartbeat (runtime registry).
    pub heartbeat_exists: bool,
    pub heartbeat_identity_matches: bool,
    // Handoff (current owner/fencing — the ownership authority).
    pub handoff_present: bool,
    pub handoff_status: Option<String>,
    pub handoff_owner_kind: Option<String>,
    pub handoff_owner_id: Option<String>,
    pub handoff_fencing: i64,
    pub handoff_version: i64,
    pub fencing_mismatch: bool,
    pub owner_changed: bool,
    // Worktree DB + filesystem identity.
    pub worktree_db_exists: bool,
    pub worktree_db_active: bool,
    pub worktree_fs_exists: bool,
    pub worktree_identity_lost: bool,
    // Command / scanner operations.
    pub active_command_op: bool,
    pub active_scanner_op: bool,
    pub command_op_reconciliation_required: bool,
    pub policy_op_reconciliation_required: bool,
    // Process runtime (ProcessManager registry).
    pub process_child_state: ProcessChildState,
    // Events.
    pub terminal_event_present: bool,
    pub resources_released_event_present: bool,
    // Step results / evidence.
    pub step_result_count: i64,
    pub evidence_count: i64,
    // Prior reconciliation operations for this run.
    pub prior_reconciliation_blocked: bool,
}

impl ObservedState {
    fn resources_active(&self) -> bool {
        self.claim_active_count > 0
            || self.claim_group_active_count > 0
            || self.lease_active
            || self.heartbeat_exists
            || self.handoff_status.as_deref() == Some("verification_owned")
    }

    fn step_state(&self, kind: ReleaseStepKind) -> Option<&str> {
        self.steps
            .iter()
            .find(|(k, _, _)| k == kind.as_str())
            .map(|(_, s, _)| s.as_str())
    }

    fn step_completed(&self, kind: ReleaseStepKind) -> bool {
        self.step_state(kind) == Some("completed")
    }

    fn steps_all_completed(&self) -> bool {
        ReleaseStepKind::ALL.iter().all(|k| self.step_completed(*k))
    }

    fn resource_steps_completed(&self) -> bool {
        [
            ReleaseStepKind::ClaimRelease,
            ReleaseStepKind::LeaseRelease,
            ReleaseStepKind::HeartbeatUnregister,
            ReleaseStepKind::HandoffRelease,
        ]
        .iter()
        .all(|k| self.step_completed(*k))
    }

    fn any_step_needs_reconciliation(&self) -> bool {
        self.steps
            .iter()
            .any(|(_, s, _)| s == "failed" || s == "reconciliation_required")
    }

    /// Whether the stored outcome class releases resources at all.
    fn outcome_releasable(&self) -> bool {
        matches!(
            self.outcome_result.as_deref(),
            Some("passed") | Some("passed_with_warnings") | Some("failed")
        )
    }

    /// Canonical, stable serialization: fixed field order, sorted lists, no
    /// Debug dumps, no memory addresses, no timestamps, no secrets.
    pub fn canonical_string(&self) -> String {
        let mut steps = self.steps.clone();
        steps.sort();
        let steps_s = steps
            .iter()
            .map(|(k, s, v)| format!("{k}:{s}:{v}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "run_lc={}|outcome={}|result={}|fo_id={}|fo_lc={}|fo_v={}|dossier={}|dossier_fp={}|dossier_conflict={}|cancel={}|steps=[{steps_s}]|claims={}|groups={}|lease={}|hb={}|hb_match={}|ho={}|ho_status={}|ho_owner={}/{}|ho_fence={}|ho_v={}|fence_mismatch={}|owner_changed={}|wt_db={}|wt_active={}|wt_fs={}|wt_lost={}|cmd_active={}|scan_active={}|cmd_rec={}|scan_rec={}|child={}|term_ev={}|rel_ev={}|results={}|evidence={}|prior_blocked={}",
            self.run_lifecycle.as_deref().unwrap_or("-"),
            self.outcome_present,
            self.outcome_result.as_deref().unwrap_or("-"),
            self.finalization_op_id.as_deref().unwrap_or("-"),
            self.finalization_op_lifecycle.as_deref().unwrap_or("-"),
            self.finalization_op_version,
            self.dossier_present,
            self.dossier_fingerprint.as_deref().unwrap_or("-"),
            self.dossier_conflict,
            self.cancellation_requested,
            self.claim_active_count,
            self.claim_group_active_count,
            self.lease_active,
            self.heartbeat_exists,
            self.heartbeat_identity_matches,
            self.handoff_present,
            self.handoff_status.as_deref().unwrap_or("-"),
            self.handoff_owner_kind.as_deref().unwrap_or("-"),
            self.handoff_owner_id.as_deref().unwrap_or("-"),
            self.handoff_fencing,
            self.handoff_version,
            self.fencing_mismatch,
            self.owner_changed,
            self.worktree_db_exists,
            self.worktree_db_active,
            self.worktree_fs_exists,
            self.worktree_identity_lost,
            self.active_command_op,
            self.active_scanner_op,
            self.command_op_reconciliation_required,
            self.policy_op_reconciliation_required,
            self.process_child_state.as_str(),
            self.terminal_event_present,
            self.resources_released_event_present,
            self.step_result_count,
            self.evidence_count,
            self.prior_reconciliation_blocked,
        )
    }

    /// Canonical fingerprint: FNV-1a 64 over the canonical string.
    pub fn fingerprint(&self) -> String {
        format!("{:016x}", fnv1a64(&self.canonical_string()))
    }
}

/// Deterministic, platform-stable FNV-1a 64-bit hash.
pub fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ── Test barrier (inert in production) ────────────────────────────────────

/// Barrier between plan persistence and side-effect execution: the reconciler
/// signals `reached` after recording classification+fingerprint, then waits
/// for `release()`. Tests mutate observed state in between to prove the plan
/// is invalidated.
#[derive(Clone)]
pub struct ReconcileGate {
    reached: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
}

impl ReconcileGate {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            reached: Arc::new(tokio::sync::Notify::new()),
            proceed: Arc::new(tokio::sync::Notify::new()),
        }
    }
    pub async fn wait_reached(&self) {
        self.reached.notified().await;
    }
    pub fn release(&self) {
        self.proceed.notify_one();
    }
    async fn pass(&self) {
        self.reached.notify_one();
        self.proceed.notified().await;
    }
}

// ── Reconciler ────────────────────────────────────────────────────────────

pub struct VerificationReconciler {
    pool: SqlitePool,
    heartbeat_registry: Arc<HeartbeatRegistry>,
    process_probe: Option<Arc<dyn ProcessStateProbe>>,
    pub reconciler_start_count: Arc<AtomicUsize>,
    pub release_counters: ReleaseCounters,
    faults: FaultPlan,
    step_gate: Option<StepGate>,
    observe_gate: Option<ReconcileGate>,
    worker_id: String,
}

impl VerificationReconciler {
    pub fn new(pool: SqlitePool, heartbeat_registry: Arc<HeartbeatRegistry>) -> Self {
        Self {
            pool,
            heartbeat_registry,
            process_probe: None,
            reconciler_start_count: Arc::new(AtomicUsize::new(0)),
            release_counters: ReleaseCounters::default(),
            faults: FaultPlan::default(),
            step_gate: None,
            observe_gate: None,
            worker_id: format!("reconciler-{}", Uuid::new_v4()),
        }
    }

    /// Wire the formal ProcessManager state probe.
    pub fn with_process_probe(mut self, probe: Arc<dyn ProcessStateProbe>) -> Self {
        self.process_probe = Some(probe);
        self
    }

    pub fn with_counters(mut self, counters: ReleaseCounters) -> Self {
        self.release_counters = counters;
        self
    }

    pub fn with_start_count(mut self, count: Arc<AtomicUsize>) -> Self {
        self.reconciler_start_count = count;
        self
    }

    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.faults = faults;
        self
    }

    pub fn with_step_gate(mut self, gate: StepGate) -> Self {
        self.step_gate = Some(gate);
        self
    }

    pub fn with_observe_gate(mut self, gate: ReconcileGate) -> Self {
        self.observe_gate = Some(gate);
        self
    }

    /// Reconcile a single verification run. Idempotent: same key+hash →
    /// Duplicate. Winner selection is one atomic INSERT; a loser reads the
    /// existing operation and NEVER classifies or executes anything.
    pub async fn reconcile(&self, req: &ReconciliationRequest) -> ReconciliationOutcome {
        // ── 0. Existing operation for this key? ─────────────────────
        if let Some(existing) = self.read_existing(req).await {
            return existing;
        }

        // ── 1. Observe durable + runtime state (production queries) ──
        let state = self.observe_state(req).await;
        let classification = Self::classify(req, &state);
        let planned_fingerprint = state.fingerprint();

        // ── 2. Atomic winner insert ──────────────────────────────────
        let op_id = format!("rec-{}", Uuid::new_v4());
        let inserted = sqlx::query(
            "INSERT INTO verification_reconciliation_operations (reconciliation_op_id, verification_run_id, idempotency_key, request_hash, observed_state_fingerprint, classification, planned_action, owner_id, fencing_token, lifecycle, started_at) VALUES (?,?,?,?,?,?,?,?,?,'running',datetime('now')) ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(&op_id)
        .bind(&req.verification_run_id)
        .bind(&req.idempotency_key)
        .bind(&req.request_hash)
        .bind(&planned_fingerprint)
        .bind(classification.as_str())
        .bind(classification.planned_action())
        .bind(&req.verification_owner_id)
        .bind(req.expected_fencing)
        .execute(&self.pool)
        .await;

        match inserted {
            Ok(r) if r.rows_affected() == 1 => {}
            Ok(_) => {
                // Loser: never classify further, never execute side effects.
                return self.read_existing(req).await.unwrap_or(
                    ReconciliationOutcome::InfrastructureError {
                        reason: "operation row vanished after conflict".into(),
                    },
                );
            }
            Err(e) => {
                return ReconciliationOutcome::InfrastructureError {
                    reason: format!("insert op: {e}"),
                }
            }
        }

        // Winner only.
        self.reconciler_start_count.fetch_add(1, Ordering::SeqCst);
        self.write_reconciliation_event(req, &op_id, "VerificationReconciliationStarted")
            .await;

        // ── 3. Execute ───────────────────────────────────────────────
        self.execute(req, &op_id, classification, &state, &planned_fingerprint)
            .await
    }

    async fn read_existing(&self, req: &ReconciliationRequest) -> Option<ReconciliationOutcome> {
        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT reconciliation_op_id, request_hash FROM verification_reconciliation_operations WHERE idempotency_key=?",
        )
        .bind(&req.idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None);
        let (op_id, eh) = existing?;
        if eh == req.request_hash {
            Some(ReconciliationOutcome::Duplicate {
                existing_op_id: op_id,
            })
        } else {
            Some(ReconciliationOutcome::IdempotencyConflict {
                existing_hash: eh,
                new_hash: req.request_hash.clone(),
            })
        }
    }

    // ── Execution ──────────────────────────────────────────────────

    async fn execute(
        &self,
        req: &ReconciliationRequest,
        op_id: &str,
        classification: ReconciliationClassification,
        state: &ObservedState,
        planned_fingerprint: &str,
    ) -> ReconciliationOutcome {
        use ReconciliationClassification as C;

        // Human-decision states: formal lifecycle + event, zero side effects.
        if classification.requires_human() {
            self.write_reconciliation_event(req, op_id, "VerificationAwaitingHuman")
                .await;
            let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='awaiting_human' WHERE reconciliation_op_id=?")
                .bind(op_id).execute(&self.pool).await;
            return ReconciliationOutcome::AwaitingHuman {
                classification: classification.clone(),
                reason: classification.as_str().into(),
            };
        }

        // Blocked states: formal lifecycle + event, zero side effects.
        if !classification.is_auto_recoverable() {
            self.write_reconciliation_event(req, op_id, "VerificationReconciliationBlocked")
                .await;
            let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked' WHERE reconciliation_op_id=?")
                .bind(op_id).execute(&self.pool).await;
            return ReconciliationOutcome::Blocked {
                classification: classification.clone(),
                reason: classification.as_str().into(),
            };
        }

        // NoOp completes immediately (no side effects to guard).
        if classification == C::NoOpAlreadyConsistent {
            self.write_reconciliation_event(req, op_id, "VerificationReconciliationNoOp")
                .await;
            self.complete_op(op_id, planned_fingerprint).await;
            return ReconciliationOutcome::Consistent;
        }

        // ── Plan-invalidation guard: re-observe BEFORE any side effect ──
        if let Some(gate) = &self.observe_gate {
            gate.pass().await;
        }
        let fresh = self.observe_state(req).await;
        let fresh_fingerprint = fresh.fingerprint();
        if fresh_fingerprint != planned_fingerprint {
            // Observed state changed after the plan was formed: the old plan
            // is DEAD. Nothing executes.
            self.write_reconciliation_event(req, op_id, "VerificationReconciliationBlocked")
                .await;
            let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked', last_error='observed_state_changed' WHERE reconciliation_op_id=?")
                .bind(op_id).execute(&self.pool).await;
            return ReconciliationOutcome::Blocked {
                classification: C::ProgressConflict,
                reason: "observed state changed after plan formation".into(),
            };
        }

        match classification {
            C::ResumeResourceRelease | C::CompleteOperationRecord => {
                let fo_id = match &state.finalization_op_id {
                    Some(id) => id.clone(),
                    None => {
                        return ReconciliationOutcome::InfrastructureError {
                            reason: "finalization operation missing for resume".into(),
                        }
                    }
                };
                let ctx = self.release_context(req, &fo_id);
                let engine = self.release_engine();
                match engine.run_release(&ctx).await {
                    ReleaseRunOutcome::Completed { executed } => {
                        self.write_reconciliation_event(
                            req,
                            op_id,
                            "VerificationReconciliationResumed",
                        )
                        .await;
                        self.complete_op(op_id, planned_fingerprint).await;
                        ReconciliationOutcome::Resumed {
                            completed_steps: executed.iter().map(|s| s.to_string()).collect(),
                        }
                    }
                    ReleaseRunOutcome::HeldByOther { step, worker_id } => {
                        // Another live worker drives the saga; zero effects here.
                        let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked', last_error='held_by_other' WHERE reconciliation_op_id=?")
                            .bind(op_id).execute(&self.pool).await;
                        ReconciliationOutcome::Blocked {
                            classification: C::ProgressConflict,
                            reason: format!("step {} held by {worker_id}", step.as_str()),
                        }
                    }
                    ReleaseRunOutcome::OwnershipLost { step, reason } => {
                        let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked', last_error=? WHERE reconciliation_op_id=?")
                            .bind(&reason).bind(op_id).execute(&self.pool).await;
                        ReconciliationOutcome::Blocked {
                            classification: C::OwnershipLost,
                            reason: format!("{} at {}", reason, step.as_str()),
                        }
                    }
                    ReleaseRunOutcome::ReconciliationRequired { step, reason } => {
                        let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked', last_error=? WHERE reconciliation_op_id=?")
                            .bind(&reason).bind(op_id).execute(&self.pool).await;
                        ReconciliationOutcome::Blocked {
                            classification: C::ProgressConflict,
                            reason: format!("{} at {}", reason, step.as_str()),
                        }
                    }
                    ReleaseRunOutcome::Crashed { step } => {
                        ReconciliationOutcome::InfrastructureError {
                            reason: format!("crash injected at {}", step.as_str()),
                        }
                    }
                    ReleaseRunOutcome::InfrastructureError { reason } => {
                        ReconciliationOutcome::InfrastructureError { reason }
                    }
                }
            }

            C::RepairMissingEvent => self.repair_missing_events(req, op_id, state).await,

            C::RepairMissingDossierLink => self.repair_missing_dossier(req, op_id, state).await,

            C::RuntimeHeartbeatStale => {
                match self
                    .heartbeat_registry
                    .remove_if_matches(
                        &req.execution_id,
                        &req.verification_owner_id,
                        req.expected_fencing,
                    )
                    .await
                {
                    HeartbeatRemoveOutcome::Removed | HeartbeatRemoveOutcome::NotFound => {
                        if !self.heartbeat_registry.exists(&req.execution_id).await {
                            self.release_counters
                                .heartbeat_unregister
                                .fetch_add(1, Ordering::SeqCst);
                        }
                        self.write_reconciliation_event(
                            req,
                            op_id,
                            "VerificationReconciliationRepaired",
                        )
                        .await;
                        self.complete_op(op_id, planned_fingerprint).await;
                        ReconciliationOutcome::Resumed {
                            completed_steps: vec!["stale_heartbeat_removed".into()],
                        }
                    }
                    HeartbeatRemoveOutcome::IdentityMismatch { owner_id, .. } => {
                        // A DIFFERENT owner's heartbeat — never touched.
                        let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked', last_error='heartbeat identity mismatch' WHERE reconciliation_op_id=?")
                            .bind(op_id).execute(&self.pool).await;
                        ReconciliationOutcome::Blocked {
                            classification: C::OwnershipLost,
                            reason: format!("heartbeat owned by {owner_id}"),
                        }
                    }
                }
            }

            // NoOp and non-auto handled above.
            _ => ReconciliationOutcome::InfrastructureError {
                reason: "unreachable classification arm".into(),
            },
        }
    }

    // ── Repairs ────────────────────────────────────────────────────

    /// Rebuild missing terminal / ResourcesReleased events from the IMMUTABLE
    /// stored outcome. Uses the finalizer's deterministic idempotency keys +
    /// INSERT OR IGNORE, so repair is exactly-once even across a concurrent
    /// finalizer retry, and response-lost repair re-entry cannot duplicate.
    /// Never re-aggregates the outcome; never re-releases resources; never
    /// deletes historical events.
    async fn repair_missing_events(
        &self,
        req: &ReconciliationRequest,
        op_id: &str,
        state: &ObservedState,
    ) -> ReconciliationOutcome {
        let fo_id = state
            .finalization_op_id
            .clone()
            .unwrap_or_else(|| op_id.to_string());
        let ctx = self.release_context(req, &fo_id);
        let mut repaired: Vec<String> = Vec::new();

        if !state.terminal_event_present {
            let event_type = if state.cancellation_requested {
                "VerificationCancelled"
            } else {
                match state.outcome_result.as_deref() {
                    Some("passed") => "VerificationPassed",
                    Some("failed") | Some("passed_with_warnings") => "VerificationFailed",
                    _ => "VerificationBlocked",
                }
            };
            let outcome_json: Option<(Option<String>,)> =
                sqlx::query_as("SELECT outcome_json FROM verification_runs WHERE run_id=?")
                    .bind(&req.verification_run_id)
                    .fetch_optional(&self.pool)
                    .await
                    .ok()
                    .flatten();
            let detail = outcome_json.and_then(|r| r.0);
            match write_finalization_event(&self.pool, &ctx, event_type, detail.as_deref()).await {
                Ok(_inserted) => repaired.push(format!("terminal_event:{event_type}")),
                Err(e) => {
                    return ReconciliationOutcome::InfrastructureError {
                        reason: format!("event repair: {e}"),
                    }
                }
            }
        }

        if state.resource_steps_completed() && !state.resources_released_event_present {
            match write_finalization_event(&self.pool, &ctx, "VerificationResourcesReleased", None)
                .await
            {
                Ok(inserted) => {
                    if inserted {
                        self.release_counters
                            .resources_released_event
                            .fetch_add(1, Ordering::SeqCst);
                    }
                    repaired.push("resources_released_event".into());
                }
                Err(e) => {
                    return ReconciliationOutcome::InfrastructureError {
                        reason: format!("release event repair: {e}"),
                    }
                }
            }
        }

        self.write_reconciliation_event(req, op_id, "VerificationReconciliationRepaired")
            .await;
        self.complete_op(op_id, "event_repaired").await;
        ReconciliationOutcome::Resumed {
            completed_steps: repaired,
        }
    }

    /// Rebuild a missing dossier STRICTLY from immutable facts: the stored
    /// outcome, persisted StepResults/Evidence, and the operation's own
    /// recorded identity. No LLM, no retry creation, no Task mutation, no
    /// outcome re-aggregation. Exactly-once via `WHERE dossier_json IS NULL`.
    async fn repair_missing_dossier(
        &self,
        req: &ReconciliationRequest,
        op_id: &str,
        state: &ObservedState,
    ) -> ReconciliationOutcome {
        let fo_id = match &state.finalization_op_id {
            Some(id) => id.clone(),
            None => {
                return ReconciliationOutcome::InfrastructureError {
                    reason: "finalization operation missing for dossier repair".into(),
                }
            }
        };
        let outcome_json: Option<(Option<String>,)> =
            sqlx::query_as("SELECT outcome_json FROM verification_runs WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        let outcome_raw = match outcome_json.and_then(|r| r.0) {
            Some(o) => o,
            None => {
                return ReconciliationOutcome::InfrastructureError {
                    reason: "immutable outcome missing".into(),
                }
            }
        };
        let outcome: harness_core::contracts::verification::VerificationOutcome =
            match serde_json::from_str(&outcome_raw) {
                Ok(o) => o,
                Err(e) => {
                    return ReconciliationOutcome::InfrastructureError {
                        reason: format!("outcome unparseable: {e}"),
                    }
                }
            };

        // Immutable step results + evidence references.
        let result_refs: Vec<(String,)> = sqlx::query_as(
            "SELECT result_id FROM verification_step_results WHERE run_id=? ORDER BY result_id",
        )
        .bind(&req.verification_run_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        let evidence_refs: Vec<(String,)> = sqlx::query_as(
            "SELECT evidence_id FROM verification_evidence WHERE run_id=? ORDER BY evidence_id",
        )
        .bind(&req.verification_run_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let result_str = format!("{:?}", outcome.result);
        let fingerprint_src = format!(
            "{}|{}|{}|{:?}",
            req.verification_run_id, req.request_hash, result_str, outcome.blockers
        );
        let fp = format!("{:016x}", fnv1a64(&fingerprint_src));

        let dossier = super::finalization::FinalizationDossier {
            run_id: req.verification_run_id.clone(),
            task_id: req.task_id.clone(),
            project_id: req.project_id.clone(),
            execution_id: req.execution_id.clone(),
            plan_fingerprint: req.request_hash.clone(),
            outcome: outcome.result.clone(),
            primary_classification: outcome
                .failure_classification
                .as_ref()
                .map(|c| c.category_name().to_string()),
            all_blocker_classifications: vec![],
            blockers: outcome.blockers.clone(),
            failed_step_ids: vec![],
            step_result_refs: result_refs.into_iter().map(|r| r.0).collect(),
            evidence_refs: evidence_refs.into_iter().map(|r| r.0).collect(),
            worktree_id: req.worktree_id.clone(),
            worktree_path: String::new(),
            baseline_commit: None,
            worktree_head: None,
            fencing_snapshot: req.expected_fencing,
            cancellation_requested: state.cancellation_requested,
            budget_facts_json: None,
            outcome_fingerprint: Some(fp.clone()),
            dossier_fingerprint: Some(fp),
            next_action: super::finalization::NextActionCategory::ReconciliationRequired,
        };
        let dossier_json = serde_json::to_string(&dossier).unwrap_or_default();
        // Content validation on the free-text parts: never store secrets.
        // (Structural field names like task_id are ours; the validator's
        // pattern check applies to human-derived text.)
        for text in dossier
            .blockers
            .iter()
            .chain(dossier.primary_classification.iter())
        {
            if super::content_validator::VerificationContentValidator::validate_text(text).is_err()
            {
                return ReconciliationOutcome::InfrastructureError {
                    reason: "dossier failed content validation".into(),
                };
            }
        }

        // Exactly-once: only fills a NULL dossier — never overwrites.
        let r = sqlx::query(
            "UPDATE verification_finalization_operations SET dossier_json=? WHERE finalization_op_id=? AND dossier_json IS NULL",
        )
        .bind(&dossier_json)
        .bind(&fo_id)
        .execute(&self.pool)
        .await;
        let wrote = matches!(r, Ok(ref x) if x.rows_affected() == 1);

        self.write_reconciliation_event(req, op_id, "VerificationReconciliationRepaired")
            .await;
        self.complete_op(op_id, "dossier_repaired").await;
        ReconciliationOutcome::Resumed {
            completed_steps: if wrote {
                vec!["dossier_rebuilt".into()]
            } else {
                vec!["dossier_already_present".into()]
            },
        }
    }

    // ── State observation (production repositories/registries only) ──

    pub(crate) async fn observe_state(&self, req: &ReconciliationRequest) -> ObservedState {
        let mut s = ObservedState::default();

        // Run lifecycle + immutable outcome.
        let run: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT lifecycle, outcome_json FROM verification_runs WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        if let Some((lc, oj)) = run {
            s.run_lifecycle = Some(lc);
            if let Some(oj) = oj {
                s.outcome_present = true;
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&oj) {
                    s.outcome_result = v
                        .get("result")
                        .and_then(|r| r.as_str())
                        .map(|r| r.to_string());
                }
            }
        }

        // Finalization operation + dossier.
        let fo: Option<(String, String, i64, Option<String>)> = sqlx::query_as(
            "SELECT finalization_op_id, lifecycle, version, dossier_json FROM verification_finalization_operations WHERE verification_run_id=?",
        )
        .bind(&req.verification_run_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        if let Some((fo_id, lc, version, dossier_json)) = fo {
            s.finalization_op_lifecycle = Some(lc);
            s.finalization_op_version = version;
            if let Some(dj) = dossier_json {
                s.dossier_present = true;
                match serde_json::from_str::<super::finalization::FinalizationDossier>(&dj) {
                    Ok(d) => {
                        s.dossier_fingerprint = d.dossier_fingerprint.clone();
                        s.cancellation_requested = d.cancellation_requested;
                        // Cross-check dossier identity against immutable facts.
                        let result_matches = match (&s.outcome_result, &d.outcome) {
                            (Some(r), o) => {
                                let o_s = serde_json::to_value(o)
                                    .ok()
                                    .and_then(|v| v.as_str().map(|x| x.to_string()))
                                    .unwrap_or_default();
                                &o_s == r
                            }
                            (None, _) => true,
                        };
                        if d.run_id != req.verification_run_id
                            || d.execution_id != req.execution_id
                            || !result_matches
                        {
                            s.dossier_conflict = true;
                        }
                    }
                    Err(_) => s.dossier_conflict = true,
                }
            }
            // Durable release steps.
            let steps: Vec<(String, String, i64)> = sqlx::query_as(
                "SELECT step_kind, state, version FROM verification_release_steps WHERE finalization_op_id=? ORDER BY step_order",
            )
            .bind(&fo_id)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();
            s.steps = steps;
            s.finalization_op_id = Some(fo_id);
        }

        // Claims + claim groups (by execution identity).
        let claims: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM resource_claims WHERE execution_id=? AND status='active'",
        )
        .bind(&req.execution_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.claim_active_count = claims.0;
        let groups: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM resource_claim_groups WHERE execution_id=? AND lifecycle='active'",
        )
        .bind(&req.execution_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.claim_group_active_count = groups.0;

        // Lease (by owner execution identity).
        let lease: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM workspace_leases WHERE owner_execution_id=? AND lifecycle='acquired'",
        )
        .bind(&req.execution_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.lease_active = lease.0 > 0;

        // Heartbeat: runtime registry presence + identity.
        s.heartbeat_exists = self.heartbeat_registry.exists(&req.execution_id).await;
        if s.heartbeat_exists {
            if let Some(entry) = self.heartbeat_registry.inspect(&req.execution_id).await {
                s.heartbeat_identity_matches = entry.owner_id == req.verification_owner_id
                    && entry.fencing_token == req.expected_fencing;
            }
        }

        // Handoff: current owner/fencing authority.
        let handoff: Option<(String, String, String, i64, i64)> = sqlx::query_as(
            "SELECT status, owner_kind, owner_id, fencing_token, version FROM resource_handoffs WHERE execution_id=?",
        )
        .bind(&req.execution_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        if let Some((status, owner_kind, owner_id, fencing, version)) = handoff {
            s.handoff_present = true;
            s.fencing_mismatch = fencing != req.expected_fencing;
            s.owner_changed = owner_kind != "verification" || owner_id != req.verification_owner_id;
            s.handoff_status = Some(status);
            s.handoff_owner_kind = Some(owner_kind);
            s.handoff_owner_id = Some(owner_id);
            s.handoff_fencing = fencing;
            s.handoff_version = version;
        }

        // Worktree: DB identity + filesystem identity cross-check.
        let wt: Option<(String, String)> =
            sqlx::query_as("SELECT worktree_path, status FROM worktrees WHERE id=?")
                .bind(&req.worktree_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        if let Some((path, status)) = wt {
            s.worktree_db_exists = true;
            s.worktree_db_active = status == "active";
            s.worktree_fs_exists = !path.is_empty() && std::path::Path::new(&path).exists();
            s.worktree_identity_lost = s.worktree_db_active && !s.worktree_fs_exists;
        }

        // Command operations.
        let cmd: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_operations WHERE verification_run_id=? AND status IN ('running','pending')",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.active_command_op = cmd.0 > 0;
        let cmd_rec: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_operations WHERE verification_run_id=? AND status='reconciliation_required'",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.command_op_reconciliation_required = cmd_rec.0 > 0;

        // Policy / scanner operations.
        let scan: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_policy_operations WHERE verification_run_id=? AND lifecycle IN ('running','pending')",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.active_scanner_op = scan.0 > 0;
        let scan_rec: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_policy_operations WHERE verification_run_id=? AND lifecycle='reconciliation_required'",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.policy_op_reconciliation_required = scan_rec.0 > 0;

        // ProcessManager child registry (formal probe).
        s.process_child_state = match &self.process_probe {
            Some(p) => p.child_state(&req.execution_id).await,
            None => ProcessChildState::Unavailable,
        };

        // Events.
        let term: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE verification_run_id=? AND event_type IN ('VerificationPassed','VerificationFailed','VerificationBlocked','VerificationCancelled')",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.terminal_event_present = term.0 > 0;
        let rel: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE verification_run_id=? AND event_type='VerificationResourcesReleased'",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.resources_released_event_present = rel.0 > 0;

        // Step results / evidence presence.
        let src: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_step_results WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));
        s.step_result_count = src.0;
        let evc: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_evidence WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));
        s.evidence_count = evc.0;

        // Prior reconciliation operations.
        let prior: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_reconciliation_operations WHERE verification_run_id=? AND lifecycle IN ('blocked','awaiting_human')",
        )
        .bind(&req.verification_run_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.prior_reconciliation_blocked = prior.0 > 0;

        s
    }

    // ── Classification (source-of-truth precedence) ─────────────────
    //
    // Precedence, enforced by evaluation order:
    // 1. Ownership authority (current handoff owner/fencing) beats every
    //    historical snapshot — stale fencing / new owner stop everything.
    // 2. Actual resource state beats ReleaseProgress/step checkpoints.
    // 3. Active/unknown process or scanner NEVER releases resources.
    // 4. Immutable run outcome beats operation lifecycle beats events/dossier.
    // 5. Worktree DB and filesystem identity must agree.

    pub(crate) fn classify(
        req: &ReconciliationRequest,
        s: &ObservedState,
    ) -> ReconciliationClassification {
        use ReconciliationClassification as C;

        let handoff_owned = s.handoff_status.as_deref() == Some("verification_owned");
        let handoff_released = s.handoff_status.as_deref() == Some("released");

        // ── 1. Ownership authority ──
        if s.handoff_present && s.fencing_mismatch {
            return C::StaleFencing;
        }
        if s.handoff_present && s.owner_changed {
            return C::OwnershipLost;
        }
        if !s.handoff_present
            && (s.claim_active_count > 0 || s.claim_group_active_count > 0 || s.lease_active)
        {
            // Resources held but no ownership record — cannot decide safely.
            return C::IrrecoverableAmbiguity;
        }

        // ── 2. Illegal resource-state combinations ──
        if handoff_released
            && (s.claim_active_count > 0 || s.claim_group_active_count > 0 || s.lease_active)
        {
            return C::HandoffStateMismatch;
        }
        if s.heartbeat_exists && handoff_released {
            return C::RuntimeHeartbeatStale;
        }

        // ── 3. Active or unknown process/scanner: never release ──
        if s.active_command_op || s.process_child_state == ProcessChildState::Active {
            return C::ActiveProcessUnknown;
        }
        if s.active_scanner_op {
            return C::ActiveScannerUnknown;
        }
        if s.command_op_reconciliation_required || s.policy_op_reconciliation_required {
            return C::ProgressConflict;
        }

        // ── 4. Durable-vs-runtime heartbeat facts (pre-outcome) ──
        if !s.outcome_present
            && handoff_owned
            && (s.claim_active_count > 0 || s.claim_group_active_count > 0 || s.lease_active)
            && !s.heartbeat_exists
        {
            return C::DurableHeartbeatMissing;
        }

        // ── 5. Immutable outcome facts ──
        if s.run_lifecycle.as_deref() == Some("completed") && !s.outcome_present {
            return C::OutcomeConflict;
        }
        if s.outcome_present && s.run_lifecycle.as_deref() != Some("completed") {
            return C::OutcomeConflict;
        }
        if !s.outcome_present {
            return C::OutcomeMissing;
        }

        // ── 6. Worktree identity ──
        if !req.worktree_id.is_empty() && !s.worktree_db_exists {
            return C::WorktreeMissing;
        }
        if s.worktree_identity_lost {
            return C::AwaitingHuman;
        }

        // ── 7. Dossier conflicts ──
        if s.dossier_conflict {
            return C::AwaitingHuman;
        }

        // ── 8. Operation/step/progress consistency ──
        if s.finalization_op_id.is_none() {
            return C::IrrecoverableAmbiguity;
        }
        if s.any_step_needs_reconciliation() {
            return C::ProgressConflict;
        }
        if s.finalization_op_lifecycle.as_deref() == Some("completed")
            && (!s.steps_all_completed() || s.resources_active())
        {
            return C::ProgressConflict;
        }
        // Step checkpoint says released but the ACTUAL resource is active —
        // actual state wins, checkpoint is wrong.
        if (s.step_completed(ReleaseStepKind::ClaimRelease)
            && (s.claim_active_count > 0 || s.claim_group_active_count > 0))
            || (s.step_completed(ReleaseStepKind::LeaseRelease) && s.lease_active)
            || (s.step_completed(ReleaseStepKind::HandoffRelease) && handoff_owned)
        {
            return C::ResourceStateMismatch;
        }

        // ── 9. Auto-recovery decisions ──
        if s.outcome_releasable() {
            if !s.steps_all_completed() {
                if s.resource_steps_completed() {
                    return C::CompleteOperationRecord;
                }
                return C::ResumeResourceRelease;
            }
        } else {
            // Blocked/Error outcomes retain resources by design; the resting
            // operation is consistent once its events/dossier exist.
            if !s.terminal_event_present {
                return C::RepairMissingEvent;
            }
            if !s.dossier_present {
                return C::RepairMissingDossierLink;
            }
            return C::NoOpAlreadyConsistent;
        }

        // Steps all completed: verify events + dossier + operation record.
        if !s.terminal_event_present || !s.resources_released_event_present {
            return C::RepairMissingEvent;
        }
        if !s.dossier_present {
            return C::RepairMissingDossierLink;
        }
        if s.finalization_op_lifecycle.as_deref() != Some("completed") {
            return C::CompleteOperationRecord;
        }
        if !s.resources_active() {
            return C::NoOpAlreadyConsistent;
        }

        C::IrrecoverableAmbiguity
    }

    // ── Helpers ────────────────────────────────────────────────────

    fn release_engine(&self) -> ReleaseEngine {
        ReleaseEngine::new(
            self.pool.clone(),
            self.heartbeat_registry.clone(),
            self.release_counters.clone(),
            self.faults.clone(),
            self.step_gate.clone(),
            self.worker_id.clone(),
        )
    }

    fn release_context(&self, req: &ReconciliationRequest, fo_id: &str) -> ReleaseContext {
        ReleaseContext {
            finalization_op_id: fo_id.to_string(),
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

    async fn complete_op(&self, op_id: &str, result_fingerprint: &str) {
        let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='completed', terminal_at=datetime('now'), result_fingerprint=? WHERE reconciliation_op_id=?")
            .bind(result_fingerprint)
            .bind(op_id)
            .execute(&self.pool)
            .await;
    }

    async fn write_reconciliation_event(
        &self,
        req: &ReconciliationRequest,
        op_id: &str,
        event_type: &str,
    ) {
        // Synthetic step_op row so the FK on verification_step_events holds
        // (INSERT OR IGNORE does NOT suppress FK violations).
        let _ = sqlx::query(
            "INSERT OR IGNORE INTO verification_step_operations \
             (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, \
              worktree_id, fencing_token, status, idempotency_key, request_hash) \
             VALUES (?,?,?,?,?,?,?,?,'reconciliation',?,?)",
        )
        .bind(op_id)
        .bind(&req.verification_run_id)
        .bind("reconciliation")
        .bind("plan-final")
        .bind(&req.execution_id)
        .bind("rec-cfg")
        .bind(&req.worktree_id)
        .bind(req.expected_fencing)
        .bind(op_id)
        .bind(&req.request_hash)
        .execute(&self.pool)
        .await;

        let eid = format!("evt-rec-{}", Uuid::new_v4());
        let ikey = format!("rec-ev-{}-{}", req.verification_run_id, event_type);
        let _ = sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&eid).bind(&req.verification_run_id).bind("reconciliation").bind(op_id)
            .bind(&req.execution_id).bind(&req.task_id).bind(&req.worktree_id)
            .bind(req.expected_fencing).bind(event_type).bind("reconciliation")
            .bind::<Option<String>>(None).bind(&ikey)
            .execute(&self.pool).await;
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    struct Ctx {
        rec: VerificationReconciler,
        db: Database,
        hb: Arc<HeartbeatRegistry>,
        #[allow(dead_code)]
        wt_dir: tempfile::TempDir,
    }

    /// Seed a run whose OUTCOME IS PERSISTED and whose release has not
    /// started: handoff verification_owned, claim+lease active, finalization
    /// operation at outcome_persisted, worktree row + real directory.
    async fn setup() -> Ctx {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("rec.db");
        let db = Database::open(&dp).await.unwrap();
        let wt_dir = tempfile::tempdir().unwrap();
        let p = db.pool.clone();
        seed(&p, wt_dir.path().to_string_lossy().as_ref()).await;
        let hb = Arc::new(HeartbeatRegistry::new());
        let rec = VerificationReconciler::new(p, hb.clone());
        Ctx {
            rec,
            db,
            hb,
            wt_dir,
        }
    }

    async fn seed(p: &SqlitePool, wt_path: &str) {
        sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','t','active')")
            .execute(p)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','t','submitted')",
        )
        .execute(p)
        .await
        .unwrap();
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')").execute(p).await.unwrap();
        sqlx::query("INSERT INTO verification_plans(plan_id,task_id,execution_id,project_id,plan_hash,plan_version,steps_json) VALUES('plan-1','t1','e1','p1','ha',1,'[]')").execute(p).await.unwrap();
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash,outcome_json,completed_at) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','completed','ik-r','hr',?,datetime('now'))")
            .bind(r#"{"result":"passed","failure_classification":null,"summary":"all required steps passed","blockers":[],"findings_count":0}"#)
            .execute(p).await.unwrap();
        sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(p).await.unwrap();
        sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(p).await.unwrap();
        sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(p).await.unwrap();
        sqlx::query("INSERT INTO worktrees(id,project_id,task_id,execution_id,repository_root,repository_identity,worktree_path,branch_name,base_commit,owner_supervisor_id,operation_id,status) VALUES('wt1','p1','t1','e1','/repo','/repo/.git',?,'br','abc','sup1','op1','active')")
            .bind(wt_path).execute(p).await.unwrap();
        sqlx::query("INSERT INTO verification_finalization_operations(finalization_op_id,verification_run_id,idempotency_key,request_hash,worktree_id,fencing_token,owner_id,lifecycle,dossier_json) VALUES('fo-1','run-1','ik-fo','h-fo','wt1',5,'verify-run-1','outcome_persisted',?)")
            .bind(dossier_json()).execute(p).await.unwrap();
    }

    fn dossier_json() -> String {
        let d = super::super::finalization::FinalizationDossier {
            run_id: "run-1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            execution_id: "e1".into(),
            plan_fingerprint: "ha".into(),
            outcome: harness_core::contracts::verification::VerificationResult::Passed,
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
            next_action: super::super::finalization::NextActionCategory::CompleteCandidate,
        };
        serde_json::to_string(&d).unwrap()
    }

    fn mkrec(ikey: &str, hash: &str) -> ReconciliationRequest {
        ReconciliationRequest {
            verification_run_id: "run-1".into(),
            execution_id: "e1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            worktree_id: "wt1".into(),
            expected_fencing: 5,
            verification_owner_id: "verify-run-1".into(),
            idempotency_key: ikey.into(),
            request_hash: hash.into(),
        }
    }

    // ── Production observe → classify → execute ────────────────────

    #[tokio::test]
    async fn test_resume_release_full_recovery() {
        let c = setup().await;
        let r = c.rec.reconcile(&mkrec("ik-1", "h-1")).await;
        assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
        // All four resource releases executed exactly once.
        assert_eq!(
            c.rec.release_counters.snapshot(),
            [1, 1, 1, 1, 1, 1],
            "resume executes every remaining step exactly once"
        );
        let claim: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(claim.0, "released");
        let fo: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(fo.0, "completed");
    }

    #[tokio::test]
    async fn test_reconcile_idempotent_duplicate_zero_side_effects() {
        let c = setup().await;
        let rq = mkrec("ik-dup", "h-dup");
        c.rec.reconcile(&rq).await;
        let before = c.rec.release_counters.snapshot();
        let r2 = c.rec.reconcile(&rq).await;
        assert!(matches!(r2, ReconciliationOutcome::Duplicate { .. }));
        assert_eq!(
            c.rec.release_counters.snapshot(),
            before,
            "duplicate executes zero side effects"
        );
        assert_eq!(c.rec.reconciler_start_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_outcome_missing_blocks_with_zero_release() {
        let c = setup().await;
        sqlx::query(
            "UPDATE verification_runs SET lifecycle='running', outcome_json=NULL WHERE run_id='run-1'",
        )
        .execute(&c.db.pool)
        .await
        .unwrap();
        // Heartbeat present so DurableHeartbeatMissing does not preempt.
        register_heartbeat(&c, "verify-run-1", 5).await;
        let r = c.rec.reconcile(&mkrec("ik-om", "h-om")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => {
                assert_eq!(classification, ReconciliationClassification::OutcomeMissing)
            }
            other => panic!("expected Blocked(OutcomeMissing), got {other:?}"),
        }
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    async fn register_heartbeat(c: &Ctx, owner: &str, fencing: i64) {
        use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};
        let entry = HeartbeatEntry {
            execution_id: "e1".into(),
            task_id: "t1".into(),
            worktree_id: "wt1".into(),
            lease_id: "l1".into(),
            claim_group_id: None,
            fencing_token: fencing,
            owner_kind: OwnerKind::Verification,
            owner_id: owner.into(),
            status: HeartbeatStatus::Healthy,
            last_heartbeat_at: None,
            cancel_token: tokio_util::sync::CancellationToken::new(),
            last_error: None,
        };
        c.hb.register(entry).await.unwrap();
    }

    #[tokio::test]
    async fn test_durable_heartbeat_missing_production_path() {
        let c = setup().await;
        // Verification still in flight: no outcome, ownership facts durable,
        // runtime heartbeat MISSING.
        sqlx::query(
            "UPDATE verification_runs SET lifecycle='running', outcome_json=NULL WHERE run_id='run-1'",
        )
        .execute(&c.db.pool)
        .await
        .unwrap();
        let r = c.rec.reconcile(&mkrec("ik-dhm", "h-dhm")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => assert_eq!(
                classification,
                ReconciliationClassification::DurableHeartbeatMissing
            ),
            other => panic!("expected Blocked(DurableHeartbeatMissing), got {other:?}"),
        }
        // Zero release side effects; no heartbeat silently created.
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
        assert!(!c.hb.exists("e1").await);
        // Formal event + blocked lifecycle persisted.
        let ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationReconciliationBlocked'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(ev.0, 1);
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(lc.0, "blocked");
    }

    #[tokio::test]
    async fn test_stale_fencing_production_path() {
        let c = setup().await;
        sqlx::query("UPDATE resource_handoffs SET fencing_token=9 WHERE handoff_id='ho-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.rec.reconcile(&mkrec("ik-sf", "h-sf")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => {
                assert_eq!(classification, ReconciliationClassification::StaleFencing)
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_ownership_lost_production_path() {
        let c = setup().await;
        sqlx::query("UPDATE resource_handoffs SET owner_kind='scheduler', owner_id='other' WHERE handoff_id='ho-1'")
            .execute(&c.db.pool).await.unwrap();
        let r = c.rec.reconcile(&mkrec("ik-ol", "h-ol")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => {
                assert_eq!(classification, ReconciliationClassification::OwnershipLost)
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
        // New owner's resources untouched.
        let claim: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(claim.0, "active");
    }

    #[tokio::test]
    async fn test_handoff_state_mismatch_production_path() {
        let c = setup().await;
        sqlx::query("UPDATE resource_handoffs SET status='released' WHERE handoff_id='ho-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.rec.reconcile(&mkrec("ik-hsm", "h-hsm")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => assert_eq!(
                classification,
                ReconciliationClassification::HandoffStateMismatch
            ),
            other => panic!("{other:?}"),
        }
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_runtime_heartbeat_stale_repaired() {
        let c = setup().await;
        // Handoff released, resources released, but a stale runtime heartbeat
        // with OUR identity survives.
        sqlx::query("UPDATE resource_handoffs SET status='released' WHERE handoff_id='ho-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE resource_claims SET status='released' WHERE id='c1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE workspace_leases SET lifecycle='released' WHERE id='l1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        register_heartbeat(&c, "verify-run-1", 5).await;
        let r = c.rec.reconcile(&mkrec("ik-rhs", "h-rhs")).await;
        assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
        assert!(!c.hb.exists("e1").await, "stale heartbeat removed");
        // Only the heartbeat side effect executed.
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 1, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_active_process_zero_release() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_step_operations(op_id,verification_run_id,step_id,plan_id,execution_id,step_config_hash,worktree_id,fencing_token,status,idempotency_key,request_hash) VALUES('op-run','run-1','step-1','plan-1','e1','cfg','wt1',5,'running','ik-op','h-op')").execute(&c.db.pool).await.unwrap();
        let r = c.rec.reconcile(&mkrec("ik-ap", "h-ap")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => assert_eq!(
                classification,
                ReconciliationClassification::ActiveProcessUnknown
            ),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            c.rec.release_counters.snapshot(),
            [0, 0, 0, 0, 0, 0],
            "active process: zero release side effects"
        );
    }

    #[tokio::test]
    async fn test_active_scanner_zero_release() {
        let c = setup().await;
        sqlx::query("INSERT INTO verification_policy_operations(policy_op_id,verification_run_id,step_id,step_kind,sequence_index,idempotency_key,request_hash,worktree_id,fencing_token,lifecycle) VALUES('pop-run','run-1','step-1','secret_scan',0,'ik-pop','h-pop','wt1',5,'running')").execute(&c.db.pool).await.unwrap();
        let r = c.rec.reconcile(&mkrec("ik-as", "h-as")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => assert_eq!(
                classification,
                ReconciliationClassification::ActiveScannerUnknown
            ),
            other => panic!("{other:?}"),
        }
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_worktree_missing_blocks() {
        let c = setup().await;
        sqlx::query("DELETE FROM worktrees WHERE id='wt1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.rec.reconcile(&mkrec("ik-wm", "h-wm")).await;
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => {
                assert_eq!(
                    classification,
                    ReconciliationClassification::WorktreeMissing
                )
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_worktree_identity_lost_awaits_human() {
        let c = setup().await;
        // DB row active but the filesystem directory is GONE.
        sqlx::query("UPDATE worktrees SET worktree_path='Z:/definitely/not/here' WHERE id='wt1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.rec.reconcile(&mkrec("ik-wil", "h-wil")).await;
        match r {
            ReconciliationOutcome::AwaitingHuman { classification, .. } => {
                assert_eq!(classification, ReconciliationClassification::AwaitingHuman)
            }
            other => panic!("expected AwaitingHuman, got {other:?}"),
        }
        // Formal persistence: lifecycle + planned_action + event exactly once.
        let row: (String, String) = sqlx::query_as("SELECT lifecycle, planned_action FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(row.0, "awaiting_human");
        assert_eq!(row.1, "none");
        let ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationAwaitingHuman'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(ev.0, 1);
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_repair_missing_terminal_event() {
        let c = setup().await;
        // Fully released state, but the terminal event is missing.
        complete_release_state(&c).await;
        let r = c.rec.reconcile(&mkrec("ik-rme", "h-rme")).await;
        assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
        let ev: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationPassed'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(ev.0, 1, "terminal event repaired from immutable outcome");
        // No resource re-release.
        assert_eq!(c.rec.release_counters.snapshot()[0..4], [0, 0, 0, 0]);
        // Response-lost: repeat with a NEW key — event stays exactly once.
        let r2 = c.rec.reconcile(&mkrec("ik-rme2", "h-rme2")).await;
        assert!(!matches!(
            r2,
            ReconciliationOutcome::InfrastructureError { .. }
        ));
        let ev2: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationPassed'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(ev2.0, 1, "repair is exactly-once");
    }

    /// Drive the durable state to fully-released via direct facts: handoff
    /// released, claim/lease released, all six steps completed, resources
    /// released event present.
    async fn complete_release_state(c: &Ctx) {
        sqlx::query("UPDATE resource_handoffs SET status='released' WHERE handoff_id='ho-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE resource_claims SET status='released' WHERE id='c1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE workspace_leases SET lifecycle='released' WHERE id='l1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE verification_finalization_operations SET lifecycle='completed' WHERE finalization_op_id='fo-1'")
            .execute(&c.db.pool).await.unwrap();
        for kind in ReleaseStepKind::ALL {
            sqlx::query("INSERT INTO verification_release_steps(release_step_id,finalization_op_id,step_kind,step_order,state,owner_id,execution_id,fencing_token) VALUES(?,?,?,?,'completed','verify-run-1','e1',5)")
                .bind(format!("rs-fo-1-{}", kind.as_str()))
                .bind("fo-1")
                .bind(kind.as_str())
                .bind(kind.order())
                .execute(&c.db.pool).await.unwrap();
        }
        // ResourcesReleased event present (terminal event intentionally absent).
        sqlx::query("INSERT OR IGNORE INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES ('fo-1','run-1','finalization','plan-final','e1','final-cfg','wt1',5,'finalization','fo-1','h-fo')")
            .execute(&c.db.pool).await.unwrap();
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-rel','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationResourcesReleased','finalization',NULL,'final-ev-run-1-VerificationResourcesReleased')")
            .execute(&c.db.pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_repair_missing_dossier() {
        let c = setup().await;
        complete_release_state(&c).await;
        // Terminal event present; dossier MISSING.
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
            .execute(&c.db.pool).await.unwrap();
        sqlx::query("UPDATE verification_finalization_operations SET dossier_json=NULL WHERE finalization_op_id='fo-1'")
            .execute(&c.db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-1','run-1','step-1','plan-1','passed',datetime('now'))")
            .execute(&c.db.pool).await.unwrap();

        let r = c.rec.reconcile(&mkrec("ik-rmd", "h-rmd")).await;
        assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
        let dj: (Option<String>,) = sqlx::query_as("SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&c.db.pool).await.unwrap();
        let dj = dj.0.expect("dossier rebuilt");
        assert!(dj.contains("\"run_id\":\"run-1\""));
        assert!(dj.contains("sr-1"), "rebuilt from immutable step results");
        assert!(!dj.contains("sk-"), "no secrets");
        // No resource side effects during repair.
        assert_eq!(c.rec.release_counters.snapshot()[0..4], [0, 0, 0, 0]);
        // Repair is exactly-once: rebuilt dossier never overwritten.
        let r2 = c.rec.reconcile(&mkrec("ik-rmd2", "h-rmd2")).await;
        assert!(!matches!(
            r2,
            ReconciliationOutcome::InfrastructureError { .. }
        ));
        let dj2: (Option<String>,) = sqlx::query_as("SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(dj2.0.unwrap(), dj, "dossier not overwritten");
    }

    #[tokio::test]
    async fn test_dossier_fingerprint_conflict_awaits_human() {
        let c = setup().await;
        complete_release_state(&c).await;
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
            .execute(&c.db.pool).await.unwrap();
        // Dossier claims a DIFFERENT outcome than the immutable run outcome.
        let conflicting = dossier_json().replace("\"passed\"", "\"failed\"");
        sqlx::query("UPDATE verification_finalization_operations SET dossier_json=? WHERE finalization_op_id='fo-1'")
            .bind(&conflicting).execute(&c.db.pool).await.unwrap();
        let r = c.rec.reconcile(&mkrec("ik-dfc", "h-dfc")).await;
        match r {
            ReconciliationOutcome::AwaitingHuman { classification, .. } => {
                assert_eq!(classification, ReconciliationClassification::AwaitingHuman)
            }
            other => panic!("expected AwaitingHuman on dossier conflict, got {other:?}"),
        }
        // Conflicting dossier NOT overwritten.
        let dj: (Option<String>,) = sqlx::query_as("SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(dj.0.unwrap(), conflicting);
    }

    #[tokio::test]
    async fn test_noop_when_fully_consistent() {
        let c = setup().await;
        complete_release_state(&c).await;
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
            .execute(&c.db.pool).await.unwrap();
        let r = c.rec.reconcile(&mkrec("ik-noop", "h-noop")).await;
        assert!(matches!(r, ReconciliationOutcome::Consistent), "{r:?}");
        assert_eq!(c.rec.release_counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_plan_invalidated_when_state_changes_before_effect() {
        let c = setup().await;
        let gate = ReconcileGate::new();
        let rec = VerificationReconciler::new(c.db.pool.clone(), c.hb.clone())
            .with_observe_gate(gate.clone());
        let pool = c.db.pool.clone();
        let handle = tokio::spawn(async move { rec.reconcile(&mkrec("ik-inv", "h-inv")).await });
        gate.wait_reached().await;
        // Mutate observed state AFTER the plan was formed: new owner takes over.
        sqlx::query("UPDATE resource_handoffs SET owner_id='new-owner', fencing_token=6 WHERE handoff_id='ho-1'")
            .execute(&pool).await.unwrap();
        gate.release();
        let r = handle.await.unwrap();
        match r {
            ReconciliationOutcome::Blocked { classification, .. } => assert_eq!(
                classification,
                ReconciliationClassification::ProgressConflict,
                "stale plan must be invalidated"
            ),
            other => panic!("expected Blocked(ProgressConflict), got {other:?}"),
        }
        // ZERO side effects: the new owner's resources are intact.
        let claim: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(claim.0, "active");
        let lease: (String,) =
            sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(lease.0, "acquired");
    }

    #[tokio::test]
    async fn test_two_pool_strict_winner_and_loser() {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("tp.db");
        let db1 = Database::open(&dp).await.unwrap();
        let db2 = Database::open(&dp).await.unwrap();
        let wt_dir = tempfile::tempdir().unwrap();
        seed(&db1.pool, wt_dir.path().to_string_lossy().as_ref()).await;

        let hb = Arc::new(HeartbeatRegistry::new());
        let counters = ReleaseCounters::default();
        let start = Arc::new(AtomicUsize::new(0));
        let rec1 = VerificationReconciler::new(db1.pool.clone(), hb.clone())
            .with_counters(counters.clone())
            .with_start_count(start.clone());
        let rec2 = VerificationReconciler::new(db2.pool.clone(), hb.clone())
            .with_counters(counters.clone())
            .with_start_count(start.clone());

        let rq1 = mkrec("ik-tp", "h-tp");
        let rq2 = mkrec("ik-tp", "h-tp");
        let (r1, r2) = tokio::join!(rec1.reconcile(&rq1), rec2.reconcile(&rq2));

        // Exactly one winner started; the loser returned Duplicate (never a
        // bare UNIQUE error surfaced as InfrastructureError).
        assert_eq!(start.load(Ordering::SeqCst), 1, "reconciler_start_count");
        let dup_count = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, ReconciliationOutcome::Duplicate { .. }))
            .count();
        let resumed_count = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, ReconciliationOutcome::Resumed { .. }))
            .count();
        assert_eq!(resumed_count, 1, "one winner resumed: {r1:?} / {r2:?}");
        assert_eq!(dup_count, 1, "one loser duplicate: {r1:?} / {r2:?}");

        // Exactly one operation row; side effects exactly once.
        let op_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&db1.pool).await.unwrap();
        assert_eq!(op_count.0, 1, "reconciliation_operation_count");
        assert_eq!(counters.snapshot(), [1, 1, 1, 1, 1, 1]);
        let completed_ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationReconciliationResumed'").fetch_one(&db1.pool).await.unwrap();
        assert_eq!(completed_ev.0, 1, "completed_event_count");
    }

    // ── Scope boundaries ────────────────────────────────────────────

    #[tokio::test]
    async fn test_reconcile_no_agent_no_retry_no_worktree_mutation() {
        let c = setup().await;
        c.rec.reconcile(&mkrec("ik-scope", "h-scope")).await;
        let ac: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ac.0, 0);
        let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ec.0, 1);
        let tl: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(tl.0, "submitted");
        let wt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE id='wt1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(wt.0, 1, "worktree record untouched");
        assert!(c.wt_dir.path().exists(), "worktree directory untouched");
    }

    // ── Canonical fingerprint ───────────────────────────────────────

    #[tokio::test]
    async fn test_canonical_fingerprint_stable_and_sensitive() {
        let c = setup().await;
        let rq = mkrec("ik-fp", "h-fp");
        let s1 = c.rec.observe_state(&rq).await;
        let s2 = c.rec.observe_state(&rq).await;
        assert_eq!(s1.fingerprint(), s2.fingerprint(), "stable across reads");
        assert!(
            !s1.canonical_string().contains("ObservedState"),
            "not a Debug dump"
        );
        // Any observed change flips the fingerprint.
        sqlx::query("UPDATE resource_handoffs SET fencing_token=6 WHERE handoff_id='ho-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let s3 = c.rec.observe_state(&rq).await;
        assert_ne!(s1.fingerprint(), s3.fingerprint());
    }
}
