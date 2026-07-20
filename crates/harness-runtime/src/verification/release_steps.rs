//! Durable release-step protocol — claim-before-side-effect execution
//! authority for verification resource release (I4-C Batch 5/6).
//!
//! Every resource side effect (Claim release, Lease release, Heartbeat
//! unregister, Handoff release, the ResourcesReleased event, and operation
//! completion) is guarded by a durable row in `verification_release_steps`:
//!
//! ```text
//! read durable step + expected version
//! → CAS pending → in_progress            (claim; exactly one winner)
//! → only the CAS winner executes the side effect
//! → CAS in_progress → completed          (worker + fencing + version bound)
//! → losers reload durable state and NEVER execute the side effect
//! ```
//!
//! Ownership (current handoff owner + fencing) is re-verified before EVERY
//! side effect, not just once at the start. A takeover or stale fencing stops
//! the worker with zero further side effects.
//!
//! Shared by `VerificationFinalizationService` and `VerificationReconciler`
//! so both use one release implementation, one ownership validator, and one
//! resume path. NEVER: creates Agents, retries, switches providers, deletes
//! Worktrees, mutates Task lifecycle, or reacquires resources.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use sqlx::SqlitePool;

use crate::scheduler::heartbeat_registry::{HeartbeatRegistry, HeartbeatRemoveOutcome};

// ── Step kinds ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReleaseStepKind {
    ClaimRelease,
    LeaseRelease,
    HeartbeatUnregister,
    HandoffRelease,
    ResourcesReleasedEvent,
    OperationCompletion,
}

impl ReleaseStepKind {
    pub const ALL: [ReleaseStepKind; 6] = [
        ReleaseStepKind::ClaimRelease,
        ReleaseStepKind::LeaseRelease,
        ReleaseStepKind::HeartbeatUnregister,
        ReleaseStepKind::HandoffRelease,
        ReleaseStepKind::ResourcesReleasedEvent,
        ReleaseStepKind::OperationCompletion,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ClaimRelease => "claim_release",
            Self::LeaseRelease => "lease_release",
            Self::HeartbeatUnregister => "heartbeat_unregister",
            Self::HandoffRelease => "handoff_release",
            Self::ResourcesReleasedEvent => "resources_released_event",
            Self::OperationCompletion => "operation_completion",
        }
    }

    pub fn order(&self) -> i64 {
        match self {
            Self::ClaimRelease => 1,
            Self::LeaseRelease => 2,
            Self::HeartbeatUnregister => 3,
            Self::HandoffRelease => 4,
            Self::ResourcesReleasedEvent => 5,
            Self::OperationCompletion => 6,
        }
    }

    /// Legacy summary label kept for the human-readable ReleaseProgress JSON.
    pub fn legacy_label(&self) -> &'static str {
        match self {
            Self::ClaimRelease => "ClaimReleased",
            Self::LeaseRelease => "LeaseReleased",
            Self::HeartbeatUnregister => "HeartbeatUnregistered",
            Self::HandoffRelease => "HandoffReleased",
            Self::ResourcesReleasedEvent => "ReleaseEventWritten",
            Self::OperationCompletion => "Completed",
        }
    }
}

// ── Step states ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseStepState {
    Pending,
    InProgress,
    Completed,
    Failed,
    ReconciliationRequired,
}

impl ReleaseStepState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::ReconciliationRequired => "reconciliation_required",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "in_progress" => Self::InProgress,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "reconciliation_required" => Self::ReconciliationRequired,
            _ => Self::Pending,
        }
    }
}

/// One durable release step row.
#[derive(Debug, Clone)]
pub struct ReleaseStepRow {
    pub release_step_id: String,
    pub finalization_op_id: String,
    pub step_kind: ReleaseStepKind,
    pub state: ReleaseStepState,
    pub worker_id: Option<String>,
    pub owner_id: String,
    pub execution_id: String,
    pub fencing_token: i64,
    pub version: i64,
    pub result_fingerprint: Option<String>,
}

/// Result of attempting to claim a release step before its side effect.
#[derive(Debug, Clone)]
pub enum StepClaimResult {
    /// This worker won the CAS and MUST be the only executor of the side effect.
    Acquired {
        release_step_id: String,
        version: i64,
    },
    /// The step already completed — the side effect MUST NOT run again.
    AlreadyCompleted { result_fingerprint: String },
    /// Another worker holds the step — this worker MUST NOT execute anything.
    HeldByOther { worker_id: String, version: i64 },
    /// Durable state moved concurrently — reload; NEVER execute on Conflict.
    Conflict {
        durable_state: ReleaseStepState,
        durable_version: i64,
    },
}

// ── Observable side-effect counters ──────────────────────────────────────

/// Shared, observable counters incremented exactly when a resource side
/// effect actually executes. Cloneable; share one instance across services
/// (two-pool tests) to observe combined exactly-once behavior.
#[derive(Clone, Default)]
pub struct ReleaseCounters {
    pub claim_release: Arc<AtomicUsize>,
    pub lease_release: Arc<AtomicUsize>,
    pub heartbeat_unregister: Arc<AtomicUsize>,
    pub handoff_release: Arc<AtomicUsize>,
    pub resources_released_event: Arc<AtomicUsize>,
    pub operation_completion: Arc<AtomicUsize>,
}

impl ReleaseCounters {
    pub fn snapshot(&self) -> [usize; 6] {
        [
            self.claim_release.load(Ordering::SeqCst),
            self.lease_release.load(Ordering::SeqCst),
            self.heartbeat_unregister.load(Ordering::SeqCst),
            self.handoff_release.load(Ordering::SeqCst),
            self.resources_released_event.load(Ordering::SeqCst),
            self.operation_completion.load(Ordering::SeqCst),
        ]
    }
}

// ── Fault injection (production-shaped, inert unless configured) ─────────

/// Injected fault modes for integration testing. The production constructor
/// installs an empty plan; faults are consumed (one-shot) so a resumed run
/// proceeds normally. Faults intercept the executor — they never fake errors
/// by deleting database rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// The side effect fails (repository error) — step transitions to failed.
    FailEffect,
    /// Simulated crash before the step is even claimed (step stays pending).
    CrashBeforeClaim,
    /// Simulated crash after claim, before the side effect (stays in_progress,
    /// side effect NOT executed).
    CrashBeforeEffect,
    /// Simulated crash after the side effect, before completion CAS (stays
    /// in_progress, side effect EXECUTED once).
    CrashAfterEffect,
}

#[derive(Clone, Default)]
pub struct FaultPlan {
    faults: Arc<Mutex<HashMap<&'static str, FaultMode>>>,
}

impl FaultPlan {
    pub fn inject(&self, kind: ReleaseStepKind, mode: FaultMode) {
        self.faults.lock().unwrap().insert(kind.as_str(), mode);
    }

    fn take(&self, kind: ReleaseStepKind, mode: FaultMode) -> bool {
        let mut m = self.faults.lock().unwrap();
        if m.get(kind.as_str()) == Some(&mode) {
            m.remove(kind.as_str());
            return true;
        }
        false
    }
}

/// A two-sided barrier: the engine signals `reached` right before the given
/// step's side effect, then waits for `release()` before continuing. Used to
/// mutate observed state between plan formation and side-effect execution.
#[derive(Clone)]
pub struct StepGate {
    kind: ReleaseStepKind,
    reached: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
}

impl StepGate {
    pub fn new(kind: ReleaseStepKind) -> Self {
        Self {
            kind,
            reached: Arc::new(tokio::sync::Notify::new()),
            proceed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Test side: wait until the engine is parked right before the side effect.
    pub async fn wait_reached(&self) {
        self.reached.notified().await;
    }

    /// Test side: allow the engine to continue.
    pub fn release(&self) {
        self.proceed.notify_one();
    }

    async fn pass(&self, kind: ReleaseStepKind) {
        if self.kind == kind {
            self.reached.notify_one();
            self.proceed.notified().await;
        }
    }
}

// ── Release context ───────────────────────────────────────────────────────

/// Identity facts the engine binds into every claim and side-effect predicate.
#[derive(Debug, Clone)]
pub struct ReleaseContext {
    pub finalization_op_id: String,
    pub verification_run_id: String,
    pub execution_id: String,
    pub task_id: String,
    pub project_id: String,
    pub worktree_id: String,
    pub expected_fencing: i64,
    pub verification_owner_id: String,
    pub request_hash: String,
}

/// Outcome of one engine pass over the six release steps.
#[derive(Debug, Clone)]
pub enum ReleaseRunOutcome {
    /// All six steps are durably completed.
    Completed {
        executed: Vec<&'static str>,
    },
    /// Another worker holds a step — zero side effects were executed here past it.
    HeldByOther {
        step: ReleaseStepKind,
        worker_id: String,
    },
    /// A step failed or its side effect cannot be proven safe to (re)run.
    ReconciliationRequired {
        step: ReleaseStepKind,
        reason: String,
    },
    /// Current handoff owner/fencing no longer match — stopped with zero
    /// further side effects; the new owner's resources were NOT touched.
    OwnershipLost {
        step: ReleaseStepKind,
        reason: String,
    },
    /// Simulated crash (fault injection only) — durable state left as-is.
    Crashed {
        step: ReleaseStepKind,
    },
    InfrastructureError {
        reason: String,
    },
}

// ── Summary JSON (human-readable; NOT the execution authority) ───────────

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ReleaseProgress {
    pub completed_steps: Vec<String>,
    pub failed_step: Option<String>,
    pub claim_rows: i64,
    pub lease_rows: i64,
    pub heartbeat_unregistered: bool,
    pub handoff_rows: i64,
}

// ── Engine ────────────────────────────────────────────────────────────────

pub struct ReleaseEngine {
    pool: SqlitePool,
    heartbeat_registry: Arc<HeartbeatRegistry>,
    pub counters: ReleaseCounters,
    faults: FaultPlan,
    gate: Option<StepGate>,
    worker_id: String,
}

impl ReleaseEngine {
    pub fn new(
        pool: SqlitePool,
        heartbeat_registry: Arc<HeartbeatRegistry>,
        counters: ReleaseCounters,
        faults: FaultPlan,
        gate: Option<StepGate>,
        worker_id: String,
    ) -> Self {
        Self {
            pool,
            heartbeat_registry,
            counters,
            faults,
            gate,
            worker_id,
        }
    }

    /// Idempotently create the six pending step rows for an operation.
    /// Deterministic primary keys make concurrent creation a no-op race.
    pub async fn ensure_steps(&self, ctx: &ReleaseContext) -> Result<(), String> {
        for kind in ReleaseStepKind::ALL {
            sqlx::query(
                "INSERT OR IGNORE INTO verification_release_steps \
                 (release_step_id, finalization_op_id, step_kind, step_order, state, \
                  owner_id, execution_id, fencing_token) \
                 VALUES (?,?,?,?, 'pending', ?,?,?)",
            )
            .bind(step_row_id(&ctx.finalization_op_id, kind))
            .bind(&ctx.finalization_op_id)
            .bind(kind.as_str())
            .bind(kind.order())
            .bind(&ctx.verification_owner_id)
            .bind(&ctx.execution_id)
            .bind(ctx.expected_fencing)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure step {}: {e}", kind.as_str()))?;
        }
        Ok(())
    }

    pub async fn load_step(
        &self,
        op_id: &str,
        kind: ReleaseStepKind,
    ) -> Result<Option<ReleaseStepRow>, String> {
        /// Raw column tuple for a verification_release_steps row.
        type StepRowTuple = (
            String,
            String,
            String,
            Option<String>,
            String,
            String,
            i64,
            i64,
            Option<String>,
        );
        let row: Option<StepRowTuple> = sqlx::query_as(
            "SELECT release_step_id, finalization_op_id, state, worker_id, owner_id, \
                        execution_id, fencing_token, version, result_fingerprint \
                 FROM verification_release_steps \
                 WHERE finalization_op_id=? AND step_kind=?",
        )
        .bind(op_id)
        .bind(kind.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("load step {}: {e}", kind.as_str()))?;
        Ok(row.map(
            |(id, op, state, worker, owner, exec, fencing, version, fp)| ReleaseStepRow {
                release_step_id: id,
                finalization_op_id: op,
                step_kind: kind,
                state: ReleaseStepState::parse(&state),
                worker_id: worker,
                owner_id: owner,
                execution_id: exec,
                fencing_token: fencing,
                version,
                result_fingerprint: fp,
            },
        ))
    }

    /// CAS pending → in_progress. Exactly one worker wins; losers get the
    /// durable truth back and MUST NOT execute the side effect.
    pub async fn claim_step(&self, row: &ReleaseStepRow) -> StepClaimResult {
        let res = sqlx::query(
            "UPDATE verification_release_steps \
             SET state='in_progress', worker_id=?, claimed_at=datetime('now'), \
                 updated_at=datetime('now'), version=version+1 \
             WHERE release_step_id=? AND state='pending' AND version=?",
        )
        .bind(&self.worker_id)
        .bind(&row.release_step_id)
        .bind(row.version)
        .execute(&self.pool)
        .await;

        match res {
            Ok(r) if r.rows_affected() == 1 => StepClaimResult::Acquired {
                release_step_id: row.release_step_id.clone(),
                version: row.version + 1,
            },
            _ => match self.load_step(&row.finalization_op_id, row.step_kind).await {
                Ok(Some(cur)) => match cur.state {
                    ReleaseStepState::Completed => StepClaimResult::AlreadyCompleted {
                        result_fingerprint: cur.result_fingerprint.unwrap_or_default(),
                    },
                    ReleaseStepState::InProgress => StepClaimResult::HeldByOther {
                        worker_id: cur.worker_id.unwrap_or_default(),
                        version: cur.version,
                    },
                    other => StepClaimResult::Conflict {
                        durable_state: other,
                        durable_version: cur.version,
                    },
                },
                _ => StepClaimResult::Conflict {
                    durable_state: ReleaseStepState::Pending,
                    durable_version: row.version,
                },
            },
        }
    }

    /// CAS an in_progress step (crashed worker) to this worker, version-bound.
    /// Only legal AFTER the actual resource state has been probed.
    async fn takeover_step(&self, row: &ReleaseStepRow) -> Option<i64> {
        let res = sqlx::query(
            "UPDATE verification_release_steps \
             SET worker_id=?, claimed_at=datetime('now'), updated_at=datetime('now'), \
                 version=version+1 \
             WHERE release_step_id=? AND state='in_progress' AND version=?",
        )
        .bind(&self.worker_id)
        .bind(&row.release_step_id)
        .bind(row.version)
        .execute(&self.pool)
        .await;
        match res {
            Ok(r) if r.rows_affected() == 1 => Some(row.version + 1),
            _ => None,
        }
    }

    /// CAS in_progress → completed, bound to worker + fencing + version.
    pub async fn complete_step(
        &self,
        row_id: &str,
        expected_version: i64,
        fencing_token: i64,
        result_fingerprint: &str,
    ) -> bool {
        let res = sqlx::query(
            "UPDATE verification_release_steps \
             SET state='completed', completed_at=datetime('now'), \
                 updated_at=datetime('now'), result_fingerprint=?, version=version+1 \
             WHERE release_step_id=? AND state='in_progress' AND worker_id=? \
               AND fencing_token=? AND version=?",
        )
        .bind(result_fingerprint)
        .bind(row_id)
        .bind(&self.worker_id)
        .bind(fencing_token)
        .bind(expected_version)
        .execute(&self.pool)
        .await;
        matches!(res, Ok(r) if r.rows_affected() == 1)
    }

    /// CAS in_progress → failed / reconciliation_required, same binding.
    pub async fn fail_step(
        &self,
        row_id: &str,
        expected_version: i64,
        fencing_token: i64,
        state: ReleaseStepState,
        error_classification: &str,
    ) -> bool {
        let res = sqlx::query(
            "UPDATE verification_release_steps \
             SET state=?, failed_at=datetime('now'), updated_at=datetime('now'), \
                 error_classification=?, version=version+1 \
             WHERE release_step_id=? AND state='in_progress' AND worker_id=? \
               AND fencing_token=? AND version=?",
        )
        .bind(state.as_str())
        .bind(error_classification)
        .bind(row_id)
        .bind(&self.worker_id)
        .bind(fencing_token)
        .bind(expected_version)
        .execute(&self.pool)
        .await;
        matches!(res, Ok(r) if r.rows_affected() == 1)
    }

    // ── Ownership: re-verified before EVERY side effect ───────────────

    /// Read the CURRENT handoff owner/fencing and reject if a new owner took
    /// over or fencing moved. For steps after HandoffRelease the handoff may
    /// legitimately be `released`, but owner + fencing must still match.
    async fn verify_ownership(
        &self,
        ctx: &ReleaseContext,
        kind: ReleaseStepKind,
    ) -> Result<(), String> {
        let row: Option<(String, String, i64, String)> = sqlx::query_as(
            "SELECT owner_kind, owner_id, fencing_token, status \
             FROM resource_handoffs WHERE execution_id=?",
        )
        .bind(&ctx.execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("ownership read: {e}"))?;

        let (owner_kind, owner_id, fencing, status) = match row {
            Some(r) => r,
            None => return Err("handoff missing".into()),
        };
        if owner_kind != "verification" || owner_id != ctx.verification_owner_id {
            return Err(format!("owner changed to {owner_kind}/{owner_id}"));
        }
        if fencing != ctx.expected_fencing {
            return Err(format!(
                "stale fencing: current={fencing} expected={}",
                ctx.expected_fencing
            ));
        }
        let post_handoff = matches!(
            kind,
            ReleaseStepKind::ResourcesReleasedEvent | ReleaseStepKind::OperationCompletion
        );
        if post_handoff {
            if status != "verification_owned" && status != "released" {
                return Err(format!("handoff status {status}"));
            }
        } else if status != "verification_owned" {
            return Err(format!("handoff status {status}, not verification_owned"));
        }

        // Worktree identity: DB record (if any) must still match the filesystem.
        // The engine never creates or deletes worktrees; a vanished worktree
        // mid-release is an anomaly that stops automatic release.
        let wt: Option<(String, String)> =
            sqlx::query_as("SELECT worktree_path, status FROM worktrees WHERE id=?")
                .bind(&ctx.worktree_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("worktree read: {e}"))?;
        if let Some((path, wt_status)) = wt {
            if wt_status == "active" && !path.is_empty() && !std::path::Path::new(&path).exists() {
                return Err(format!("worktree filesystem missing: {path}"));
            }
        }
        Ok(())
    }

    // ── Side effects (identity + owner + fencing bound) ───────────────

    /// Has this step's side effect already been applied to the actual
    /// resources? Ok(true)=applied, Ok(false)=definitely not applied,
    /// Err=indeterminate (never re-run on indeterminate).
    async fn effect_applied(
        &self,
        ctx: &ReleaseContext,
        kind: ReleaseStepKind,
    ) -> Result<bool, String> {
        match kind {
            ReleaseStepKind::ClaimRelease => {
                let claims: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM resource_claims WHERE execution_id=? AND status='active'",
                )
                .bind(&ctx.execution_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("claim probe: {e}"))?;
                let groups: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM resource_claim_groups WHERE execution_id=? AND lifecycle='active'",
                )
                .bind(&ctx.execution_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("claim group probe: {e}"))?;
                Ok(claims.0 == 0 && groups.0 == 0)
            }
            ReleaseStepKind::LeaseRelease => {
                let active: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM workspace_leases WHERE owner_execution_id=? AND lifecycle='acquired'",
                )
                .bind(&ctx.execution_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("lease probe: {e}"))?;
                Ok(active.0 == 0)
            }
            ReleaseStepKind::HeartbeatUnregister => {
                Ok(!self.heartbeat_registry.exists(&ctx.execution_id).await)
            }
            ReleaseStepKind::HandoffRelease => {
                let row: Option<(String, String)> = sqlx::query_as(
                    "SELECT status, owner_id FROM resource_handoffs WHERE execution_id=?",
                )
                .bind(&ctx.execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("handoff probe: {e}"))?;
                match row {
                    Some((status, owner)) => {
                        Ok(status == "released" && owner == ctx.verification_owner_id)
                    }
                    None => Err("handoff missing".into()),
                }
            }
            ReleaseStepKind::ResourcesReleasedEvent => {
                let n: (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM verification_step_events \
                     WHERE verification_run_id=? AND event_type='VerificationResourcesReleased'",
                )
                .bind(&ctx.verification_run_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("event probe: {e}"))?;
                Ok(n.0 > 0)
            }
            ReleaseStepKind::OperationCompletion => {
                let lc: Option<(String,)> = sqlx::query_as(
                    "SELECT lifecycle FROM verification_finalization_operations \
                     WHERE finalization_op_id=?",
                )
                .bind(&ctx.finalization_op_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("op probe: {e}"))?;
                Ok(lc.map(|l| l.0 == "completed").unwrap_or(false))
            }
        }
    }

    /// Execute the actual side effect. Only ever called by the CAS winner.
    /// Every UPDATE binds row identity + owner/execution + live state, and
    /// fencing/version where the schema carries them.
    async fn execute_effect(
        &self,
        ctx: &ReleaseContext,
        kind: ReleaseStepKind,
    ) -> Result<(), String> {
        match kind {
            ReleaseStepKind::ClaimRelease => {
                // Claim groups: full identity + fencing + version CAS.
                let groups: Vec<(String, i64, i64)> = sqlx::query_as(
                    "SELECT group_id, fencing_token, version FROM resource_claim_groups \
                     WHERE execution_id=? AND lifecycle='active'",
                )
                .bind(&ctx.execution_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| format!("claim groups read: {e}"))?;
                for (group_id, fencing, version) in &groups {
                    let r = sqlx::query(
                        "UPDATE resource_claim_groups \
                         SET lifecycle='released', released_at=datetime('now'), \
                             release_reason='verification_finalized', \
                             updated_at=datetime('now'), version=version+1 \
                         WHERE group_id=? AND execution_id=? AND lifecycle='active' \
                           AND fencing_token=? AND version=?",
                    )
                    .bind(group_id)
                    .bind(&ctx.execution_id)
                    .bind(fencing)
                    .bind(version)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| format!("claim group release: {e}"))?;
                    if r.rows_affected() != 1 {
                        return Err(format!("claim group {group_id} CAS lost"));
                    }
                }
                // Individual claims: row identity + execution identity + live state.
                let claims: Vec<(String,)> = sqlx::query_as(
                    "SELECT id FROM resource_claims WHERE execution_id=? AND status='active'",
                )
                .bind(&ctx.execution_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| format!("claims read: {e}"))?;
                let mut released = groups.len() as i64;
                for (claim_id,) in &claims {
                    let r = sqlx::query(
                        "UPDATE resource_claims \
                         SET status='released', released_at=datetime('now') \
                         WHERE id=? AND execution_id=? AND status='active'",
                    )
                    .bind(claim_id)
                    .bind(&ctx.execution_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| format!("claim release: {e}"))?;
                    released += r.rows_affected() as i64;
                }
                if released > 0 {
                    self.counters.claim_release.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                } else if claims.is_empty() && groups.is_empty() {
                    // Nothing to release — claims were already released by a
                    // concurrent engine (or never existed). Do NOT count: the
                    // side effect was counted by whoever performed the actual
                    // release. When two engines race, both would enter this
                    // branch and double-count otherwise.
                    Ok(())
                } else {
                    Err("claim rows changed concurrently".into())
                }
            }
            ReleaseStepKind::LeaseRelease => {
                // Resolve the concrete lease (prefer the handoff's lease_id).
                let lease: Option<(String, Option<i64>)> = sqlx::query_as(
                    "SELECT l.id, l.fencing_token FROM workspace_leases l \
                     WHERE l.owner_execution_id=? AND l.lifecycle='acquired'",
                )
                .bind(&ctx.execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("lease read: {e}"))?;
                let (lease_id, lease_fencing) = match lease {
                    Some(l) => l,
                    // No acquired lease for this execution — it was already
                    // released by a concurrent engine (or a previous run).
                    // Do NOT increment the counter here: the side effect was
                    // already counted by whoever performed the actual UPDATE.
                    // When two engines race through this path both would
                    // increment, producing lease_release == 2.
                    None => {
                        return Ok(());
                    }
                };
                // NULL-safe fencing snapshot bind (`IS ?`): the release only
                // applies if the lease's fencing is exactly what we observed.
                let r = sqlx::query(
                    "UPDATE workspace_leases \
                     SET lifecycle='released', released_at=datetime('now'), \
                         release_reason='verification_finalized', updated_at=datetime('now') \
                     WHERE id=? AND owner_execution_id=? AND lifecycle='acquired' \
                       AND fencing_token IS ?",
                )
                .bind(&lease_id)
                .bind(&ctx.execution_id)
                .bind(lease_fencing)
                .execute(&self.pool)
                .await
                .map_err(|e| format!("lease release: {e}"))?;
                if r.rows_affected() == 1 {
                    self.counters.lease_release.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                } else {
                    // CAS lost — a concurrent engine released the lease first.
                    // Probe the actual resource: if it's now 'released' the
                    // side effect is applied; don't fail the step.
                    match self.effect_applied(ctx, kind).await {
                        Ok(true) => Ok(()),
                        _ => Err(format!("lease {lease_id} CAS lost")),
                    }
                }
            }
            ReleaseStepKind::HeartbeatUnregister => {
                match self
                    .heartbeat_registry
                    .remove_if_matches(
                        &ctx.execution_id,
                        &ctx.verification_owner_id,
                        ctx.expected_fencing,
                    )
                    .await
                {
                    HeartbeatRemoveOutcome::Removed => {
                        self.counters
                            .heartbeat_unregister
                            .fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    HeartbeatRemoveOutcome::NotFound => {
                        // Already removed by a concurrent engine (or a
                        // previous run). Do NOT increment the counter:
                        // the side effect was already counted by whoever
                        // performed the actual removal.  When two engines
                        // race, both would increment here otherwise,
                        // producing heartbeat_unregister == 2 (N1/C8).
                        Ok(())
                    }
                    HeartbeatRemoveOutcome::IdentityMismatch {
                        owner_id,
                        fencing_token,
                    } => Err(format!(
                        "heartbeat identity mismatch: owner={owner_id} fencing={fencing_token}"
                    )),
                }
            }
            ReleaseStepKind::HandoffRelease => {
                let row: Option<(String, i64)> = sqlx::query_as(
                    "SELECT handoff_id, version FROM resource_handoffs WHERE execution_id=?",
                )
                .bind(&ctx.execution_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("handoff read: {e}"))?;
                let (handoff_id, version) = match row {
                    Some(r) => r,
                    None => return Err("handoff missing".into()),
                };
                let r = sqlx::query(
                    "UPDATE resource_handoffs \
                     SET status='released', updated_at=datetime('now'), version=version+1 \
                     WHERE handoff_id=? AND status='verification_owned' \
                       AND owner_kind='verification' AND owner_id=? \
                       AND fencing_token=? AND version=?",
                )
                .bind(&handoff_id)
                .bind(&ctx.verification_owner_id)
                .bind(ctx.expected_fencing)
                .bind(version)
                .execute(&self.pool)
                .await
                .map_err(|e| format!("handoff release: {e}"))?;
                if r.rows_affected() == 1 {
                    self.counters.handoff_release.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                } else {
                    Err(format!("handoff {handoff_id} CAS lost"))
                }
            }
            ReleaseStepKind::ResourcesReleasedEvent => {
                let inserted = write_finalization_event(
                    &self.pool,
                    ctx,
                    "VerificationResourcesReleased",
                    None,
                )
                .await?;
                if inserted {
                    self.counters
                        .resources_released_event
                        .fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
            ReleaseStepKind::OperationCompletion => {
                // Accept 'reconciliation_required' in addition to the normal
                // release-lifecycle states. When two engines race through the
                // release saga, a spurious mark_reconciliation call by one
                // engine (triggered by a complete_step CAS that lost to the
                // other engine) can set the lifecycle to
                // reconciliation_required while the second engine is about to
                // execute this step. Accepting that state here lets the second
                // engine finish the saga instead of failing the step.
                let r = sqlx::query(
                    "UPDATE verification_finalization_operations \
                     SET lifecycle='completed', terminal_at=datetime('now') \
                     WHERE finalization_op_id=? \
                       AND lifecycle IN ('releasing_resources','outcome_persisted','reconciliation_required')",
                )
                .bind(&ctx.finalization_op_id)
                .execute(&self.pool)
                .await
                .map_err(|e| format!("op completion: {e}"))?;
                if r.rows_affected() == 1 {
                    self.counters
                        .operation_completion
                        .fetch_add(1, Ordering::SeqCst);
                    Ok(())
                } else {
                    match self.effect_applied(ctx, kind).await {
                        Ok(true) => Ok(()),
                        _ => Err("operation completion CAS lost".into()),
                    }
                }
            }
        }
    }

    /// Run (or resume) the six-step release saga. Exactly-once per side
    /// effect is guaranteed by the per-step claim CAS; interrupted steps are
    /// resolved by probing the ACTUAL resource state before any re-execution.
    pub async fn run_release(&self, ctx: &ReleaseContext) -> ReleaseRunOutcome {
        if let Err(e) = self.ensure_steps(ctx).await {
            return ReleaseRunOutcome::InfrastructureError { reason: e };
        }
        // Summary lifecycle (state-CAS'd; no-op when already past this point).
        let _ = sqlx::query(
            "UPDATE verification_finalization_operations \
             SET lifecycle='releasing_resources' \
             WHERE finalization_op_id=? AND lifecycle='outcome_persisted'",
        )
        .bind(&ctx.finalization_op_id)
        .execute(&self.pool)
        .await;

        let mut executed: Vec<&'static str> = Vec::new();

        for kind in ReleaseStepKind::ALL {
            if self.faults.take(kind, FaultMode::CrashBeforeClaim) {
                return ReleaseRunOutcome::Crashed { step: kind };
            }
            let row = match self.load_step(&ctx.finalization_op_id, kind).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    return ReleaseRunOutcome::InfrastructureError {
                        reason: format!("step row missing: {}", kind.as_str()),
                    }
                }
                Err(e) => return ReleaseRunOutcome::InfrastructureError { reason: e },
            };

            let claimed_version = match row.state {
                ReleaseStepState::Completed => continue,
                ReleaseStepState::Failed | ReleaseStepState::ReconciliationRequired => {
                    return ReleaseRunOutcome::ReconciliationRequired {
                        step: kind,
                        reason: "step previously failed".into(),
                    }
                }
                ReleaseStepState::InProgress => {
                    // A worker crashed while holding this step. NEVER blindly
                    // re-execute: probe the actual resource state first.
                    match self.effect_applied(ctx, kind).await {
                        Ok(true) => {
                            // Side effect provably applied — record completion
                            // WITHOUT re-executing.
                            match self.takeover_step(&row).await {
                                Some(v) => {
                                    if !self
                                        .complete_step(
                                            &row.release_step_id,
                                            v,
                                            row.fencing_token,
                                            "recovered_already_applied",
                                        )
                                        .await
                                    {
                                        return ReleaseRunOutcome::HeldByOther {
                                            step: kind,
                                            worker_id: "unknown".into(),
                                        };
                                    }
                                    continue;
                                }
                                None => {
                                    return ReleaseRunOutcome::HeldByOther {
                                        step: kind,
                                        worker_id: row.worker_id.unwrap_or_default(),
                                    }
                                }
                            }
                        }
                        Ok(false) => {
                            // Definitely not applied. Ownership must still be
                            // fully intact before this worker may take over.
                            if let Err(reason) = self.verify_ownership(ctx, kind).await {
                                let _ = self
                                    .fail_step(
                                        &row.release_step_id,
                                        row.version,
                                        row.fencing_token,
                                        ReleaseStepState::ReconciliationRequired,
                                        &reason,
                                    )
                                    .await;
                                let _ = write_finalization_event(
                                    &self.pool,
                                    ctx,
                                    "VerificationResourceReleaseFailed",
                                    Some(kind.as_str()),
                                )
                                .await;
                                return ReleaseRunOutcome::OwnershipLost { step: kind, reason };
                            }
                            match self.takeover_step(&row).await {
                                Some(v) => v,
                                None => {
                                    return ReleaseRunOutcome::HeldByOther {
                                        step: kind,
                                        worker_id: row.worker_id.unwrap_or_default(),
                                    }
                                }
                            }
                        }
                        Err(reason) => {
                            // Indeterminate — never re-run on a guess.
                            let _ = self
                                .fail_step(
                                    &row.release_step_id,
                                    row.version,
                                    row.fencing_token,
                                    ReleaseStepState::ReconciliationRequired,
                                    &reason,
                                )
                                .await;
                            let _ = write_finalization_event(
                                &self.pool,
                                ctx,
                                "VerificationResourceReleaseFailed",
                                Some(kind.as_str()),
                            )
                            .await;
                            return ReleaseRunOutcome::ReconciliationRequired {
                                step: kind,
                                reason,
                            };
                        }
                    }
                }
                ReleaseStepState::Pending => match self.claim_step(&row).await {
                    StepClaimResult::Acquired { version, .. } => version,
                    StepClaimResult::AlreadyCompleted { .. } => continue,
                    StepClaimResult::HeldByOther { worker_id, .. } => {
                        return ReleaseRunOutcome::HeldByOther {
                            step: kind,
                            worker_id,
                        }
                    }
                    StepClaimResult::Conflict { .. } => {
                        // Reload once: the concurrent writer may have completed it.
                        match self.load_step(&ctx.finalization_op_id, kind).await {
                            Ok(Some(cur)) if cur.state == ReleaseStepState::Completed => continue,
                            Ok(Some(cur)) if cur.state == ReleaseStepState::InProgress => {
                                return ReleaseRunOutcome::HeldByOther {
                                    step: kind,
                                    worker_id: cur.worker_id.unwrap_or_default(),
                                }
                            }
                            _ => {
                                return ReleaseRunOutcome::ReconciliationRequired {
                                    step: kind,
                                    reason: "step claim conflict".into(),
                                }
                            }
                        }
                    }
                },
            };

            // ── CAS winner path: ownership re-verified BEFORE the effect ──
            if let Err(reason) = self.verify_ownership(ctx, kind).await {
                let _ = self
                    .fail_step(
                        &row.release_step_id,
                        claimed_version,
                        row.fencing_token,
                        ReleaseStepState::ReconciliationRequired,
                        &reason,
                    )
                    .await;
                let _ = write_finalization_event(
                    &self.pool,
                    ctx,
                    "VerificationResourceReleaseFailed",
                    Some(kind.as_str()),
                )
                .await;
                return ReleaseRunOutcome::OwnershipLost { step: kind, reason };
            }

            if let Some(gate) = &self.gate {
                gate.pass(kind).await;
            }
            if self.faults.take(kind, FaultMode::CrashBeforeEffect) {
                return ReleaseRunOutcome::Crashed { step: kind };
            }
            if self.faults.take(kind, FaultMode::FailEffect) {
                let _ = self
                    .fail_step(
                        &row.release_step_id,
                        claimed_version,
                        row.fencing_token,
                        ReleaseStepState::Failed,
                        "injected_repository_failure",
                    )
                    .await;
                let _ = write_finalization_event(
                    &self.pool,
                    ctx,
                    "VerificationResourceReleaseFailed",
                    Some(kind.as_str()),
                )
                .await;
                return ReleaseRunOutcome::ReconciliationRequired {
                    step: kind,
                    reason: format!("injected failure at {}", kind.as_str()),
                };
            }

            if let Err(reason) = self.execute_effect(ctx, kind).await {
                let _ = self
                    .fail_step(
                        &row.release_step_id,
                        claimed_version,
                        row.fencing_token,
                        ReleaseStepState::Failed,
                        &reason,
                    )
                    .await;
                let _ = write_finalization_event(
                    &self.pool,
                    ctx,
                    "VerificationResourceReleaseFailed",
                    Some(kind.as_str()),
                )
                .await;
                return ReleaseRunOutcome::ReconciliationRequired { step: kind, reason };
            }

            if self.faults.take(kind, FaultMode::CrashAfterEffect) {
                return ReleaseRunOutcome::Crashed { step: kind };
            }

            if !self
                .complete_step(
                    &row.release_step_id,
                    claimed_version,
                    row.fencing_token,
                    "applied",
                )
                .await
            {
                // The completion CAS may have lost to a concurrent engine that
                // took over and completed the step. If the step is now durably
                // 'completed', the side effect was already applied (and counted)
                // by THIS engine — just continue; do NOT spuriously escalate to
                // ReconciliationRequired, which would call mark_reconciliation
                // and corrupt the operation lifecycle out from under the
                // concurrent engine.
                match self.load_step(&ctx.finalization_op_id, kind).await {
                    Ok(Some(cur)) if cur.state == ReleaseStepState::Completed => {
                        // Another engine completed the step — continue.
                    }
                    _ => {
                        return ReleaseRunOutcome::ReconciliationRequired {
                            step: kind,
                            reason: "completion CAS lost".into(),
                        };
                    }
                }
            }
            executed.push(kind.as_str());
            self.write_summary(ctx).await;
        }

        ReleaseRunOutcome::Completed { executed }
    }

    /// Human-readable summary of durable step state. NOT the execution
    /// authority — verification_release_steps rows are.
    async fn write_summary(&self, ctx: &ReleaseContext) {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT step_kind, state FROM verification_release_steps \
             WHERE finalization_op_id=? ORDER BY step_order",
        )
        .bind(&ctx.finalization_op_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        let mut progress = ReleaseProgress::default();
        for kind in ReleaseStepKind::ALL {
            if rows
                .iter()
                .any(|(k, s)| k == kind.as_str() && s == "completed")
            {
                progress.completed_steps.push(kind.legacy_label().into());
            }
        }
        if let Some((k, _)) = rows
            .iter()
            .find(|(_, s)| s == "failed" || s == "reconciliation_required")
        {
            progress.failed_step = Some(k.clone());
        }
        progress.heartbeat_unregistered = progress
            .completed_steps
            .iter()
            .any(|s| s == "HeartbeatUnregistered");
        let json = serde_json::to_string(&progress).unwrap_or_default();
        let _ = sqlx::query(
            "UPDATE verification_finalization_operations \
             SET release_progress_json=?, resources_released_at=datetime('now') \
             WHERE finalization_op_id=?",
        )
        .bind(&json)
        .bind(&ctx.finalization_op_id)
        .execute(&self.pool)
        .await;
    }
}

/// Deterministic step row id — concurrent ensure_steps insertions collapse.
pub fn step_row_id(op_id: &str, kind: ReleaseStepKind) -> String {
    format!("rs-{}-{}", op_id, kind.as_str())
}

/// Shared finalization-event writer. Deterministic idempotency key
/// (`final-ev-<run>-<type>`) + INSERT OR IGNORE make writes exactly-once
/// across Finalizer AND Reconciler. Returns whether a row was inserted.
pub async fn write_finalization_event(
    pool: &SqlitePool,
    ctx: &ReleaseContext,
    event_type: &str,
    detail: Option<&str>,
) -> Result<bool, String> {
    // Synthetic step_op row so the FK on verification_step_events is satisfied.
    sqlx::query(
        "INSERT OR IGNORE INTO verification_step_operations \
         (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, \
          worktree_id, fencing_token, status, idempotency_key, request_hash) \
         VALUES (?,?,?,?,?,?,?,?,'finalization',?,?)",
    )
    .bind(&ctx.finalization_op_id)
    .bind(&ctx.verification_run_id)
    .bind("finalization")
    .bind("plan-final")
    .bind(&ctx.execution_id)
    .bind("final-cfg")
    .bind(&ctx.worktree_id)
    .bind(ctx.expected_fencing)
    .bind(&ctx.finalization_op_id)
    .bind(&ctx.request_hash)
    .execute(pool)
    .await
    .map_err(|e| format!("synthetic step_op: {e}"))?;

    let eid = format!("evt-final-{}", uuid::Uuid::new_v4());
    let ikey = format!("final-ev-{}-{}", ctx.verification_run_id, event_type);
    let r = sqlx::query(
        "INSERT OR IGNORE INTO verification_step_events \
         (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, \
          worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(&eid)
    .bind(&ctx.verification_run_id)
    .bind("finalization")
    .bind(&ctx.finalization_op_id)
    .bind(&ctx.execution_id)
    .bind(&ctx.task_id)
    .bind(&ctx.worktree_id)
    .bind(ctx.expected_fencing)
    .bind(event_type)
    .bind("finalization")
    .bind(detail)
    .bind(&ikey)
    .execute(pool)
    .await
    .map_err(|e| format!("finalization event {event_type}: {e}"))?;
    Ok(r.rows_affected() == 1)
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::scheduler::heartbeat_registry::{HeartbeatEntry, HeartbeatStatus, OwnerKind};

    struct Ctx {
        db: Database,
        hb: Arc<HeartbeatRegistry>,
        ctx: ReleaseContext,
    }

    async fn setup() -> Ctx {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("rel.db");
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
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash,outcome_json,completed_at) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','completed','ik-r','hr','{}',datetime('now'))").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_finalization_operations(finalization_op_id,verification_run_id,idempotency_key,request_hash,worktree_id,fencing_token,owner_id,lifecycle) VALUES('fo-1','run-1','ik-fo','h-fo','wt1',5,'verify-run-1','outcome_persisted')").execute(&p).await.unwrap();
        let hb = Arc::new(HeartbeatRegistry::new());
        let ctx = ReleaseContext {
            finalization_op_id: "fo-1".into(),
            verification_run_id: "run-1".into(),
            execution_id: "e1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            worktree_id: "wt1".into(),
            expected_fencing: 5,
            verification_owner_id: "verify-run-1".into(),
            request_hash: "h-fo".into(),
        };
        Ctx { db, hb, ctx }
    }

    fn engine(c: &Ctx, worker: &str) -> ReleaseEngine {
        ReleaseEngine::new(
            c.db.pool.clone(),
            c.hb.clone(),
            ReleaseCounters::default(),
            FaultPlan::default(),
            None,
            worker.into(),
        )
    }

    async fn register_heartbeat(c: &Ctx) {
        let entry = HeartbeatEntry {
            execution_id: "e1".into(),
            task_id: "t1".into(),
            worktree_id: "wt1".into(),
            lease_id: "l1".into(),
            claim_group_id: None,
            fencing_token: 5,
            owner_kind: OwnerKind::Verification,
            owner_id: "verify-run-1".into(),
            status: HeartbeatStatus::Healthy,
            last_heartbeat_at: None,
            cancel_token: tokio_util::sync::CancellationToken::new(),
            last_error: None,
        };
        c.hb.register(entry).await.unwrap();
    }

    #[tokio::test]
    async fn test_claim_cas_single_winner() {
        let c = setup().await;
        let e1 = engine(&c, "w1");
        let e2 = engine(&c, "w2");
        e1.ensure_steps(&c.ctx).await.unwrap();
        let row = e1
            .load_step("fo-1", ReleaseStepKind::ClaimRelease)
            .await
            .unwrap()
            .unwrap();
        let r1 = e1.claim_step(&row).await;
        assert!(matches!(r1, StepClaimResult::Acquired { .. }), "{r1:?}");
        // Second worker with the SAME observed version loses and learns the holder.
        let r2 = e2.claim_step(&row).await;
        match r2 {
            StepClaimResult::HeldByOther { worker_id, .. } => assert_eq!(worker_id, "w1"),
            other => panic!("loser must see HeldByOther, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_complete_cas_bound_to_worker_and_fencing() {
        let c = setup().await;
        let e1 = engine(&c, "w1");
        let e2 = engine(&c, "w2");
        e1.ensure_steps(&c.ctx).await.unwrap();
        let row = e1
            .load_step("fo-1", ReleaseStepKind::ClaimRelease)
            .await
            .unwrap()
            .unwrap();
        let v = match e1.claim_step(&row).await {
            StepClaimResult::Acquired { version, .. } => version,
            other => panic!("{other:?}"),
        };
        // Wrong worker cannot complete.
        assert!(!e2.complete_step(&row.release_step_id, v, 5, "x").await);
        // Wrong fencing cannot complete.
        assert!(!e1.complete_step(&row.release_step_id, v, 99, "x").await);
        // Wrong version cannot complete.
        assert!(!e1.complete_step(&row.release_step_id, v + 7, 5, "x").await);
        // Exact binding completes.
        assert!(e1.complete_step(&row.release_step_id, v, 5, "x").await);
    }

    #[tokio::test]
    async fn test_full_release_all_steps_exactly_once() {
        let c = setup().await;
        register_heartbeat(&c).await;
        let e1 = engine(&c, "w1");
        let out = e1.run_release(&c.ctx).await;
        assert!(
            matches!(out, ReleaseRunOutcome::Completed { .. }),
            "{out:?}"
        );
        assert_eq!(e1.counters.snapshot(), [1, 1, 1, 1, 1, 1]);
        // Re-running the saga is a durable no-op: zero new side effects.
        let e2 = engine(&c, "w2");
        let out2 = e2.run_release(&c.ctx).await;
        assert!(
            matches!(out2, ReleaseRunOutcome::Completed { .. }),
            "{out2:?}"
        );
        assert_eq!(e2.counters.snapshot(), [0, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_owner_change_stops_before_side_effect() {
        let c = setup().await;
        // Takeover happens BEFORE release starts executing effects.
        sqlx::query("UPDATE resource_handoffs SET owner_kind='scheduler', owner_id='other', fencing_token=9 WHERE handoff_id='ho-1'")
            .execute(&c.db.pool).await.unwrap();
        let e1 = engine(&c, "w1");
        let out = e1.run_release(&c.ctx).await;
        assert!(
            matches!(out, ReleaseRunOutcome::OwnershipLost { .. }),
            "{out:?}"
        );
        assert_eq!(e1.counters.snapshot(), [0, 0, 0, 0, 0, 0]);
        // The new owner's resources were NOT touched.
        let claim: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(claim.0, "active");
        let lease: (String,) =
            sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        assert_eq!(lease.0, "acquired");
        // Step is durably marked reconciliation_required.
        let st: (String,) = sqlx::query_as(
            "SELECT state FROM verification_release_steps WHERE finalization_op_id='fo-1' AND step_kind='claim_release'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(st.0, "reconciliation_required");
    }

    #[tokio::test]
    async fn test_in_progress_with_effect_applied_completes_without_rerun() {
        let c = setup().await;
        let e1 = engine(&c, "w1");
        // Crash after the Claim effect, before completion CAS.
        let faults = FaultPlan::default();
        faults.inject(ReleaseStepKind::ClaimRelease, FaultMode::CrashAfterEffect);
        let crashed = ReleaseEngine::new(
            c.db.pool.clone(),
            c.hb.clone(),
            ReleaseCounters::default(),
            faults,
            None,
            "w-crash".into(),
        );
        let out = crashed.run_release(&c.ctx).await;
        assert!(matches!(out, ReleaseRunOutcome::Crashed { .. }));
        assert_eq!(crashed.counters.snapshot()[0], 1, "effect executed once");
        // Resume with a fresh worker: MUST NOT re-execute the claim effect.
        register_heartbeat(&c).await;
        let out2 = e1.run_release(&c.ctx).await;
        assert!(
            matches!(out2, ReleaseRunOutcome::Completed { .. }),
            "{out2:?}"
        );
        assert_eq!(
            e1.counters.snapshot(),
            [0, 1, 1, 1, 1, 1],
            "claim effect count == 0 on resume"
        );
    }

    #[tokio::test]
    async fn test_in_progress_not_applied_retaken_and_executed_once() {
        let c = setup().await;
        // Crash after claiming the step but BEFORE the side effect.
        let faults = FaultPlan::default();
        faults.inject(ReleaseStepKind::ClaimRelease, FaultMode::CrashBeforeEffect);
        let crashed = ReleaseEngine::new(
            c.db.pool.clone(),
            c.hb.clone(),
            ReleaseCounters::default(),
            faults,
            None,
            "w-crash".into(),
        );
        let out = crashed.run_release(&c.ctx).await;
        assert!(matches!(out, ReleaseRunOutcome::Crashed { .. }));
        assert_eq!(crashed.counters.snapshot()[0], 0);
        let st: (String,) = sqlx::query_as(
            "SELECT state FROM verification_release_steps WHERE finalization_op_id='fo-1' AND step_kind='claim_release'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(st.0, "in_progress");
        // Resume: ownership intact + resource provably active → takeover + run once.
        let e1 = engine(&c, "w1");
        let out2 = e1.run_release(&c.ctx).await;
        assert!(
            matches!(out2, ReleaseRunOutcome::Completed { .. }),
            "{out2:?}"
        );
        assert_eq!(e1.counters.snapshot()[0], 1);
    }

    #[tokio::test]
    async fn test_heartbeat_identity_mismatch_not_removed() {
        let c = setup().await;
        // Register a heartbeat that belongs to a DIFFERENT owner/fencing epoch.
        let entry = HeartbeatEntry {
            execution_id: "e1".into(),
            task_id: "t1".into(),
            worktree_id: "wt1".into(),
            lease_id: "l1".into(),
            claim_group_id: None,
            fencing_token: 42,
            owner_kind: OwnerKind::Scheduler,
            owner_id: "someone-else".into(),
            status: HeartbeatStatus::Healthy,
            last_heartbeat_at: None,
            cancel_token: tokio_util::sync::CancellationToken::new(),
            last_error: None,
        };
        c.hb.register(entry).await.unwrap();
        let e1 = engine(&c, "w1");
        let out = e1.run_release(&c.ctx).await;
        assert!(
            matches!(out, ReleaseRunOutcome::ReconciliationRequired { .. }),
            "{out:?}"
        );
        // Foreign heartbeat retained; unregister count is zero.
        assert!(c.hb.exists("e1").await);
        assert_eq!(e1.counters.snapshot()[2], 0);
    }

    #[tokio::test]
    async fn test_claim_group_fenced_release() {
        let c = setup().await;
        sqlx::query("INSERT INTO resource_claim_groups(group_id,project_id,task_id,execution_id,repository_identity,fencing_token,request_hash,lifecycle) VALUES('g1','p1','t1','e1','/repo',7,'rh','active')")
            .execute(&c.db.pool).await.unwrap();
        register_heartbeat(&c).await;
        let e1 = engine(&c, "w1");
        let out = e1.run_release(&c.ctx).await;
        assert!(
            matches!(out, ReleaseRunOutcome::Completed { .. }),
            "{out:?}"
        );
        let g: (String, i64) = sqlx::query_as(
            "SELECT lifecycle, version FROM resource_claim_groups WHERE group_id='g1'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(g.0, "released");
        assert_eq!(g.1, 2, "group version CAS bumped exactly once");
    }
}
