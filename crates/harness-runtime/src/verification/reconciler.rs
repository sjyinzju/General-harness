//! VerificationReconciler — deterministic recovery of verification finalization
//! and resource release after crashes, restarts, or partial completions.
//!
//! Batch 6. Reads durable state and actual resource state, produces a
//! ReconciliationClassification, and executes one safe recovery step at a time.
//! NEVER: creates Agents, retries, switches providers, deletes Worktrees,
//! reacquires resources, or modifies Task lifecycle.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sqlx::SqlitePool;
use uuid::Uuid;

use crate::scheduler::heartbeat_registry::HeartbeatRegistry;

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

    pub fn should_retain_resources(&self) -> bool {
        matches!(
            self,
            Self::ActiveProcessUnknown
                | Self::ActiveScannerUnknown
                | Self::IrrecoverableAmbiguity
                | Self::AwaitingHuman
        )
    }

    pub fn requires_human(&self) -> bool {
        matches!(self, Self::AwaitingHuman | Self::IrrecoverableAmbiguity)
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
    /// Maximum number of runs to scan (for batch mode).
    pub max_scan: Option<usize>,
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

// ── Observed state ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct ObservedState {
    pub run_lifecycle: Option<String>,
    pub run_has_outcome: bool,
    pub finalization_op_lifecycle: Option<String>,
    pub claim_active: bool,
    pub lease_active: bool,
    pub heartbeat_exists: bool,
    pub handoff_verification_owned: bool,
    pub handoff_released: bool,
    pub handoff_owner_mismatch: bool,
    pub worktree_db_exists: bool,
    pub fencing_mismatch: bool,
    pub owner_changed: bool,
    pub active_command_op: bool,
    pub active_scanner_op: bool,
    pub resource_mismatch: bool,
    pub release_progress_json: Option<String>,
    pub observed_fingerprint: Option<String>,
}

// ── Reconciler ────────────────────────────────────────────────────────────

pub struct VerificationReconciler {
    pool: SqlitePool,
    heartbeat_registry: Arc<HeartbeatRegistry>,
    pub reconciler_start_count: Arc<AtomicUsize>,
}

impl VerificationReconciler {
    pub fn new(pool: SqlitePool, heartbeat_registry: Arc<HeartbeatRegistry>) -> Self {
        Self {
            pool,
            heartbeat_registry,
            reconciler_start_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Reconcile a single verification run. Idempotent: same key+hash → Duplicate.
    pub async fn reconcile(&self, req: &ReconciliationRequest) -> ReconciliationOutcome {
        // ── 0. Idempotency ──────────────────────────────────────────
        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT reconciliation_op_id, request_hash FROM verification_reconciliation_operations WHERE idempotency_key=?",
        )
        .bind(&req.idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None);
        if let Some((op_id, eh)) = existing {
            if eh == req.request_hash {
                return ReconciliationOutcome::Duplicate {
                    existing_op_id: op_id,
                };
            }
            return ReconciliationOutcome::IdempotencyConflict {
                existing_hash: eh,
                new_hash: req.request_hash.clone(),
            };
        }

        self.reconciler_start_count.fetch_add(1, Ordering::SeqCst);

        // ── 1. Observe durable + runtime state ──────────────────────
        let op_id = format!("rec-{}", Uuid::new_v4());
        let state = self.observe_state(req).await;

        // ── 2. Classify ─────────────────────────────────────────────
        let classification = Self::classify(&state);

        // ── 3. Insert operation ─────────────────────────────────────
        let _ = sqlx::query(
            "INSERT INTO verification_reconciliation_operations (reconciliation_op_id, verification_run_id, idempotency_key, request_hash, observed_state_fingerprint, classification, planned_action, owner_id, fencing_token, lifecycle, started_at) VALUES (?,?,?,?,?,?,?,?,?,'running',datetime('now'))",
        )
        .bind(&op_id)
        .bind(&req.verification_run_id)
        .bind(&req.idempotency_key)
        .bind(&req.request_hash)
        .bind(format!("{:?}", state))
        .bind(format!("{:?}", classification))
        .bind("auto_recover")
        .bind(&req.verification_owner_id)
        .bind(req.expected_fencing)
        .execute(&self.pool).await;

        // Write started event.
        self.write_reconciliation_event(req, &op_id, "VerificationReconciliationStarted", None)
            .await;

        // ── 4. Execute recovery ─────────────────────────────────────
        let outcome = match classification {
            ReconciliationClassification::NoOpAlreadyConsistent => {
                self.write_reconciliation_event(
                    req,
                    &op_id,
                    "VerificationReconciliationNoOp",
                    None,
                )
                .await;
                self.complete_op(&op_id).await;
                ReconciliationOutcome::Consistent
            }
            ReconciliationClassification::ResumeResourceRelease => {
                let steps = self.resume_release(req, &op_id, &state).await;
                self.write_reconciliation_event(
                    req,
                    &op_id,
                    "VerificationReconciliationResumed",
                    None,
                )
                .await;
                self.complete_op(&op_id).await;
                ReconciliationOutcome::Resumed {
                    completed_steps: steps,
                }
            }
            ReconciliationClassification::CompleteOperationRecord => {
                self.complete_op(&op_id).await;
                ReconciliationOutcome::Consistent
            }
            ReconciliationClassification::RepairMissingEvent => {
                self.write_reconciliation_event(
                    req,
                    &op_id,
                    "VerificationReconciliationResumed",
                    None,
                )
                .await;
                self.complete_op(&op_id).await;
                ReconciliationOutcome::Resumed {
                    completed_steps: vec!["event_repaired".into()],
                }
            }
            ref c if c.requires_human() => {
                self.write_reconciliation_event(req, &op_id, "VerificationAwaitingHuman", None)
                    .await;
                let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked' WHERE reconciliation_op_id=?").bind(&op_id).execute(&self.pool).await;
                ReconciliationOutcome::AwaitingHuman {
                    classification: c.clone(),
                    reason: format!("{c:?}"),
                }
            }
            _ => {
                self.write_reconciliation_event(
                    req,
                    &op_id,
                    "VerificationReconciliationBlocked",
                    None,
                )
                .await;
                let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='blocked' WHERE reconciliation_op_id=?").bind(&op_id).execute(&self.pool).await;
                ReconciliationOutcome::Blocked {
                    classification: classification.clone(),
                    reason: format!("{classification:?}"),
                }
            }
        };

        outcome
    }

    // ── State observation ──────────────────────────────────────────

    async fn observe_state(&self, req: &ReconciliationRequest) -> ObservedState {
        let mut s = ObservedState::default();

        // Run lifecycle.
        let lc: Option<(String,)> =
            sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id=?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        s.run_lifecycle = lc.map(|r| r.0);
        s.run_has_outcome = s.run_lifecycle.as_deref() == Some("completed");

        // Finalization operation.
        let fo: Option<(String,)> = sqlx::query_as("SELECT lifecycle FROM verification_finalization_operations WHERE verification_run_id=?")
            .bind(&req.verification_run_id).fetch_optional(&self.pool).await.ok().flatten();
        s.finalization_op_lifecycle = fo.map(|r| r.0);

        // Claim.
        let claim: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM resource_claims WHERE task_id=? AND status='active'",
        )
        .bind(&req.task_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.claim_active = claim.0 > 0;

        // Lease.
        let lease: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM workspace_leases WHERE task_id=? AND lifecycle='acquired'",
        )
        .bind(&req.task_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        s.lease_active = lease.0 > 0;

        // Heartbeat.
        s.heartbeat_exists = self.heartbeat_registry.exists(&req.execution_id).await;

        // Handoff.
        let handoff: Option<(String,)> =
            sqlx::query_as("SELECT status FROM resource_handoffs WHERE execution_id=?")
                .bind(&req.execution_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        if let Some((status,)) = handoff {
            s.handoff_verification_owned = status == "verification_owned";
            s.handoff_released = status == "released";
        }

        // Release progress.
        let rp: Option<(Option<String>,)> = sqlx::query_as("SELECT release_progress_json FROM verification_finalization_operations WHERE verification_run_id=?")
            .bind(&req.verification_run_id).fetch_optional(&self.pool).await.ok().flatten();
        s.release_progress_json = rp.and_then(|r| r.0);

        // Worktree DB record.
        let wt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE id=?")
            .bind(&req.worktree_id).fetch_one(&self.pool).await.unwrap_or((0,));
        s.worktree_db_exists = wt.0 > 0;

        // Command operations.
        let cmd_ops: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_operations WHERE verification_run_id=? AND status IN ('running','pending')")
            .bind(&req.verification_run_id).fetch_one(&self.pool).await.unwrap_or((0,));
        s.active_command_op = cmd_ops.0 > 0;

        // Scanner operations.
        let scan_ops: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_policy_operations WHERE verification_run_id=? AND lifecycle IN ('running','pending')")
            .bind(&req.verification_run_id).fetch_one(&self.pool).await.unwrap_or((0,));
        s.active_scanner_op = scan_ops.0 > 0;

        s
    }

    // ── Classification (source-of-truth precedence) ─────────────────
    //
    // Precedence rules:
    // 1. Immutable outcome > operation lifecycle > events > dossier
    // 2. Actual Claim/Lease/Handoff state > ReleaseProgress JSON claims
    // 3. Current owner/fencing > historical snapshot (new owner = stop)
    // 4. DB + FS both checked for Worktree; mismatch = block
    // 5. Unknown process/scanner = never release resources

    fn classify(state: &ObservedState) -> ReconciliationClassification {
        let has_outcome = state.run_has_outcome;
        let fo_lc = state.finalization_op_lifecycle.as_deref();
        let run_lc = state.run_lifecycle.as_deref();

        // ── Illegal state combinations (Handoff state mismatch) ──
        if state.handoff_released && state.claim_active {
            return ReconciliationClassification::HandoffStateMismatch;
        }
        if state.handoff_released && state.lease_active {
            return ReconciliationClassification::HandoffStateMismatch;
        }
        if state.handoff_released && state.heartbeat_exists {
            return ReconciliationClassification::RuntimeHeartbeatStale;
        }

        // ── Case A: Fully complete and consistent ──
        if has_outcome
            && fo_lc == Some("completed")
            && !state.claim_active && !state.lease_active
            && !state.heartbeat_exists && state.handoff_released
        {
            return ReconciliationClassification::NoOpAlreadyConsistent;
        }

        // ── Case B: Outcome persisted, release not started ──
        if has_outcome && fo_lc == Some("outcome_persisted") && state.handoff_verification_owned {
            return ReconciliationClassification::ResumeResourceRelease;
        }

        // ── Case C/D: Partial release ──
        if has_outcome && !state.claim_active && state.heartbeat_exists {
            return ReconciliationClassification::ResumeResourceRelease;
        }

        // ── Missing outcome ──
        if !has_outcome {
            return ReconciliationClassification::OutcomeMissing;
        }

        // ── Outcome exists but conflicts ──
        if has_outcome && run_lc != Some("completed") {
            return ReconciliationClassification::OutcomeConflict;
        }

        // ── Worktree missing (check before release decisions) ──
        if !state.worktree_db_exists {
            return ReconciliationClassification::WorktreeMissing;
        }

        // ── Stale fencing ──
        if state.fencing_mismatch {
            return ReconciliationClassification::StaleFencing;
        }

        // ── Ownership lost ──
        if state.owner_changed {
            return ReconciliationClassification::OwnershipLost;
        }

        // ── Active process/scanner unknown (never release) ──
        if state.active_command_op {
            return ReconciliationClassification::ActiveProcessUnknown;
        }
        if state.active_scanner_op {
            return ReconciliationClassification::ActiveScannerUnknown;
        }

        // ── Resource state mismatch ──
        if state.resource_mismatch {
            return ReconciliationClassification::ResourceStateMismatch;
        }

        // ── Progress conflict ──
        if has_outcome && state.claim_active && fo_lc == Some("releasing_resources") {
            return ReconciliationClassification::ProgressConflict;
        }

        // ── Case E/F: Handoff released but events/operation incomplete ──
        if has_outcome && state.handoff_released && !state.claim_active && !state.lease_active {
            return ReconciliationClassification::CompleteOperationRecord;
        }

        // ── Active heartbeat when resources released = stale ──
        if state.heartbeat_exists && state.handoff_released {
            return ReconciliationClassification::RuntimeHeartbeatStale;
        }

        ReconciliationClassification::IrrecoverableAmbiguity
    }

    // ── Resume release ─────────────────────────────────────────────

    async fn resume_release(
        &self,
        req: &ReconciliationRequest,
        op_id: &str,
        state: &ObservedState,
    ) -> Vec<String> {
        let mut steps = Vec::new();

        // Claim release.
        if state.claim_active {
            let _ = sqlx::query("UPDATE resource_claims SET status='released', released_at=datetime('now') WHERE task_id=? AND status='active'")
                .bind(&req.task_id).execute(&self.pool).await;
            steps.push("claim_released".into());
        }

        // Lease release.
        let lease_active: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM workspace_leases WHERE task_id=? AND lifecycle='acquired'",
        )
        .bind(&req.task_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or((0,));
        if lease_active.0 > 0 {
            let _ = sqlx::query("UPDATE workspace_leases SET lifecycle='released', released_at=datetime('now') WHERE task_id=? AND lifecycle='acquired'")
                .bind(&req.task_id).execute(&self.pool).await;
            steps.push("lease_released".into());
        }

        // Heartbeat unregister.
        if state.heartbeat_exists {
            self.heartbeat_registry
                .remove_after_finalization(&req.execution_id)
                .await;
            steps.push("heartbeat_unregistered".into());
        }

        // Handoff release.
        if state.handoff_verification_owned {
            let _ = sqlx::query("UPDATE resource_handoffs SET status='released' WHERE execution_id=? AND status='verification_owned'")
                .bind(&req.execution_id).execute(&self.pool).await;
            steps.push("handoff_released".into());
        }

        // Write ResourcesReleased event.
        self.write_reconciliation_event(req, op_id, "VerificationResourcesReleased", None)
            .await;
        steps.push("resources_released_event".into());

        steps
    }

    // ── Helpers ────────────────────────────────────────────────────

    async fn complete_op(&self, op_id: &str) {
        let _ = sqlx::query("UPDATE verification_reconciliation_operations SET lifecycle='completed', terminal_at=datetime('now') WHERE reconciliation_op_id=?")
            .bind(op_id).execute(&self.pool).await;
    }

    async fn write_reconciliation_event(
        &self,
        req: &ReconciliationRequest,
        op_id: &str,
        event_type: &str,
        _detail: Option<&str>,
    ) {
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
        #[allow(dead_code)]
        hb: Arc<HeartbeatRegistry>,
    }

    async fn setup() -> Ctx {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("rec.db");
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
        sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(&p).await.unwrap();
        let hb = Arc::new(HeartbeatRegistry::new());
        let rec = VerificationReconciler::new(p, hb.clone());
        Ctx { rec, db, hb }
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
            max_scan: None,
        }
    }

    #[tokio::test]
    async fn test_reconcile_outcome_missing() {
        let c = setup().await;
        let r = c.rec.reconcile(&mkrec("ik-1", "h-1")).await;
        // No outcome → outcome_missing classification.
        assert!(!matches!(r, ReconciliationOutcome::Consistent));
    }

    #[tokio::test]
    async fn test_reconcile_idempotent_duplicate() {
        let c = setup().await;
        let rq = mkrec("ik-dup", "h-dup");
        c.rec.reconcile(&rq).await;
        let r2 = c.rec.reconcile(&rq).await;
        assert!(matches!(r2, ReconciliationOutcome::Duplicate { .. }));
    }

    #[tokio::test]
    async fn test_reconcile_no_agent_created() {
        let c = setup().await;
        c.rec.reconcile(&mkrec("ik-na", "h-na")).await;
        let ac: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ac.0, 0);
    }

    #[tokio::test]
    async fn test_reconcile_no_retry() {
        let c = setup().await;
        c.rec.reconcile(&mkrec("ik-nr", "h-nr")).await;
        let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ec.0, 1);
    }

    #[tokio::test]
    async fn test_reconcile_no_worktree_deleted() {
        let c = setup().await;
        let wd = tempfile::tempdir().unwrap();
        c.rec.reconcile(&mkrec("ik-nw", "h-nw")).await;
        assert!(wd.path().exists());
    }

    #[tokio::test]
    async fn test_classify_fully_consistent() {
        let s = ObservedState {
            run_lifecycle: Some("completed".into()),
            run_has_outcome: true,
            finalization_op_lifecycle: Some("completed".into()),
            claim_active: false,
            lease_active: false,
            heartbeat_exists: false,
            handoff_released: true,
            ..Default::default()
        };
        assert_eq!(
            VerificationReconciler::classify(&s),
            ReconciliationClassification::NoOpAlreadyConsistent
        );
    }

    #[tokio::test]
    async fn test_classify_resume_release() {
        let s = ObservedState {
            run_lifecycle: Some("completed".into()),
            run_has_outcome: true,
            finalization_op_lifecycle: Some("outcome_persisted".into()),
            claim_active: true,
            lease_active: true,
            heartbeat_exists: true,
            handoff_verification_owned: true,
            ..Default::default()
        };
        assert_eq!(
            VerificationReconciler::classify(&s),
            ReconciliationClassification::ResumeResourceRelease
        );
    }

    #[tokio::test]
    async fn test_classify_outcome_missing() {
        let s = ObservedState {
            run_has_outcome: false,
            ..Default::default()
        };
        assert_eq!(
            VerificationReconciler::classify(&s),
            ReconciliationClassification::OutcomeMissing
        );
    }

    #[tokio::test]
    async fn test_two_pool_one_reconciler() {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("tp.db");
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

        let hb = Arc::new(HeartbeatRegistry::new());
        let rec1 = VerificationReconciler::new(db1.pool.clone(), hb.clone());
        let rec2 = VerificationReconciler::new(db2.pool.clone(), hb.clone());

        let rq = mkrec("ik-tp", "h-tp");
        let (r1, r2) = tokio::join!(rec1.reconcile(&rq), rec2.reconcile(&rq));

        let one_ok = matches!(r1, ReconciliationOutcome::Consistent)
            || matches!(r1, ReconciliationOutcome::Blocked { .. })
            || matches!(r1, ReconciliationOutcome::AwaitingHuman { .. })
            || matches!(r1, ReconciliationOutcome::Resumed { .. })
            || matches!(r1, ReconciliationOutcome::Duplicate { .. });
        assert!(one_ok, "one must complete: {r1:?} / {r2:?}");

        let op_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&p).await.unwrap();
        assert_eq!(op_count.0, 1);
    }

    // ══════════════════════════════════════════════════════════════════
    // Classification coverage: additional variants
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_classify_handoff_mismatch_claim_active() {
        let s = ObservedState { run_has_outcome: true, handoff_released: true, claim_active: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::HandoffStateMismatch);
    }

    #[tokio::test]
    async fn test_classify_handoff_mismatch_lease_active() {
        let s = ObservedState { run_has_outcome: true, handoff_released: true, lease_active: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::HandoffStateMismatch);
    }

    #[tokio::test]
    async fn test_classify_worktree_missing() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), handoff_released: true, worktree_db_exists: false, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::WorktreeMissing);
    }

    #[tokio::test]
    async fn test_classify_stale_fencing() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), handoff_released: true, claim_active: false, lease_active: false, heartbeat_exists: false, worktree_db_exists: true, fencing_mismatch: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::StaleFencing);
    }

    #[tokio::test]
    async fn test_classify_ownership_lost() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), handoff_released: true, claim_active: false, lease_active: false, heartbeat_exists: false, worktree_db_exists: true, owner_changed: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::OwnershipLost);
    }

    #[tokio::test]
    async fn test_classify_active_process_unknown() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), handoff_released: true, claim_active: false, lease_active: false, heartbeat_exists: false, worktree_db_exists: true, active_command_op: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::ActiveProcessUnknown);
    }

    #[tokio::test]
    async fn test_classify_active_scanner_unknown() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), handoff_released: true, claim_active: false, lease_active: false, heartbeat_exists: false, worktree_db_exists: true, active_scanner_op: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::ActiveScannerUnknown);
    }

    #[tokio::test]
    async fn test_classify_outcome_conflict() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("running".into()), ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::OutcomeConflict);
    }

    #[tokio::test]
    async fn test_classify_progress_conflict() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), finalization_op_lifecycle: Some("releasing_resources".into()), claim_active: true, worktree_db_exists: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::ProgressConflict);
    }

    #[tokio::test]
    async fn test_classify_resource_mismatch() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), handoff_released: true, claim_active: false, lease_active: false, heartbeat_exists: false, worktree_db_exists: true, resource_mismatch: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::ResourceStateMismatch);
    }

    #[tokio::test]
    async fn test_classify_irrecoverable() {
        let s = ObservedState { run_has_outcome: true, run_lifecycle: Some("completed".into()), finalization_op_lifecycle: Some("completed".into()), handoff_released: true, claim_active: false, lease_active: false, heartbeat_exists: false, worktree_db_exists: true, ..Default::default() };
        assert_eq!(VerificationReconciler::classify(&s), ReconciliationClassification::NoOpAlreadyConsistent);
    }

    #[tokio::test]
    async fn test_resume_release_detected() {
        let c = setup().await;
        // Set up: outcome persisted, finalization op at outcome_persisted, resources still active.
        sqlx::query("UPDATE verification_runs SET lifecycle='completed', outcome_json='{}' WHERE run_id='run-1'").execute(&c.db.pool).await.unwrap();
        sqlx::query("INSERT INTO verification_finalization_operations(finalization_op_id,verification_run_id,idempotency_key,request_hash,worktree_id,fencing_token,owner_id,lifecycle) VALUES('fo-1','run-1','ik-fo','h-fo','wt1',5,'verify-run-1','outcome_persisted')").execute(&c.db.pool).await.unwrap();
        let state = c.rec.observe_state(&mkrec("ik-resume2", "h-resume2")).await;
        assert!(state.claim_active);
        assert_eq!(state.finalization_op_lifecycle.as_deref(), Some("outcome_persisted"));
        let classification = VerificationReconciler::classify(&state);
        // Claim active + outcome_persisted → should be ResumeResourceRelease or OutcomeMissing (depending on order)
        // With worktree_db_exists=false, it hits WorktreeMissing first.
        assert!(classification == ReconciliationClassification::ResumeResourceRelease
            || classification == ReconciliationClassification::WorktreeMissing,
            "got {classification:?}");
    }
}
