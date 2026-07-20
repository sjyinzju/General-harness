//! Verification finalization recovery — REAL fault injection, crash/restart,
//! response-lost, resume-from-step strict counting, and two-pool exactly-once
//! coverage for the I4-C Batch 5 release protocol.
//!
//! Faults are injected through the production-shaped `FaultPlan` on the
//! release engine (fail / crash before claim / before effect / after effect)
//! — never by deleting database rows. Crash/restart tests open a SECOND
//! file-backed pool + service and resume from durable state with FRESH
//! counters, so "only the remaining steps executed" is observed directly.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use harness_runtime::db::Database;
use harness_runtime::scheduler::heartbeat_registry::{
    HeartbeatEntry, HeartbeatRegistry, HeartbeatStatus, OwnerKind,
};
use harness_runtime::verification::{
    FaultMode, FaultPlan, FinalizationOutcome, FinalizationRequest, ReleaseCounters,
    ReleaseStepKind, VerificationFinalizationService,
};
use sqlx::SqlitePool;

// ── Shared setup ──────────────────────────────────────────────────────────

/// Seed a RUNNING verification run ready to finalize: one passed step
/// result, handoff verification_owned (fencing 5), acquired lease, active
/// claim, worktree DB row pointing at a REAL directory.
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
    sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','running','ik-r','hr')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO worktrees(id,project_id,task_id,execution_id,repository_root,repository_identity,worktree_path,branch_name,base_commit,owner_supervisor_id,operation_id,status) VALUES('wt1','p1','t1','e1','/repo','/repo/.git',?,'br','abc','sup1','op1','active')")
        .bind(wt_path).execute(p).await.unwrap();
    sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-1','run-1','step-1','plan-1','passed',datetime('now'))").execute(p).await.unwrap();
}

fn mkreq(ikey: &str) -> FinalizationRequest {
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
        request_hash: format!("h-{ikey}"),
        cancellation_requested: false,
        budget_facts_json: None,
    }
}

async fn register_hb(hb: &Arc<HeartbeatRegistry>) {
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
    hb.register(entry).await.unwrap();
}

struct Env {
    _db_dir: tempfile::TempDir,
    db_path: std::path::PathBuf,
    db: Database,
    hb: Arc<HeartbeatRegistry>,
    wt_dir: tempfile::TempDir,
}

async fn env() -> Env {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("fin.db");
    let db = Database::open(&db_path).await.unwrap();
    let wt_dir = tempfile::tempdir().unwrap();
    seed(&db.pool, wt_dir.path().to_string_lossy().as_ref()).await;
    let hb = Arc::new(HeartbeatRegistry::new());
    register_hb(&hb).await;
    Env {
        _db_dir: db_dir,
        db_path,
        db,
        hb,
        wt_dir,
    }
}

fn svc(
    pool: &SqlitePool,
    hb: &Arc<HeartbeatRegistry>,
    counters: &ReleaseCounters,
    faults: &FaultPlan,
) -> VerificationFinalizationService {
    VerificationFinalizationService::new(pool.clone(), hb.clone())
        .with_counters(counters.clone())
        .with_faults(faults.clone())
}

async fn step_state(p: &SqlitePool, kind: ReleaseStepKind) -> Option<String> {
    let r: Option<(String,)> = sqlx::query_as(
        "SELECT rs.state FROM verification_release_steps rs \
         JOIN verification_finalization_operations fo ON fo.finalization_op_id=rs.finalization_op_id \
         WHERE fo.verification_run_id='run-1' AND rs.step_kind=?",
    )
    .bind(kind.as_str())
    .fetch_optional(p)
    .await
    .unwrap();
    r.map(|x| x.0)
}

async fn op_lifecycle(p: &SqlitePool) -> String {
    let r: (String,) = sqlx::query_as(
        "SELECT lifecycle FROM verification_finalization_operations WHERE verification_run_id='run-1'",
    )
    .fetch_one(p)
    .await
    .unwrap();
    r.0
}

async fn claim_status(p: &SqlitePool) -> String {
    let r: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
        .fetch_one(p)
        .await
        .unwrap();
    r.0
}

async fn lease_lifecycle(p: &SqlitePool) -> String {
    let r: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
        .fetch_one(p)
        .await
        .unwrap();
    r.0
}

async fn handoff_status(p: &SqlitePool) -> String {
    let r: (String,) =
        sqlx::query_as("SELECT status FROM resource_handoffs WHERE handoff_id='ho-1'")
            .fetch_one(p)
            .await
            .unwrap();
    r.0
}

async fn event_count(p: &SqlitePool, ty: &str) -> i64 {
    let r: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM verification_step_events WHERE verification_run_id='run-1' AND event_type=?",
    )
    .bind(ty)
    .fetch_one(p)
    .await
    .unwrap();
    r.0
}

async fn assert_worktree_retained(e: &Env) {
    assert!(e.wt_dir.path().exists(), "worktree directory retained");
    let wt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE id='wt1'")
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
    assert_eq!(wt.0, 1, "worktree DB record retained");
}

async fn assert_no_forbidden_mutations(p: &SqlitePool) {
    let ac: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions")
        .fetch_one(p)
        .await
        .unwrap();
    assert_eq!(ac.0, 0, "no Agent created");
    let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
        .fetch_one(p)
        .await
        .unwrap();
    assert_eq!(ec.0, 1, "no retry/Execution created");
    let tl: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'")
        .fetch_one(p)
        .await
        .unwrap();
    assert_eq!(tl.0, "submitted", "Task lifecycle untouched");
}

// ══════════════════════════════════════════════════════════════════════
// 14 fault injections — scenarios 1-6: injected side-effect failures
// ══════════════════════════════════════════════════════════════════════

/// Common assertions for an injected failure at `failed_kind`:
/// prior steps completed exactly once, failed step durable 'failed',
/// later steps still pending, ReleaseFailed event exactly once, operation
/// reconciliation_required, worktree retained, no forbidden mutations.
async fn assert_fail_at(e: &Env, counters: &ReleaseCounters, failed_kind: ReleaseStepKind) {
    let p = &e.db.pool;
    assert_eq!(
        step_state(p, failed_kind).await.as_deref(),
        Some("failed"),
        "failed step durable"
    );
    for kind in ReleaseStepKind::ALL {
        if kind.order() < failed_kind.order() {
            assert_eq!(
                step_state(p, kind).await.as_deref(),
                Some("completed"),
                "{} completed before injected failure",
                kind.as_str()
            );
        } else if kind.order() > failed_kind.order() {
            assert_eq!(
                step_state(p, kind).await.as_deref(),
                Some("pending"),
                "{} never claimed after injected failure",
                kind.as_str()
            );
        }
    }
    assert_eq!(
        event_count(p, "VerificationResourceReleaseFailed").await,
        1,
        "ReleaseFailed event exactly once"
    );
    assert_eq!(op_lifecycle(p).await, "reconciliation_required");
    assert_worktree_retained(e).await;
    assert_no_forbidden_mutations(p).await;
    // Exactly-once for completed side effects, zero for the failed one.
    let snap = counters.snapshot();
    for (i, kind) in ReleaseStepKind::ALL.iter().enumerate() {
        if kind.order() < failed_kind.order() {
            assert_eq!(snap[i], 1, "{} executed once", kind.as_str());
        } else {
            assert_eq!(snap[i], 0, "{} not executed", kind.as_str());
        }
    }
}

#[tokio::test]
async fn fault_1_claim_release_failure() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::ClaimRelease, FaultMode::FailEffect);
    svc(&e.db.pool, &e.hb, &counters, &faults)
        .finalize(&mkreq("f1"))
        .await;
    assert_fail_at(&e, &counters, ReleaseStepKind::ClaimRelease).await;
    assert_eq!(claim_status(&e.db.pool).await, "active");
    assert_eq!(lease_lifecycle(&e.db.pool).await, "acquired");
    assert!(e.hb.exists("e1").await);
    assert_eq!(handoff_status(&e.db.pool).await, "verification_owned");
}

#[tokio::test]
async fn fault_2_claim_ok_lease_failure() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::LeaseRelease, FaultMode::FailEffect);
    svc(&e.db.pool, &e.hb, &counters, &faults)
        .finalize(&mkreq("f2"))
        .await;
    assert_fail_at(&e, &counters, ReleaseStepKind::LeaseRelease).await;
    assert_eq!(claim_status(&e.db.pool).await, "released");
    assert_eq!(lease_lifecycle(&e.db.pool).await, "acquired");
    assert!(e.hb.exists("e1").await);
    assert_eq!(handoff_status(&e.db.pool).await, "verification_owned");
}

#[tokio::test]
async fn fault_3_lease_ok_heartbeat_failure() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::HeartbeatUnregister, FaultMode::FailEffect);
    svc(&e.db.pool, &e.hb, &counters, &faults)
        .finalize(&mkreq("f3"))
        .await;
    assert_fail_at(&e, &counters, ReleaseStepKind::HeartbeatUnregister).await;
    assert_eq!(claim_status(&e.db.pool).await, "released");
    assert_eq!(lease_lifecycle(&e.db.pool).await, "released");
    assert!(e.hb.exists("e1").await, "heartbeat retained");
    assert_eq!(handoff_status(&e.db.pool).await, "verification_owned");
}

#[tokio::test]
async fn fault_4_heartbeat_ok_handoff_failure() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::HandoffRelease, FaultMode::FailEffect);
    svc(&e.db.pool, &e.hb, &counters, &faults)
        .finalize(&mkreq("f4"))
        .await;
    assert_fail_at(&e, &counters, ReleaseStepKind::HandoffRelease).await;
    assert!(!e.hb.exists("e1").await, "heartbeat unregistered");
    assert_eq!(handoff_status(&e.db.pool).await, "verification_owned");
}

#[tokio::test]
async fn fault_5_handoff_ok_event_failure() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(
        ReleaseStepKind::ResourcesReleasedEvent,
        FaultMode::FailEffect,
    );
    svc(&e.db.pool, &e.hb, &counters, &faults)
        .finalize(&mkreq("f5"))
        .await;
    assert_fail_at(&e, &counters, ReleaseStepKind::ResourcesReleasedEvent).await;
    assert_eq!(handoff_status(&e.db.pool).await, "released");
    assert_eq!(
        event_count(&e.db.pool, "VerificationResourcesReleased").await,
        0,
        "released event not written"
    );
}

#[tokio::test]
async fn fault_6_operation_completion_failure() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::OperationCompletion, FaultMode::FailEffect);
    svc(&e.db.pool, &e.hb, &counters, &faults)
        .finalize(&mkreq("f6"))
        .await;
    assert_fail_at(&e, &counters, ReleaseStepKind::OperationCompletion).await;
    assert_eq!(
        event_count(&e.db.pool, "VerificationResourcesReleased").await,
        1
    );
}

// ══════════════════════════════════════════════════════════════════════
// Scenarios 7-11: response lost after outcome / after each release step
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn fault_7_response_lost_after_full_outcome() {
    let e = env().await;
    let counters = ReleaseCounters::default();
    let s = svc(&e.db.pool, &e.hb, &counters, &FaultPlan::default());
    let r1 = s.finalize(&mkreq("f7")).await;
    assert!(matches!(r1, FinalizationOutcome::Finalized { .. }));
    assert_eq!(counters.snapshot(), [1, 1, 1, 1, 1, 1]);
    // Caller lost the response: same key again.
    let r2 = s.finalize(&mkreq("f7")).await;
    assert!(
        matches!(r2, FinalizationOutcome::Duplicate { .. }),
        "{r2:?}"
    );
    assert_eq!(counters.snapshot(), [1, 1, 1, 1, 1, 1], "zero new effects");
    let fo: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_finalization_operations")
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
    assert_eq!(fo.0, 1);
    assert_eq!(event_count(&e.db.pool, "VerificationPassed").await, 1);
}

/// Response lost between a step's side effect and its completion CAS: the
/// effect executed once, the step is left in_progress. Same-key re-entry
/// resumes: the interrupted step is completed from the ACTUAL resource state
/// WITHOUT re-execution; only later steps execute.
async fn response_lost_after(kind: ReleaseStepKind, ikey: &str) {
    let e = env().await;
    let c1 = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(kind, FaultMode::CrashAfterEffect);
    let s1 = svc(&e.db.pool, &e.hb, &c1, &faults);
    let r1 = s1.finalize(&mkreq(ikey)).await;
    assert!(
        matches!(r1, FinalizationOutcome::InfrastructureError { .. }),
        "{r1:?}"
    );
    let idx = (kind.order() - 1) as usize;
    assert_eq!(c1.snapshot()[idx], 1, "effect executed exactly once");
    assert_eq!(
        step_state(&e.db.pool, kind).await.as_deref(),
        Some("in_progress"),
        "step left in_progress at crash point"
    );

    // Re-entry with the same key on a FRESH service (fresh counters).
    let c2 = ReleaseCounters::default();
    let s2 = svc(&e.db.pool, &e.hb, &c2, &FaultPlan::default());
    let r2 = s2.finalize(&mkreq(ikey)).await;
    assert!(
        matches!(r2, FinalizationOutcome::Finalized { .. }),
        "{r2:?}"
    );
    let snap = c2.snapshot();
    for k in ReleaseStepKind::ALL {
        let i = (k.order() - 1) as usize;
        if k.order() <= kind.order() {
            assert_eq!(snap[i], 0, "{} NOT re-executed on resume", k.as_str());
        } else {
            assert_eq!(snap[i], 1, "{} executed once on resume", k.as_str());
        }
    }
    // Final state: everything exactly once.
    assert_eq!(claim_status(&e.db.pool).await, "released");
    assert_eq!(lease_lifecycle(&e.db.pool).await, "released");
    assert!(!e.hb.exists("e1").await);
    assert_eq!(handoff_status(&e.db.pool).await, "released");
    assert_eq!(
        event_count(&e.db.pool, "VerificationResourcesReleased").await,
        1
    );
    assert_eq!(event_count(&e.db.pool, "VerificationPassed").await, 1);
    assert_eq!(op_lifecycle(&e.db.pool).await, "completed");
    assert_worktree_retained(&e).await;
    assert_no_forbidden_mutations(&e.db.pool).await;
}

#[tokio::test]
async fn fault_8_response_lost_after_claim() {
    response_lost_after(ReleaseStepKind::ClaimRelease, "f8").await;
}

#[tokio::test]
async fn fault_9_response_lost_after_lease() {
    response_lost_after(ReleaseStepKind::LeaseRelease, "f9").await;
}

#[tokio::test]
async fn fault_10_response_lost_after_heartbeat() {
    response_lost_after(ReleaseStepKind::HeartbeatUnregister, "f10").await;
}

#[tokio::test]
async fn fault_11_response_lost_after_handoff() {
    response_lost_after(ReleaseStepKind::HandoffRelease, "f11").await;
}

// ══════════════════════════════════════════════════════════════════════
// Scenario 12: mid-release owner/fencing takeover
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn fault_12_mid_release_takeover_stops_everything() {
    let e = env().await;
    let c1 = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::ClaimRelease, FaultMode::CrashAfterEffect);
    let s1 = svc(&e.db.pool, &e.hb, &c1, &faults);
    let _ = s1.finalize(&mkreq("f12")).await;
    assert_eq!(c1.snapshot()[0], 1, "claim released before takeover");

    // A NEW owner takes over with a higher fencing token mid-release.
    sqlx::query("UPDATE resource_handoffs SET owner_kind='scheduler', owner_id='new-owner', fencing_token=9, version=version+1 WHERE handoff_id='ho-1'")
        .execute(&e.db.pool).await.unwrap();

    // Resume attempt by the OLD worker's replacement: must stop before ANY
    // further side effect.
    let c2 = ReleaseCounters::default();
    let s2 = svc(&e.db.pool, &e.hb, &c2, &FaultPlan::default());
    let r2 = s2.finalize(&mkreq("f12")).await;
    assert!(
        matches!(r2, FinalizationOutcome::OwnershipLost { .. }),
        "{r2:?}"
    );
    assert_eq!(c2.snapshot(), [0, 0, 0, 0, 0, 0], "zero side effects");
    // The NEW owner's resources are untouched.
    assert_eq!(lease_lifecycle(&e.db.pool).await, "acquired");
    assert!(e.hb.exists("e1").await, "heartbeat untouched");
    let ho: (String, String, i64) = sqlx::query_as(
        "SELECT status, owner_id, fencing_token FROM resource_handoffs WHERE handoff_id='ho-1'",
    )
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(
        (ho.0.as_str(), ho.1.as_str(), ho.2),
        ("verification_owned", "new-owner", 9)
    );
    // The interrupted Claim step's effect had ALREADY applied, so resume
    // records it completed WITHOUT re-execution (pure bookkeeping); the NEXT
    // step is durably parked reconciliation_required at the ownership check.
    assert_eq!(
        step_state(&e.db.pool, ReleaseStepKind::ClaimRelease)
            .await
            .as_deref(),
        Some("completed")
    );
    assert_eq!(
        step_state(&e.db.pool, ReleaseStepKind::LeaseRelease)
            .await
            .as_deref(),
        Some("reconciliation_required")
    );
    assert_worktree_retained(&e).await;
}

// ══════════════════════════════════════════════════════════════════════
// Scenario 13: Worktree disappears mid-release
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn fault_13_worktree_disappears_mid_release() {
    let e = env().await;
    let c1 = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(ReleaseStepKind::ClaimRelease, FaultMode::CrashAfterEffect);
    let s1 = svc(&e.db.pool, &e.hb, &c1, &faults);
    let _ = s1.finalize(&mkreq("f13")).await;

    // The worktree directory vanishes while its DB record is still active.
    std::fs::remove_dir_all(e.wt_dir.path()).unwrap();

    let c2 = ReleaseCounters::default();
    let s2 = svc(&e.db.pool, &e.hb, &c2, &FaultPlan::default());
    let r2 = s2.finalize(&mkreq("f13")).await;
    assert!(
        matches!(r2, FinalizationOutcome::OwnershipLost { .. }),
        "release must stop on worktree identity loss: {r2:?}"
    );
    assert_eq!(c2.snapshot(), [0, 0, 0, 0, 0, 0], "zero side effects");
    assert_eq!(lease_lifecycle(&e.db.pool).await, "acquired");
    assert!(e.hb.exists("e1").await);
    assert_eq!(handoff_status(&e.db.pool).await, "verification_owned");
    // Worktree DB record retained (never deleted, never auto-repaired).
    let wt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE id='wt1'")
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
    assert_eq!(wt.0, 1);
}

// ══════════════════════════════════════════════════════════════════════
// Crash matrix: process crash + restart on a NEW pool/service
// ══════════════════════════════════════════════════════════════════════

/// Crash at `mode`/`kind` with service 1, DESTROY it (drop service + close
/// pool), reopen the same file with a NEW pool + service, resume with the
/// same idempotency key, and return the resume counters.
async fn crash_and_restart(
    e: &Env,
    kind: ReleaseStepKind,
    mode: FaultMode,
    ikey: &str,
) -> ReleaseCounters {
    let c1 = ReleaseCounters::default();
    let faults = FaultPlan::default();
    faults.inject(kind, mode);
    {
        let s1 = svc(&e.db.pool, &e.hb, &c1, &faults);
        let r1 = s1.finalize(&mkreq(ikey)).await;
        assert!(
            matches!(r1, FinalizationOutcome::InfrastructureError { .. }),
            "{r1:?}"
        );
        drop(s1);
    }
    e.db.pool.close().await;

    // Restart: fresh pool + fresh service against the same durable file.
    let db2 = Database::open(&e.db_path).await.unwrap();
    let c2 = ReleaseCounters::default();
    let s2 = svc(&db2.pool, &e.hb, &c2, &FaultPlan::default());
    let r2 = s2.finalize(&mkreq(ikey)).await;
    assert!(
        matches!(r2, FinalizationOutcome::Finalized { .. }),
        "{r2:?}"
    );

    // Full exactly-once end state.
    assert_eq!(claim_status(&db2.pool).await, "released");
    assert_eq!(lease_lifecycle(&db2.pool).await, "released");
    assert!(!e.hb.exists("e1").await);
    assert_eq!(handoff_status(&db2.pool).await, "released");
    assert_eq!(event_count(&db2.pool, "VerificationPassed").await, 1);
    assert_eq!(
        event_count(&db2.pool, "VerificationResourcesReleased").await,
        1
    );
    assert_eq!(op_lifecycle(&db2.pool).await, "completed");
    let fo: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_finalization_operations")
        .fetch_one(&db2.pool)
        .await
        .unwrap();
    assert_eq!(fo.0, 1, "same operation resumed, no second op");
    c2
}

#[tokio::test]
async fn crash_after_outcome_commit_restart_runs_all_steps() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::ClaimRelease,
        FaultMode::CrashBeforeClaim,
        "cm1",
    )
    .await;
    assert_eq!(c2.snapshot(), [1, 1, 1, 1, 1, 1]);
}

#[tokio::test]
async fn crash_after_claim_step_claimed_before_effect() {
    let e = env().await;
    // Step claimed (in_progress), side effect NOT executed. Restart probes
    // the ACTUAL resource (claim still active), takes over, executes once.
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::ClaimRelease,
        FaultMode::CrashBeforeEffect,
        "cm2",
    )
    .await;
    assert_eq!(c2.snapshot(), [1, 1, 1, 1, 1, 1]);
}

#[tokio::test]
async fn crash_after_claim_effect_restart_skips_claim() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::ClaimRelease,
        FaultMode::CrashAfterEffect,
        "cm3",
    )
    .await;
    assert_eq!(c2.snapshot(), [0, 1, 1, 1, 1, 1]);
}

#[tokio::test]
async fn crash_after_lease_effect_restart() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::LeaseRelease,
        FaultMode::CrashAfterEffect,
        "cm4",
    )
    .await;
    assert_eq!(c2.snapshot(), [0, 0, 1, 1, 1, 1]);
}

#[tokio::test]
async fn crash_after_heartbeat_effect_restart() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::HeartbeatUnregister,
        FaultMode::CrashAfterEffect,
        "cm5",
    )
    .await;
    assert_eq!(c2.snapshot(), [0, 0, 0, 1, 1, 1]);
}

#[tokio::test]
async fn crash_after_handoff_effect_restart() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::HandoffRelease,
        FaultMode::CrashAfterEffect,
        "cm6",
    )
    .await;
    assert_eq!(c2.snapshot(), [0, 0, 0, 0, 1, 1]);
}

#[tokio::test]
async fn crash_after_released_event_restart() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::ResourcesReleasedEvent,
        FaultMode::CrashAfterEffect,
        "cm7",
    )
    .await;
    assert_eq!(c2.snapshot(), [0, 0, 0, 0, 0, 1]);
}

#[tokio::test]
async fn crash_before_operation_completion_restart() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::OperationCompletion,
        FaultMode::CrashBeforeClaim,
        "cm8",
    )
    .await;
    assert_eq!(c2.snapshot(), [0, 0, 0, 0, 0, 1]);
}

// ══════════════════════════════════════════════════════════════════════
// Resume-from-step strict counting (five durable starting states)
// ══════════════════════════════════════════════════════════════════════

/// Produce the durable state "released up to but not including `next`"
/// via a crash BEFORE claiming `next`, then resume on a new pool/service
/// and assert the exact per-step side-effect counts.
async fn resume_state_counts(next: ReleaseStepKind, ikey: &str, expected: [usize; 4]) {
    let e = env().await;
    let c2 = crash_and_restart(&e, next, FaultMode::CrashBeforeClaim, ikey).await;
    let snap = c2.snapshot();
    assert_eq!(
        [snap[0], snap[1], snap[2], snap[3]],
        expected,
        "resume-from-{} strict counts",
        next.as_str()
    );
}

#[tokio::test]
async fn resume_counts_release_not_started() {
    // claim=1, lease=1, heartbeat=1, handoff=1
    resume_state_counts(ReleaseStepKind::ClaimRelease, "rs1", [1, 1, 1, 1]).await;
}

#[tokio::test]
async fn resume_counts_claim_completed() {
    resume_state_counts(ReleaseStepKind::LeaseRelease, "rs2", [0, 1, 1, 1]).await;
}

#[tokio::test]
async fn resume_counts_lease_completed() {
    resume_state_counts(ReleaseStepKind::HeartbeatUnregister, "rs3", [0, 0, 1, 1]).await;
}

#[tokio::test]
async fn resume_counts_heartbeat_completed() {
    resume_state_counts(ReleaseStepKind::HandoffRelease, "rs4", [0, 0, 0, 1]).await;
}

#[tokio::test]
async fn resume_counts_handoff_completed_only_event_and_completion() {
    let e = env().await;
    let c2 = crash_and_restart(
        &e,
        ReleaseStepKind::ResourcesReleasedEvent,
        FaultMode::CrashBeforeClaim,
        "rs5",
    )
    .await;
    let snap = c2.snapshot();
    assert_eq!(
        [snap[0], snap[1], snap[2], snap[3]],
        [0, 0, 0, 0],
        "all resource side effects zero"
    );
    assert_eq!(snap[4], 1, "resources_released_event backfilled once");
    assert_eq!(snap[5], 1, "operation completion backfilled once");
}

// ══════════════════════════════════════════════════════════════════════
// Two-pool finalizer: strict exactly-once
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn two_pool_finalizer_strict_exactly_once() {
    // Run 200 iterations — the race is intermittent and the fix must hold
    // for the full 200/200 without a single counter deviation.
    for iteration in 0..200 {
        let e = env().await;
        let db2 = Database::open(&e.db_path).await.unwrap();

        let counters = ReleaseCounters::default();
        let start = Arc::new(AtomicUsize::new(0));
        let s1 = VerificationFinalizationService::new(e.db.pool.clone(), e.hb.clone())
            .with_counters(counters.clone())
            .with_start_count(start.clone());
        let s2 = VerificationFinalizationService::new(db2.pool.clone(), e.hb.clone())
            .with_counters(counters.clone())
            .with_start_count(start.clone());

        let rq1 = mkreq("tp");
        let rq2 = mkreq("tp");
        let (r1, r2) = tokio::join!(s1.finalize(&rq1), s2.finalize(&rq2));

        // finalizer_start_count == 1 (atomic insert winner only).
        assert_eq!(
            start.load(Ordering::SeqCst),
            1,
            "finalizer_start_count at iteration {iteration}"
        );
        // Loser never surfaces a bare UNIQUE error.
        for r in [&r1, &r2] {
            assert!(
                !matches!(r, FinalizationOutcome::InfrastructureError { .. }),
                "loser must not return InfrastructureError at iteration {iteration}: {r:?}"
            );
        }
        assert!(
            [&r1, &r2]
                .iter()
                .any(|r| matches!(r, FinalizationOutcome::Finalized { .. })),
            "one winner finalizes at iteration {iteration}: {r1:?} / {r2:?}"
        );

        let p = &e.db.pool;
        let fo: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM verification_finalization_operations")
                .fetch_one(p)
                .await
                .unwrap();
        assert_eq!(
            fo.0, 1,
            "finalization_operation_count == 1 at iteration {iteration}"
        );
        let outcome: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_runs WHERE lifecycle='completed' AND outcome_json IS NOT NULL",
        )
        .fetch_one(p)
        .await
        .unwrap();
        assert_eq!(
            outcome.0, 1,
            "final_outcome_count == 1 at iteration {iteration}"
        );
        assert_eq!(
            event_count(p, "VerificationPassed").await,
            1,
            "terminal_event_count == 1 at iteration {iteration}"
        );

        let snap = counters.snapshot();
        assert_eq!(
            snap,
            [1, 1, 1, 1, 1, 1],
            "claim/lease/heartbeat/handoff/released-event/completion each exactly once at iteration {iteration}"
        );
        // Per-step strict assertions.
        assert_eq!(
            snap[0], 1,
            "claim_release          == 1 at iteration {iteration}"
        );
        assert_eq!(
            snap[1], 1,
            "lease_release          == 1 at iteration {iteration}"
        );
        assert_eq!(
            snap[2], 1,
            "heartbeat_release      == 1 at iteration {iteration}"
        );
        assert_eq!(
            snap[3], 1,
            "handoff_release        == 1 at iteration {iteration}"
        );
        assert_eq!(
            snap[4], 1,
            "released_event         == 1 at iteration {iteration}"
        );
        assert_eq!(
            snap[5], 1,
            "operation_completion   == 1 at iteration {iteration}"
        );

        assert!(
            !e.hb.exists("e1").await,
            "heartbeat_unregister_count == 1 at iteration {iteration}"
        );
        assert_eq!(
            event_count(p, "VerificationResourcesReleased").await,
            1,
            "resources_released_event_count == 1 at iteration {iteration}"
        );
        let dossier: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_finalization_operations WHERE dossier_json IS NOT NULL",
        )
        .fetch_one(p)
        .await
        .unwrap();
        assert_eq!(dossier.0, 1, "dossier_count == 1 at iteration {iteration}");
        assert_no_forbidden_mutations(p).await;

        // duplicate effect == 0: re-run with fresh counters must be all-zero.
        let c2 = ReleaseCounters::default();
        let s3 = VerificationFinalizationService::new(e.db.pool.clone(), e.hb.clone())
            .with_counters(c2.clone());
        let r3 = s3.finalize(&mkreq("tp")).await;
        assert!(
            matches!(r3, FinalizationOutcome::Duplicate { .. }),
            "re-finalize must return Duplicate at iteration {iteration}"
        );
        assert_eq!(
            c2.snapshot(),
            [0, 0, 0, 0, 0, 0],
            "duplicate effect == 0 at iteration {iteration}"
        );

        // raw unique error == 0: loser must not surface UNIQUE error.
        // (already checked above via the !InfrastructureError assertion)
    }
}
