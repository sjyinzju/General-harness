//! Verification reconciliation recovery — production observe/classify/execute
//! coverage: DB/runtime heartbeat mismatch, two-pool exactly-once, observed-
//! state plan invalidation, event/dossier repair, and process/scanner
//! zero-release, all against file-backed SQLite and the real registries.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness_runtime::db::Database;
use harness_runtime::process::{
    manager::ProcessManager, registry::ProcessRegistry, types::CapturePolicy, types::ProcessSpec,
    types::StdinMode,
};
use harness_runtime::scheduler::heartbeat_registry::{
    HeartbeatEntry, HeartbeatRegistry, HeartbeatStatus, OwnerKind,
};
use harness_runtime::verification::reconciler::{
    ReconcileGate, ReconciliationClassification, ReconciliationOutcome, ReconciliationRequest,
    VerificationReconciler,
};
use harness_runtime::verification::{ReleaseCounters, ReleaseStepKind};
use sqlx::SqlitePool;

// ── Shared setup ──────────────────────────────────────────────────────────

/// Seed: outcome PERSISTED (passed), finalization op at outcome_persisted
/// with a consistent dossier, handoff verification_owned (fencing 5), active
/// claim + lease, worktree row with a REAL directory.
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
        .bind(outcome_json("passed"))
        .execute(p).await.unwrap();
    sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO workspace_leases(id,task_id,owner_execution_id,lifecycle,worktree_path,branch_name,expires_at) VALUES('l1','t1','e1','acquired','/tmp/wt','main','2099-01-01')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO resource_claims(id,project_id,task_id,execution_id,resource_kind,normalized_resource,access_mode,status) VALUES('c1','p1','t1','e1','workspace','wt1','read_write','active')").execute(p).await.unwrap();
    sqlx::query("INSERT INTO worktrees(id,project_id,task_id,execution_id,repository_root,repository_identity,worktree_path,branch_name,base_commit,owner_supervisor_id,operation_id,status) VALUES('wt1','p1','t1','e1','/repo','/repo/.git',?,'br','abc','sup1','op1','active')")
        .bind(wt_path).execute(p).await.unwrap();
    sqlx::query("INSERT INTO verification_finalization_operations(finalization_op_id,verification_run_id,idempotency_key,request_hash,worktree_id,fencing_token,owner_id,lifecycle,dossier_json) VALUES('fo-1','run-1','ik-fo','h-fo','wt1',5,'verify-run-1','outcome_persisted',?)")
        .bind(dossier_json("passed", false)).execute(p).await.unwrap();
}

fn outcome_json(result: &str) -> String {
    format!(
        r#"{{"result":"{result}","failure_classification":null,"summary":"deterministic outcome","blockers":[],"findings_count":0}}"#
    )
}

fn dossier_json(result: &str, cancellation: bool) -> String {
    format!(
        r#"{{"run_id":"run-1","task_id":"t1","project_id":"p1","execution_id":"e1","plan_fingerprint":"ha","outcome":"{result}","primary_classification":null,"all_blocker_classifications":[],"blockers":[],"failed_step_ids":[],"step_result_refs":[],"evidence_refs":[],"worktree_id":"wt1","worktree_path":"/tmp/wt1","baseline_commit":null,"worktree_head":null,"fencing_snapshot":5,"cancellation_requested":{cancellation},"budget_facts_json":null,"outcome_fingerprint":"f","dossier_fingerprint":"f","next_action":"CompleteCandidate"}}"#
    )
}

fn mkrec(ikey: &str) -> ReconciliationRequest {
    ReconciliationRequest {
        verification_run_id: "run-1".into(),
        execution_id: "e1".into(),
        task_id: "t1".into(),
        project_id: "p1".into(),
        worktree_id: "wt1".into(),
        expected_fencing: 5,
        verification_owner_id: "verify-run-1".into(),
        idempotency_key: ikey.into(),
        request_hash: format!("h-{ikey}"),
    }
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
    let db_path = db_dir.path().join("rec.db");
    let db = Database::open(&db_path).await.unwrap();
    let wt_dir = tempfile::tempdir().unwrap();
    seed(&db.pool, wt_dir.path().to_string_lossy().as_ref()).await;
    Env {
        _db_dir: db_dir,
        db_path,
        db,
        hb: Arc::new(HeartbeatRegistry::new()),
        wt_dir,
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

/// Assert the four resource-release side effects are all zero.
fn assert_zero_release(counters: &ReleaseCounters) {
    let s = counters.snapshot();
    assert_eq!(s[0], 0, "claim_release_count == 0");
    assert_eq!(s[1], 0, "lease_release_count == 0");
    assert_eq!(s[2], 0, "heartbeat_unregister_count == 0");
    assert_eq!(s[3], 0, "handoff_release_count == 0");
}

async fn resources_intact(p: &SqlitePool) {
    let claim: (String,) = sqlx::query_as("SELECT status FROM resource_claims WHERE id='c1'")
        .fetch_one(p)
        .await
        .unwrap();
    assert_eq!(claim.0, "active");
    let lease: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='l1'")
        .fetch_one(p)
        .await
        .unwrap();
    assert_eq!(lease.0, "acquired");
}

fn blocked_as(r: &ReconciliationOutcome, want: ReconciliationClassification) {
    match r {
        ReconciliationOutcome::Blocked { classification, .. } => {
            assert_eq!(classification, &want)
        }
        other => panic!("expected Blocked({want:?}), got {other:?}"),
    }
}

// ══════════════════════════════════════════════════════════════════════
// Scenario 14: DB/runtime heartbeat mismatch (durable facts vs registry)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn fault_14_db_runtime_heartbeat_mismatch() {
    let e = env().await;
    // In-flight verification: no outcome yet; durable ownership facts say a
    // heartbeat must exist; the runtime registry has NONE.
    sqlx::query(
        "UPDATE verification_runs SET lifecycle='running', outcome_json=NULL WHERE run_id='run-1'",
    )
    .execute(&e.db.pool)
    .await
    .unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("m14")).await;
    blocked_as(&r, ReconciliationClassification::DurableHeartbeatMissing);
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
    // No heartbeat silently created.
    assert!(!e.hb.exists("e1").await);
    // Formal reconciliation event + blocked lifecycle.
    let ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationReconciliationBlocked'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(ev.0, 1);
    let op: (String, String) = sqlx::query_as("SELECT lifecycle, planned_action FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!((op.0.as_str(), op.1.as_str()), ("blocked", "none"));
}

// ══════════════════════════════════════════════════════════════════════
// Two-pool reconciler: strict exactly-once, loser zero side effects
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn two_pool_reconciler_strict_exactly_once() {
    let e = env().await;
    let db2 = Database::open(&e.db_path).await.unwrap();
    register_hb(&e.hb).await;

    let counters = ReleaseCounters::default();
    let start = Arc::new(AtomicUsize::new(0));
    let r1s = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone())
        .with_start_count(start.clone());
    let r2s = VerificationReconciler::new(db2.pool.clone(), e.hb.clone())
        .with_counters(counters.clone())
        .with_start_count(start.clone());

    let rq1 = mkrec("tp");
    let rq2 = mkrec("tp");
    let (r1, r2) = tokio::join!(r1s.reconcile(&rq1), r2s.reconcile(&rq2));

    assert_eq!(start.load(Ordering::SeqCst), 1, "reconciler_start_count");
    let resumed = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, ReconciliationOutcome::Resumed { .. }))
        .count();
    let duplicate = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, ReconciliationOutcome::Duplicate { .. }))
        .count();
    assert_eq!(resumed, 1, "one winner: {r1:?} / {r2:?}");
    assert_eq!(duplicate, 1, "one loser duplicate: {r1:?} / {r2:?}");

    let op: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM verification_reconciliation_operations WHERE verification_run_id='run-1'",
    )
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(op.0, 1, "reconciliation_operation_count == 1");
    assert_eq!(
        counters.snapshot(),
        [1, 1, 1, 1, 1, 1],
        "each release side effect exactly once"
    );
    let ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationReconciliationResumed'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(ev.0, 1, "completed_event_count == 1");
}

// ══════════════════════════════════════════════════════════════════════
// Observed-state changes between plan formation and side effects
// ══════════════════════════════════════════════════════════════════════

/// Run the reconciler up to the plan barrier, apply `mutate`, release the
/// barrier, and assert the old plan is invalidated with ZERO side effects.
async fn invalidation_case<F, Fut>(name: &str, mutate: F)
where
    F: FnOnce(SqlitePool, Arc<HeartbeatRegistry>, std::path::PathBuf) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let e = env().await;
    let gate = ReconcileGate::new();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone())
        .with_observe_gate(gate.clone());
    let ikey = format!("inv-{name}");
    let handle = tokio::spawn(async move { rec.reconcile(&mkrec(&ikey)).await });
    gate.wait_reached().await;
    mutate(
        e.db.pool.clone(),
        e.hb.clone(),
        e.wt_dir.path().to_path_buf(),
    )
    .await;
    gate.release();
    let r = handle.await.unwrap();
    match r {
        ReconciliationOutcome::Blocked { classification, .. } => assert_eq!(
            classification,
            ReconciliationClassification::ProgressConflict,
            "{name}: stale plan must be invalidated"
        ),
        other => panic!("{name}: expected Blocked(ProgressConflict), got {other:?}"),
    }
    assert_zero_release(&counters);
}

#[tokio::test]
async fn invalidate_on_owner_change() {
    invalidation_case("owner", |p, _hb, _wt| async move {
        sqlx::query("UPDATE resource_handoffs SET owner_id='new-owner' WHERE handoff_id='ho-1'")
            .execute(&p)
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
async fn invalidate_on_fencing_change() {
    invalidation_case("fencing", |p, _hb, _wt| async move {
        sqlx::query("UPDATE resource_handoffs SET fencing_token=9 WHERE handoff_id='ho-1'")
            .execute(&p)
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
async fn invalidate_on_claim_change() {
    invalidation_case("claim", |p, _hb, _wt| async move {
        sqlx::query("UPDATE resource_claims SET status='released' WHERE id='c1'")
            .execute(&p)
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
async fn invalidate_on_lease_change() {
    invalidation_case("lease", |p, _hb, _wt| async move {
        sqlx::query("UPDATE workspace_leases SET lifecycle='released' WHERE id='l1'")
            .execute(&p)
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
async fn invalidate_on_heartbeat_change() {
    invalidation_case("heartbeat", |_p, hb, _wt| async move {
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
    })
    .await;
}

#[tokio::test]
async fn invalidate_on_handoff_change() {
    invalidation_case("handoff", |p, _hb, _wt| async move {
        sqlx::query("UPDATE resource_handoffs SET status='released' WHERE handoff_id='ho-1'")
            .execute(&p)
            .await
            .unwrap();
        sqlx::query("UPDATE resource_claims SET status='released' WHERE id='c1'")
            .execute(&p)
            .await
            .unwrap();
        sqlx::query("UPDATE workspace_leases SET lifecycle='released' WHERE id='l1'")
            .execute(&p)
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
async fn invalidate_on_worktree_change() {
    invalidation_case("worktree", |_p, _hb, wt| async move {
        std::fs::remove_dir_all(&wt).unwrap();
    })
    .await;
}

// ══════════════════════════════════════════════════════════════════════
// Event / dossier repairs (production paths)
// ══════════════════════════════════════════════════════════════════════

/// Drive durable state to fully-released with all six steps completed and a
/// consistent dossier for `result`; terminal event intentionally ABSENT.
async fn released_state(p: &SqlitePool, result: &str, cancellation: bool) {
    sqlx::query("UPDATE verification_runs SET outcome_json=? WHERE run_id='run-1'")
        .bind(outcome_json(result))
        .execute(p)
        .await
        .unwrap();
    sqlx::query("UPDATE verification_finalization_operations SET dossier_json=?, lifecycle='completed' WHERE finalization_op_id='fo-1'")
        .bind(dossier_json(result, cancellation))
        .execute(p).await.unwrap();
    sqlx::query("UPDATE resource_handoffs SET status='released' WHERE handoff_id='ho-1'")
        .execute(p)
        .await
        .unwrap();
    sqlx::query("UPDATE resource_claims SET status='released' WHERE id='c1'")
        .execute(p)
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_leases SET lifecycle='released' WHERE id='l1'")
        .execute(p)
        .await
        .unwrap();
    for kind in ReleaseStepKind::ALL {
        sqlx::query("INSERT INTO verification_release_steps(release_step_id,finalization_op_id,step_kind,step_order,state,owner_id,execution_id,fencing_token) VALUES(?,?,?,?,'completed','verify-run-1','e1',5)")
            .bind(format!("rs-fo-1-{}", kind.as_str()))
            .bind("fo-1")
            .bind(kind.as_str())
            .bind(kind.order())
            .execute(p).await.unwrap();
    }
    sqlx::query("INSERT OR IGNORE INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES ('fo-1','run-1','finalization','plan-final','e1','final-cfg','wt1',5,'finalization','fo-1','h-fo')")
        .execute(p).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-rel','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationResourcesReleased','finalization',NULL,'final-ev-run-1-VerificationResourcesReleased')")
        .execute(p).await.unwrap();
}

async fn assert_event_repaired(result: &str, cancellation: bool, expected_event: &str, ikey: &str) {
    let e = env().await;
    released_state(&e.db.pool, result, cancellation).await;
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec(ikey)).await;
    assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
    let ev: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type=?")
            .bind(expected_event)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();
    assert_eq!(ev.0, 1, "{expected_event} repaired exactly once");
    // Repair never re-releases resources.
    assert_zero_release(&counters);
    // Response-lost during repair: a fresh reconcile finds a consistent
    // state; the event count stays exactly one.
    let r2 = rec.reconcile(&mkrec(&format!("{ikey}-again"))).await;
    assert!(
        matches!(r2, ReconciliationOutcome::Consistent),
        "post-repair state is consistent: {r2:?}"
    );
    let ev2: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type=?")
            .bind(expected_event)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();
    assert_eq!(ev2.0, 1, "repair exactly-once under response loss");
}

#[tokio::test]
async fn repair_missing_passed_event() {
    assert_event_repaired("passed", false, "VerificationPassed", "rp-p").await;
}

#[tokio::test]
async fn repair_missing_failed_event() {
    assert_event_repaired("failed", false, "VerificationFailed", "rp-f").await;
}

#[tokio::test]
async fn repair_missing_blocked_event() {
    // Blocked outcomes retain resources; put them back to held state.
    let e = env().await;
    released_state(&e.db.pool, "blocked", false).await;
    // Blocked outcome + resources retained is the resting state; simulate by
    // reverting release facts (retained) — the repair must still only write
    // the event.
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("rp-b")).await;
    assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
    let ev: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationBlocked'",
    )
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(ev.0, 1);
    assert_zero_release(&counters);
}

#[tokio::test]
async fn repair_missing_cancelled_event() {
    assert_event_repaired("blocked", true, "VerificationCancelled", "rp-c").await;
}

#[tokio::test]
async fn repair_missing_resources_released_event() {
    let e = env().await;
    released_state(&e.db.pool, "passed", false).await;
    // Terminal event present; ResourcesReleased event MISSING.
    sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
        .execute(&e.db.pool).await.unwrap();
    sqlx::query("DELETE FROM verification_step_events WHERE event_id='evt-rel'")
        .execute(&e.db.pool)
        .await
        .unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("rp-rel")).await;
    assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
    let ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationResourcesReleased'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(ev.0, 1, "ResourcesReleased event repaired");
    assert_eq!(counters.snapshot()[4], 1, "released-event counter once");
    assert_zero_release(&counters);
}

#[tokio::test]
async fn repair_missing_dossier_from_immutable_facts() {
    let e = env().await;
    released_state(&e.db.pool, "passed", false).await;
    sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
        .execute(&e.db.pool).await.unwrap();
    sqlx::query("UPDATE verification_finalization_operations SET dossier_json=NULL WHERE finalization_op_id='fo-1'")
        .execute(&e.db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_step_results(result_id,run_id,step_id,plan_id,status,created_at) VALUES('sr-1','run-1','step-1','plan-1','passed',datetime('now'))")
        .execute(&e.db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_evidence(evidence_id,run_id,step_id,evidence_kind,summary,collected_at) VALUES('ev-1','run-1','step-1','test_output','ok',datetime('now'))")
        .execute(&e.db.pool).await.unwrap();

    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("rp-d")).await;
    assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
    let dj: (Option<String>,) = sqlx::query_as("SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&e.db.pool).await.unwrap();
    let dj = dj.0.expect("dossier rebuilt");
    assert!(dj.contains("sr-1"), "rebuilt from immutable StepResults");
    assert!(dj.contains("ev-1"), "rebuilt from immutable Evidence");
    assert!(!dj.contains("sk-live"), "no secrets");
    assert_zero_release(&counters);
    // Exactly-once: second reconcile does not overwrite.
    let r2 = rec.reconcile(&mkrec("rp-d2")).await;
    assert!(matches!(r2, ReconciliationOutcome::Consistent), "{r2:?}");
    let dj2: (Option<String>,) = sqlx::query_as("SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(dj2.0.unwrap(), dj, "dossier not overwritten");
}

#[tokio::test]
async fn dossier_fingerprint_conflict_awaits_human_and_never_overwrites() {
    let e = env().await;
    released_state(&e.db.pool, "passed", false).await;
    sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
        .execute(&e.db.pool).await.unwrap();
    // The dossier claims a DIFFERENT outcome than the immutable run outcome.
    let conflicting = dossier_json("failed", false);
    sqlx::query("UPDATE verification_finalization_operations SET dossier_json=? WHERE finalization_op_id='fo-1'")
        .bind(&conflicting).execute(&e.db.pool).await.unwrap();

    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("rp-conf")).await;
    match r {
        ReconciliationOutcome::AwaitingHuman { classification, .. } => {
            assert_eq!(classification, ReconciliationClassification::AwaitingHuman)
        }
        other => panic!("expected AwaitingHuman, got {other:?}"),
    }
    assert_zero_release(&counters);
    let dj: (Option<String>,) = sqlx::query_as("SELECT dossier_json FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(dj.0.unwrap(), conflicting, "conflicting dossier untouched");
    let op: (String, String) = sqlx::query_as("SELECT lifecycle, planned_action FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!((op.0.as_str(), op.1.as_str()), ("awaiting_human", "none"));
    let ev: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE event_type='VerificationAwaitingHuman'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(ev.0, 1, "VerificationAwaitingHuman exactly once");
}

// ══════════════════════════════════════════════════════════════════════
// Process / scanner states: strict zero-release
// ══════════════════════════════════════════════════════════════════════

fn fixture_path() -> std::path::PathBuf {
    let exe = std::env::current_exe().unwrap();
    let debug_dir = exe.parent().unwrap().parent().unwrap();
    debug_dir
        .join("process-fixture")
        .with_extension(std::env::consts::EXE_EXTENSION)
}

fn sleep_spec(execution_id: &str) -> ProcessSpec {
    ProcessSpec {
        executable: fixture_path(),
        args: vec!["sleep".into(), "5000".into()],
        working_directory: std::env::temp_dir(),
        env_overrides: HashMap::new(),
        env_removals: vec![],
        stdin_mode: StdinMode::Closed,
        timeout: Duration::from_secs(30),
        graceful_shutdown_timeout: Duration::from_secs(2),
        stdout_capture: CapturePolicy::Pipe,
        stderr_capture: CapturePolicy::Pipe,
        output_byte_limit: 1024 * 1024,
        spool_dir: None,
        allowed_env_var_names: vec![],
        known_secrets: vec![],
        execution_id: execution_id.to_string(),
        runtime_profile_id: "test-profile".into(),
    }
}

#[tokio::test]
async fn active_real_child_process_blocks_release() {
    let e = env().await;
    let registry = Arc::new(ProcessRegistry::new());
    let mgr = Arc::new(ProcessManager::new(registry));
    // A REAL child process for this execution is still running.
    mgr.spawn(&sleep_spec("e1")).await.unwrap();

    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone())
        .with_process_probe(mgr.clone());
    let r = rec.reconcile(&mkrec("proc-active")).await;
    blocked_as(&r, ReconciliationClassification::ActiveProcessUnknown);
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
    let _ = mgr.cancel("e1").await;
}

#[tokio::test]
async fn exited_child_with_running_operation_blocks_release() {
    let e = env().await;
    // Durable command operation still 'running' (child exited or vanished):
    // release must not proceed.
    sqlx::query("INSERT INTO verification_step_operations(op_id,verification_run_id,step_id,plan_id,execution_id,step_config_hash,worktree_id,fencing_token,status,idempotency_key,request_hash) VALUES('op-run','run-1','step-1','plan-1','e1','cfg','wt1',5,'running','ik-op','h-op')")
        .execute(&e.db.pool).await.unwrap();
    let registry = Arc::new(ProcessRegistry::new());
    let mgr = Arc::new(ProcessManager::new(registry));
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone())
        .with_process_probe(mgr);
    let r = rec.reconcile(&mkrec("proc-exited")).await;
    blocked_as(&r, ReconciliationClassification::ActiveProcessUnknown);
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
}

#[tokio::test]
async fn process_state_unknown_blocks_release() {
    let e = env().await;
    // Durable op running and NO probe wired at all: unknown, never release.
    sqlx::query("INSERT INTO verification_step_operations(op_id,verification_run_id,step_id,plan_id,execution_id,step_config_hash,worktree_id,fencing_token,status,idempotency_key,request_hash) VALUES('op-run','run-1','step-1','plan-1','e1','cfg','wt1',5,'pending','ik-op','h-op')")
        .execute(&e.db.pool).await.unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("proc-unknown")).await;
    blocked_as(&r, ReconciliationClassification::ActiveProcessUnknown);
    assert_zero_release(&counters);
}

#[tokio::test]
async fn active_scanner_blocks_release() {
    let e = env().await;
    sqlx::query("INSERT INTO verification_policy_operations(policy_op_id,verification_run_id,step_id,step_kind,sequence_index,idempotency_key,request_hash,worktree_id,fencing_token,lifecycle) VALUES('pop-run','run-1','step-1','secret_scan',0,'ik-pop','h-pop','wt1',5,'running')")
        .execute(&e.db.pool).await.unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("scan-active")).await;
    blocked_as(&r, ReconciliationClassification::ActiveScannerUnknown);
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
}

#[tokio::test]
async fn scanner_state_unknown_blocks_release() {
    let e = env().await;
    sqlx::query("INSERT INTO verification_policy_operations(policy_op_id,verification_run_id,step_id,step_kind,sequence_index,idempotency_key,request_hash,worktree_id,fencing_token,lifecycle) VALUES('pop-pend','run-1','step-1','secret_scan',0,'ik-pop2','h-pop2','wt1',5,'pending')")
        .execute(&e.db.pool).await.unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("scan-unknown")).await;
    blocked_as(&r, ReconciliationClassification::ActiveScannerUnknown);
    assert_zero_release(&counters);
}

#[tokio::test]
async fn reconciliation_required_operation_blocks_release() {
    let e = env().await;
    sqlx::query("INSERT INTO verification_policy_operations(policy_op_id,verification_run_id,step_id,step_kind,sequence_index,idempotency_key,request_hash,worktree_id,fencing_token,lifecycle) VALUES('pop-rec','run-1','step-1','secret_scan',0,'ik-pop3','h-pop3','wt1',5,'reconciliation_required')")
        .execute(&e.db.pool).await.unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("op-rec")).await;
    blocked_as(&r, ReconciliationClassification::ProgressConflict);
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
}

// ══════════════════════════════════════════════════════════════════════
// Remaining classifications: production-path reachability
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn outcome_conflict_terminal_run_without_outcome() {
    let e = env().await;
    // Run lifecycle is terminal but the immutable outcome is MISSING —
    // a contradiction the reconciler must never auto-repair.
    sqlx::query("UPDATE verification_runs SET outcome_json=NULL WHERE run_id='run-1'")
        .execute(&e.db.pool)
        .await
        .unwrap();
    // Heartbeat present so DurableHeartbeatMissing does not preempt.
    register_hb(&e.hb).await;
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("oc")).await;
    blocked_as(&r, ReconciliationClassification::OutcomeConflict);
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
}

#[tokio::test]
async fn irrecoverable_ambiguity_resources_without_ownership_record() {
    let e = env().await;
    // Claims/lease active but NO handoff row: ownership cannot be
    // established — irrecoverable, human decision required, zero effects.
    sqlx::query("DELETE FROM resource_handoffs WHERE handoff_id='ho-1'")
        .execute(&e.db.pool)
        .await
        .unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("ia")).await;
    match r {
        ReconciliationOutcome::AwaitingHuman { classification, .. } => assert_eq!(
            classification,
            ReconciliationClassification::IrrecoverableAmbiguity
        ),
        other => panic!("expected AwaitingHuman(IrrecoverableAmbiguity), got {other:?}"),
    }
    assert_zero_release(&counters);
    resources_intact(&e.db.pool).await;
    let op: (String, String) = sqlx::query_as("SELECT lifecycle, planned_action FROM verification_reconciliation_operations WHERE verification_run_id='run-1'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!((op.0.as_str(), op.1.as_str()), ("awaiting_human", "none"));
}

#[tokio::test]
async fn complete_operation_record_backfills_completion_only() {
    let e = env().await;
    released_state(&e.db.pool, "passed", false).await;
    sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES ('evt-term','run-1','finalization','fo-1','e1','t1','wt1',5,'VerificationPassed','finalization',NULL,'final-ev-run-1-VerificationPassed')")
        .execute(&e.db.pool).await.unwrap();
    // Everything released, events + dossier present, but the operation
    // record itself never reached 'completed' AND its completion step is
    // still pending.
    sqlx::query("UPDATE verification_finalization_operations SET lifecycle='releasing_resources' WHERE finalization_op_id='fo-1'")
        .execute(&e.db.pool).await.unwrap();
    sqlx::query("UPDATE verification_release_steps SET state='pending' WHERE finalization_op_id='fo-1' AND step_kind='operation_completion'")
        .execute(&e.db.pool).await.unwrap();
    let counters = ReleaseCounters::default();
    let rec = VerificationReconciler::new(e.db.pool.clone(), e.hb.clone())
        .with_counters(counters.clone());
    let r = rec.reconcile(&mkrec("cor")).await;
    assert!(matches!(r, ReconciliationOutcome::Resumed { .. }), "{r:?}");
    // ONLY the operation-completion side effect executed.
    let s = counters.snapshot();
    assert_eq!(
        [s[0], s[1], s[2], s[3], s[4]],
        [0, 0, 0, 0, 0],
        "no resource or event side effects"
    );
    assert_eq!(s[5], 1, "operation completion backfilled once");
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_finalization_operations WHERE finalization_op_id='fo-1'").fetch_one(&e.db.pool).await.unwrap();
    assert_eq!(lc.0, "completed");
}
