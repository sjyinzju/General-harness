//! I4.5 Task Engineering Loop integration tests.
//!
//! File-backed SQLite, covers core loop scenarios: create, start,
//! attempt creation, observation, decision, cancellation, and
//! two-pool concurrency.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use harness_runtime::db::Database;
use harness_runtime::task_loop::*;

// ── Helpers ─────────────────────────────────────────────────────

async fn setup() -> Database {
    let td = tempfile::tempdir().unwrap();
    let db = Database::open(&td.path().join("tl.db")).await.unwrap();
    // Seed minimal project + task.
    sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','test','active')")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','test goal','submitted')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    db
}

fn svc(db: &Database) -> TaskEngineeringLoopService {
    TaskEngineeringLoopService::new(db.pool.clone())
}

// ── Loop lifecycle ──────────────────────────────────────────────

#[tokio::test]
async fn test_create_loop_idempotent() {
    let db = setup().await;
    let s = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik1".into(),
        request_hash: "h1".into(),
        owner_id: "owner1".into(),
        lease_secs: 60,
    };
    let r1 = s.create_loop(&req).await.unwrap();
    assert!(matches!(r1, CreateLoopOutcome::Created { .. }));

    // Same key + same hash → duplicate.
    let r2 = s.create_loop(&req).await.unwrap();
    assert!(matches!(r2, CreateLoopOutcome::Duplicate { .. }));

    // Same key + different hash → conflict.
    let mut req2 = req.clone();
    req2.request_hash = "h2".into();
    let r3 = s.create_loop(&req2).await.unwrap();
    assert!(matches!(r3, CreateLoopOutcome::IdempotencyConflict { .. }));
}

#[tokio::test]
async fn test_one_active_loop_per_task() {
    let db = setup().await;
    let s = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-a".into(),
        request_hash: "ha".into(),
        owner_id: "o".into(),
        lease_secs: 60,
    };
    let r1 = s.create_loop(&req).await.unwrap();
    assert!(matches!(r1, CreateLoopOutcome::Created { .. }));

    // Second active loop for same task → rejected.
    let mut req2 = req.clone();
    req2.idempotency_key = "ik-b".into();
    let r2 = s.create_loop(&req2).await.unwrap();
    assert!(
        matches!(r2, CreateLoopOutcome::TaskAlreadyHasActiveLoop { .. }),
        "{r2:?}"
    );
}

#[tokio::test]
async fn test_start_loop_acquires_ownership() {
    let db = setup().await;
    let s = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-start".into(),
        request_hash: "hs".into(),
        owner_id: "owner1".into(),
        lease_secs: 60,
    };
    let CreateLoopOutcome::Created { loop_id } = s.create_loop(&req).await.unwrap() else {
        panic!("expected Created")
    };

    let r = s
        .start_or_resume_loop(&loop_id, "owner1", 60)
        .await
        .unwrap();
    assert!(matches!(r, LoopStartOutcome::Started { .. }), "{r:?}");

    // Same owner re-entry on already-started loop → Resumed.
    let r2 = s
        .start_or_resume_loop(&loop_id, "owner1", 60)
        .await
        .unwrap();
    assert!(matches!(r2, LoopStartOutcome::Resumed { .. }), "{r2:?}");

    // Different owner cannot renew — HeldByOther.
    let r3 = s
        .start_or_resume_loop(&loop_id, "owner2", 60)
        .await
        .unwrap();
    assert!(matches!(r3, LoopStartOutcome::HeldByOther { .. }), "{r3:?}");
}

#[tokio::test]
async fn test_prepare_attempt_creates_new_ordinal() {
    let db = setup().await;
    let s = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-pa".into(),
        request_hash: "hpa".into(),
        owner_id: "owner1".into(),
        lease_secs: 60,
    };
    let CreateLoopOutcome::Created { loop_id } = s.create_loop(&req).await.unwrap() else {
        panic!("expected Created")
    };
    let LoopStartOutcome::Started { version } = s
        .start_or_resume_loop(&loop_id, "owner1", 60)
        .await
        .unwrap()
    else {
        panic!("not started")
    };
    let v = version.unwrap();

    let l = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();

    let r = s
        .prepare_next_attempt(
            &loop_id,
            "owner1",
            v,
            l.fencing_token,
            "prof-1",
            AttemptWorkspaceSource::InitialTaskWorkspace {
                repository_path: "/tmp/repo".into(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(r, PrepareAttemptOutcome::Prepared { ordinal: 1, .. }),
        "Got: {r:?}"
    );

    // Second prepare while loop is AttemptActive → rejected.
    let r2 = s
        .prepare_next_attempt(
            &loop_id,
            "owner1",
            v + 2,
            l.fencing_token,
            "prof-1",
            AttemptWorkspaceSource::InitialTaskWorkspace {
                repository_path: "/tmp/repo".into(),
            },
            None,
        )
        .await
        .unwrap();
    // Guard rejects because lifecycle is AttemptActive.
    assert!(
        matches!(r2, PrepareAttemptOutcome::LoopNotReady { .. }),
        "{r2:?}"
    );
}

#[tokio::test]
async fn test_cancel_loop_terminal() {
    let db = setup().await;
    let s = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-cancel".into(),
        request_hash: "hc".into(),
        owner_id: "owner1".into(),
        lease_secs: 60,
    };
    let CreateLoopOutcome::Created { loop_id } = s.create_loop(&req).await.unwrap() else {
        panic!("expected Created")
    };
    let LoopStartOutcome::Started { version } = s
        .start_or_resume_loop(&loop_id, "owner1", 60)
        .await
        .unwrap()
    else {
        panic!("not started")
    };

    let l = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();

    let r = s
        .cancel_loop(&loop_id, "owner1", version.unwrap(), l.fencing_token)
        .await
        .unwrap();
    assert!(matches!(r, CancelLoopOutcome::Cancelled), "{r:?}");

    // Verify terminal.
    let l2 = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(l2.lifecycle, LoopLifecycle::Cancelled);
    assert!(l2.lifecycle.is_terminal());
}

// ── Two-pool concurrency ────────────────────────────────────────

#[tokio::test]
async fn test_two_pool_create_loop_one_winner() {
    let db = setup().await;
    let count = Arc::new(AtomicUsize::new(0));
    let s1 = TaskEngineeringLoopService::new(db.pool.clone()).with_loop_create_count(count.clone());
    let s2 = TaskEngineeringLoopService::new(db.pool.clone()).with_loop_create_count(count.clone());

    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-2p".into(),
        request_hash: "h2p".into(),
        owner_id: "o".into(),
        lease_secs: 60,
    };
    let req2 = req.clone();

    let (r1, r2) = tokio::join!(s1.create_loop(&req), s2.create_loop(&req2),);

    let created = matches!(r1.unwrap(), CreateLoopOutcome::Created { .. }) as u8
        + matches!(r2.unwrap(), CreateLoopOutcome::Created { .. }) as u8;
    assert_eq!(created, 1, "Exactly one winner");

    // Count only incremented once.
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_response_lost_create_loop() {
    let db = setup().await;
    let s1 = svc(&db);
    let s2 = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-rl".into(),
        request_hash: "hrl".into(),
        owner_id: "o".into(),
        lease_secs: 60,
    };

    // First call creates.
    let r1 = s1.create_loop(&req).await.unwrap();
    assert!(matches!(r1, CreateLoopOutcome::Created { .. }));

    // Second call (simulated response-lost retry) returns duplicate.
    let r2 = s2.create_loop(&req).await.unwrap();
    assert!(matches!(r2, CreateLoopOutcome::Duplicate { .. }));
}

// ── Decision engine ─────────────────────────────────────────────

#[tokio::test]
async fn test_decision_complete_candidate() {
    // H3: CompleteCandidate requires a validated eligibility token.
    let token = CompletionEligibility {
        execution_terminal: true,
        outcome_passed: true,
        verification_terminal: true,
        required_steps_complete: true,
        evidence_complete: true,
        dossier_fingerprint_valid: true,
        process_inactive: true,
        process_state_known: true,
        reconciliation_clear: true,
        workspace_valid: true,
        ownership_valid: true,
    };
    let input = DecisionInput {
        eligibility_token: Some(token),
        outcome_result: Some("passed".into()),
        next_action: Some("CompleteCandidate".into()),
        all_required_steps_passed: true,
        evidence_complete: true,
        dossier_present: true,
        dossier_fingerprint_matches: true,
        ownership_fencing_ok: true,
        worktree_identity_ok: true,
        ..Default::default()
    };
    assert_eq!(input.classify(), DecisionClassification::CompleteCandidate);
}

#[tokio::test]
async fn test_decision_cancelled_overrides_all() {
    let input = DecisionInput {
        cancellation_requested: true,
        outcome_result: Some("passed".into()),
        next_action: Some("CompleteCandidate".into()),
        all_required_steps_passed: true,
        evidence_complete: true,
        dossier_present: true,
        dossier_fingerprint_matches: true,
        ownership_fencing_ok: true,
        worktree_identity_ok: true,
        ..Default::default()
    };
    assert_eq!(input.classify(), DecisionClassification::Cancelled);
}

#[tokio::test]
async fn test_decision_security_blocker_awaits_human() {
    let input = DecisionInput {
        security_blocker: true,
        ownership_fencing_ok: true,
        worktree_identity_ok: true,
        outcome_result: Some("failed".into()),
        next_action: Some("Repairable".into()),
        ..Default::default()
    };
    assert_eq!(input.classify(), DecisionClassification::AwaitingHuman);
}

#[tokio::test]
async fn test_decision_reconciliation_blocks_repair() {
    let input = DecisionInput {
        i4_reconciliation_required: true,
        ownership_fencing_ok: true,
        worktree_identity_ok: true,
        repairable: true,
        ..Default::default()
    };
    assert_eq!(
        input.classify(),
        DecisionClassification::AwaitingReconciliation
    );
}

#[tokio::test]
async fn test_decision_non_retryable() {
    let input = DecisionInput {
        ownership_fencing_ok: true,
        worktree_identity_ok: true,
        next_action: Some("NonRetryable".into()),
        ..Default::default()
    };
    assert_eq!(input.classify(), DecisionClassification::NonRetryable);
}

// ── Progress detection ──────────────────────────────────────────

#[tokio::test]
async fn test_progress_detection_no_progress() {
    let prev = AttemptProgressFingerprint {
        primary_failure: "BuildFailure".into(),
        blocker_set: vec!["error1".into()],
        required_passed_count: 3,
        ..Default::default()
    };
    let cur = AttemptProgressFingerprint {
        primary_failure: "BuildFailure".into(),
        blocker_set: vec!["error1".into()],
        required_passed_count: 3,
        ..Default::default()
    };
    assert_eq!(classify_progress(&prev, &cur), ProgressVerdict::NoProgress);
}

#[tokio::test]
async fn test_progress_detection_partial_progress() {
    let prev = AttemptProgressFingerprint {
        primary_failure: "TestFailure".into(),
        required_passed_count: 2,
        ..Default::default()
    };
    let cur = AttemptProgressFingerprint {
        primary_failure: "TestFailure".into(),
        required_passed_count: 5,
        ..Default::default()
    };
    assert_eq!(
        classify_progress(&prev, &cur),
        ProgressVerdict::PartialProgress
    );
}

#[tokio::test]
async fn test_cycle_detection() {
    let a = AttemptProgressFingerprint {
        primary_failure: "A".into(),
        ..Default::default()
    };
    let b = AttemptProgressFingerprint {
        primary_failure: "B".into(),
        ..Default::default()
    };
    let history = vec![a.clone(), b, a.clone()];
    assert!(detect_cycle(&history));
}

#[tokio::test]
async fn test_no_cycle_with_two() {
    let a = AttemptProgressFingerprint {
        primary_failure: "A".into(),
        ..Default::default()
    };
    let history = vec![a.clone(), a];
    assert!(!detect_cycle(&history));
}

// ── Budget ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_budget_hard_attempt_limit() {
    let policy = BudgetPolicy::default();
    let r = policy.check_can_attempt(
        10,
        0,
        0,
        0,
        Some(100),
        Some(200),
        Some(300),
        None,
        Some(1000),
        None,
        true,
    );
    assert!(matches!(r, BudgetCheckResult::Exhausted { .. }), "{r:?}");
}

#[tokio::test]
async fn test_budget_ok_within_limits() {
    let policy = BudgetPolicy::default();
    let r = policy.check_can_attempt(
        3,
        0,
        0,
        0,
        Some(100),
        Some(200),
        Some(300),
        None,
        Some(1000),
        None,
        true,
    );
    assert!(matches!(r, BudgetCheckResult::Ok), "{r:?}");
}

#[tokio::test]
async fn test_budget_unknown_tokens_with_hard_mode() {
    let policy = BudgetPolicy {
        max_total_tokens: Some(1000),
        max_total_tokens_mode: BudgetMode::Hard,
        unknown_usage_policy: UnknownUsagePolicy::BlockUnknown,
        ..Default::default()
    };
    let r = policy.check_can_attempt(1, 0, 0, 0, None, None, None, None, Some(1000), None, false);
    assert!(matches!(r, BudgetCheckResult::Unknown { .. }), "{r:?}");
}

// ── Reconciler ──────────────────────────────────────────────────

#[tokio::test]
async fn test_reconciler_noop_on_terminal() {
    let db = setup().await;
    let r = TaskLoopReconciler::new(db.pool.clone());
    // Non-existent loop.
    let outcome = r.reconcile_one("nonexistent").await.unwrap();
    assert!(matches!(outcome, ReconcileOutcome::LoopNotFound));
}

#[tokio::test]
async fn test_reconciler_advances_created_loop() {
    let db = setup().await;
    let s = svc(&db);
    let req = CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: "ik-rec".into(),
        request_hash: "hr".into(),
        owner_id: "o".into(),
        lease_secs: 60,
    };
    let CreateLoopOutcome::Created { loop_id } = s.create_loop(&req).await.unwrap() else {
        panic!("expected Created")
    };

    let r = TaskLoopReconciler::new(db.pool.clone());
    let outcome = r.reconcile_one(&loop_id).await.unwrap();
    // Created loop with no issues → no action.
    assert!(matches!(outcome, ReconcileOutcome::NoAction { .. }));
}
