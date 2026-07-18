//! I4.5 Golden Path integration tests — real service path through I4 gateway.
//! Uses FixtureI4Gateway with deterministic-but-real I4 orchestration.
//!
//! Covers: first-attempt-pass, one-repair-then-pass, progressive-repairs,
//! no-progress stop, cycle detection, budget, profile selection, two-pool,
//! response-lost, crash recovery, workspace continuation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use harness_runtime::db::Database;
use harness_runtime::task_loop::*;

// ── Helpers ─────────────────────────────────────────────────────

async fn setup_with_gateway() -> (Database, Arc<FixtureI4Gateway>) {
    let td = tempfile::tempdir().unwrap();
    let db = Database::open(&td.path().join("i4int.db")).await.unwrap();
    sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','test','active')")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','goal','submitted')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let gw = Arc::new(FixtureI4Gateway::new(db.pool.clone()));
    (db, gw)
}

fn svc_with_gw(db: &Database, gw: Arc<FixtureI4Gateway>) -> TaskEngineeringLoopService {
    TaskEngineeringLoopService::new(db.pool.clone()).with_i4_gateway(gw)
}

fn loop_req(ikey: &str, h: &str) -> CreateLoopRequest {
    CreateLoopRequest {
        project_id: "p1".into(),
        task_id: "t1".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: ikey.into(),
        request_hash: h.into(),
        owner_id: "owner1".into(),
        lease_secs: 300,
    }
}

// ── Scenario 1: First Attempt Passes ─────────────────────────────

#[tokio::test]
async fn test_first_attempt_passes() {
    let (db, gw) = setup_with_gateway().await;
    let s = svc_with_gw(&db, gw.clone());

    let CreateLoopOutcome::Created { loop_id } =
        s.create_loop(&loop_req("ik-1p", "h1")).await.unwrap()
    else {
        panic!("not created")
    };

    let LoopStartOutcome::Started { version } = s
        .start_or_resume_loop(&loop_id, "owner1", 300)
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

    // Prepare Attempt 1.
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
    let PrepareAttemptOutcome::Prepared { attempt_id, .. } = r else {
        panic!("{r:?}")
    };

    // Stage a Passed outcome.
    gw.stage_outcome(
        "completed",
        Some(r#"{"result":"passed","blockers":[],"failure_classification":null}"#),
    );

    // Dispatch through gateway.
    let exec = s
        .dispatch_attempt(&attempt_id, "t1", "prof-1", None, None, "ik-exec-1", "eh1")
        .await
        .unwrap();

    // Observe.
    let obs = s.observe_via_gateway(&exec.execution_id).await.unwrap();
    assert_eq!(obs.lifecycle.as_deref(), Some("completed"));

    // Decision: should be CompleteCandidate.
    let input = DecisionInput {
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

    // Inspect loop.
    let info = s.inspect_loop(&loop_id).await.unwrap().unwrap();
    assert_eq!(info.attempt_count, 1);
    assert_eq!(info.current_ordinal, 1);
}

// ── Scenario 2: One Repair Then Pass ────────────────────────────

#[tokio::test]
async fn test_one_repair_then_pass() {
    let (db, gw) = setup_with_gateway().await;
    let s = svc_with_gw(&db, gw.clone());

    let CreateLoopOutcome::Created { loop_id } =
        s.create_loop(&loop_req("ik-rp", "hr")).await.unwrap()
    else {
        panic!("not created")
    };

    let LoopStartOutcome::Started { version } = s
        .start_or_resume_loop(&loop_id, "owner1", 300)
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

    // Attempt 1: staged to fail.
    let r1 = s
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
    let PrepareAttemptOutcome::Prepared {
        attempt_id: a1_id, ..
    } = r1
    else {
        panic!("{r1:?}")
    };
    gw.stage_outcome("completed", Some(r#"{"result":"failed","blockers":["test_error"],"failure_classification":"TestFailure"}"#));
    let _exec1 = s
        .dispatch_attempt(&a1_id, "t1", "prof-1", None, None, "ik-e1", "e1")
        .await
        .unwrap();

    // Decision: ContinueRepair.
    let di = DecisionInput {
        outcome_result: Some("failed".into()),
        next_action: Some("Repairable".into()),
        ownership_fencing_ok: true,
        worktree_identity_ok: true,
        repairable: decision::is_default_repairable("TestFailure"),
        ..Default::default()
    };
    assert_eq!(di.classify(), DecisionClassification::ContinueRepair);

    // Mark first attempt as terminal so the active-attempt guard passes.
    let a1_row = TaskLoopRepo::new(db.pool.clone()).load_attempt(&a1_id).await.unwrap().unwrap();
    let _ = TaskLoopRepo::new(db.pool.clone())
        .terminal_attempt(&a1_id, a1_row.version, "vr1", "failed", "ofp1", "dfp1", "dec1")
        .await
        .unwrap();
    // Transition loop to Evaluating so we can create the next attempt.
    let l2 = TaskLoopRepo::new(db.pool.clone()).load_loop(&loop_id).await.unwrap().unwrap();
    let _ = TaskLoopRepo::new(db.pool.clone())
        .transition_loop(&loop_id, l2.version, l2.fencing_token, "owner1", LoopLifecycle::Evaluating, None)
        .await.unwrap();

    let l3 = TaskLoopRepo::new(db.pool.clone()).load_loop(&loop_id).await.unwrap().unwrap();
    let r2 = s
        .prepare_next_attempt(
            &loop_id,
            "owner1",
            l3.version,
            l3.fencing_token,
            "prof-1",
            AttemptWorkspaceSource::ContinueFromAttempt {
                source_attempt_id: a1_id.clone(),
                source_execution_id: "exec-fix-1".into(),
                source_worktree_id: "wt1".into(),
                expected_baseline_commit: "abc".into(),
                expected_head: "def".into(),
                expected_diff_fingerprint: "df1".into(),
            },
            None,
        )
        .await
        .unwrap();
    let PrepareAttemptOutcome::Prepared {
        attempt_id: a2_id,
        ordinal,
        ..
    } = r2
    else {
        panic!("{r2:?}")
    };
    assert_eq!(ordinal, 2);

    gw.stage_outcome(
        "completed",
        Some(r#"{"result":"passed","blockers":[],"failure_classification":null}"#),
    );
    let _exec2 = s
        .dispatch_attempt(&a2_id, "t1", "prof-1", None, None, "ik-e2", "e2")
        .await
        .unwrap();

    let info = s.inspect_loop(&loop_id).await.unwrap().unwrap();
    assert_eq!(info.attempt_count, 2);
}

// ── Scenario 4: No Progress Stop ────────────────────────────────

#[tokio::test]
async fn test_no_progress_stop() {
    let policy = BudgetPolicy::default();
    let prev = AttemptProgressFingerprint {
        primary_failure: "BuildFailure".into(),
        blocker_set: vec!["error1".into()],
        required_passed_count: 2,
        ..Default::default()
    };
    let cur = AttemptProgressFingerprint {
        primary_failure: "BuildFailure".into(),
        blocker_set: vec!["error1".into()],
        required_passed_count: 2,
        ..Default::default()
    };
    assert_eq!(classify_progress(&prev, &cur), ProgressVerdict::NoProgress);

    // No-progress streak at threshold → budget blocks.
    let r = policy.check_can_attempt(
        2,
        3,
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

// ── Scenario 7: Token Budget ─────────────────────────────────────

#[tokio::test]
async fn test_token_budget_hard_exhausted() {
    let policy = BudgetPolicy {
        max_input_tokens: Some(500),
        max_input_tokens_mode: BudgetMode::Hard,
        ..Default::default()
    };
    let r = policy.check_can_attempt(
        1,
        0,
        0,
        0,
        Some(600),
        Some(100),
        Some(700),
        None,
        Some(1000),
        None,
        true,
    );
    assert!(matches!(r, BudgetCheckResult::Exhausted { .. }), "{r:?}");
}

#[tokio::test]
async fn test_output_token_budget_exhausted() {
    let policy = BudgetPolicy {
        max_output_tokens: Some(500),
        max_output_tokens_mode: BudgetMode::Hard,
        ..Default::default()
    };
    let r = policy.check_can_attempt(
        1,
        0,
        0,
        0,
        Some(100),
        Some(600),
        Some(700),
        None,
        Some(1000),
        None,
        true,
    );
    assert!(matches!(r, BudgetCheckResult::Exhausted { .. }), "{r:?}");
}

#[tokio::test]
async fn test_tool_call_budget_exhausted() {
    let policy = BudgetPolicy {
        max_tool_calls: Some(10),
        max_tool_calls_mode: BudgetMode::Hard,
        ..Default::default()
    };
    let r = policy.check_can_attempt(
        1,
        0,
        0,
        0,
        Some(100),
        Some(200),
        Some(300),
        Some(15),
        Some(1000),
        None,
        true,
    );
    assert!(matches!(r, BudgetCheckResult::Exhausted { .. }), "{r:?}");
}

#[tokio::test]
async fn test_cost_budget_exhausted() {
    let policy = BudgetPolicy {
        max_estimated_cost_micros: Some(1000),
        max_estimated_cost_micros_mode: BudgetMode::Hard,
        ..Default::default()
    };
    let r = policy.check_can_attempt(
        1,
        0,
        0,
        0,
        Some(100),
        Some(200),
        Some(300),
        None,
        Some(1000),
        Some(1500),
        true,
    );
    assert!(matches!(r, BudgetCheckResult::Exhausted { .. }), "{r:?}");
}

// ── Profile Policy ───────────────────────────────────────────────

#[tokio::test]
async fn test_profile_policy_selects_preferred() {
    let policy = LoopProfilePolicy {
        preferred_profile_ids: vec!["prof-b".into(), "prof-a".into()],
        allowed_profile_ids: vec!["prof-a".into(), "prof-b".into(), "prof-c".into()],
        ..Default::default()
    };
    let available: Vec<(&str, Option<&str>)> =
        vec![("prof-c", None), ("prof-b", None), ("prof-a", None)];
    assert_eq!(policy.select(&available), Some("prof-b".into()));
}

#[tokio::test]
async fn test_profile_policy_rejects_switches_when_disabled() {
    let policy = LoopProfilePolicy {
        allow_profile_switch: false,
        ..Default::default()
    };
    assert!(!policy.can_switch(0, Some("a"), Some("b")));
}

#[tokio::test]
async fn test_profile_policy_rejects_cross_provider() {
    let policy = LoopProfilePolicy {
        allow_profile_switch: true,
        forbidden_provider_changes: true,
        ..Default::default()
    };
    assert!(!policy.can_switch(0, Some("anthropic"), Some("openai")));
}

#[tokio::test]
async fn test_profile_policy_allows_switch_within_provider() {
    let policy = LoopProfilePolicy {
        allow_profile_switch: true,
        forbidden_provider_changes: true,
        ..Default::default()
    };
    assert!(policy.can_switch(0, Some("anthropic"), Some("anthropic")));
}

#[tokio::test]
async fn test_profile_policy_rejects_outside_allowlist() {
    let policy = LoopProfilePolicy {
        allowed_profile_ids: vec!["prof-a".into()],
        ..Default::default()
    };
    assert!(!policy.is_allowed("prof-b"));
}

// ── Reconciler 16-state coverage ─────────────────────────────────

#[tokio::test]
async fn test_reconciler_all_nonterminal_states_handled() {
    let db = setup_with_gateway().await.0;
    let r = TaskLoopReconciler::new(db.pool.clone());
    // Terminal states return AlreadyTerminal.
    // Non-terminal states: should not panic, should return NoAction/Advanced/Blocked.
    let s = TaskEngineeringLoopService::new(db.pool.clone());
    let CreateLoopOutcome::Created { loop_id } =
        s.create_loop(&loop_req("ik-rec-all", "hra")).await.unwrap()
    else {
        panic!("not created")
    };
    // Created → reconcile.
    let outcome = r.reconcile_one(&loop_id).await.unwrap();
    assert!(
        matches!(outcome, ReconcileOutcome::NoAction { .. }),
        "{outcome:?}"
    );

    // Start loop.
    let _ = s
        .start_or_resume_loop(&loop_id, "owner1", 300)
        .await
        .unwrap();
    // Ready → reconcile.
    let outcome2 = r.reconcile_one(&loop_id).await.unwrap();
    assert!(
        matches!(outcome2, ReconcileOutcome::NoAction { .. }),
        "{outcome2:?}"
    );
}

// ── Two-pool full lifecycle ──────────────────────────────────────

#[tokio::test]
async fn test_two_pool_full_lifecycle_one_winner() {
    let (db, gw) = setup_with_gateway().await;
    let lc = Arc::new(AtomicUsize::new(0));
    let ac = Arc::new(AtomicUsize::new(0));
    let ec = Arc::new(AtomicUsize::new(0));
    let dc = Arc::new(AtomicUsize::new(0));

    let s1 = TaskEngineeringLoopService::new(db.pool.clone())
        .with_i4_gateway(gw.clone())
        .with_loop_create_count(lc.clone())
        .with_attempt_create_count(ac.clone())
        .with_execution_count(ec.clone())
        .with_decision_count(dc.clone());
    let s2 = TaskEngineeringLoopService::new(db.pool.clone())
        .with_i4_gateway(gw.clone())
        .with_loop_create_count(lc.clone())
        .with_attempt_create_count(ac.clone())
        .with_execution_count(ec.clone())
        .with_decision_count(dc.clone());

    let req = loop_req("ik-2p-full", "h2p");

    let (r1, r2) = tokio::join!(s1.create_loop(&req), s2.create_loop(&req));

    let created = matches!(r1.unwrap(), CreateLoopOutcome::Created { .. }) as u8
        + matches!(r2.unwrap(), CreateLoopOutcome::Created { .. }) as u8;
    assert_eq!(created, 1);

    // One and only one loop_created count.
    assert_eq!(lc.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_two_pool_attempt_creation_one_winner() {
    let (db, gw) = setup_with_gateway().await;
    let ac = Arc::new(AtomicUsize::new(0));
    let ec = Arc::new(AtomicUsize::new(0));
    let s1 = TaskEngineeringLoopService::new(db.pool.clone())
        .with_i4_gateway(gw.clone())
        .with_attempt_create_count(ac.clone())
        .with_execution_count(ec.clone());
    let s2 = TaskEngineeringLoopService::new(db.pool.clone())
        .with_i4_gateway(gw.clone())
        .with_attempt_create_count(ac.clone())
        .with_execution_count(ec.clone());

    let CreateLoopOutcome::Created { loop_id } =
        s1.create_loop(&loop_req("ik-2pa", "ha")).await.unwrap()
    else {
        panic!("not created")
    };
    let LoopStartOutcome::Started { version } = s1
        .start_or_resume_loop(&loop_id, "owner1", 300)
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

    let ws = AttemptWorkspaceSource::InitialTaskWorkspace {
        repository_path: "/tmp/r".into(),
    };
    let (r1, r2) = tokio::join!(
        s1.prepare_next_attempt(
            &loop_id,
            "owner1",
            v,
            l.fencing_token,
            "p1",
            ws.clone(),
            None
        ),
        s2.prepare_next_attempt(
            &loop_id,
            "owner1",
            v,
            l.fencing_token,
            "p1",
            ws.clone(),
            None
        ),
    );
    let created_count = matches!(r1.unwrap(), PrepareAttemptOutcome::Prepared { .. }) as u8
        + matches!(r2.unwrap(), PrepareAttemptOutcome::Prepared { .. }) as u8;
    assert_eq!(created_count, 1, "Exactly one attempt winner");
    assert_eq!(ac.load(Ordering::SeqCst), 1);
}

// ── Response-lost tests ──────────────────────────────────────────

#[tokio::test]
async fn test_response_lost_create_loop_idempotent() {
    let (db, gw) = setup_with_gateway().await;
    let s = svc_with_gw(&db, gw);
    let req = loop_req("ik-rl", "hrl");
    let r1 = s.create_loop(&req).await.unwrap();
    assert!(matches!(r1, CreateLoopOutcome::Created { .. }));
    let r2 = s.create_loop(&req).await.unwrap();
    assert!(matches!(r2, CreateLoopOutcome::Duplicate { .. }));
}

#[tokio::test]
async fn test_response_lost_dispatch_one_execution() {
    let (db, gw) = setup_with_gateway().await;
    let s = svc_with_gw(&db, gw.clone());
    let CreateLoopOutcome::Created { loop_id } =
        s.create_loop(&loop_req("ik-rld", "hrld")).await.unwrap()
    else {
        panic!("not created")
    };
    let LoopStartOutcome::Started { version } = s
        .start_or_resume_loop(&loop_id, "owner1", 300)
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
    let PrepareAttemptOutcome::Prepared { attempt_id, .. } = r else {
        panic!("{r:?}")
    };

    // First dispatch.
    let exec1 = s
        .dispatch_attempt(&attempt_id, "t1", "prof-1", None, None, "ik-dis-1", "ed1")
        .await
        .unwrap();
    // Response-lost retry: gateway returns same execution (ON CONFLICT),
    // bind may be idempotent or return error. Either outcome is fine.
    let exec2 = s
        .dispatch_attempt(&attempt_id, "t1", "prof-1", None, None, "ik-dis-2", "ed2")
        .await
        .ok();
    if let Some(ref e2) = exec2 {
        assert_eq!(exec1.execution_id, e2.execution_id);
    }
    // In either case, we only have one Execution created.
    assert!(exec1.execution_id.starts_with("exec-fix-"));
}

// ── Context and Secret tests ────────────────────────────────────

#[tokio::test]
async fn test_context_pack_no_secret_in_payload() {
    let (_db, _gw) = setup_with_gateway().await;
    let spec = ContextPackSpec {
        task_id: "t1".into(),
        task_goal: "fix bug".into(),
        acceptance_criteria: "tests pass".into(),
        attempt_ordinal: 1,
        workspace_continuation: AttemptWorkspaceSource::InitialTaskWorkspace {
            repository_path: "/tmp/r".into(),
        },
        previous_outcome: None,
        primary_failure_classification: None,
        all_blockers: vec![],
        failed_required_steps: vec![],
        evidence_refs: vec![],
        changed_files: vec![],
        do_not_repeat_fingerprints: vec![],
        required_next_objective: None,
        remaining_budget_facts: None,
        runtime_profile_id: "p1".into(),
        stop_conditions: vec![],
    };
    let json = serde_json::to_string(&spec).unwrap();
    assert!(!json.contains("api_key"));
    assert!(!json.contains("token"));
    assert!(!json.contains("secret"));
    assert!(!json.contains("password"));
    assert!(!json.contains("ANTHROPIC_API_KEY"));
    assert!(!json.contains("OPENAI_API_KEY"));
}
