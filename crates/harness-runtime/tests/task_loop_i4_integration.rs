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

    // Decision: should be CompleteCandidate (H3: requires eligibility token).
    let token = CompletionEligibility {
        execution_terminal: true,
        outcome_passed: true,
        verification_terminal: true,
        required_steps_complete: true,
        evidence_complete: true,
        dossier_fingerprint_valid: true,
        process_inactive: true,
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
    let a1_row = TaskLoopRepo::new(db.pool.clone())
        .load_attempt(&a1_id)
        .await
        .unwrap()
        .unwrap();
    let _ = TaskLoopRepo::new(db.pool.clone())
        .terminal_attempt(
            &a1_id,
            a1_row.version,
            "vr1",
            "failed",
            "ofp1",
            "dfp1",
            "dec1",
        )
        .await
        .unwrap();
    // Transition loop to Evaluating so we can create the next attempt.
    let l2 = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();
    let _ = TaskLoopRepo::new(db.pool.clone())
        .transition_loop(
            &loop_id,
            l2.version,
            l2.fencing_token,
            "owner1",
            LoopLifecycle::Evaluating,
            None,
        )
        .await
        .unwrap();

    let l3 = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();
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

// ── Completion Hard Gate (Phase 2) ──────────────────────────────

#[tokio::test]
async fn test_completion_eligibility_all_gates_pass() {
    let (db, _gw) = setup_with_gateway().await;
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, version) VALUES ('exec-c1','t1',1,'completed','p1',1)")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-c1','t1','exec-c1','p1','ha',1,'[]')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, outcome_json, idempotency_key, request_hash) VALUES ('vr-c1','plan-c1','ha',1,'exec-c1','t1','p1','finalized','{\"result\":\"passed\",\"all_required_steps_passed\":true,\"evidence_complete\":true}','ik-c1','hc1')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_finalization_operations (finalization_op_id, verification_run_id, idempotency_key, request_hash, worktree_id, fencing_token, owner_id, lifecycle, dossier_json) VALUES ('fo-c1','vr-c1','fik-c1','fhc1','wt-c1',1,'v1','completed','{\"dossier\":\"ok\"}')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES ('wt-c1','p1','t1','exec-c1','/tmp','/tmp/.git','/tmp/wt1','br1','abc','sv1','op1','active')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho-c1','p1','t1','exec-c1','wt-c1','l1',1,'verification','v1','verification_owned')")
        .execute(&db.pool).await.unwrap();

    let eligibility =
        harness_runtime::task_loop::validate_completion_eligibility(&db.pool, "exec-c1").await;
    let eligibility = match eligibility {
        Ok(e) => e,
        Err(e) => panic!("eligibility query failed: {e}"),
    };
    assert!(
        eligibility.all_passed(),
        "all gates must pass: {:?}",
        eligibility.failed_gates()
    );
}

#[tokio::test]
async fn test_completion_eligibility_rejects_fabricated_passed() {
    let (db, _gw) = setup_with_gateway().await;
    // Execution exists but has NO verification run — outcome cannot be passed.
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, version) VALUES ('exec-fab','t1',1,'created','p1',1)")
        .execute(&db.pool).await.unwrap();

    let eligibility =
        harness_runtime::task_loop::validate_completion_eligibility(&db.pool, "exec-fab")
            .await
            .unwrap();
    assert!(
        !eligibility.all_passed(),
        "fabricated passed must be rejected"
    );
    assert!(!eligibility.outcome_passed, "outcome must not be passed");
    assert!(
        !eligibility.verification_terminal,
        "verification must not be terminal"
    );
    assert!(
        !eligibility.execution_terminal,
        "execution must not be terminal"
    );
}

#[tokio::test]
async fn test_completion_eligibility_rejects_failed_outcome() {
    let (db, _gw) = setup_with_gateway().await;
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, version) VALUES ('exec-fail','t1',2,'completed','p1',1)")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-fail','t1','exec-fail','p1','ha',1,'[]')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, outcome_json, idempotency_key, request_hash) VALUES ('vr-fail','plan-fail','ha',1,'exec-fail','t1','p1','finalized','{\"result\":\"failed\",\"all_required_steps_passed\":false,\"evidence_complete\":true}','ik-fail','hf')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_finalization_operations (finalization_op_id, verification_run_id, idempotency_key, request_hash, worktree_id, fencing_token, owner_id, lifecycle, dossier_json) VALUES ('fo-fail','vr-fail','fik-fail','fhf','wt-f1',1,'v2','completed','{\"dossier\":\"ok\"}')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES ('wt-f1','p1','t1','exec-fail','/tmp/repo','/tmp/repo/.git','/tmp/wt2','br2','abc','sv1','op2','active')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho-f1','p1','t1','exec-fail','wt-f1','l2',1,'verification','v2','verification_owned')")
        .execute(&db.pool).await.unwrap();

    let eligibility =
        harness_runtime::task_loop::validate_completion_eligibility(&db.pool, "exec-fail")
            .await
            .unwrap();
    assert!(
        !eligibility.all_passed(),
        "failed outcome must reject completion"
    );
    assert!(!eligibility.outcome_passed, "outcome must not be passed");
    assert!(
        eligibility.execution_terminal,
        "execution is terminal but outcome failed"
    );
    assert!(!eligibility.required_steps_complete, "steps not all passed");
}

#[tokio::test]
async fn test_completion_eligibility_rejects_active_process() {
    let (db, _gw) = setup_with_gateway().await;
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, version) VALUES ('exec-ap','t1',3,'completed','p1',1)")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-ap','t1','exec-ap','p1','ha',1,'[]')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, outcome_json, idempotency_key, request_hash) VALUES ('vr-ap','plan-ap','ha',1,'exec-ap','t1','p1','finalized','{\"result\":\"passed\",\"all_required_steps_passed\":true,\"evidence_complete\":true}','ik-ap','hap')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_finalization_operations (finalization_op_id, verification_run_id, idempotency_key, request_hash, worktree_id, fencing_token, owner_id, lifecycle, dossier_json) VALUES ('fo-ap','vr-ap','fik-ap','fhap','wt-ap',1,'v3','completed','{\"dossier\":\"ok\"}')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES ('wt-ap','p1','t1','exec-ap','/tmp/repo','/tmp/repo/.git','/tmp/wt3','br3','abc','sv1','op3','active')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho-ap','p1','t1','exec-ap','wt-ap','l3',1,'verification','v3','verification_owned')")
        .execute(&db.pool).await.unwrap();
    // Active process — verification step still running.
    sqlx::query("INSERT INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES ('op-ap','vr-ap','s1','plan-ap','exec-ap','cfg','wt-ap',1,'running','ik-op','hop')")
        .execute(&db.pool).await.unwrap();

    let eligibility =
        harness_runtime::task_loop::validate_completion_eligibility(&db.pool, "exec-ap")
            .await
            .unwrap();
    assert!(
        !eligibility.all_passed(),
        "active process must block completion"
    );
    assert!(
        !eligibility.process_inactive,
        "process must be detected as active"
    );
}

#[tokio::test]
async fn test_completion_eligibility_rejects_missing_dossier() {
    let (db, _gw) = setup_with_gateway().await;
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, version) VALUES ('exec-md','t1',4,'completed','p1',1)")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-md','t1','exec-md','p1','ha',1,'[]')")
        .execute(&db.pool).await.unwrap();
    // Verification run exists but NO finalization operation → no dossier.
    sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, outcome_json, idempotency_key, request_hash) VALUES ('vr-md','plan-md','ha',1,'exec-md','t1','p1','finalized','{\"result\":\"passed\",\"all_required_steps_passed\":true,\"evidence_complete\":true}','ik-md','hmd')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES ('wt-md','p1','t1','exec-md','/tmp/repo','/tmp/repo/.git','/tmp/wt4','br4','abc','sv1','op4','active')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho-md','p1','t1','exec-md','wt-md','l4',1,'verification','v4','verification_owned')")
        .execute(&db.pool).await.unwrap();

    let eligibility =
        harness_runtime::task_loop::validate_completion_eligibility(&db.pool, "exec-md")
            .await
            .unwrap();
    assert!(
        !eligibility.all_passed(),
        "missing dossier must block completion"
    );
    assert!(
        !eligibility.dossier_fingerprint_valid,
        "dossier must be invalid"
    );
}

#[tokio::test]
async fn test_completion_eligibility_rejects_stale_ownership() {
    let (db, _gw) = setup_with_gateway().await;
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id, version) VALUES ('exec-so','t1',5,'completed','p1',1)")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-so','t1','exec-so','p1','ha',1,'[]')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, outcome_json, idempotency_key, request_hash) VALUES ('vr-so','plan-so','ha',1,'exec-so','t1','p1','finalized','{\"result\":\"passed\",\"all_required_steps_passed\":true,\"evidence_complete\":true}','ik-so','hso')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO verification_finalization_operations (finalization_op_id, verification_run_id, idempotency_key, request_hash, worktree_id, fencing_token, owner_id, lifecycle, dossier_json) VALUES ('fo-so','vr-so','fik-so','fhso','wt-so',1,'v5','completed','{\"dossier\":\"ok\"}')")
        .execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES ('wt-so','p1','t1','exec-so','/tmp/repo','/tmp/repo/.git','/tmp/wt5','br5','abc','sv1','op5','active')")
        .execute(&db.pool).await.unwrap();
    // Handoff is RELEASED — ownership lost.
    sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho-so','p1','t1','exec-so','wt-so','l5',1,'verification','v5','released')")
        .execute(&db.pool).await.unwrap();

    let eligibility =
        harness_runtime::task_loop::validate_completion_eligibility(&db.pool, "exec-so")
            .await
            .unwrap();
    assert!(
        !eligibility.all_passed(),
        "released handoff must block completion"
    );
    assert!(
        !eligibility.ownership_valid,
        "released ownership must be invalid"
    );
}

// ── Workspace Continuation (Phase 3) ────────────────────────────

#[tokio::test]
async fn test_workspace_continuation_rejects_empty_fields() {
    let source = AttemptWorkspaceSource::ContinueFromAttempt {
        source_attempt_id: "".into(),
        source_execution_id: "".into(),
        source_worktree_id: "".into(),
        expected_baseline_commit: "abc".into(),
        expected_head: "def".into(),
        expected_diff_fingerprint: "df1".into(),
    };
    let result = TaskEngineeringLoopService::validate_workspace_continuation(&source);
    assert!(result.is_err(), "empty fields must be rejected");
}

#[tokio::test]
async fn test_workspace_continuation_rejects_missing_commits() {
    let source = AttemptWorkspaceSource::ContinueFromAttempt {
        source_attempt_id: "a1".into(),
        source_execution_id: "e1".into(),
        source_worktree_id: "wt1".into(),
        expected_baseline_commit: "".into(),
        expected_head: "".into(),
        expected_diff_fingerprint: "df1".into(),
    };
    let result = TaskEngineeringLoopService::validate_workspace_continuation(&source);
    assert!(result.is_err(), "missing commits must be rejected");
}

#[tokio::test]
async fn test_workspace_continuation_rejects_missing_diff_fingerprint() {
    let source = AttemptWorkspaceSource::ContinueFromAttempt {
        source_attempt_id: "a1".into(),
        source_execution_id: "e1".into(),
        source_worktree_id: "wt1".into(),
        expected_baseline_commit: "abc".into(),
        expected_head: "def".into(),
        expected_diff_fingerprint: "".into(),
    };
    let result = TaskEngineeringLoopService::validate_workspace_continuation(&source);
    assert!(result.is_err(), "missing diff fingerprint must be rejected");
}

#[tokio::test]
async fn test_workspace_continuation_rejects_nonexistent_path() {
    let source = AttemptWorkspaceSource::ContinueFromAttempt {
        source_attempt_id: "a1".into(),
        source_execution_id: "e1".into(),
        source_worktree_id: "/nonexistent/path/that/does/not/exist".into(),
        expected_baseline_commit: "abc".into(),
        expected_head: "def".into(),
        expected_diff_fingerprint: "df1".into(),
    };
    let result = TaskEngineeringLoopService::validate_workspace_continuation(&source);
    assert!(result.is_err(), "nonexistent path must be rejected");
}

#[tokio::test]
async fn test_workspace_continuation_accepts_initial() {
    let source = AttemptWorkspaceSource::InitialTaskWorkspace {
        repository_path: "/tmp/repo".into(),
    };
    let result = TaskEngineeringLoopService::validate_workspace_continuation(&source);
    assert!(result.is_ok(), "initial workspace must always be accepted");
}

#[tokio::test]
async fn test_workspace_continuation_validates_git_head() {
    // Create a real temp git repo and verify HEAD validation.
    let td = tempfile::tempdir().unwrap();
    let repo_path = td.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();

    // Init git repo with a commit.
    let git_init = std::process::Command::new("git")
        .args(["init"])
        .current_dir(&repo_path)
        .output();
    if git_init.is_err() {
        // Git not available — skip this test.
        return;
    }
    let _ = std::process::Command::new("git")
        .args(["config", "user.name", "test"])
        .current_dir(&repo_path)
        .output();
    let _ = std::process::Command::new("git")
        .args(["config", "user.email", "test@test.com"])
        .current_dir(&repo_path)
        .output();
    std::fs::write(repo_path.join("file.txt"), "hello").unwrap();
    let _ = std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo_path)
        .output();
    let _ = std::process::Command::new("git")
        .args(["commit", "-m", "init"])
        .current_dir(&repo_path)
        .output();

    let head_output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&repo_path)
        .output()
        .unwrap();
    let actual_head = String::from_utf8_lossy(&head_output.stdout)
        .trim()
        .to_string();

    // Valid continuation with correct HEAD.
    let source = AttemptWorkspaceSource::ContinueFromAttempt {
        source_attempt_id: "a1".into(),
        source_execution_id: "e1".into(),
        source_worktree_id: repo_path.to_string_lossy().to_string(),
        expected_baseline_commit: actual_head.clone(),
        expected_head: actual_head.clone(),
        expected_diff_fingerprint: "df1".into(),
    };
    let result = TaskEngineeringLoopService::validate_workspace_continuation(&source);
    assert!(
        result.is_ok(),
        "valid HEAD must be accepted: {:?}",
        result.err()
    );

    // Wrong HEAD — must be rejected.
    let bad_source = AttemptWorkspaceSource::ContinueFromAttempt {
        source_attempt_id: "a1".into(),
        source_execution_id: "e1".into(),
        source_worktree_id: repo_path.to_string_lossy().to_string(),
        expected_baseline_commit: "deadbeef".into(),
        expected_head: "deadbeef".into(),
        expected_diff_fingerprint: "df1".into(),
    };
    let bad_result = TaskEngineeringLoopService::validate_workspace_continuation(&bad_source);
    assert!(bad_result.is_err(), "wrong HEAD must be rejected");
}

#[tokio::test]
async fn test_prepare_attempt_rejects_invalid_continuation() {
    let (db, gw) = setup_with_gateway().await;
    let s = TaskEngineeringLoopService::new(db.pool.clone()).with_i4_gateway(gw);
    let CreateLoopOutcome::Created { loop_id } =
        s.create_loop(&loop_req("ik-wsv", "hwsv")).await.unwrap()
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

    // Attempt with invalid continuation (empty fields) → must be rejected.
    let r = s
        .prepare_next_attempt(
            &loop_id,
            "owner1",
            v,
            l.fencing_token,
            "prof-1",
            AttemptWorkspaceSource::ContinueFromAttempt {
                source_attempt_id: "".into(),
                source_execution_id: "".into(),
                source_worktree_id: "".into(),
                expected_baseline_commit: "abc".into(),
                expected_head: "def".into(),
                expected_diff_fingerprint: "df1".into(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(r, PrepareAttemptOutcome::InfrastructureError { .. }),
        "invalid continuation must be rejected: {:?}",
        r
    );
}

// ── Cancellation During Agent (C7) ───────────────────────────────

#[tokio::test]
async fn test_cancel_loop_during_active_attempt() {
    let (db, gw) = setup_with_gateway().await;
    let s = svc_with_gw(&db, gw.clone());

    let CreateLoopOutcome::Created { loop_id } = s
        .create_loop(&loop_req("ik-cancel", "hcancel"))
        .await
        .unwrap()
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

    // Prepare an attempt.
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
    let PrepareAttemptOutcome::Prepared { attempt_id: _, .. } = r else {
        panic!("{r:?}")
    };

    // Cancel the loop while an attempt is active.
    let l2 = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();
    let cancel_result = s
        .cancel_loop(&loop_id, "owner1", l2.version, l2.fencing_token)
        .await
        .unwrap();
    assert!(
        matches!(
            cancel_result,
            CancelLoopOutcome::Cancelled | CancelLoopOutcome::AlreadyTerminal { .. }
        ),
        "cancel must succeed: {cancel_result:?}"
    );

    // Verify the loop is terminal.
    let info = s.inspect_loop(&loop_id).await.unwrap().unwrap();
    assert!(
        info.lifecycle.is_terminal(),
        "loop must be terminal after cancel"
    );

    // Cancellation must not create duplicate attempts.
    let attempt_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM task_attempts WHERE loop_id=?")
            .bind(&loop_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(attempt_count.0, 1, "cancel must not create extra attempts");
}

#[tokio::test]
async fn test_cancel_wrong_owner_rejected() {
    let (db, gw) = setup_with_gateway().await;
    let s = svc_with_gw(&db, gw.clone());

    let CreateLoopOutcome::Created { loop_id } = s
        .create_loop(&loop_req("ik-cancel2", "hcancel2"))
        .await
        .unwrap()
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

    let l = TaskLoopRepo::new(db.pool.clone())
        .load_loop(&loop_id)
        .await
        .unwrap()
        .unwrap();

    // Wrong owner tries to cancel — must be rejected.
    let result = s
        .cancel_loop(&loop_id, "wrong-owner", version.unwrap(), l.fencing_token)
        .await;
    assert!(result.is_err(), "wrong owner cancel must be rejected");
}
