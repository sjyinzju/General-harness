//! I8A IPC interaction tests — snapshot, events long-poll, and the
//! Request-Ledger wrapping of interactive mutations.
//!
//! These tests exercise `SupervisorCommandHandler::handle_request` directly
//! (the Named Pipe transport itself is covered by `src/ipc/tests.rs`), so the
//! full path TUI → IPC envelope → ledger → production services → repository
//! is verified without a live pipe server.

use std::path::PathBuf;
use std::sync::Arc;

use harness_core::contracts::goal::{
    ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator, GoalSpec,
    GoalState, SuccessCriterion, VerificationPolicy,
};
use harness_core::contracts::ipc::{IpcCommand, IpcResponseStatus};
use harness_core::ErrorCode;
use harness_runtime::db::Database;
use harness_runtime::goal::service::GoalLoopService;
use harness_runtime::goal::{ClarificationQuestion, PlanProposal, ProposedMilestone, ProposedTask};
use harness_runtime::idempotency;
use harness_runtime::ipc::{IpcCommandHandler, IpcRequestContext};
use harness_runtime::production_graph::ProductionGraph;
use harness_runtime::supervisor::command_handler::SupervisorCommandHandler;
use serde_json::json;
use sha2::Digest;

// ── Environment ──────────────────────────────────────────────────────

struct Env {
    db: Database,
    graph: ProductionGraph,
    handler: SupervisorCommandHandler,
}

fn isolation_dir(label: &str) -> PathBuf {
    // Use C:\Temp to avoid git worktree detection (the user profile may be
    // inside a git worktree).
    let base = std::path::PathBuf::from(
        std::env::var("HARNESS_WORKTREE_ROOT").unwrap_or_else(|_| r"C:\Temp".to_string()),
    );
    let dir = base.join(format!("i8a-ipc-{}-{}", label, uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create isolation dir");
    dir
}

/// Build the production graph (the ONLY sanctioned composition root) on an
/// in-memory DB and wrap its SupervisorServices in a command handler.
async fn setup(label: &str) -> Env {
    let db = Database::open_in_memory().await.unwrap();

    let iso = isolation_dir(label);
    let repo_root = iso.join("repo");
    std::fs::create_dir_all(&repo_root).expect("create repo dir");
    let _ = std::process::Command::new("git")
        .args(["init", "."])
        .current_dir(&repo_root)
        .output();
    let _ = std::process::Command::new("git")
        .args([
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@test",
            "commit",
            "--allow-empty",
            "-m",
            "initial",
        ])
        .current_dir(&repo_root)
        .output();

    let run_context = Arc::new(
        harness_runtime::liveness::RunContext::create(&iso, "i8a-ipc", false)
            .expect("create run context"),
    );
    let worktree_root = run_context
        .managed_temp()
        .map(|t| t.path().to_path_buf())
        .unwrap_or_else(|| iso.join("tmp"));

    let graph = ProductionGraph::build(db.pool.clone(), &worktree_root, &repo_root, run_context)
        .expect("build production graph");
    let handler =
        SupervisorCommandHandler::new(db.pool.clone(), graph.supervisor_services.clone(), None, 0);
    Env { db, graph, handler }
}

fn ctx(idempotency_key: &str) -> IpcRequestContext {
    IpcRequestContext {
        request_id: format!("req-{}", uuid::Uuid::new_v4()),
        idempotency_key: idempotency_key.to_string(),
        client_pid: 4242,
    }
}

/// Mirror of the handler's hash: sha256(command || canonical payload).
fn request_hash(command: &IpcCommand, payload: &serde_json::Value) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(command.as_str().as_bytes());
    hasher.update(serde_json::to_string(payload).unwrap().as_bytes());
    format!("{:x}", hasher.finalize())
}

// ── Goal fixtures (same shape as interaction_protocol.rs) ────────────

fn make_test_goal(name: &str, interactive: bool) -> GoalSpec {
    let criterion_id = format!("c-{}", uuid::Uuid::new_v4());
    GoalSpec {
        goal_id: format!("g-{}", uuid::Uuid::new_v4()),
        revision: 1,
        title: name.into(),
        objective: format!("Complete: {}", name),
        repository_id: "test-repo".into(),
        target_ref: "refs/heads/main".into(),
        initial_base_head: "abc123def456".into(),
        success_criteria: vec![SuccessCriterion {
            criterion_id,
            description: "All tasks complete successfully".into(),
            evidence_policy: EvidencePolicy::TaskTerminalResult,
            verification_policy: VerificationPolicy::ExistenceOnly,
            subjectivity: CriterionSubjectivity::Objective,
            required: true,
        }],
        constraints: vec![],
        non_goals: vec![],
        budget: GoalBudget {
            max_plan_revisions: 5,
            max_total_tasks: 10,
            max_active_tasks: 4,
            max_consecutive_failures: 3,
            max_no_progress_iterations: 5,
            ..Default::default()
        },
        approval_policy: ApprovalPolicy {
            require_initial_plan_approval: interactive,
            ..Default::default()
        },
        created_by: GoalCreator::User {
            user_id: "test-user".into(),
            user_name: None,
        },
        created_at: chrono::Utc::now(),
    }
}

fn make_proposal(goal: &GoalSpec) -> PlanProposal {
    let criterion_ref = goal.success_criteria[0].criterion_id.clone();
    PlanProposal {
        schema_version: "1.0".into(),
        goal_summary: "test plan".into(),
        assumptions: vec![],
        milestones: vec![ProposedMilestone {
            client_ref: "m1".into(),
            title: "Milestone 1".into(),
            objective: "Deliver everything".into(),
            success_criteria_refs: vec![criterion_ref],
            dependencies: vec![],
            priority: 1,
        }],
        tasks: vec![
            ProposedTask {
                client_ref: "t1".into(),
                milestone_ref: "m1".into(),
                title: "Task 1".into(),
                objective: "Do the first thing".into(),
                acceptance_criteria: vec!["it works".into()],
                dependencies: vec![],
                expected_evidence: vec!["task_terminal_result".into()],
                expected_resource_scope: vec![],
                risk_level: "low".into(),
                requires_approval: false,
            },
            ProposedTask {
                client_ref: "t2".into(),
                milestone_ref: "m1".into(),
                title: "Task 2".into(),
                objective: "Do the second thing".into(),
                acceptance_criteria: vec!["it also works".into()],
                dependencies: vec!["t1".into()],
                expected_evidence: vec!["task_terminal_result".into()],
                expected_resource_scope: vec![],
                risk_level: "low".into(),
                requires_approval: false,
            },
        ],
        risks: vec![],
        completion_strategy: "all tasks complete".into(),
    }
}

fn make_questions() -> Vec<ClarificationQuestion> {
    vec![ClarificationQuestion {
        question_id: "q-1".into(),
        prompt: "Which database should be used?".into(),
        choices: vec!["sqlite".into(), "postgres".into()],
        required: true,
        reason: "objective does not name a storage engine".into(),
    }]
}

async fn goal_state(pool: &sqlx::SqlitePool, goal_id: &str) -> String {
    let (state,): (String,) = sqlx::query_as("SELECT state FROM goals WHERE goal_id = ?")
        .bind(goal_id)
        .fetch_one(pool)
        .await
        .unwrap();
    state
}

async fn event_count(pool: &sqlx::SqlitePool, goal_id: &str, event_type: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM goal_events WHERE goal_id = ? AND event_type = ?")
        .bind(goal_id)
        .bind(event_type)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn persist_plan(
    svc: &GoalLoopService,
    goal: &GoalSpec,
) -> harness_core::contracts::plan::PlanRevision {
    svc.persist_plan_revision(
        &goal.goal_id,
        &make_proposal(goal),
        "planner-test",
        &format!("inv-{}", uuid::Uuid::new_v4()),
        "abc123def456",
        1,
    )
    .await
    .unwrap()
}

/// Interactive goal parked in WaitingForApproval with a pending plan approval.
async fn goal_with_pending_plan_approval(
    env: &Env,
    label: &str,
) -> (
    GoalSpec,
    harness_core::contracts::plan::PlanRevision,
    String,
) {
    let svc = &env.graph.goal_loop_service;
    let goal = svc.create_goal(make_test_goal(label, true)).await.unwrap();
    let plan = persist_plan(svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();
    (goal, plan, approval.approval_id)
}

/// Non-interactive goal driven to Active (the only pausable state).
async fn active_goal(env: &Env, label: &str) -> GoalSpec {
    let svc = &env.graph.goal_loop_service;
    let goal = svc.create_goal(make_test_goal(label, false)).await.unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Planning)
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Active)
        .await
        .unwrap();
    goal
}

// ── goal.snapshot ────────────────────────────────────────────────────

#[tokio::test]
async fn snapshot_projects_goal_plan_and_pending_approval() {
    let env = setup("snap").await;
    let (goal, plan, approval_id) = goal_with_pending_plan_approval(&env, "snap").await;

    let out = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalSnapshot,
            &json!({"goal_id": goal.goal_id}),
        )
        .await
        .unwrap();
    assert!(matches!(out.status, IpcResponseStatus::Success));

    let snap = out.payload;
    assert_eq!(snap["goal"]["goal_id"], goal.goal_id.as_str());
    assert_eq!(snap["goal"]["state"], "waiting_for_approval");
    assert_eq!(
        snap["latest_plan"]["plan_revision_id"],
        plan.plan_revision_id.as_str()
    );
    // No plan is active yet — only validated + pending approval.
    assert!(snap["active_plan"].is_null());
    assert_eq!(snap["tasks"].as_array().unwrap().len(), 2);
    let pending = snap["pending_interactions"].as_array().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["approval_id"], approval_id.as_str());
    assert_eq!(
        pending[0]["plan_revision_id"],
        plan.plan_revision_id.as_str()
    );
    assert!(snap["last_event_sequence"].as_i64().unwrap() >= 1);
}

#[tokio::test]
async fn snapshot_requires_goal_id_and_is_never_ledgered() {
    let env = setup("snap-guard").await;

    let err = env
        .handler
        .handle_request(&ctx("some-key"), &IpcCommand::GoalSnapshot, &json!({}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("goal_id"));

    // Read-only commands bypass the ledger even with an idempotency key.
    let goal = env
        .graph
        .goal_loop_service
        .create_goal(make_test_goal("snap-guard", false))
        .await
        .unwrap();
    let out = env
        .handler
        .handle_request(
            &ctx("some-key"),
            &IpcCommand::GoalSnapshot,
            &json!({"goal_id": goal.goal_id}),
        )
        .await
        .unwrap();
    assert!(matches!(out.status, IpcResponseStatus::Success));
    let ledger_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM idempotency_records")
        .fetch_one(&env.db.pool)
        .await
        .unwrap();
    assert_eq!(ledger_rows, 0, "snapshot must not touch the request ledger");
}

// ── goal.events ──────────────────────────────────────────────────────

#[tokio::test]
async fn events_resume_from_sequence() {
    let env = setup("events").await;
    let svc = &env.graph.goal_loop_service;
    let goal = svc
        .create_goal(make_test_goal("events", true))
        .await
        .unwrap();
    svc.request_clarification(&goal.goal_id, &make_questions())
        .await
        .unwrap();

    let out = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalEvents,
            &json!({"goal_id": goal.goal_id, "after_sequence": 0}),
        )
        .await
        .unwrap();
    let first = out.payload;
    let count = first["count"].as_i64().unwrap();
    let last = first["last_sequence"].as_i64().unwrap();
    assert!(count >= 1);
    assert!(first["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["event_type"] == "clarification_requested"));

    // Resuming from the cursor returns nothing new and keeps the cursor.
    let out = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalEvents,
            &json!({"goal_id": goal.goal_id, "after_sequence": last}),
        )
        .await
        .unwrap();
    assert_eq!(out.payload["count"], 0);
    assert_eq!(out.payload["last_sequence"], last);
}

#[tokio::test]
async fn events_long_poll_returns_promptly_on_new_event() {
    let env = setup("longpoll").await;
    let svc = env.graph.goal_loop_service.clone();
    let goal = svc
        .create_goal(make_test_goal("longpoll", false))
        .await
        .unwrap();

    // Drain the pre-existing events (goal creation) to get the live cursor.
    let drained = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalEvents,
            &json!({"goal_id": goal.goal_id, "after_sequence": 0}),
        )
        .await
        .unwrap();
    let cursor = drained.payload["last_sequence"].as_i64().unwrap();

    let goal_id = goal.goal_id.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        svc.record_intervention(&goal_id, "prefer sqlite", None, "user")
            .await
            .unwrap();
    });

    let started = std::time::Instant::now();
    let out = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalEvents,
            &json!({"goal_id": goal.goal_id, "after_sequence": cursor, "wait_ms": 15000}),
        )
        .await
        .unwrap();
    writer.await.unwrap();

    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "long-poll must return promptly, waited {:?}",
        started.elapsed()
    );
    assert!(out.payload["count"].as_i64().unwrap() >= 1);
    assert!(out.payload["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["event_type"] == "user_intervention_recorded"));
}

#[tokio::test]
async fn reconnect_snapshot_then_events_is_gapless() {
    let env = setup("reconnect").await;
    let svc = &env.graph.goal_loop_service;
    let goal = svc
        .create_goal(make_test_goal("reconnect", true))
        .await
        .unwrap();
    svc.request_clarification(&goal.goal_id, &make_questions())
        .await
        .unwrap();

    // Reconnect step 1: snapshot gives the resume cursor.
    let snap = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalSnapshot,
            &json!({"goal_id": goal.goal_id}),
        )
        .await
        .unwrap();
    let cursor = snap.payload["last_event_sequence"].as_i64().unwrap();

    // Something happens between snapshot and subscribe.
    svc.record_intervention(&goal.goal_id, "note for planner", None, "user")
        .await
        .unwrap();

    // Reconnect step 2: events after the cursor yield exactly the new event.
    let out = env
        .handler
        .handle_request(
            &ctx(""),
            &IpcCommand::GoalEvents,
            &json!({"goal_id": goal.goal_id, "after_sequence": cursor}),
        )
        .await
        .unwrap();
    let events = out.payload["events"].as_array().unwrap().clone();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_type"], "user_intervention_recorded");
    assert!(events[0]["sequence"].as_i64().unwrap() > cursor);
}

// ── Request Ledger semantics ─────────────────────────────────────────

#[tokio::test]
async fn ledgered_pause_replay_returns_duplicate_without_second_side_effect() {
    let env = setup("ledger-pause").await;
    let goal = active_goal(&env, "ledger-pause").await;
    let payload = json!({"goal_id": goal.goal_id});

    let first = env
        .handler
        .handle_request(&ctx("pause-1"), &IpcCommand::GoalPause, &payload)
        .await
        .unwrap();
    assert!(matches!(first.status, IpcResponseStatus::Success));
    assert_eq!(first.payload["applied"], true);
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "paused");

    // Crash-retry: new request_id, same idempotency key + payload.
    let second = env
        .handler
        .handle_request(&ctx("pause-1"), &IpcCommand::GoalPause, &payload)
        .await
        .unwrap();
    assert!(matches!(second.status, IpcResponseStatus::Duplicate));
    assert_eq!(
        second.payload, first.payload,
        "replay must echo the stored result"
    );
    assert_eq!(
        event_count(&env.db.pool, &goal.goal_id, "goal_paused").await,
        1,
        "replay must not append a second goal_paused event"
    );
}

#[tokio::test]
async fn completed_key_with_different_payload_is_conflict() {
    let env = setup("ledger-conflict").await;
    let goal = active_goal(&env, "ledger-conflict").await;

    env.handler
        .handle_request(
            &ctx("shared-key"),
            &IpcCommand::GoalPause,
            &json!({"goal_id": goal.goal_id}),
        )
        .await
        .unwrap();

    // Same key, different command+payload: must be rejected, not replayed.
    let err = env
        .handler
        .handle_request(
            &ctx("shared-key"),
            &IpcCommand::GoalResume,
            &json!({"goal_id": goal.goal_id}),
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, ErrorCode::Conflict), "got: {err}");
    assert_eq!(
        goal_state(&env.db.pool, &goal.goal_id).await,
        "paused",
        "conflicting request must have no side effect"
    );
}

#[tokio::test]
async fn pending_key_with_different_payload_is_conflict() {
    let env = setup("ledger-pending").await;
    let goal = active_goal(&env, "ledger-pending").await;

    // Another client holds a pending claim with a different payload hash.
    let other_payload = json!({"goal_id": "someone-else"});
    let hash = request_hash(&IpcCommand::GoalPause, &other_payload);
    idempotency::try_claim(&env.db.pool, "ipc-pending-key", &hash, 600)
        .await
        .unwrap()
        .expect("claim must be granted");

    let err = env
        .handler
        .handle_request(
            &ctx("pending-key"),
            &IpcCommand::GoalPause,
            &json!({"goal_id": goal.goal_id}),
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, ErrorCode::Conflict), "got: {err}");
}

#[tokio::test]
async fn pending_key_with_same_payload_is_accepted_in_flight() {
    let env = setup("ledger-inflight").await;
    let goal = active_goal(&env, "ledger-inflight").await;
    let payload = json!({"goal_id": goal.goal_id});

    // Simulate a crash mid-execution: the claim is pending, no result yet.
    let hash = request_hash(&IpcCommand::GoalPause, &payload);
    idempotency::try_claim(&env.db.pool, "ipc-inflight-key", &hash, 600)
        .await
        .unwrap()
        .expect("claim must be granted");

    let out = env
        .handler
        .handle_request(&ctx("inflight-key"), &IpcCommand::GoalPause, &payload)
        .await
        .unwrap();
    assert!(matches!(out.status, IpcResponseStatus::Accepted));
    assert_eq!(out.payload["state"], "in_flight");
    assert_eq!(
        goal_state(&env.db.pool, &goal.goal_id).await,
        "active",
        "in-flight duplicate must not execute the command again"
    );
}

#[tokio::test]
async fn empty_idempotency_key_bypasses_ledger() {
    let env = setup("ledger-bypass").await;
    let goal = active_goal(&env, "ledger-bypass").await;
    let payload = json!({"goal_id": goal.goal_id});

    let first = env
        .handler
        .handle_request(&ctx(""), &IpcCommand::GoalPause, &payload)
        .await
        .unwrap();
    assert!(matches!(first.status, IpcResponseStatus::Success));
    assert_eq!(first.payload["applied"], true);

    // Second pause without a key: service-level idempotency, not the ledger.
    let second = env
        .handler
        .handle_request(&ctx(""), &IpcCommand::GoalPause, &payload)
        .await
        .unwrap();
    assert!(matches!(second.status, IpcResponseStatus::Success));
    assert_eq!(second.payload["applied"], false);

    let ledger_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM idempotency_records")
        .fetch_one(&env.db.pool)
        .await
        .unwrap();
    assert_eq!(ledger_rows, 0);
}

// ── goal.intervene ───────────────────────────────────────────────────

#[tokio::test]
async fn intervene_threads_request_id_and_replays_as_duplicate() {
    let env = setup("intervene").await;
    let goal = env
        .graph
        .goal_loop_service
        .create_goal(make_test_goal("intervene", false))
        .await
        .unwrap();
    let payload = json!({"goal_id": goal.goal_id, "message": "prefer sqlite over postgres"});

    let rctx = IpcRequestContext {
        request_id: "req-fixed-123".into(),
        idempotency_key: "int-1".into(),
        client_pid: 7,
    };
    let first = env
        .handler
        .handle_request(&rctx, &IpcCommand::GoalIntervene, &payload)
        .await
        .unwrap();
    assert!(matches!(first.status, IpcResponseStatus::Success));
    assert_eq!(first.payload["status"], "recorded");

    // The envelope request_id is threaded into the row for provenance.
    let (request_id,): (Option<String>,) =
        sqlx::query_as("SELECT request_id FROM user_interventions WHERE goal_id = ?")
            .bind(&goal.goal_id)
            .fetch_one(&env.db.pool)
            .await
            .unwrap();
    assert_eq!(request_id.as_deref(), Some("req-fixed-123"));

    // Retry with a NEW request_id but same key + payload: the hash is bound
    // to the client payload, so this replays instead of conflicting.
    let second = env
        .handler
        .handle_request(&ctx("int-1"), &IpcCommand::GoalIntervene, &payload)
        .await
        .unwrap();
    assert!(matches!(second.status, IpcResponseStatus::Duplicate));
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_interventions WHERE goal_id = ?")
        .bind(&goal.goal_id)
        .fetch_one(&env.db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1, "replay must not record a second intervention");
}

// ── Plan approval lifecycle over IPC ─────────────────────────────────

#[tokio::test]
async fn approve_plan_via_ipc_activates_goal_and_replays_as_duplicate() {
    let env = setup("approve").await;
    let (goal, plan, approval_id) = goal_with_pending_plan_approval(&env, "approve").await;
    let payload = json!({
        "approval_id": approval_id,
        "expected_plan_revision_id": plan.plan_revision_id,
    });

    let first = env
        .handler
        .handle_request(&ctx("appr-1"), &IpcCommand::GoalApprove, &payload)
        .await
        .unwrap();
    assert!(matches!(first.status, IpcResponseStatus::Success));
    assert_eq!(first.payload["status"], "approved");
    assert_eq!(first.payload["applied"], true);
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "active");

    let (plan_state,): (String,) =
        sqlx::query_as("SELECT state FROM plan_revisions WHERE plan_revision_id = ?")
            .bind(&plan.plan_revision_id)
            .fetch_one(&env.db.pool)
            .await
            .unwrap();
    assert_eq!(plan_state, "active");

    let second = env
        .handler
        .handle_request(&ctx("appr-1"), &IpcCommand::GoalApprove, &payload)
        .await
        .unwrap();
    assert!(matches!(second.status, IpcResponseStatus::Duplicate));
    assert_eq!(
        event_count(&env.db.pool, &goal.goal_id, "plan_approved").await,
        1
    );
}

#[tokio::test]
async fn stale_approve_decision_is_rejected() {
    let env = setup("stale").await;
    let (goal, _plan, approval_id) = goal_with_pending_plan_approval(&env, "stale").await;

    let err = env
        .handler
        .handle_request(
            &ctx("stale-1"),
            &IpcCommand::GoalApprove,
            &json!({
                "approval_id": approval_id,
                "expected_plan_revision_id": "pr-some-older-revision",
            }),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("stale"), "got: {err}");
    assert_eq!(
        goal_state(&env.db.pool, &goal.goal_id).await,
        "waiting_for_approval",
        "stale decision must leave the goal untouched"
    );
}

#[tokio::test]
async fn reject_plan_via_ipc_is_terminal() {
    let env = setup("reject").await;
    let (goal, plan, approval_id) = goal_with_pending_plan_approval(&env, "reject").await;

    let out = env
        .handler
        .handle_request(
            &ctx("rej-1"),
            &IpcCommand::GoalReject,
            &json!({
                "approval_id": approval_id,
                "expected_plan_revision_id": plan.plan_revision_id,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(out.status, IpcResponseStatus::Success));
    assert_eq!(out.payload["status"], "rejected");
    assert_eq!(out.payload["applied"], true);

    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "cancelled");
    let (plan_state,): (String,) =
        sqlx::query_as("SELECT state FROM plan_revisions WHERE plan_revision_id = ?")
            .bind(&plan.plan_revision_id)
            .fetch_one(&env.db.pool)
            .await
            .unwrap();
    assert_eq!(plan_state, "rejected");
}

#[tokio::test]
async fn request_changes_via_ipc_returns_goal_to_planning() {
    let env = setup("changes").await;
    let (goal, _plan, approval_id) = goal_with_pending_plan_approval(&env, "changes").await;

    let out = env
        .handler
        .handle_request(
            &ctx("chg-1"),
            &IpcCommand::GoalRequestChanges,
            &json!({
                "approval_id": approval_id,
                "feedback": "split task 2 into smaller steps",
            }),
        )
        .await
        .unwrap();
    assert!(matches!(out.status, IpcResponseStatus::Success));
    assert_eq!(out.payload["status"], "changes_requested");
    assert_eq!(out.payload["applied"], true);
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "planning");

    // The feedback is preserved as data for the next planning iteration.
    let (classification,): (String,) =
        sqlx::query_as("SELECT classification FROM user_interventions WHERE goal_id = ?")
            .bind(&goal.goal_id)
            .fetch_one(&env.db.pool)
            .await
            .unwrap();
    assert_eq!(classification, "plan_change_required");
}

// ── goal.answer over IPC ─────────────────────────────────────────────

#[tokio::test]
async fn answer_clarification_via_ipc_returns_goal_to_planning() {
    let env = setup("answer").await;
    let svc = &env.graph.goal_loop_service;
    let goal = svc
        .create_goal(make_test_goal("answer", true))
        .await
        .unwrap();
    let approval = svc
        .request_clarification(&goal.goal_id, &make_questions())
        .await
        .unwrap();

    let out = env
        .handler
        .handle_request(
            &ctx("ans-1"),
            &IpcCommand::GoalAnswer,
            &json!({
                "approval_id": approval.approval_id,
                "answers": {"q-1": "sqlite"},
            }),
        )
        .await
        .unwrap();
    assert!(matches!(out.status, IpcResponseStatus::Success));
    assert_eq!(out.payload["status"], "answered");
    assert_eq!(out.payload["applied"], true);
    assert_eq!(out.payload["goal_id"], goal.goal_id.as_str());
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "planning");
}

// ── goal.cancel idempotency ──────────────────────────────────────────

#[tokio::test]
async fn cancel_is_idempotent_with_and_without_ledger() {
    let env = setup("cancel").await;
    let goal = active_goal(&env, "cancel").await;
    let payload = json!({"goal_id": goal.goal_id});

    let first = env
        .handler
        .handle_request(&ctx("can-1"), &IpcCommand::GoalCancel, &payload)
        .await
        .unwrap();
    assert!(matches!(first.status, IpcResponseStatus::Success));
    assert_eq!(first.payload["applied"], true);
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "cancelled");

    // Ledger replay.
    let replay = env
        .handler
        .handle_request(&ctx("can-1"), &IpcCommand::GoalCancel, &payload)
        .await
        .unwrap();
    assert!(matches!(replay.status, IpcResponseStatus::Duplicate));

    // Fresh request without a key: service-level idempotent no-op.
    let again = env
        .handler
        .handle_request(&ctx(""), &IpcCommand::GoalCancel, &payload)
        .await
        .unwrap();
    assert!(matches!(again.status, IpcResponseStatus::Success));
    assert_eq!(again.payload["applied"], false);
}
