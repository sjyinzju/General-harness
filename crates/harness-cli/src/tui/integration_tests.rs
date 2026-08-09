//! I8B integration tests — the TUI control path against a REAL in-process
//! Supervisor over a live Named Pipe.
//!
//! Path covered: user input → reducer effects → `PipeGateway` (framed IPC
//! envelope) → `IpcServer` → `SupervisorCommandHandler` → Request Ledger →
//! `GoalLoopService` → SQLite. No mocks on the server side; the gateway is
//! the production `PipeGateway`.

use std::path::PathBuf;
use std::sync::Arc;

use harness_core::contracts::goal::{
    ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator, GoalSpec,
    GoalState, SuccessCriterion, VerificationPolicy,
};
use harness_runtime::db::Database;
use harness_runtime::goal::{ClarificationQuestion, PlanProposal, ProposedMilestone, ProposedTask};
use harness_runtime::ipc::IpcServer;
use harness_runtime::production_graph::ProductionGraph;
use harness_runtime::supervisor::command_handler::SupervisorCommandHandler;
use serde_json::{json, Value};

use super::action::{Effect, TuiAction};
use super::gateway::{
    parse_events, parse_goal_list, parse_snapshot, GatewayReply, PipeGateway, TuiGateway,
};
use super::reducer::reduce;
use super::state::{InputMode, TuiAppState};

// ── Environment: in-process Supervisor behind a real Named Pipe ──────

struct Env {
    #[allow(dead_code)]
    db: Database,
    graph: ProductionGraph,
    gateway: PipeGateway,
}

fn isolation_dir(label: &str) -> PathBuf {
    // Same convention as the I8A IPC tests: stay out of any git worktree.
    let base = std::path::PathBuf::from(
        std::env::var("HARNESS_WORKTREE_ROOT").unwrap_or_else(|_| r"C:\Temp".to_string()),
    );
    let dir = base.join(format!("i8b-tui-{}-{}", label, uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create isolation dir");
    dir
}

/// Build the production graph on an in-memory DB, wrap it in a command
/// handler, and serve it on a unique Named Pipe endpoint.
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
        harness_runtime::liveness::RunContext::create(&iso, "i8b-tui", false)
            .expect("create run context"),
    );
    let worktree_root = run_context
        .managed_temp()
        .map(|t| t.path().to_path_buf())
        .unwrap_or_else(|| iso.join("tmp"));

    let graph = ProductionGraph::build(db.pool.clone(), &worktree_root, &repo_root, run_context)
        .expect("build production graph");
    let handler = Arc::new(SupervisorCommandHandler::new(
        db.pool.clone(),
        graph.supervisor_services.clone(),
        None,
        0,
    ));

    let endpoint = format!("i8b-tui-{}-{}", label, uuid::Uuid::new_v4());
    let server = IpcServer::new(
        harness_core::contracts::ipc::IpcConfig::default(),
        handler,
        db.pool.clone(),
    );
    let serve_endpoint = endpoint.clone();
    tokio::spawn(async move {
        let _ = server.serve(&serve_endpoint).await;
    });

    let gateway = PipeGateway::new(endpoint);
    // Wait for the listener to come up (bind is lazy until first accept).
    for _ in 0..100 {
        if gateway.ping().await {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Env { db, graph, gateway }
}

// ── Fixtures (same shape as the I8A IPC interaction tests) ───────────

fn test_goal(name: &str, interactive: bool) -> GoalSpec {
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
            user_id: "tui-user".into(),
            user_name: None,
        },
        created_at: chrono::Utc::now(),
    }
}

fn test_proposal(goal: &GoalSpec) -> PlanProposal {
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
        tasks: vec![ProposedTask {
            client_ref: "t1".into(),
            milestone_ref: "m1".into(),
            title: "Task 1".into(),
            objective: "Do the thing".into(),
            acceptance_criteria: vec!["it works".into()],
            dependencies: vec![],
            expected_evidence: vec!["task_terminal_result".into()],
            expected_resource_scope: vec![],
            risk_level: "low".into(),
            requires_approval: false,
        }],
        risks: vec![],
        completion_strategy: "all tasks complete".into(),
    }
}

fn test_questions() -> Vec<ClarificationQuestion> {
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

/// Interactive goal parked in WaitingForApproval with a pending plan approval.
async fn goal_with_pending_plan_approval(env: &Env) -> (GoalSpec, String, String) {
    let svc = &env.graph.goal_loop_service;
    let goal = svc.create_goal(test_goal("approval", true)).await.unwrap();
    let plan = svc
        .persist_plan_revision(
            &goal.goal_id,
            &test_proposal(&goal),
            "planner-test",
            &format!("inv-{}", uuid::Uuid::new_v4()),
            "abc123def456",
            1,
        )
        .await
        .unwrap();
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &test_proposal(&goal))
        .await
        .unwrap();
    (goal, plan.plan_revision_id, approval.approval_id)
}

/// Non-interactive goal driven to Active (the only pausable state).
async fn active_goal(env: &Env) -> GoalSpec {
    let svc = &env.graph.goal_loop_service;
    let goal = svc
        .create_goal(test_goal("lifecycle", false))
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Planning)
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Active)
        .await
        .unwrap();
    goal
}

// ── Reducer-side helpers ─────────────────────────────────────────────

fn type_text(state: &mut TuiAppState, text: &str) {
    for c in text.chars() {
        reduce(state, TuiAction::CharInput(c));
    }
}

/// Extract (command, payload, key) triples from Ipc effects, in order.
fn ipc_effects(effects: Vec<Effect>) -> Vec<(String, Value, String)> {
    effects
        .into_iter()
        .filter_map(|e| match e {
            Effect::Ipc {
                command,
                payload,
                key,
                ..
            } => Some((command, payload, key)),
            _ => None,
        })
        .collect()
}

/// Feed a snapshot into the reducer and return the post-reduce state.
async fn attach_via_snapshot(state: &mut TuiAppState, gw: &PipeGateway, goal_id: &str) {
    let reply = gw
        .send(
            "goal.snapshot",
            json!({"goal_id": goal_id}),
            &format!("read-{}", uuid::Uuid::new_v4()),
        )
        .await
        .unwrap();
    let snapshot = parse_snapshot(reply).expect("snapshot must parse");
    reduce(
        state,
        TuiAction::SnapshotReceived {
            snapshot: Box::new(snapshot),
        },
    );
}

fn console_state() -> TuiAppState {
    let mut s = TuiAppState::new("i8b-integration");
    s.term_cols = 120;
    s.term_rows = 36;
    s
}

// ── 1. Goal submit: type → create → start → snapshot → list ──────────

#[tokio::test]
async fn submit_new_goal_round_trip_over_pipe() {
    let env = setup("submit").await;
    let gw = env.gateway.clone();
    assert!(gw.ping().await, "health probe must succeed");

    let mut state = console_state();
    type_text(&mut state, "Implement the demo widget");
    let effects = ipc_effects(reduce(&mut state, TuiAction::Submit));
    assert_eq!(effects.len(), 2, "goal.create then goal.start");
    assert_eq!(effects[0].0, "goal.create");
    assert_eq!(effects[1].0, "goal.start");

    for (command, payload, key) in &effects {
        let reply = gw.send(command, payload.clone(), key).await.unwrap();
        assert!(reply.is_ok(), "{command} failed: {reply:?}");
    }

    let goal_id = state
        .active_goal_id
        .clone()
        .expect("goal attached on submit");
    let snapshot = parse_snapshot(
        gw.send("goal.snapshot", json!({"goal_id": goal_id}), "read-1")
            .await
            .unwrap(),
    )
    .expect("snapshot parses");
    assert_eq!(snapshot.goal.goal_id, goal_id);
    assert_eq!(snapshot.goal.title, "Implement the demo widget");
    assert!(
        ["planning", "waiting_for_approval"].contains(&snapshot.goal.state.as_str()),
        "interactive goal parks before execution, got {}",
        snapshot.goal.state
    );

    let rows = parse_goal_list(gw.send("goal.list", json!({}), "read-2").await.unwrap())
        .expect("goal.list parses");
    let row = rows.iter().find(|r| r.goal_id == goal_id).expect("listed");
    assert!(!row.state.is_empty(), "goal.list projects state");
    assert!(!row.created_at.is_empty(), "goal.list projects created_at");
    assert!(!row.updated_at.is_empty(), "goal.list projects updated_at");
}

// ── 2. Clarification: modal rebuilt from snapshot, answer is ledgered ─

#[tokio::test]
async fn clarification_answer_flow_with_ledger_replay() {
    let env = setup("clarify").await;
    let svc = &env.graph.goal_loop_service;
    let goal = svc.create_goal(test_goal("clarify", true)).await.unwrap();
    svc.request_clarification(&goal.goal_id, &test_questions())
        .await
        .unwrap();

    let gw = env.gateway.clone();
    let mut state = console_state();
    attach_via_snapshot(&mut state, &gw, &goal.goal_id).await;
    assert!(state.pending.clarify_approval_id.is_some(), "modal opened");
    assert!(matches!(state.input_mode, InputMode::Answer));

    type_text(&mut state, "sqlite");
    let effects = ipc_effects(reduce(&mut state, TuiAction::Submit));
    assert_eq!(effects.len(), 1);
    let (command, payload, key) = &effects[0];
    assert_eq!(command, "goal.answer");
    assert_eq!(payload["answers"]["q-1"], "sqlite");

    let reply = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(reply.is_ok(), "answer failed: {reply:?}");
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "planning");

    // Crash-retry with the SAME key + payload: the ledger replays, the
    // answer is not applied twice.
    let replay = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(
        matches!(replay, GatewayReply::Duplicate(_)),
        "expected Duplicate replay, got {replay:?}"
    );
}

// ── 3. Plan approval: 'a' approves with the bound plan revision ──────

#[tokio::test]
async fn plan_approval_via_decision_key_activates_goal() {
    let env = setup("approve").await;
    let (goal, plan_revision_id, _approval_id) = goal_with_pending_plan_approval(&env).await;

    let gw = env.gateway.clone();
    let mut state = console_state();
    attach_via_snapshot(&mut state, &gw, &goal.goal_id).await;
    assert!(state.pending.approve_approval_id.is_some(), "modal opened");
    assert_eq!(
        state.pending.approve_plan_revision_id.as_deref(),
        Some(plan_revision_id.as_str()),
        "approval binds the exact plan revision from the snapshot"
    );

    let effects = ipc_effects(reduce(&mut state, TuiAction::CharInput('a')));
    assert_eq!(effects.len(), 1);
    let (command, payload, key) = &effects[0];
    assert_eq!(command, "goal.approve");
    assert_eq!(payload["expected_plan_revision_id"], plan_revision_id);

    let reply = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(reply.is_ok(), "approve failed: {reply:?}");
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "active");

    let replay = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(matches!(replay, GatewayReply::Duplicate(_)));
}

// ── 4. Request changes: 'e' then feedback returns the goal to planning

#[tokio::test]
async fn request_changes_flow_returns_goal_to_planning() {
    let env = setup("changes").await;
    let (goal, _plan_revision_id, _approval_id) = goal_with_pending_plan_approval(&env).await;

    let gw = env.gateway.clone();
    let mut state = console_state();
    attach_via_snapshot(&mut state, &gw, &goal.goal_id).await;

    reduce(&mut state, TuiAction::CharInput('e'));
    assert!(matches!(state.input_mode, InputMode::PlanChanges));
    type_text(&mut state, "split the task into smaller steps");
    let effects = ipc_effects(reduce(&mut state, TuiAction::Submit));
    assert_eq!(effects.len(), 1);
    let (command, payload, key) = &effects[0];
    assert_eq!(command, "goal.request_changes");
    assert_eq!(payload["feedback"], "split the task into smaller steps");

    let reply = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(reply.is_ok(), "request_changes failed: {reply:?}");
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "planning");
}

// ── 5. Intervention + event stream: the user note reaches the ledger ─

#[tokio::test]
async fn intervention_and_events_flow_over_pipe() {
    let env = setup("intervene").await;
    let goal = active_goal(&env).await;

    let gw = env.gateway.clone();
    let mut state = console_state();
    attach_via_snapshot(&mut state, &gw, &goal.goal_id).await;
    let cursor_after_snapshot = state.cursor;

    type_text(&mut state, "prefer sqlite over postgres");
    let effects = ipc_effects(reduce(&mut state, TuiAction::Submit));
    assert_eq!(effects.len(), 1);
    let (command, payload, key) = &effects[0];
    assert_eq!(command, "goal.intervene");
    assert_eq!(payload["message"], "prefer sqlite over postgres");
    let reply = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(reply.is_ok(), "intervene failed: {reply:?}");

    // The event stream carries the intervention forward from the cursor.
    let events_reply = gw
        .send(
            "goal.events",
            json!({"goal_id": goal.goal_id, "after_sequence": cursor_after_snapshot, "wait_ms": 5000}),
            "read-events",
        )
        .await
        .unwrap();
    let (events, last) = parse_events(events_reply).expect("events parse");
    assert!(
        events
            .iter()
            .any(|e| e.event_type == "user_intervention_recorded"),
        "intervention event must appear after the snapshot cursor"
    );

    reduce(
        &mut state,
        TuiAction::EventsReceived {
            events: events.clone(),
        },
    );
    assert!(state.cursor > cursor_after_snapshot, "cursor advances");
    assert_eq!(state.cursor, last.max(cursor_after_snapshot));
    assert!(state.panels_dirty, "events mark panels for resnapshot");
}

// ── 6. Pause/Resume/Cancel via slash commands ────────────────────────

#[tokio::test]
async fn pause_resume_cancel_via_console_commands() {
    let env = setup("lifecycle").await;
    let goal = active_goal(&env).await;

    let gw = env.gateway.clone();
    let mut state = console_state();
    attach_via_snapshot(&mut state, &gw, &goal.goal_id).await;

    // /pause
    type_text(&mut state, "/pause");
    let effects = ipc_effects(reduce(&mut state, TuiAction::Submit));
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].0, "goal.pause");
    let (command, payload, key) = &effects[0];
    assert!(gw
        .send(command, payload.clone(), key)
        .await
        .unwrap()
        .is_ok());
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "paused");

    // Ledger replay of the same pause: Duplicate, no second event.
    let replay = gw.send(command, payload.clone(), key).await.unwrap();
    assert!(matches!(replay, GatewayReply::Duplicate(_)));

    // Same key, DIFFERENT command: Conflict, surfaced, never retried.
    let conflict = gw.send("goal.resume", payload.clone(), key).await.unwrap();
    assert!(
        matches!(conflict, GatewayReply::Conflict(_)),
        "same key with different payload must conflict, got {conflict:?}"
    );
    reduce(
        &mut state,
        TuiAction::MutationConflict {
            slot: super::action::MutationSlot::Pause,
            message: "conflict".into(),
        },
    );
    assert!(
        state.toast.is_some(),
        "conflict must be visible to the user"
    );
    assert_eq!(
        goal_state(&env.db.pool, &goal.goal_id).await,
        "paused",
        "conflicting request has no side effect"
    );

    // /resume with a fresh key.
    type_text(&mut state, "/resume");
    let effects = ipc_effects(reduce(&mut state, TuiAction::Submit));
    assert_eq!(effects[0].0, "goal.resume");
    let (command, payload, key) = &effects[0];
    assert!(gw
        .send(command, payload.clone(), key)
        .await
        .unwrap()
        .is_ok());
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "active");

    // /cancel asks for confirmation, 'y' sends goal.cancel.
    type_text(&mut state, "/cancel");
    let effects = reduce(&mut state, TuiAction::Submit);
    assert!(effects.is_empty(), "cancel requires confirmation first");
    assert!(state.pending.cancel_confirm);
    let effects = ipc_effects(reduce(&mut state, TuiAction::CharInput('y')));
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].0, "goal.cancel");
    let (command, payload, key) = &effects[0];
    assert!(gw
        .send(command, payload.clone(), key)
        .await
        .unwrap()
        .is_ok());
    assert_eq!(goal_state(&env.db.pool, &goal.goal_id).await, "cancelled");
}

// ── 7. Reconnect projection: snapshot cursor → events is gapless ─────

#[tokio::test]
async fn reconnect_snapshot_then_events_is_gapless_over_pipe() {
    let env = setup("reconnect").await;
    let svc = &env.graph.goal_loop_service;
    let goal = svc.create_goal(test_goal("reconnect", true)).await.unwrap();
    svc.request_clarification(&goal.goal_id, &test_questions())
        .await
        .unwrap();

    let gw = env.gateway.clone();
    let mut state = console_state();
    attach_via_snapshot(&mut state, &gw, &goal.goal_id).await;
    let cursor = state.cursor;
    assert!(cursor >= 1, "snapshot provides the resume cursor");

    // Something happens "while disconnected".
    svc.record_intervention(&goal.goal_id, "note for planner", None, "user")
        .await
        .unwrap();

    // Reconnect: events after the cursor yield exactly the missed event.
    let (events, _) = parse_events(
        gw.send(
            "goal.events",
            json!({"goal_id": goal.goal_id, "after_sequence": cursor}),
            "read-resume",
        )
        .await
        .unwrap(),
    )
    .expect("events parse");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "user_intervention_recorded");
}
