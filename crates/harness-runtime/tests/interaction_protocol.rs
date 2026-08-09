//! I8A Human Interaction Protocol — FSM, idempotency, and concurrency tests.
//!
//! Covers: clarification request/answer, plan approval (stale guards),
//! request-changes, interventions, pause/resume, dispatch gates, and the
//! atomic goal-event sequence.

use harness_core::contracts::goal::{
    ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator, GoalSpec,
    GoalState, SuccessCriterion, VerificationPolicy,
};
use harness_runtime::db::Database;
use harness_runtime::goal::interaction::InteractionOutcome;
use harness_runtime::goal::repo::GoalRepo;
use harness_runtime::goal::service::GoalLoopService;
use harness_runtime::goal::{
    ApprovalState, ApprovalType, ClarificationQuestion, InterventionClassification,
    InterventionState, PlanProposal, ProposedMilestone, ProposedTask,
};

// ── Helpers ──────────────────────────────────────────────────────────

async fn setup() -> (Database, GoalLoopService) {
    let db = Database::open_in_memory().await.unwrap();
    let svc = GoalLoopService::new(db.pool.clone());
    (db, svc)
}

fn make_test_goal(name: &str, interactive: bool) -> GoalSpec {
    // criterion_id is globally unique in goal_success_criteria — derive it
    // per goal so parallel tests on one DB never collide.
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

/// Persist a Validated plan revision for the goal (interactive step 1–5).
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

// ── Clarification protocol ───────────────────────────────────────────

#[tokio::test]
async fn clarification_request_parks_goal_in_wfa() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("clarify", true))
        .await
        .unwrap();

    let approval = svc
        .request_clarification(&goal.goal_id, &make_questions())
        .await
        .unwrap();

    assert_eq!(
        approval.approval_type,
        ApprovalType::ProvideMissingInformation
    );
    assert_eq!(approval.state, ApprovalState::Pending);
    assert_eq!(
        goal_state(&db.pool, &goal.goal_id).await,
        "waiting_for_approval"
    );
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "clarification_requested").await,
        1
    );

    let repo = GoalRepo::new(db.pool.clone());
    let pending = repo.list_pending_approvals(&goal.goal_id).await.unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn clarification_with_no_questions_is_rejected() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("clarify-empty", true))
        .await
        .unwrap();

    let err = svc
        .request_clarification(&goal.goal_id, &[])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("at least one question"),
        "got: {err}"
    );
}

#[tokio::test]
async fn answer_clarification_returns_goal_to_planning() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("answer", true))
        .await
        .unwrap();
    let approval = svc
        .request_clarification(&goal.goal_id, &make_questions())
        .await
        .unwrap();

    let answers = serde_json::json!({"q-1": "sqlite"});
    let outcome = svc
        .answer_clarification(&goal.goal_id, &approval.approval_id, &answers, "user:tui")
        .await
        .unwrap();

    assert_eq!(outcome, InteractionOutcome::Applied);
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "planning");
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "clarification_answered").await,
        1
    );

    // The answer is durably stored on the approval row.
    let repo = GoalRepo::new(db.pool.clone());
    let resolved = repo
        .get_approval(&approval.approval_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.state, ApprovalState::Approved);
    assert_eq!(resolved.response, Some(answers));
    assert_eq!(resolved.resolved_by.as_deref(), Some("user:tui"));
}

#[tokio::test]
async fn answer_clarification_replay_is_a_no_op() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("answer-replay", true))
        .await
        .unwrap();
    let approval = svc
        .request_clarification(&goal.goal_id, &make_questions())
        .await
        .unwrap();

    let answers = serde_json::json!({"q-1": "sqlite"});
    let first = svc
        .answer_clarification(&goal.goal_id, &approval.approval_id, &answers, "user:tui")
        .await
        .unwrap();
    let replay = svc
        .answer_clarification(&goal.goal_id, &approval.approval_id, &answers, "user:tui")
        .await
        .unwrap();

    assert_eq!(first, InteractionOutcome::Applied);
    assert_eq!(replay, InteractionOutcome::AlreadyInState);
    // No duplicate events, no duplicate transitions.
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "clarification_answered").await,
        1
    );
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "planning");
}

#[tokio::test]
async fn answer_clarification_rejects_foreign_goal() {
    let (_db, svc) = setup().await;
    let goal_a = svc
        .create_goal(make_test_goal("owner", true))
        .await
        .unwrap();
    let goal_b = svc
        .create_goal(make_test_goal("intruder", true))
        .await
        .unwrap();
    let approval = svc
        .request_clarification(&goal_a.goal_id, &make_questions())
        .await
        .unwrap();

    let err = svc
        .answer_clarification(
            &goal_b.goal_id,
            &approval.approval_id,
            &serde_json::json!({}),
            "user:tui",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not belong"), "got: {err}");
}

#[tokio::test]
async fn answer_clarification_rejects_wrong_approval_type() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("wrong-type", true))
        .await
        .unwrap();
    let plan = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();

    let err = svc
        .answer_clarification(
            &goal.goal_id,
            &approval.approval_id,
            &serde_json::json!({}),
            "user:tui",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not a clarification"),
        "got: {err}"
    );
}

#[tokio::test]
async fn answer_clarification_unknown_approval_is_not_found() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("missing", true))
        .await
        .unwrap();
    let err = svc
        .answer_clarification(&goal.goal_id, "ap-missing", &serde_json::json!({}), "u")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}

// ── Plan approval protocol ───────────────────────────────────────────

#[tokio::test]
async fn plan_approval_full_flow_activates_plan_and_goal() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("approve", true))
        .await
        .unwrap();
    let plan = persist_plan(&svc, &goal).await;

    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();
    assert_eq!(approval.approval_type, ApprovalType::ApproveInitialPlan);
    assert_eq!(
        approval.plan_revision_id.as_deref(),
        Some(plan.plan_revision_id.as_str())
    );
    assert_eq!(
        goal_state(&db.pool, &goal.goal_id).await,
        "waiting_for_approval"
    );
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "plan_approval_requested").await,
        1
    );

    let outcome = svc
        .approve_plan(
            &goal.goal_id,
            &approval.approval_id,
            "user:tui",
            Some(&plan.plan_revision_id),
        )
        .await
        .unwrap();

    assert_eq!(outcome, InteractionOutcome::Applied);
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "active");
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "plan_approved").await,
        1
    );

    let repo = GoalRepo::new(db.pool.clone());
    let revision = repo
        .get_plan_revision(&plan.plan_revision_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        revision.state,
        harness_core::contracts::plan::PlanState::Active
    );
}

#[tokio::test]
async fn approve_plan_replay_is_a_no_op() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("approve-replay", true))
        .await
        .unwrap();
    let plan = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();

    let first = svc
        .approve_plan(&goal.goal_id, &approval.approval_id, "user:tui", None)
        .await
        .unwrap();
    let replay = svc
        .approve_plan(&goal.goal_id, &approval.approval_id, "user:tui", None)
        .await
        .unwrap();

    assert_eq!(first, InteractionOutcome::Applied);
    assert_eq!(replay, InteractionOutcome::AlreadyInState);
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "plan_approved").await,
        1
    );
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "active");
}

#[tokio::test]
async fn approve_plan_rejects_mismatched_expected_revision() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("stale-expected", true))
        .await
        .unwrap();
    let plan = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();

    let err = svc
        .approve_plan(
            &goal.goal_id,
            &approval.approval_id,
            "user:tui",
            Some("pr-other"),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("stale"), "got: {err}");
    // Nothing was applied.
    assert_eq!(
        goal_state(&db.pool, &goal.goal_id).await,
        "waiting_for_approval"
    );
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "plan_approved").await,
        0
    );
}

#[tokio::test]
async fn approve_plan_rejects_superseded_revision() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("stale-superseded", true))
        .await
        .unwrap();
    let plan_v1 = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan_v1, &make_proposal(&goal))
        .await
        .unwrap();

    // A newer revision lands before the user decides.
    let _plan_v2 = persist_plan(&svc, &goal).await;

    let err = svc
        .approve_plan(
            &goal.goal_id,
            &approval.approval_id,
            "user:tui",
            Some(&plan_v1.plan_revision_id),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("superseded"), "got: {err}");
    // The stale decision resolved nothing.
    let repo = GoalRepo::new(db.pool.clone());
    let unresolved = repo
        .get_approval(&approval.approval_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unresolved.state, ApprovalState::Pending);
}

#[tokio::test]
async fn request_plan_approval_supersedes_older_pending_requests() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("supersede", true))
        .await
        .unwrap();
    let plan_v1 = persist_plan(&svc, &goal).await;
    let old = svc
        .request_plan_approval(&goal.goal_id, &plan_v1, &make_proposal(&goal))
        .await
        .unwrap();

    let plan_v2 = persist_plan(&svc, &goal).await;
    let new = svc
        .request_plan_approval(&goal.goal_id, &plan_v2, &make_proposal(&goal))
        .await
        .unwrap();

    let repo = GoalRepo::new(db.pool.clone());
    let pending = repo.list_pending_approvals(&goal.goal_id).await.unwrap();
    assert_eq!(pending.len(), 1, "only the newest request may stay pending");
    assert_eq!(pending[0].approval_id, new.approval_id);

    let cancelled = repo.get_approval(&old.approval_id).await.unwrap().unwrap();
    assert_eq!(cancelled.state, ApprovalState::Cancelled);
    assert_eq!(cancelled.resolved_by.as_deref(), Some("system:superseded"));
}

// ── Request changes ──────────────────────────────────────────────────

#[tokio::test]
async fn request_plan_changes_rejects_revision_and_records_feedback() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("changes", true))
        .await
        .unwrap();
    let plan = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();

    let outcome = svc
        .request_plan_changes(
            &goal.goal_id,
            &approval.approval_id,
            "split task 2 into smaller steps",
            "user:tui",
        )
        .await
        .unwrap();

    assert_eq!(outcome, InteractionOutcome::Applied);
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "planning");
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "plan_changes_requested").await,
        1
    );

    let repo = GoalRepo::new(db.pool.clone());
    let revision = repo
        .get_plan_revision(&plan.plan_revision_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        revision.state,
        harness_core::contracts::plan::PlanState::Rejected
    );

    // Feedback became planner input for the next revision.
    let interventions = repo
        .list_interventions(&goal.goal_id, Some("received"))
        .await
        .unwrap();
    assert_eq!(interventions.len(), 1);
    assert_eq!(
        interventions[0].classification,
        InterventionClassification::PlanChangeRequired
    );
    assert_eq!(interventions[0].message, "split task 2 into smaller steps");
}

#[tokio::test]
async fn request_plan_changes_replay_is_a_no_op() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("changes-replay", true))
        .await
        .unwrap();
    let plan = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();

    let first = svc
        .request_plan_changes(&goal.goal_id, &approval.approval_id, "feedback", "user:tui")
        .await
        .unwrap();
    let replay = svc
        .request_plan_changes(&goal.goal_id, &approval.approval_id, "feedback", "user:tui")
        .await
        .unwrap();

    assert_eq!(first, InteractionOutcome::Applied);
    assert_eq!(replay, InteractionOutcome::AlreadyInState);
    let repo = GoalRepo::new(db.pool.clone());
    let interventions = repo.list_interventions(&goal.goal_id, None).await.unwrap();
    assert_eq!(interventions.len(), 1, "replay must not duplicate feedback");
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "plan_changes_requested").await,
        1
    );
}

// ── User interventions ───────────────────────────────────────────────

#[tokio::test]
async fn record_intervention_stores_message_as_data() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("intervene", false))
        .await
        .unwrap();

    let iv = svc
        .record_intervention(&goal.goal_id, "please prefer sqlite", Some("req-1"), "ipc")
        .await
        .unwrap();

    assert_eq!(
        iv.classification,
        InterventionClassification::ConstraintAddition
    );
    assert_eq!(iv.state, InterventionState::Received);
    assert_eq!(iv.request_id.as_deref(), Some("req-1"));
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "user_intervention_recorded").await,
        1
    );
}

#[tokio::test]
async fn record_intervention_rejects_empty_message() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("intervene-empty", false))
        .await
        .unwrap();
    let err = svc
        .record_intervention(&goal.goal_id, "   ", None, "ipc")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("must not be empty"), "got: {err}");
}

#[tokio::test]
async fn record_intervention_rejects_terminal_goal() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("intervene-terminal", false))
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Cancelled)
        .await
        .unwrap();

    let err = svc
        .record_intervention(&goal.goal_id, "too late", None, "ipc")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("terminal"), "got: {err}");
}

#[tokio::test]
async fn interventions_are_marked_applied_on_activation() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("iv-applied", true))
        .await
        .unwrap();
    svc.record_intervention(&goal.goal_id, "constraint A", None, "ipc")
        .await
        .unwrap();
    svc.record_intervention(&goal.goal_id, "constraint B", None, "ipc")
        .await
        .unwrap();

    let plan = persist_plan(&svc, &goal).await;
    let approval = svc
        .request_plan_approval(&goal.goal_id, &plan, &make_proposal(&goal))
        .await
        .unwrap();
    svc.approve_plan(&goal.goal_id, &approval.approval_id, "user:tui", None)
        .await
        .unwrap();

    let repo = GoalRepo::new(db.pool.clone());
    let received = repo
        .list_interventions(&goal.goal_id, Some("received"))
        .await
        .unwrap();
    assert!(
        received.is_empty(),
        "activation must consume received interventions"
    );
    let applied = repo
        .list_interventions(&goal.goal_id, Some("applied"))
        .await
        .unwrap();
    assert_eq!(applied.len(), 2);
    for iv in &applied {
        assert_eq!(
            iv.applied_plan_revision_id.as_deref(),
            Some(plan.plan_revision_id.as_str())
        );
    }
}

// ── Pause / Resume ───────────────────────────────────────────────────

#[tokio::test]
async fn pause_and_resume_are_idempotent() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("pause", false))
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Planning)
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Active)
        .await
        .unwrap();

    assert_eq!(
        svc.pause_goal(&goal.goal_id).await.unwrap(),
        InteractionOutcome::Applied
    );
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "paused");
    assert_eq!(
        svc.pause_goal(&goal.goal_id).await.unwrap(),
        InteractionOutcome::AlreadyInState
    );
    assert_eq!(event_count(&db.pool, &goal.goal_id, "goal_paused").await, 1);

    assert_eq!(
        svc.resume_goal(&goal.goal_id).await.unwrap(),
        InteractionOutcome::Applied
    );
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "active");
    assert_eq!(
        svc.resume_goal(&goal.goal_id).await.unwrap(),
        InteractionOutcome::AlreadyInState
    );
    assert_eq!(
        event_count(&db.pool, &goal.goal_id, "goal_resumed").await,
        1
    );
}

#[tokio::test]
async fn pause_from_blocked_is_legal() {
    let (db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("pause-blocked", false))
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Planning)
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Active)
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Blocked)
        .await
        .unwrap();

    assert_eq!(
        svc.pause_goal(&goal.goal_id).await.unwrap(),
        InteractionOutcome::Applied
    );
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "paused");
}

#[tokio::test]
async fn pause_rejects_illegal_states() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("pause-draft", false))
        .await
        .unwrap();
    // Draft cannot pause.
    assert!(svc.pause_goal(&goal.goal_id).await.is_err());
    // Planning cannot pause either (FSM has no Planning → Paused edge).
    svc.transition_goal(&goal.goal_id, GoalState::Planning)
        .await
        .unwrap();
    assert!(svc.pause_goal(&goal.goal_id).await.is_err());
}

#[tokio::test]
async fn resume_rejects_non_paused_states() {
    let (_db, svc) = setup().await;
    let goal = svc
        .create_goal(make_test_goal("resume-draft", false))
        .await
        .unwrap();
    assert!(svc.resume_goal(&goal.goal_id).await.is_err());
}

// ── Dispatch gates ───────────────────────────────────────────────────

#[tokio::test]
async fn drive_goal_loop_gates_on_paused_and_wfa() {
    let (db, svc) = setup().await;
    // Paused: loop returns without doing anything (no planner is configured,
    // so any attempt to plan would error — Ok proves the gate fired).
    let goal = svc
        .create_goal(make_test_goal("gate-paused", false))
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Planning)
        .await
        .unwrap();
    svc.transition_goal(&goal.goal_id, GoalState::Active)
        .await
        .unwrap();
    svc.pause_goal(&goal.goal_id).await.unwrap();
    svc.drive_goal_loop(&goal.goal_id).await.unwrap();
    assert_eq!(goal_state(&db.pool, &goal.goal_id).await, "paused");

    // WaitingForApproval: same gate.
    let goal2 = svc
        .create_goal(make_test_goal("gate-wfa", true))
        .await
        .unwrap();
    svc.request_clarification(&goal2.goal_id, &make_questions())
        .await
        .unwrap();
    svc.drive_goal_loop(&goal2.goal_id).await.unwrap();
    assert_eq!(
        goal_state(&db.pool, &goal2.goal_id).await,
        "waiting_for_approval"
    );
}

// ── Event sequence atomicity ─────────────────────────────────────────

#[tokio::test]
async fn goal_event_sequence_is_unique_under_concurrency() {
    // File-backed DB: concurrent writers over a real WAL pool.
    let td = tempfile::tempdir().unwrap();
    let db = Database::open(&td.path().join("iact.db")).await.unwrap();
    let svc = GoalLoopService::new(db.pool.clone());
    let goal = svc
        .create_goal(make_test_goal("concurrent-events", false))
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..16 {
        let repo = GoalRepo::new(db.pool.clone());
        let goal_id = goal.goal_id.clone();
        handles.push(tokio::spawn(async move {
            repo.append_goal_event(&goal_id, "stress_event", &format!("{{\"i\":{i}}}"))
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    let seqs: Vec<(i64,)> = sqlx::query_as(
        "SELECT sequence_num FROM goal_events WHERE goal_id = ? AND event_type = 'stress_event' ORDER BY sequence_num",
    )
    .bind(&goal.goal_id)
    .fetch_all(&db.pool)
    .await
    .unwrap();

    assert_eq!(seqs.len(), 16);
    let mut unique: Vec<i64> = seqs.iter().map(|s| s.0).collect();
    unique.dedup();
    assert_eq!(unique.len(), 16, "sequence numbers must be strictly unique");
}

// ── Crash-recovery shaped invariants ─────────────────────────────────

#[tokio::test]
async fn pending_approval_survives_service_restart() {
    // Same pool, new service instance — the "restart" is a new writer over
    // the same durable state.
    let td = tempfile::tempdir().unwrap();
    let db = Database::open(&td.path().join("restart.db")).await.unwrap();
    let goal_id;
    let approval_id;
    {
        let svc = GoalLoopService::new(db.pool.clone());
        let goal = svc
            .create_goal(make_test_goal("restart", true))
            .await
            .unwrap();
        let approval = svc
            .request_clarification(&goal.goal_id, &make_questions())
            .await
            .unwrap();
        goal_id = goal.goal_id;
        approval_id = approval.approval_id;
    }

    let svc2 = GoalLoopService::new(db.pool.clone());
    // The parked state and pending approval are still there.
    assert_eq!(goal_state(&db.pool, &goal_id).await, "waiting_for_approval");
    let outcome = svc2
        .answer_clarification(
            &goal_id,
            &approval_id,
            &serde_json::json!({"q-1": "a"}),
            "u",
        )
        .await
        .unwrap();
    assert_eq!(outcome, InteractionOutcome::Applied);
    assert_eq!(goal_state(&db.pool, &goal_id).await, "planning");
}
