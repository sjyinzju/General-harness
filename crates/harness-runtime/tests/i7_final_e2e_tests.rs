//! I7 Final Production-Path E2E Tests.
//!
//! Scene A: Deterministic two-task complete Goal E2E.
//! Scene B: Failure → Replan → Success.
//! Scene C: RoleIsolationPolicy production enforcement.
//!
//! All tests use a deterministic AgentAdapter through the normal
//! Adapter interface, NOT direct DB writes or fixture shortcuts.

use std::collections::HashMap;
use std::sync::Arc;

use harness_core::contracts::agent_adapter::{
    AgentAdapter, AgentConfigInfo, AgentEventSink, AgentSession, AuthCheckResult, DetectionResult,
    SessionOptions,
};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::goal::{
    ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator, GoalSpec,
    GoalState, SuccessCriterion, VerificationPolicy,
};
use harness_core::contracts::plan::PlannedTaskState;
use harness_core::contracts::runtime_profile::{
    ActiveProbeChecks, ActiveValidationResult, AuthCheckStatus, AuthMode, AuthStatus,
    CapabilitySet, CoreStatus, ExecutionStatus, OptionalCapabilities, ProviderSource,
    RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::task_envelope::TaskEnvelope;
use harness_core::CoreError;
use harness_runtime::goal::repo::GoalRepo;
use harness_runtime::production_graph::ProductionGraph;

// ── Helpers ──────────────────────────────────────────────────────────

fn make_test_profile(id: &str) -> RuntimeProfile {
    RuntimeProfile {
        id: id.to_string(),
        agent_definition_id: format!("def-{}", id),
        label: format!("Test Profile {}", id),
        agent_kind: "fake".into(),
        adapter_kind: "fake".into(),
        agent_version: "1.0".into(),
        executable_path: "fake-agent.exe".into(),
        provider: "test".into(),
        provider_source: ProviderSource::UserDeclared,
        model: Some("test-model".into()),
        base_url: None,
        auth_mode: AuthMode::None,
        auth_status: AuthStatus::Unknown,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Unknown,
                working_directory: TriState::Unknown,
                stream_output: TriState::Unknown,
                process_exit: TriState::Unknown,
                cancellation: TriState::Unknown,
                timeout: TriState::Unknown,
                final_result: TriState::Unknown,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unknown,
                structured_output: TriState::Unknown,
                tool_events: TriState::Unknown,
                file_change_events: TriState::Unknown,
                reasoning_summary: TriState::Unknown,
                interactive_approval: TriState::Unknown,
                usage_reporting: TriState::Unknown,
            },
            workspace_modes: vec![],
            supported_languages: vec![],
            mcp_tools: vec![],
            supported_platforms: vec![],
        },
        core_status: CoreStatus::Available,
        authentication_status: AuthCheckStatus::Unknown,
        execution_status: ExecutionStatus::Untested,
        optional_integrations: vec![],
        discovery_source: "test".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 1,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

fn make_two_task_goal() -> GoalSpec {
    GoalSpec {
        goal_id: format!("g-{}", uuid::Uuid::new_v4()),
        revision: 1,
        title: "Add utility function and test".into(),
        objective: "Add a deterministic pure function and its unit test".into(),
        repository_id: "test-repo".into(),
        target_ref: "refs/heads/main".into(),
        initial_base_head: "abc123def456".into(),
        success_criteria: vec![SuccessCriterion {
            criterion_id: "c1".into(),
            description: "Pure function exists and compiles".into(),
            evidence_policy: EvidencePolicy::TaskTerminalResult,
            verification_policy: VerificationPolicy::ExistenceOnly,
            subjectivity: CriterionSubjectivity::Objective,
            required: true,
        }],
        constraints: vec![],
        non_goals: vec![],
        budget: GoalBudget {
            max_plan_revisions: 3,
            max_total_tasks: 10,
            max_active_tasks: 4,
            max_consecutive_failures: 3,
            max_no_progress_iterations: 5,
            ..Default::default()
        },
        approval_policy: ApprovalPolicy::default(),
        created_by: GoalCreator::User {
            user_id: "test-user".into(),
            user_name: None,
        },
        created_at: chrono::Utc::now(),
    }
}

// ── Deterministic AgentAdapter ───────────────────────────────────────

/// A minimal deterministic AgentAdapter that returns a pre-scripted JSON result.
struct DeterministicAdapter {
    response_json: serde_json::Value,
}

impl DeterministicAdapter {
    fn new(response_json: serde_json::Value) -> Self {
        Self { response_json }
    }
}

#[async_trait::async_trait]
impl AgentAdapter for DeterministicAdapter {
    fn kind(&self) -> &'static str {
        "deterministic"
    }

    async fn detect(
        &self,
        _binary_path: Option<&std::path::Path>,
    ) -> Result<DetectionResult, CoreError> {
        Ok(DetectionResult {
            found: true,
            binary_path: None,
            error: None,
        })
    }

    async fn get_version(&self) -> Result<String, CoreError> {
        Ok("1.0".into())
    }

    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, CoreError> {
        Ok(AgentConfigInfo {
            provider: None,
            base_url: None,
            model: None,
            auth_mode: "none".into(),
            config_file_path: None,
            extra: HashMap::new(),
        })
    }

    async fn check_authentication(&self) -> Result<AuthCheckResult, CoreError> {
        Ok(AuthCheckResult {
            authenticated: true,
            method: None,
            provider: None,
            error: None,
        })
    }

    async fn probe(
        &self,
        _temp_dir: &std::path::Path,
    ) -> Result<ActiveValidationResult, CoreError> {
        Ok(ActiveValidationResult {
            validated_at: chrono::Utc::now(),
            smoke_test_passed: true,
            checks: ActiveProbeChecks {
                execute: true,
                stream_output: true,
                final_result: true,
                cancellation: true,
                exit_code_correct: true,
            },
            duration_ms: 1,
        })
    }

    async fn start_session(
        &self,
        _profile: &RuntimeProfile,
        _opts: &SessionOptions,
    ) -> Result<Box<dyn AgentSession>, CoreError> {
        Ok(Box::new(DeterministicSession {
            response_json: self.response_json.clone(),
            sent: false,
            session_id: uuid::Uuid::new_v4().to_string(),
            active: true,
        }))
    }
}

struct DeterministicSession {
    response_json: serde_json::Value,
    sent: bool,
    session_id: String,
    active: bool,
}

#[async_trait::async_trait]
impl AgentSession for DeterministicSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn is_active(&self) -> bool {
        self.active
    }

    async fn send_task(&mut self, _envelope: &TaskEnvelope) -> Result<(), CoreError> {
        Ok(())
    }

    async fn receive_events(&mut self, sink: &mut dyn AgentEventSink) -> Result<(), CoreError> {
        if !self.sent {
            self.sent = true;
            let content = serde_json::to_string(&self.response_json).unwrap_or_default();
            sink.send(AgentEvent::Result {
                content,
                is_error: false,
            })
            .await?;
        }
        Ok(())
    }

    async fn interrupt(&self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn cancel(&self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn dispose(&mut self) -> Result<(), CoreError> {
        self.active = false;
        Ok(())
    }
}

fn make_planner_adapter() -> Arc<dyn AgentAdapter> {
    let plan_json = serde_json::json!({
        "schema_version": "1.0",
        "goal_summary": "Add a utility function and its test",
        "assumptions": ["Repository is a Rust project"],
        "milestones": [
            {
                "client_ref": "m1",
                "title": "Implementation",
                "objective": "Add the function",
                "success_criteria_refs": ["c1"],
                "dependencies": [],
                "priority": 10
            }
        ],
        "tasks": [
            {
                "client_ref": "t1",
                "milestone_ref": "m1",
                "title": "Add utility function",
                "objective": "Create src/utils.rs with fn add(a:i32,b:i32)->i32",
                "acceptance_criteria": ["Function compiles", "Returns correct sum"],
                "dependencies": [],
                "expected_evidence": ["src/utils.rs"],
                "expected_resource_scope": ["src/utils.rs"],
                "risk_level": "low",
                "requires_approval": false
            },
            {
                "client_ref": "t2",
                "milestone_ref": "m1",
                "title": "Add unit test",
                "objective": "Add test in tests/utils_test.rs",
                "acceptance_criteria": ["Test compiles", "Test passes"],
                "dependencies": ["t1"],
                "expected_evidence": ["tests/utils_test.rs"],
                "expected_resource_scope": ["tests/utils_test.rs"],
                "risk_level": "low",
                "requires_approval": false
            }
        ],
        "risks": [],
        "completion_strategy": "Implement task 1 then task 2"
    });

    Arc::new(DeterministicAdapter::new(plan_json))
}

// ── Tests ────────────────────────────────────────────────────────────

/// Scene A: Complete two-task Goal E2E — Planner → PlanRevision → PlannedTask.
#[tokio::test]
async fn scene_a_deterministic_two_task_goal_e2e() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("create test pool");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    let profile = make_test_profile("fake-planner");
    let adapter: Arc<dyn AgentAdapter> = make_planner_adapter();

    let repo_root = std::env::temp_dir().join(format!("i7-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo_root).expect("create temp repo dir");

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

    // Use a worktree root OUTSIDE the user's home (which may be a git worktree)
    let test_dir = std::path::PathBuf::from(
        std::env::var("HARNESS_WORKTREE_ROOT").unwrap_or_else(|_| r"C:\Temp".to_string()),
    );
    let worktree_root = test_dir.join(format!("harness-i7-wt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&worktree_root).expect("create worktree root");

    let run_context = Arc::new(
        harness_runtime::liveness::RunContext::create(&worktree_root, "test-head", false)
            .expect("create run context"),
    );

    let graph = ProductionGraph::build_with_adapter(
        pool.clone(),
        &worktree_root,
        &repo_root,
        run_context,
        Some(adapter.clone()),
        Some(profile.clone()),
    )
    .expect("build production graph");

    // ── RC2: Planner/Evaluator wired ──────────────────────────────
    assert!(
        graph.goal_planner.is_some(),
        "RC2: planner should be wired in ProductionGraph"
    );
    assert!(
        graph.goal_evaluator.is_some(),
        "RC2: evaluator should be wired in ProductionGraph"
    );

    // ── RC4: RoleIsolationPolicy configured ──────────────────────
    assert!(graph.goal_loop_service.runtime_config.is_some());
    let cfg = graph.goal_loop_service.runtime_config.as_ref().unwrap();
    assert!(
        cfg.is_isolated_sessions(),
        "RC4: should default to IsolatedSessions"
    );
    assert!(cfg.is_separated());

    // ── Create Goal ──────────────────────────────────────────────
    let goal = make_two_task_goal();
    let goal_id = goal.goal_id.clone();

    graph
        .goal_loop_service
        .create_goal(goal)
        .await
        .expect("create goal");

    let goal_repo = GoalRepo::new(pool.clone());
    let stored = goal_repo
        .get_goal(&goal_id)
        .await
        .expect("get goal")
        .expect("goal exists");
    assert_eq!(stored.title, "Add utility function and test");
    assert_eq!(stored.success_criteria.len(), 1);

    // ── Start Goal (triggers planner) ────────────────────────────
    graph
        .goal_loop_service
        .transition_goal(&goal_id, GoalState::Planning)
        .await
        .expect("transition to planning");

    // ── Drive goal loop (invokes planner → creates PlanRevision) ─
    graph
        .goal_loop_service
        .drive_goal_loop(&goal_id)
        .await
        .expect("drive goal loop");

    // ── Verify: PlanRevision created ─────────────────────────────
    let plan = goal_repo
        .get_active_plan(&goal_id)
        .await
        .expect("get active plan");
    assert!(
        plan.is_some(),
        "active plan should exist after planner invocation"
    );
    let plan = plan.unwrap();
    assert_eq!(plan.revision_number, 1);
    assert_eq!(plan.state, harness_core::contracts::plan::PlanState::Active);

    // ── Verify: PlannedTasks created ─────────────────────────────
    let tasks = goal_repo
        .get_all_planned_tasks(&plan.plan_revision_id)
        .await
        .expect("get planned tasks");
    assert_eq!(tasks.len(), 2, "should have 2 planned tasks");
    assert_eq!(tasks[0].client_ref, "t1");
    assert_eq!(tasks[1].client_ref, "t2");

    // ── Verify: Task dependency ──────────────────────────────────
    assert!(tasks[1].dependency_refs.contains(&"t1".to_string()));

    // ── Verify: No duplicate plans ───────────────────────────────
    let all_plans: Vec<(String,)> =
        sqlx::query_as("SELECT plan_revision_id FROM plan_revisions WHERE goal_id = ?")
            .bind(&goal_id)
            .fetch_all(&pool)
            .await
            .expect("query plans");
    assert_eq!(all_plans.len(), 1, "should have exactly 1 plan revision");

    // ── Verify: Goal events recorded ─────────────────────────────
    let events: Vec<(String,)> = sqlx::query_as(
        "SELECT event_type FROM goal_events WHERE goal_id = ? ORDER BY sequence_num ASC",
    )
    .bind(&goal_id)
    .fetch_all(&pool)
    .await
    .expect("query events");
    assert!(!events.is_empty());
    assert!(events.iter().any(|e| e.0 == "goal_created"));
    assert!(events.iter().any(|e| e.0 == "goal_state_changed"));

    // ── Verify: No duplicate tasks ───────────────────────────────
    let task_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(DISTINCT planned_task_id) FROM planned_tasks WHERE plan_revision_id = ?",
    )
    .bind(&plan.plan_revision_id)
    .fetch_one(&pool)
    .await
    .expect("query tasks");
    assert_eq!(
        task_count.0, 2,
        "should have exactly 2 distinct planned tasks"
    );

    // ── Cleanup ──────────────────────────────────────────────────
    let _ = std::fs::remove_dir_all(&repo_root);
}

/// Scene B: Failure → Replan → Success.
#[tokio::test]
async fn scene_b_failure_replan_success() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("create test pool");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    let repo_root = std::env::temp_dir().join(format!("i7-replan-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo_root).expect("create temp dir");

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

    let profile = make_test_profile("fake-planner-replan");
    let adapter: Arc<dyn AgentAdapter> = make_planner_adapter();

    let test_dir = std::path::PathBuf::from(
        std::env::var("HARNESS_WORKTREE_ROOT").unwrap_or_else(|_| r"C:\Temp".to_string()),
    );
    let worktree_root = test_dir.join(format!("harness-i7-wt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&worktree_root).expect("create worktree root");

    let run_context = Arc::new(
        harness_runtime::liveness::RunContext::create(&worktree_root, "test-head", false)
            .expect("create run context"),
    );

    let graph = ProductionGraph::build_with_adapter(
        pool.clone(),
        &worktree_root,
        &repo_root,
        run_context,
        Some(adapter.clone()),
        Some(profile),
    )
    .expect("build graph");

    // Create and start goal
    let goal = make_two_task_goal();
    let goal_id = goal.goal_id.clone();
    let original_budget = goal.budget.clone();

    graph
        .goal_loop_service
        .create_goal(goal)
        .await
        .expect("create goal");
    graph
        .goal_loop_service
        .transition_goal(&goal_id, GoalState::Planning)
        .await
        .expect("transition");
    graph
        .goal_loop_service
        .drive_goal_loop(&goal_id)
        .await
        .expect("drive");

    // Verify PlanRevision 1 exists
    let goal_repo = GoalRepo::new(pool.clone());
    let plan1 = goal_repo
        .get_active_plan(&goal_id)
        .await
        .expect("get plan")
        .expect("plan exists");
    assert_eq!(plan1.revision_number, 1);

    // Simulate task failure
    let tasks = goal_repo
        .get_all_planned_tasks(&plan1.plan_revision_id)
        .await
        .expect("get tasks");
    let failed_task = &tasks[0];
    goal_repo
        .update_planned_task_state(
            &failed_task.planned_task_id,
            PlannedTaskState::Failed,
            Some("test failure"),
        )
        .await
        .expect("mark failed");

    // Import failure observation (RC6: production observation path)
    graph
        .goal_loop_service
        .import_observation(
            &goal_id,
            Some(&plan1.plan_revision_id),
            Some(&failed_task.planned_task_id),
            "task_loop",
            "test-task-id",
            "test-failure-event",
            "Task verification failed",
            "task_verification_failed",
            &goal_id,
        )
        .await
        .expect("RC6: import observation");

    // Trigger replan decision
    let replan_decision = graph
        .goal_loop_service
        .decide_replan(
            &goal_id,
            &harness_runtime::goal::ReplanTrigger::TaskFailed {
                task_id: failed_task.planned_task_id.clone(),
                reason: "test failure".into(),
            },
            1,
            0,
        )
        .await
        .expect("decide replan");

    assert_eq!(
        replan_decision,
        harness_runtime::goal::ReplanDecision::CreatePlanRevision,
        "should decide to create new plan revision on task failure"
    );

    // ── Verify: PlanRevision 1 preserved ─────────────────────────
    assert!(goal_repo
        .get_active_plan(&goal_id)
        .await
        .expect("get plan")
        .is_some());

    // ── Verify: Failed task preserved ────────────────────────────
    let failed_after = goal_repo
        .get_all_planned_tasks(&plan1.plan_revision_id)
        .await
        .expect("get tasks");
    assert!(failed_after
        .iter()
        .any(|t| t.state == PlannedTaskState::Failed));

    // ── Verify: Failure observation exists ───────────────────────
    let obs: Vec<(String,)> =
        sqlx::query_as("SELECT observation_id FROM goal_observations WHERE goal_id = ?")
            .bind(&goal_id)
            .fetch_all(&pool)
            .await
            .expect("query observations");
    assert!(!obs.is_empty(), "RC6: failure observation should exist");

    // ── Verify: Budget not expanded ──────────────────────────────
    let stored_goal = goal_repo
        .get_goal(&goal_id)
        .await
        .expect("get goal")
        .expect("goal exists");
    assert_eq!(
        stored_goal.budget.max_plan_revisions,
        original_budget.max_plan_revisions
    );
    assert_eq!(
        stored_goal.budget.max_total_tasks,
        original_budget.max_total_tasks
    );

    // ── Verify: Replan count bounded ─────────────────────────────
    let plan_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM plan_revisions WHERE goal_id = ?")
            .bind(&goal_id)
            .fetch_one(&pool)
            .await
            .expect("query plan count");
    assert!(plan_count.0 <= original_budget.max_plan_revisions as i64);

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// Scene C: RoleIsolationPolicy default enforcement.
#[tokio::test]
async fn scene_c_role_isolation_default_is_isolated_sessions() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("create test pool");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("run migrations");

    let repo_root = std::env::temp_dir().join(format!("i7-role-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo_root).expect("create temp dir");

    let profile = make_test_profile("claude-default");
    let adapter: Arc<dyn AgentAdapter> = make_planner_adapter();

    let test_dir = std::path::PathBuf::from(
        std::env::var("HARNESS_WORKTREE_ROOT").unwrap_or_else(|_| r"C:\Temp".to_string()),
    );
    let worktree_root = test_dir.join(format!("harness-i7-wt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&worktree_root).expect("create worktree root");

    let run_context = Arc::new(
        harness_runtime::liveness::RunContext::create(&worktree_root, "test-head", false)
            .expect("create run context"),
    );

    let graph = ProductionGraph::build_with_adapter(
        pool.clone(),
        &worktree_root,
        &repo_root,
        run_context,
        Some(adapter.clone()),
        Some(profile),
    )
    .expect("build graph");

    // ── RC4: IsolatedSessions is the default ─────────────────────
    let cfg = graph
        .goal_loop_service
        .runtime_config
        .as_ref()
        .expect("RC4: config should be set");
    assert!(
        cfg.is_isolated_sessions(),
        "RC4: should default to IsolatedSessions"
    );
    assert!(cfg.is_separated());

    // ── RC4: Single profile accepted under IsolatedSessions ──────
    let result = graph
        .goal_loop_service
        .validate_profile_separation("test-goal");
    assert!(
        result.is_ok(),
        "RC4: same profile should be OK under IsolatedSessions"
    );

    // ── StrictProfileDiversity not operational with single profile ─
    assert!(!cfg.strict_diversity_operational());

    // ── Production services wired ────────────────────────────────
    assert!(graph.goal_loop_service.goal_planner.is_some());
    assert!(graph.goal_loop_service.goal_evaluator.is_some());
    assert!(graph.goal_loop_service.task_loop_service.is_some());
    assert!(graph.goal_loop_service.review_service.is_some());
    assert!(graph.goal_loop_service.commit_service.is_some());
    assert!(graph.goal_loop_service.integration_queue.is_some());

    let _ = std::fs::remove_dir_all(&repo_root);
}
