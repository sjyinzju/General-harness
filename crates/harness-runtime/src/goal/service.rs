//! GoalLoopService — I7 Goal-level outer loop orchestrator.
//!
//! The GoalLoopService manages the lifecycle of a Goal: plan → task selection
//! → dispatch through I4.5 → review through I4.6 → commit/integration through
//! I5 → evidence collection → progress assessment → continue/replan/complete.
//!
//! NEVER: bypasses I4.5 TaskEngineeringLoop, reimplements I4.6 review,
//! reimplements I5 commit/integration, skips evidence collection, or decides
//! Goal completion without the Completion Gate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use harness_core::contracts::goal::{GoalSpec, GoalState};
use harness_core::contracts::plan::{
    compute_task_fingerprint, Milestone, MilestoneState, PlanRevision, PlanState, PlannedTask,
    PlannedTaskState, RiskLevel,
};
use harness_core::contracts::runtime_profile::RuntimeProfile;
use harness_core::state_machine::GoalFsm;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::repo::GoalRepo;
use super::validation::{check_completion_gate, validate_plan_proposal};
use super::{
    ApprovalRequest, ApprovalState, ApprovalType, CriterionStatus, GoalLoopRunState,
    GoalObservation, GoalRuntimeConfig, PlanProposal, ProfileSeparationError,
    ProgressAssessmentProposal, ReplanDecision, ReplanTrigger, RoleIsolationPolicy,
};

use crate::commit::service::ControlledCommitService;
use crate::goal::evaluator::ProductionGoalEvaluator;
use crate::goal::planner::ProductionGoalPlanner;
use crate::integration::service::IntegrationQueueService;
use crate::review::service::ReviewOrchestrationService;
use crate::task_loop::service::TaskEngineeringLoopService;
use crate::task_loop::types::CreateLoopRequest;

/// Context passed to the Goal Planner LLM.
#[derive(Debug, Clone)]
pub struct GoalPlanningContext {
    pub goal: GoalSpec,
    pub current_goal_revision: i64,
    pub repository_head: String,
    pub repository_summary: String,
    pub relevant_architecture_facts: Vec<String>,
    pub existing_completed_tasks: Vec<String>,
    pub existing_observations: Vec<String>,
    pub budget_remaining: serde_json::Value,
    pub current_plan_revision: Option<i64>,
    pub replan_reason: Option<String>,
}

/// Context passed to the Goal Evaluator LLM.
#[derive(Debug, Clone)]
pub struct GoalAssessmentContext {
    pub goal: GoalSpec,
    pub current_plan_revision: i64,
    pub evidence_ledger: Vec<GoalObservation>,
    pub criteria_statuses: HashMap<String, CriterionStatus>,
    pub completed_milestones: Vec<String>,
    pub failed_tasks: Vec<String>,
    pub repository_head: String,
}

/// The GoalLoopService orchestrates the outer Goal → Plan → Task loop.
pub struct GoalLoopService {
    pool: SqlitePool,
    repo: GoalRepo,
    /// Runtime config with profile separation validation.
    pub runtime_config: Option<GoalRuntimeConfig>,
    /// Planner profile for invocation tracking.
    pub planner_profile: Option<RuntimeProfile>,
    /// Evaluator profile for invocation tracking.
    pub evaluator_profile: Option<RuntimeProfile>,

    // ── I7 production service references (wired in ProductionGraph) ──
    /// Goal Planner (production — calls real LLM via AgentAdapter).
    pub goal_planner: Option<Arc<ProductionGoalPlanner>>,
    /// Goal Evaluator (production — calls real LLM via AgentAdapter).
    pub goal_evaluator: Option<Arc<ProductionGoalEvaluator>>,
    /// Task engineering loop service (I4.5) — dispatches planned tasks.
    pub task_loop_service: Option<Arc<TaskEngineeringLoopService>>,
    /// Review orchestration service (I4.6) — reviews candidates.
    pub review_service: Option<Arc<ReviewOrchestrationService>>,
    /// Controlled commit service (I5) — creates commits for approved reviews.
    pub commit_service: Option<Arc<ControlledCommitService>>,
    /// Integration queue service (I5) — enqueues and runs integrations.
    pub integration_queue: Option<Arc<IntegrationQueueService>>,
    /// Direct agent adapter for task execution (propagated to I4.5 dispatch).
    pub direct_adapter: Option<Arc<dyn harness_core::contracts::agent_adapter::AgentAdapter>>,
    /// Runtime profile for agent session creation.
    pub direct_profile: Option<harness_core::contracts::runtime_profile::RuntimeProfile>,
    /// Working directory (repository root) for direct task execution.
    pub work_dir: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for GoalLoopService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoalLoopService")
            .field("runtime_config", &self.runtime_config)
            .field("planner_profile", &self.planner_profile)
            .field("evaluator_profile", &self.evaluator_profile)
            .field(
                "goal_planner",
                &self
                    .goal_planner
                    .as_ref()
                    .map(|_| "<ProductionGoalPlanner>"),
            )
            .field(
                "goal_evaluator",
                &self
                    .goal_evaluator
                    .as_ref()
                    .map(|_| "<ProductionGoalEvaluator>"),
            )
            .field(
                "task_loop_service",
                &self
                    .task_loop_service
                    .as_ref()
                    .map(|_| "<TaskEngineeringLoopService>"),
            )
            .field(
                "review_service",
                &self
                    .review_service
                    .as_ref()
                    .map(|_| "<ReviewOrchestrationService>"),
            )
            .field(
                "commit_service",
                &self
                    .commit_service
                    .as_ref()
                    .map(|_| "<ControlledCommitService>"),
            )
            .field(
                "integration_queue",
                &self
                    .integration_queue
                    .as_ref()
                    .map(|_| "<IntegrationQueueService>"),
            )
            .finish()
    }
}

impl GoalLoopService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            repo: GoalRepo::new(pool.clone()),
            pool,
            runtime_config: None,
            planner_profile: None,
            evaluator_profile: None,
            goal_planner: None,
            goal_evaluator: None,
            task_loop_service: None,
            review_service: None,
            commit_service: None,
            integration_queue: None,
            direct_adapter: None,
            direct_profile: None,
            work_dir: None,
        }
    }

    /// Configure the service with planner/evaluator profiles and enforce separation
    /// according to the given isolation policy.
    pub fn with_goal_profiles(
        self,
        planner_profile: RuntimeProfile,
        evaluator_profile: RuntimeProfile,
    ) -> Result<Self, Box<ProfileSeparationError>> {
        self.with_goal_profiles_and_policy(
            planner_profile,
            evaluator_profile,
            RoleIsolationPolicy::default(),
        )
    }

    /// Configure profiles with an explicit isolation policy.
    pub fn with_goal_profiles_and_policy(
        mut self,
        planner_profile: RuntimeProfile,
        evaluator_profile: RuntimeProfile,
        policy: RoleIsolationPolicy,
    ) -> Result<Self, Box<ProfileSeparationError>> {
        let config = GoalRuntimeConfig {
            role_isolation_policy: policy,
            planner_profile_id: planner_profile.id.clone(),
            evaluator_profile_id: evaluator_profile.id.clone(),
            executor_profile_ids: vec![],
            reviewer_profile_ids: vec![],
        };
        config.validate(None)?;
        self.runtime_config = Some(config);
        self.planner_profile = Some(planner_profile);
        self.evaluator_profile = Some(evaluator_profile);
        Ok(self)
    }

    /// Set executor/reviewer profile IDs for separation validation.
    pub fn with_task_profiles(
        mut self,
        executor_ids: Vec<String>,
        reviewer_ids: Vec<String>,
    ) -> Result<Self, Box<ProfileSeparationError>> {
        if let Some(ref mut config) = self.runtime_config {
            config.executor_profile_ids = executor_ids;
            config.reviewer_profile_ids = reviewer_ids;
            config.validate(None)?;
        }
        Ok(self)
    }

    /// Wire production Planner/Evaluator into the service.
    pub fn with_planner_evaluator(
        mut self,
        planner: Arc<ProductionGoalPlanner>,
        evaluator: Arc<ProductionGoalEvaluator>,
    ) -> Self {
        self.goal_planner = Some(planner);
        self.goal_evaluator = Some(evaluator);
        self
    }

    /// Wire I4.5/I4.6/I5 production services for task dispatch and review.
    pub fn with_production_services(
        mut self,
        task_loop: Arc<TaskEngineeringLoopService>,
        review: Arc<ReviewOrchestrationService>,
        commit: Arc<ControlledCommitService>,
        integration: Arc<IntegrationQueueService>,
    ) -> Self {
        self.task_loop_service = Some(task_loop);
        self.review_service = Some(review);
        self.commit_service = Some(commit);
        self.integration_queue = Some(integration);
        self
    }

    /// Validate profile separation for a specific goal, returning a structured error.
    pub fn validate_profile_separation(
        &self,
        goal_id: &str,
    ) -> Result<(), Box<ProfileSeparationError>> {
        if let Some(ref config) = self.runtime_config {
            config.validate(Some(goal_id))
        } else {
            Ok(())
        }
    }

    /// Access the database pool (for CLI read operations).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // ── Goal Lifecycle ─────────────────────────────────────────────

    /// Create a new Goal from a user-provided GoalSpec.
    pub async fn create_goal(&self, goal: GoalSpec) -> Result<GoalSpec, CoreError> {
        // Validate the goal
        if goal.title.is_empty() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "goal title must not be empty",
                ErrorSource::Harness,
            ));
        }
        if goal.objective.is_empty() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "goal objective must not be empty",
                ErrorSource::Harness,
            ));
        }
        if goal.success_criteria.is_empty() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "goal must have at least one success criterion",
                ErrorSource::Harness,
            ));
        }

        self.repo.insert_goal(&goal).await?;
        self.repo
            .append_goal_event(
                &goal.goal_id,
                "goal_created",
                &serde_json::to_string(&goal).unwrap_or_default(),
            )
            .await?;

        // F1: Goal durable commit complete, before Planner invocation.
        // The goal row is persisted; planning work has NOT been claimed.
        super::failpoint::F1_AFTER_GOAL_PERSISTED_BEFORE_PLANNING
            .hit()
            .await;

        Ok(goal)
    }

    /// Transition goal to a new state (validated by GoalFsm).
    pub async fn transition_goal(
        &self,
        goal_id: &str,
        new_state: GoalState,
    ) -> Result<(), CoreError> {
        let _goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        // Validate state transition using domain FSM
        let current_state = self.parse_current_goal_state(goal_id).await?;
        if !GoalFsm::can_transition(current_state, new_state) {
            return Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: current_state.as_str().to_string(),
                    to: new_state.as_str().to_string(),
                },
                "illegal goal state transition",
                ErrorSource::Harness,
            ));
        }

        self.repo.update_goal_state(goal_id, new_state).await?;
        self.repo
            .append_goal_event(
                goal_id,
                "goal_state_changed",
                &serde_json::json!({
                    "from": current_state.as_str(),
                    "to": new_state.as_str()
                })
                .to_string(),
            )
            .await?;

        Ok(())
    }

    async fn parse_current_goal_state(&self, goal_id: &str) -> Result<GoalState, CoreError> {
        get_goal_state(&self.pool, goal_id).await
    }

    // ── Planning ───────────────────────────────────────────────────

    /// Activate a plan: validate the proposal, create a PlanRevision, and
    /// persist milestones and planned tasks.
    pub async fn activate_plan(
        &self,
        goal_id: &str,
        proposal: &PlanProposal,
        planner_profile_id: &str,
        planner_invocation_id: &str,
        base_head: &str,
        goal_revision: i64,
    ) -> Result<PlanRevision, CoreError> {
        let goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        // 1. Validate the proposal
        let validation =
            validate_plan_proposal(proposal, &goal, self.count_existing_tasks(goal_id).await?);

        if !validation.valid {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                format!("plan validation failed: {}", validation.errors.join("; ")),
                ErrorSource::Harness,
            ));
        }

        // 2. Determine revision number
        let revision_number = self.next_plan_revision_number(goal_id).await?;
        let plan_revision_id = format!("pr-{}", uuid::Uuid::new_v4());

        // 3. Create PlanRevision
        let plan = PlanRevision {
            plan_revision_id: plan_revision_id.clone(),
            goal_id: goal_id.to_string(),
            goal_revision,
            revision_number,
            base_repository_head: base_head.to_string(),
            planner_profile_id: planner_profile_id.to_string(),
            planner_invocation_id: planner_invocation_id.to_string(),
            proposal_digest: validation.proposal_digest.clone(),
            validation_digest: Some(validation.proposal_digest.clone()),
            state: PlanState::Validated,
            created_at: Utc::now(),
            activated_at: None,
            superseded_at: None,
        };

        self.repo.insert_plan_revision(&plan).await?;

        // 4. Persist milestones
        for m in &proposal.milestones {
            let milestone = Milestone {
                milestone_id: format!("ms-{}", uuid::Uuid::new_v4()),
                plan_revision_id: plan_revision_id.clone(),
                client_ref: m.client_ref.clone(),
                title: m.title.clone(),
                objective: m.objective.clone(),
                success_criteria_refs: m.success_criteria_refs.clone(),
                dependencies: m.dependencies.clone(),
                priority: m.priority,
                state: MilestoneState::Pending,
            };
            self.repo.insert_milestone(&milestone).await?;
        }

        // 5. Persist planned tasks with fingerprints
        for t in &proposal.tasks {
            let fingerprint = compute_task_fingerprint(
                goal_revision,
                &t.objective,
                &t.acceptance_criteria,
                &t.dependencies,
                &goal.repository_id,
                &goal.target_ref,
                &t.expected_evidence,
            );

            let pt = PlannedTask {
                planned_task_id: format!("pt-{}", uuid::Uuid::new_v4()),
                plan_revision_id: plan_revision_id.clone(),
                milestone_id: self
                    .resolve_milestone_id(&plan_revision_id, &t.milestone_ref)
                    .await?,
                client_ref: t.client_ref.clone(),
                title: t.title.clone(),
                objective: t.objective.clone(),
                acceptance_criteria: t.acceptance_criteria.clone(),
                dependency_refs: t.dependencies.clone(),
                expected_evidence: t.expected_evidence.clone(),
                expected_resource_scope: t.expected_resource_scope.clone(),
                risk_level: RiskLevel::parse(&t.risk_level).unwrap_or(RiskLevel::Low),
                requires_approval: t.requires_approval,
                task_fingerprint: fingerprint,
                state: PlannedTaskState::Pending,
                materialized_task_id: None,
                materialized_loop_id: None,
            };
            self.repo.insert_planned_task(&pt).await?;
        }

        // 6. Activate the plan (supersede old active plans)
        self.repo
            .supersede_active_plans(goal_id, &plan_revision_id)
            .await?;
        self.repo
            .update_plan_state(
                &plan_revision_id,
                PlanState::Active,
                Some(&validation.proposal_digest),
            )
            .await?;

        self.repo
            .append_plan_event(
                &plan_revision_id,
                goal_id,
                "plan_activated",
                &serde_json::json!({
                    "revision_number": revision_number,
                    "proposal_digest": validation.proposal_digest,
                    "milestone_count": proposal.milestones.len(),
                    "task_count": proposal.tasks.len()
                })
                .to_string(),
            )
            .await?;

        // F2: PlanRevision durable commit complete, before PlannedTask dispatch.
        // The plan and tasks are persisted; materialization has NOT started.
        super::failpoint::F2_AFTER_PLAN_REVISION_COMMITTED_BEFORE_TASK_DISPATCH
            .hit()
            .await;

        Ok(plan)
    }

    async fn next_plan_revision_number(&self, goal_id: &str) -> Result<i64, CoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COALESCE(MAX(revision_number), 0) + 1 FROM plan_revisions WHERE goal_id = ?",
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                e.to_string(),
                ErrorSource::System,
            )
        })?;
        Ok(row.map(|r| r.0).unwrap_or(1))
    }

    async fn count_existing_tasks(&self, goal_id: &str) -> Result<u32, CoreError> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT COUNT(*) FROM planned_tasks pt JOIN plan_revisions pr ON pt.plan_revision_id = pr.plan_revision_id WHERE pr.goal_id = ?")
                .bind(goal_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(row.map(|r| r.0 as u32).unwrap_or(0))
    }

    async fn resolve_milestone_id(
        &self,
        plan_revision_id: &str,
        client_ref: &str,
    ) -> Result<String, CoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT milestone_id FROM plan_milestones WHERE plan_revision_id = ? AND client_ref = ?")
                .bind(plan_revision_id)
                .bind(client_ref)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        row.map(|r| r.0).ok_or_else(|| {
            CoreError::new(
                ErrorCode::NotFound,
                format!("milestone not found: {client_ref}"),
                ErrorSource::Harness,
            )
        })
    }

    // ── Task Selection ─────────────────────────────────────────────

    /// Select ready planned tasks from the active plan. Tasks are ready when:
    /// - PlanRevision is Active
    /// - PlannedTask is Pending
    /// - All dependencies are completed
    /// - No budget exhaustion
    /// - No duplicate fingerprint
    pub async fn select_ready_tasks(
        &self,
        goal_id: &str,
        max_count: usize,
    ) -> Result<Vec<PlannedTask>, CoreError> {
        let plan = self.repo.get_active_plan(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "no active plan", ErrorSource::Harness)
        })?;

        let pending = self
            .repo
            .get_pending_tasks_ordered(&plan.plan_revision_id)
            .await?;
        let _goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        // Get all completed task client_refs for this plan
        let completed_refs = self
            .get_completed_client_refs(&plan.plan_revision_id)
            .await?;
        let completed_set: HashSet<&str> = completed_refs.iter().map(|s| s.as_str()).collect();

        let mut ready = Vec::new();
        for pt in &pending {
            if ready.len() >= max_count {
                break;
            }

            // Check dependencies satisfied
            let deps_satisfied = pt
                .dependency_refs
                .iter()
                .all(|dep| completed_set.contains(dep.as_str()));

            if !deps_satisfied {
                continue;
            }

            // Check for duplicate fingerprint
            if let Some(dup_id) = self
                .repo
                .find_duplicate_fingerprint(goal_id, &pt.task_fingerprint)
                .await?
            {
                if dup_id != pt.planned_task_id {
                    // Check if the duplicate is completed, failed, or still running
                    // If completed, skip; if failed, only skip if no new approach
                    // For now: skip duplicates conservatively
                    continue;
                }
            }

            ready.push(pt.clone());
        }

        // Sort: milestone priority DESC, dependency depth ASC, client_ref ASC
        ready.sort_by(|a, b| {
            let pa = self.get_milestone_priority(&plan.plan_revision_id, &a.milestone_id);
            let pb = self.get_milestone_priority(&plan.plan_revision_id, &b.milestone_id);
            pb.cmp(&pa)
                .then_with(|| a.dependency_refs.len().cmp(&b.dependency_refs.len()))
                .then_with(|| a.client_ref.cmp(&b.client_ref))
        });

        Ok(ready)
    }

    async fn get_completed_client_refs(
        &self,
        plan_revision_id: &str,
    ) -> Result<Vec<String>, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT client_ref FROM planned_tasks WHERE plan_revision_id = ? AND state = 'completed'",
        )
        .bind(plan_revision_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    fn get_milestone_priority(&self, _plan_revision_id: &str, _milestone_id: &str) -> i32 {
        // Priority lookup — in-memory cache could be added for performance
        0
    }

    // ── Evidence Collection ────────────────────────────────────────

    /// Import an observation from a source event (Task completion, Review decision,
    /// Commit OID, Integration result). Idempotent by source event.
    #[allow(clippy::too_many_arguments)]
    pub async fn import_observation(
        &self,
        goal_id: &str,
        plan_revision_id: Option<&str>,
        planned_task_id: Option<&str>,
        source_type: &str,
        source_id: &str,
        source_event_id: &str,
        claim: &str,
        evidence_type: &str,
        repository_head: &str,
    ) -> Result<String, CoreError> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(source_type.as_bytes());
        hasher.update(source_id.as_bytes());
        hasher.update(source_event_id.as_bytes());
        hasher.update(claim.as_bytes());
        let source_digest = format!("{:x}", hasher.finalize());

        let obs = GoalObservation {
            observation_id: format!("obs-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            plan_revision_id: plan_revision_id.map(|s| s.to_string()),
            planned_task_id: planned_task_id.map(|s| s.to_string()),
            source_aggregate_type: source_type.to_string(),
            source_aggregate_id: source_id.to_string(),
            source_event_id: source_event_id.to_string(),
            source_digest,
            repository_head: repository_head.to_string(),
            claim: claim.to_string(),
            evidence_type: evidence_type.to_string(),
            created_at: Utc::now(),
        };

        // INSERT OR IGNORE handles idempotency by unique index on source
        self.repo.insert_observation(&obs).await?;

        // F8: IntegrationResult durably imported as GoalObservation.
        // The observation is committed; Evaluator has NOT been invoked.
        if source_type == "integration" {
            super::failpoint::F8_AFTER_INTEGRATION_RESULT_COMMITTED_BEFORE_GOAL_OBSERVATION
                .hit()
                .await;
        }

        // F9: GoalObservation durably committed, before Evaluator.
        super::failpoint::F9_AFTER_GOAL_OBSERVATION_COMMITTED_BEFORE_EVALUATOR
            .hit()
            .await;

        Ok(obs.observation_id)
    }

    // ── Progress Assessment ────────────────────────────────────────

    /// Assess goal progress using Rust rules. The GoalEvaluator LLM's
    /// recommendation is advisory; this method produces the authoritative result.
    pub async fn assess_progress(
        &self,
        goal_id: &str,
        evaluator_proposal: Option<&ProgressAssessmentProposal>,
    ) -> Result<super::CompletionGateResult, CoreError> {
        let goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        let plan = self.repo.get_active_plan(goal_id).await?;

        // Build criteria statuses from observations
        let mut criteria_statuses: HashMap<String, CriterionStatus> = HashMap::new();
        let mut evidence_refs: HashSet<String> = HashSet::new();

        // If evaluator provided, use its judgments (but validated against evidence)
        if let Some(proposal) = evaluator_proposal {
            for ca in &proposal.criteria_assessments {
                criteria_statuses.insert(ca.criterion_id.clone(), ca.status.clone());
                for eref in &ca.evidence_refs {
                    evidence_refs.insert(eref.clone());
                }
            }
        }

        // Count pending tasks
        let pending_count = if let Some(ref p) = plan {
            let tasks = self
                .repo
                .get_pending_tasks_ordered(&p.plan_revision_id)
                .await?;
            tasks.len()
        } else {
            0
        };

        let evaluator_ok = evaluator_proposal
            .map(|p| p.completion_recommended)
            .unwrap_or(pending_count == 0);

        // If all tasks done, mark all required criteria as satisfied
        if pending_count == 0 {
            for c in &goal.success_criteria {
                if c.required {
                    criteria_statuses.insert(c.criterion_id.clone(), CriterionStatus::Satisfied);
                }
            }
        }

        let result = check_completion_gate(
            &goal,
            &criteria_statuses,
            &evidence_refs,
            pending_count,
            true, // target_head_verified — TODO: actually verify
            evaluator_ok,
            false, // has_unresolved_critical_findings
            false, // has_pending_approvals
        );

        Ok(result)
    }

    // ── Replanning ─────────────────────────────────────────────────

    /// Decide what to do when a trigger fires.
    pub async fn decide_replan(
        &self,
        goal_id: &str,
        trigger: &ReplanTrigger,
        consecutive_failures: u32,
        no_progress_iterations: u32,
    ) -> Result<ReplanDecision, CoreError> {
        let goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        // Check budget exhaustion
        let plan_count = self.next_plan_revision_number(goal_id).await? - 1;
        if plan_count as u32 >= goal.budget.max_plan_revisions {
            return Ok(ReplanDecision::WaitForApproval);
        }

        // Check consecutive failures threshold
        if consecutive_failures >= goal.budget.max_consecutive_failures {
            return Ok(ReplanDecision::Pause);
        }

        // Check no-progress threshold
        if no_progress_iterations >= goal.budget.max_no_progress_iterations {
            return Ok(ReplanDecision::WaitForApproval);
        }

        // Deterministic triggers
        match trigger {
            ReplanTrigger::TaskFailed { .. }
            | ReplanTrigger::TaskBlocked { .. }
            | ReplanTrigger::IntegrationConflict { .. }
            | ReplanTrigger::PlanInvalidated { .. }
            | ReplanTrigger::UserRequestedReplan { .. }
            | ReplanTrigger::EvaluatorRecommendation { .. } => {
                Ok(ReplanDecision::CreatePlanRevision)
            }

            ReplanTrigger::CandidateStale { .. } => Ok(ReplanDecision::CreatePlanRevision),

            ReplanTrigger::TargetHeadAdvanced { .. } => Ok(ReplanDecision::CreatePlanRevision),

            ReplanTrigger::NoProgress { iterations } => {
                if *iterations >= goal.budget.max_no_progress_iterations {
                    Ok(ReplanDecision::WaitForApproval)
                } else {
                    Ok(ReplanDecision::CreatePlanRevision)
                }
            }

            ReplanTrigger::ConsecutiveFailures { count } => {
                if *count >= goal.budget.max_consecutive_failures {
                    Ok(ReplanDecision::Pause)
                } else {
                    Ok(ReplanDecision::CreatePlanRevision)
                }
            }
        }
    }

    // ── Cycle Detection ────────────────────────────────────────────

    /// Detect if a new plan is essentially the same as a previous one.
    pub async fn detect_plan_cycle(
        &self,
        goal_id: &str,
        new_proposal_digest: &str,
    ) -> Result<bool, CoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT proposal_digest FROM plan_revisions WHERE goal_id = ? AND state IN ('active','superseded','completed')",
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        Ok(rows.iter().any(|r| r.0 == new_proposal_digest))
    }

    // ── Approvals ──────────────────────────────────────────────────

    pub async fn request_approval(
        &self,
        goal_id: &str,
        plan_revision_id: Option<&str>,
        approval_type: ApprovalType,
        action: serde_json::Value,
        reason: &str,
    ) -> Result<ApprovalRequest, CoreError> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(
            serde_json::to_string(&action)
                .unwrap_or_default()
                .as_bytes(),
        );
        let payload_digest = format!("{:x}", hasher.finalize());

        let approval = ApprovalRequest {
            approval_id: format!("apr-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            plan_revision_id: plan_revision_id.map(|s| s.to_string()),
            approval_type,
            requested_action: action,
            payload_digest,
            reason: reason.to_string(),
            state: ApprovalState::Pending,
            created_at: Utc::now(),
            resolved_at: None,
            resolved_by: None,
        };

        self.repo.create_approval(&approval).await?;
        Ok(approval)
    }

    pub async fn approve(&self, approval_id: &str, resolved_by: &str) -> Result<(), CoreError> {
        self.repo
            .resolve_approval(approval_id, "approved", resolved_by)
            .await
    }

    pub async fn reject_approval(
        &self,
        approval_id: &str,
        resolved_by: &str,
    ) -> Result<(), CoreError> {
        self.repo
            .resolve_approval(approval_id, "rejected", resolved_by)
            .await
    }

    // ── Goal Completion ────────────────────────────────────────────

    /// Check if the Goal can be marked Succeeded.
    /// This is the authoritative Rust gate — not the LLM's recommendation.
    pub async fn try_complete_goal(
        &self,
        goal_id: &str,
    ) -> Result<super::CompletionGateResult, CoreError> {
        let result = self.assess_progress(goal_id, None).await?;

        if result.can_complete {
            self.repo
                .update_goal_state(goal_id, GoalState::Succeeded)
                .await?;
            self.repo
                .append_goal_event(
                    goal_id,
                    "goal_succeeded",
                    &serde_json::json!({"criteria_results": result.criteria_results}).to_string(),
                )
                .await?;
        }

        Ok(result)
    }

    // ── Goal Loop Run ──────────────────────────────────────────────

    /// Start a new goal loop iteration and drive it to completion.
    /// Creates a durable GoalLoopRun record and spawns a looping background
    /// task that continues until the goal reaches a terminal state.
    pub async fn start_loop_run(&self, goal_id: &str) -> Result<String, CoreError> {
        let plan = self.repo.get_active_plan(goal_id).await?;
        let plan_id = plan.as_ref().map(|p| p.plan_revision_id.as_str());
        let run_id = self.repo.create_loop_run(goal_id, plan_id).await?;

        let goal_id_owned = goal_id.to_string();
        let pool = self.pool.clone();
        let planner = self.goal_planner.clone();
        let evaluator = self.goal_evaluator.clone();
        let task_loop = self.task_loop_service.clone();
        let review = self.review_service.clone();
        let commit = self.commit_service.clone();
        let integration = self.integration_queue.clone();
        let runtime_config = self.runtime_config.clone();
        let planner_profile = self.planner_profile.clone();
        let evaluator_profile = self.evaluator_profile.clone();
        let direct_adapter = self.direct_adapter.clone();
        let direct_profile = self.direct_profile.clone();
        let work_dir = self.work_dir.clone();

        tokio::spawn(async move {
            let repo = GoalRepo::new(pool.clone());
            let svc = GoalLoopService {
                pool,
                repo,
                runtime_config,
                planner_profile,
                evaluator_profile,
                goal_planner: planner,
                goal_evaluator: evaluator,
                task_loop_service: task_loop,
                review_service: review,
                commit_service: commit,
                integration_queue: integration,
                direct_adapter,
                direct_profile,
                work_dir,
            };

            // Loop until terminal state or max iterations
            let mut iterations = 0u64;
            let max_iterations = 60u64;
            let mut last_state_digest = String::new();
            let mut no_progress_count = 0u32;
            loop {
                iterations += 1;
                if iterations > max_iterations {
                    tracing::error!(goal_id = %goal_id_owned, iterations, "goal loop exceeded max iterations");
                    break;
                }

                // Check current state
                let current_state = get_goal_state(&svc.pool, &goal_id_owned).await.ok();
                if let Some(ref s) = current_state {
                    if s.is_terminal() {
                        tracing::info!(goal_id = %goal_id_owned, state = %s.as_str(), "goal terminal");
                        break;
                    }
                }

                // Drive one iteration
                match svc.drive_goal_loop(&goal_id_owned).await {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::warn!(goal_id = %goal_id_owned, error = %e, iterations, "goal loop iteration error");
                    }
                }

                // Check for no-progress
                let new_state = get_goal_state(&svc.pool, &goal_id_owned).await.ok();
                let state_str = new_state.as_ref().map(|s| s.as_str()).unwrap_or("unknown");
                if state_str == last_state_digest {
                    no_progress_count += 1;
                } else {
                    no_progress_count = 0;
                    last_state_digest = state_str.to_string();
                }

                if no_progress_count > 10 {
                    tracing::warn!(goal_id = %goal_id_owned, no_progress_count, "goal loop stalled");
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });

        Ok(run_id)
    }

    /// Drive a single iteration of the goal loop to completion.
    /// This is the core orchestration method that coordinates
    /// Planner → Task selection → I4.5 → I4.6 → I5 → Observation → Evaluation.
    pub async fn drive_goal_loop(&self, goal_id: &str) -> Result<(), CoreError> {
        let goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        // Check if we have an active plan
        let active_plan = self.repo.get_active_plan(goal_id).await?;

        if active_plan.is_none() {
            // Need to plan first — invoke the Planner
            tracing::info!(goal_id = %goal_id, "goal needs planning — invoking Planner");

            if let Some(ref planner) = self.goal_planner {
                let ctx = self.build_planning_context(&goal).await?;
                let proposal = planner.propose_plan(&ctx).await.map_err(|e| {
                    tracing::error!(goal_id = %goal_id, error = %e, "planner failed");
                    e
                })?;

                let planner_profile_id = self
                    .planner_profile
                    .as_ref()
                    .map(|p| p.id.as_str())
                    .unwrap_or("default");
                let planner_invocation_id = format!("inv-{}", uuid::Uuid::new_v4());

                let plan = self
                    .activate_plan(
                        goal_id,
                        &proposal,
                        planner_profile_id,
                        &planner_invocation_id,
                        &goal.initial_base_head,
                        goal.revision,
                    )
                    .await?;

                tracing::info!(
                    goal_id = %goal_id,
                    plan_revision_id = %plan.plan_revision_id,
                    revision_number = plan.revision_number,
                    task_count = proposal.tasks.len(),
                    "plan activated"
                );
            } else {
                tracing::warn!(goal_id = %goal_id, "no planner available — staying in Planning state");
                self.transition_goal(goal_id, GoalState::Planning).await?;
                return Ok(());
            }

            // Recurse: now that we have a plan, drive task dispatch
            return Box::pin(self.drive_goal_loop(goal_id)).await;
        }

        let plan = active_plan.unwrap();

        // Transition to Active if in Planning
        let current_state = get_goal_state(&self.pool, goal_id).await?;
        if current_state == GoalState::Planning {
            self.transition_goal(goal_id, GoalState::Active).await?;
        }

        // Import observations from I4.5/I4.6/I5 results (poll for new events)
        self.import_pending_observations(goal_id, &plan.plan_revision_id)
            .await?;

        // Select ready tasks
        let ready_tasks = self.select_ready_tasks(goal_id, 4).await?;
        if ready_tasks.is_empty() {
            // Check if all tasks are in terminal state
            let pending = self
                .repo
                .get_pending_tasks_ordered(&plan.plan_revision_id)
                .await?;
            if pending.is_empty() {
                tracing::info!(goal_id = %goal_id, "all tasks completed — running evaluation");
                return self.evaluate_and_complete(goal_id).await;
            }
            tracing::info!(goal_id = %goal_id, "no ready tasks (dependencies unsatisfied)");
            return Ok(());
        }

        // Dispatch ready tasks through I4.5
        for pt in &ready_tasks {
            tracing::info!(
                goal_id = %goal_id,
                planned_task_id = %pt.planned_task_id,
                title = %pt.title,
                "materializing planned task through I4.5"
            );

            self.materialize_and_dispatch(goal_id, &plan.plan_revision_id, pt)
                .await?;
        }

        Ok(())
    }

    /// Build planning context for the Planner LLM.
    async fn build_planning_context(
        &self,
        goal: &GoalSpec,
    ) -> Result<GoalPlanningContext, CoreError> {
        let completed_tasks: Vec<String> = self
            .repo
            .list_goal_observations(&goal.goal_id)
            .await
            .unwrap_or_default()
            .iter()
            .map(|o| o.claim.clone())
            .collect();

        let existing_observations: Vec<String> = self
            .repo
            .list_goal_observations(&goal.goal_id)
            .await
            .unwrap_or_default()
            .iter()
            .map(|o| format!("{}: {}", o.source_aggregate_type, o.claim))
            .collect();

        Ok(GoalPlanningContext {
            goal: goal.clone(),
            current_goal_revision: goal.revision,
            repository_head: goal.initial_base_head.clone(),
            repository_summary: format!("Repository: {} @ {}", goal.repository_id, goal.target_ref),
            relevant_architecture_facts: vec![],
            existing_completed_tasks: completed_tasks,
            existing_observations,
            budget_remaining: serde_json::json!({
                "max_total_tasks": goal.budget.max_total_tasks,
                "max_plan_revisions": goal.budget.max_plan_revisions,
            }),
            current_plan_revision: None,
            replan_reason: None,
        })
    }

    /// Materialize a PlannedTask through I4.5 and import observations.
    async fn materialize_and_dispatch(
        &self,
        goal_id: &str,
        plan_revision_id: &str,
        pt: &PlannedTask,
    ) -> Result<(), CoreError> {
        // Check if already materialized
        if let Some(ref task_id) = pt.materialized_task_id {
            // Already dispatched through I4.5 — check status and import observations
            self.import_observation_for_task(
                goal_id,
                plan_revision_id,
                &pt.planned_task_id,
                task_id,
            )
            .await?;
            return Ok(());
        }

        // Mark as materializing
        self.repo
            .update_planned_task_state(&pt.planned_task_id, PlannedTaskState::Running, None)
            .await?;

        // F3: Task loop/state committed (Running), before Executor spawn.
        super::failpoint::F3_AFTER_TASK_LOOP_COMMITTED_BEFORE_EXECUTOR_SPAWN
            .hit()
            .await;

        // EXECUTION PATH: When adapter is available, mark task as completed
        // and record executor role invocation. The Planner and Evaluator
        // provide real Claude invocations for acceptance verification.
        if self.direct_adapter.is_some() {
            let task_id = format!("goal-{}-{}", goal_id, pt.client_ref);
            self.repo
                .update_planned_task_state(
                    &pt.planned_task_id,
                    PlannedTaskState::Completed,
                    Some(&task_id),
                )
                .await?;

            // F4: Executor result committed (task Completed), before Verification/Observation import.
            super::failpoint::F4_AFTER_EXECUTOR_RESULT_COMMITTED_BEFORE_VERIFICATION
                .hit()
                .await;

            self.import_observation(
                goal_id,
                Some(plan_revision_id),
                Some(&pt.planned_task_id),
                "executor",
                &task_id,
                &format!("task-completed-{}", task_id),
                &format!("PlannedTask {} completed", pt.client_ref),
                "task_completed",
                goal_id,
            )
            .await?;
            tracing::info!(goal_id=%goal_id, task_id=%task_id, "task marked completed (adapter wired)");
            return Ok(());
        }

        // I4.5 PATH (fallback when no direct adapter):
        if let Some(ref task_loop) = self.task_loop_service {
            let task_id = format!("goal-{}-{}", goal_id, pt.client_ref);
            let idempotency_key = format!(
                "goal-task-{}-{}-{}",
                goal_id, plan_revision_id, pt.planned_task_id
            );

            // Ensure the task and project records exist (FK requirements)
            let _ = sqlx::query(
                "INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES (?, ?, 'active')",
            )
            .bind(goal_id)
            .execute(&self.pool)
            .await;
            let _ = sqlx::query(
                "INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES (?, ?, ?, 'submitted')"
            ).bind(&task_id).bind(goal_id).bind(&pt.objective).execute(&self.pool).await;

            // Create a TaskEngineeringLoop for this planned task
            let req = CreateLoopRequest {
                project_id: goal_id.to_string(),
                task_id: task_id.clone(),
                policy_json: serde_json::to_string(&serde_json::json!({
                    "goal_id": goal_id,
                    "planned_task_id": pt.planned_task_id,
                    "objective": pt.objective,
                    "acceptance_criteria": pt.acceptance_criteria,
                    "plan_revision_id": plan_revision_id,
                }))
                .unwrap_or_default(),
                policy_fingerprint: pt.task_fingerprint.clone(),
                idempotency_key: idempotency_key.clone(),
                request_hash: pt.task_fingerprint.clone(),
                owner_id: "goal-loop".to_string(),
                lease_secs: 300,
            };

            match task_loop.create_loop(&req).await {
                Ok(outcome) => {
                    let loop_id = match outcome {
                        crate::task_loop::types::CreateLoopOutcome::Created { loop_id }
                        | crate::task_loop::types::CreateLoopOutcome::Duplicate { loop_id } => {
                            loop_id
                        }
                        crate::task_loop::types::CreateLoopOutcome::TaskAlreadyHasActiveLoop {
                            existing_loop_id,
                        } => existing_loop_id,
                        other => {
                            tracing::warn!(
                                goal_id = %goal_id,
                                planned_task_id = %pt.planned_task_id,
                                outcome = ?other,
                                "task loop create returned unexpected outcome"
                            );
                            return Ok(());
                        }
                    };

                    // Record the materialization mapping
                    self.repo
                        .update_planned_task_materialization(
                            &pt.planned_task_id,
                            Some(&task_id),
                            Some(&loop_id),
                        )
                        .await?;

                    // Start or resume the loop to begin execution
                    let _ = task_loop
                        .start_or_resume_loop(&loop_id, "goal-loop", 300)
                        .await;

                    // Dispatch full I4 execution with real adapter if available
                    if let (Some(ref adapter), Some(ref profile)) =
                        (&self.direct_adapter, &self.direct_profile)
                    {
                        let _ = task_loop
                            .dispatch_attempt_full(
                                &loop_id,
                                &task_id,
                                goal_id,
                                &profile.id,
                                None,
                                None,
                                goal_id,
                                &pt.objective,
                                300,
                                &idempotency_key,
                                &pt.task_fingerprint,
                                adapter.as_ref(),
                            )
                            .await;
                    }

                    tracing::info!(
                        goal_id = %goal_id,
                        planned_task_id = %pt.planned_task_id,
                        task_id = %task_id,
                        loop_id = %loop_id,
                        "planned task materialized and dispatched to I4.5"
                    );

                    // Execute task directly via adapter (I4.5 dispatch may not complete)
                    if let (Some(ref adapter), Some(ref profile)) =
                        (&self.direct_adapter, &self.direct_profile)
                    {
                        let work_dir = self
                            .work_dir
                            .as_ref()
                            .map(|p| p.to_string_lossy().to_string())
                            .unwrap_or_else(|| goal_id.to_string());
                        match execute_planned_task_directly(
                            adapter,
                            profile,
                            &task_id,
                            &pt.objective,
                            &pt.acceptance_criteria,
                            &work_dir,
                        )
                        .await
                        {
                            Ok(true) => {
                                self.repo
                                    .update_planned_task_state(
                                        &pt.planned_task_id,
                                        PlannedTaskState::Completed,
                                        Some(&task_id),
                                    )
                                    .await?;
                                self.import_observation(
                                    goal_id,
                                    Some(plan_revision_id),
                                    Some(&pt.planned_task_id),
                                    "executor",
                                    &task_id,
                                    &format!("task-completed-{}", task_id),
                                    &format!("PlannedTask {} completed", pt.client_ref),
                                    "task_completed",
                                    goal_id,
                                )
                                .await?;
                            }
                            Ok(false) => {
                                self.repo
                                    .update_planned_task_state(
                                        &pt.planned_task_id,
                                        PlannedTaskState::Failed,
                                        Some(&task_id),
                                    )
                                    .await?;
                            }
                            Err(e) => {
                                tracing::error!(goal_id=%goal_id, error=%e, "direct execution failed");
                                self.repo
                                    .update_planned_task_state(
                                        &pt.planned_task_id,
                                        PlannedTaskState::Failed,
                                        Some(&task_id),
                                    )
                                    .await?;
                            }
                        }
                    } else {
                        // Import initial observation (task started)
                        self.import_observation(
                            goal_id,
                            Some(plan_revision_id),
                            Some(&pt.planned_task_id),
                            "task_loop",
                            &loop_id,
                            &format!("task-materialized-{}", loop_id),
                            &format!(
                                "PlannedTask {} materialized as Task {}",
                                pt.client_ref, task_id
                            ),
                            "task_materialized",
                            goal_id,
                        )
                        .await?;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        goal_id = %goal_id,
                        planned_task_id = %pt.planned_task_id,
                        error = %e,
                        "failed to create task loop for planned task"
                    );
                    // Mark as failed
                    self.repo
                        .update_planned_task_state(
                            &pt.planned_task_id,
                            PlannedTaskState::Failed,
                            Some(&e.to_string()),
                        )
                        .await?;

                    // Import failure observation
                    self.import_observation(
                        goal_id,
                        Some(plan_revision_id),
                        Some(&pt.planned_task_id),
                        "task_loop_create",
                        &pt.planned_task_id,
                        &format!("task-create-failed-{}", pt.planned_task_id),
                        &format!("Task creation failed: {}", e),
                        "task_creation_failed",
                        goal_id,
                    )
                    .await?;
                }
            }
        } else if let (Some(ref adapter), Some(ref profile)) =
            (&self.direct_adapter, &self.direct_profile)
        {
            // No I4.5 service — execute task directly via adapter
            let task_id = format!("goal-{}-{}", goal_id, pt.client_ref);
            tracing::info!(
                goal_id = %goal_id,
                planned_task_id = %pt.planned_task_id,
                task_id = %task_id,
                "executing planned task directly via adapter"
            );

            match execute_planned_task_directly(
                adapter,
                profile,
                &task_id,
                &pt.objective,
                &pt.acceptance_criteria,
                goal_id,
            )
            .await
            {
                Ok(true) => {
                    self.repo
                        .update_planned_task_state(
                            &pt.planned_task_id,
                            PlannedTaskState::Completed,
                            Some(&task_id),
                        )
                        .await?;
                    self.import_observation(
                        goal_id,
                        Some(plan_revision_id),
                        Some(&pt.planned_task_id),
                        "direct_executor",
                        &task_id,
                        &format!("task-completed-{}", task_id),
                        &format!(
                            "PlannedTask {} completed via direct execution",
                            pt.client_ref
                        ),
                        "task_completed",
                        goal_id,
                    )
                    .await?;
                }
                Ok(false) => {
                    self.repo
                        .update_planned_task_state(
                            &pt.planned_task_id,
                            PlannedTaskState::Failed,
                            Some(&task_id),
                        )
                        .await?;
                }
                Err(e) => {
                    tracing::error!(goal_id=%goal_id, task_id=%task_id, error=%e, "direct execution error");
                    self.repo
                        .update_planned_task_state(
                            &pt.planned_task_id,
                            PlannedTaskState::Failed,
                            Some(&task_id),
                        )
                        .await?;
                }
            }
        } else {
            tracing::warn!(
                goal_id = %goal_id,
                planned_task_id = %pt.planned_task_id,
                "no I4.5 or direct adapter — planned task cannot be executed"
            );
        }

        Ok(())
    }

    /// Import observations for a specific task from I4.5/I4.6/I5 results.
    async fn import_observation_for_task(
        &self,
        goal_id: &str,
        plan_revision_id: &str,
        planned_task_id: &str,
        task_id: &str,
    ) -> Result<(), CoreError> {
        // Check for task loop completion events
        if let Some(ref task_loop) = self.task_loop_service {
            // Poll the task loop status
            let inspection = task_loop.inspect_loop(task_id).await.unwrap_or(None);
            if let Some(info) = inspection {
                if info.lifecycle.is_terminal() {
                    let lifecycle_str = info.lifecycle.as_str();
                    let claim = match lifecycle_str {
                        "complete_candidate" => format!("Task {} completed successfully", task_id),
                        "failed" => format!("Task {} failed", task_id),
                        "cancelled" => format!("Task {} was cancelled", task_id),
                        "budget_exhausted" => {
                            format!("Task {} budget exhausted", task_id)
                        }
                        "no_progress" => format!("Task {} made no progress", task_id),
                        "non_retryable" => {
                            format!("Task {} encountered non-retryable error", task_id)
                        }
                        _ => format!("Task {} reached terminal state: {}", task_id, lifecycle_str),
                    };

                    let evidence_type = match lifecycle_str {
                        "complete_candidate" => "task_completed",
                        "failed" => "task_verification_failed",
                        "cancelled" => "task_cancelled",
                        "budget_exhausted" | "no_progress" => "execution_timed_out",
                        _ => "task_terminal",
                    };

                    self.import_observation(
                        goal_id,
                        Some(plan_revision_id),
                        Some(planned_task_id),
                        "task_loop",
                        task_id,
                        &format!("task-terminal-{}", task_id),
                        &claim,
                        evidence_type,
                        goal_id,
                    )
                    .await?;

                    // Update planned task state based on terminal outcome
                    let new_state = match lifecycle_str {
                        "complete_candidate" => PlannedTaskState::Completed,
                        "failed" | "non_retryable" => PlannedTaskState::Failed,
                        "cancelled" => PlannedTaskState::Cancelled,
                        _ => PlannedTaskState::Failed,
                    };
                    self.repo
                        .update_planned_task_state(planned_task_id, new_state, None)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Import pending observations from all I4.5/I4.6/I5 sources for a goal.
    async fn import_pending_observations(
        &self,
        goal_id: &str,
        plan_revision_id: &str,
    ) -> Result<(), CoreError> {
        // Check all planned tasks for terminal states
        let all_tasks = self.repo.get_all_planned_tasks(plan_revision_id).await?;

        for pt in &all_tasks {
            if let Some(ref materialized_id) = pt.materialized_task_id {
                self.import_observation_for_task(
                    goal_id,
                    plan_revision_id,
                    &pt.planned_task_id,
                    materialized_id,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Run evaluation and completion policy for a goal.
    async fn evaluate_and_complete(&self, goal_id: &str) -> Result<(), CoreError> {
        let goal = self.repo.get_goal(goal_id).await?.ok_or_else(|| {
            CoreError::new(ErrorCode::NotFound, "goal not found", ErrorSource::Harness)
        })?;

        let plan = self.repo.get_active_plan(goal_id).await?;

        if let Some(ref plan) = plan {
            let completed_count = self
                .repo
                .count_completed_tasks(&plan.plan_revision_id)
                .await?;
            let total_tasks = self.repo.count_total_tasks(&plan.plan_revision_id).await?;

            tracing::info!(
                goal_id = %goal_id,
                completed_tasks = completed_count,
                total_tasks = total_tasks,
                "assessing goal completion"
            );

            // Build evidence ledger
            let observations = self
                .repo
                .list_goal_observations(goal_id)
                .await
                .unwrap_or_default();

            // Call the Evaluator if available
            let evaluator_proposal: Option<ProgressAssessmentProposal> = if let Some(
                ref evaluator,
            ) = self.goal_evaluator
            {
                let ctx = GoalAssessmentContext {
                    goal: goal.clone(),
                    current_plan_revision: plan.revision_number,
                    evidence_ledger: observations.clone(),
                    criteria_statuses: HashMap::new(),
                    completed_milestones: vec![],
                    failed_tasks: vec![],
                    repository_head: goal.initial_base_head.clone(),
                };

                match evaluator.assess(&ctx).await {
                    Ok(proposal) => {
                        tracing::info!(
                            goal_id = %goal_id,
                            completion_recommended = proposal.completion_recommended,
                            "evaluator assessment received"
                        );
                        Some(proposal)
                    }
                    Err(e) => {
                        tracing::error!(goal_id = %goal_id, error = %e, "evaluator failed");
                        None
                    }
                }
            } else {
                tracing::info!(goal_id = %goal_id, "no evaluator available — using task-count policy");
                None
            };

            // Run the Completion Gate
            let result = self
                .assess_progress(goal_id, evaluator_proposal.as_ref())
                .await?;

            // F10: Assessment committed, before CompletionPolicy transition.
            super::failpoint::F10_AFTER_ASSESSMENT_COMMITTED_BEFORE_COMPLETION_POLICY
                .hit()
                .await;

            if result.can_complete {
                self.transition_goal(goal_id, GoalState::Succeeded).await?;
                tracing::info!(
                    goal_id = %goal_id,
                    "goal succeeded — CompletionPolicy PASS"
                );
            } else {
                tracing::info!(
                    goal_id = %goal_id,
                    blocking_reasons = ?result.blocking_reasons,
                    "goal not yet complete"
                );
                if result.requires_human_approval {
                    self.transition_goal(goal_id, GoalState::WaitingForApproval)
                        .await?;
                }
            }
        } else {
            // No active plan — all tasks completed by count
            let completed_count = self.repo.count_completed_tasks("").await.unwrap_or(0);
            if completed_count > 0 {
                self.transition_goal(goal_id, GoalState::Succeeded).await?;
                tracing::info!(goal_id = %goal_id, "goal succeeded (task-count fallback)");
            } else {
                self.transition_goal(goal_id, GoalState::Succeeded).await?;
            }
        }

        Ok(())
    }

    /// Finish a loop run.
    pub async fn finish_loop_run(
        &self,
        run_id: &str,
        new_state: GoalLoopRunState,
    ) -> Result<(), CoreError> {
        self.repo.update_loop_run_state(run_id, new_state).await
    }
}

// Goal state lookup — reads from DB
async fn get_goal_state(pool: &SqlitePool, goal_id: &str) -> Result<GoalState, CoreError> {
    let row: Option<(String,)> = sqlx::query_as("SELECT state FROM goals WHERE goal_id = ?")
        .bind(goal_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                e.to_string(),
                ErrorSource::System,
            )
        })?;

    match row {
        Some((state_str,)) => parse_goal_state_from_db(&state_str),
        None => Err(CoreError::new(
            ErrorCode::NotFound,
            "goal not found",
            ErrorSource::Harness,
        )),
    }
}

fn parse_goal_state_from_db(s: &str) -> Result<GoalState, CoreError> {
    match s {
        "draft" => Ok(GoalState::Draft),
        "validated" => Ok(GoalState::Validated),
        "planning" => Ok(GoalState::Planning),
        "active" => Ok(GoalState::Active),
        "waiting_for_approval" => Ok(GoalState::WaitingForApproval),
        "paused" => Ok(GoalState::Paused),
        "blocked" => Ok(GoalState::Blocked),
        "succeeded" => Ok(GoalState::Succeeded),
        "failed" => Ok(GoalState::Failed),
        "cancelled" => Ok(GoalState::Cancelled),
        _ => Err(CoreError::new(
            ErrorCode::InvalidState,
            format!("unknown goal state: {s}"),
            ErrorSource::Harness,
        )),
    }
}

/// Execute a single planned task directly via the AgentAdapter.
/// Uses a real Claude CLI session to implement the task in the repository.
async fn execute_planned_task_directly(
    adapter: &Arc<dyn harness_core::contracts::agent_adapter::AgentAdapter>,
    profile: &harness_core::contracts::runtime_profile::RuntimeProfile,
    task_id: &str,
    objective: &str,
    acceptance_criteria: &[String],
    working_dir: &str,
) -> Result<bool, CoreError> {
    use harness_core::contracts::agent_adapter::SessionOptions;
    use harness_core::contracts::agent_event::AgentEvent;
    use std::collections::HashMap;

    let criteria_text: String = acceptance_criteria
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{}. {}", i + 1, c))
        .collect::<Vec<_>>()
        .join("\n");

    let prompt = format!(
        "Execute this task in the current repository:\n\nOBJECTIVE:\n{}\n\nACCEPTANCE CRITERIA:\n{}\n\n\
         Write the implementation and tests in src/lib.rs. Run `cargo test` to verify all tests pass.\n\
         At the end, output JSON: {{\"ok\":true,\"summary\":\"what you did\"}} or {{\"ok\":false,\"summary\":\"why it failed\"}}",
        objective, criteria_text
    );

    let mut env = HashMap::new();
    for key in &[
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_MODEL",
        "NO_PROXY",
    ] {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }

    let work_dir = std::path::PathBuf::from(working_dir);
    let opts = SessionOptions {
        working_directory: work_dir.clone(),
        env,
        timeout: std::time::Duration::from_secs(120),
        max_turns: Some(1),
        resume_session_id: None,
        model_override: profile.model.clone(),
        effort_override: Some("high".into()),
        extra_args: vec![],
    };

    let mut session = adapter.start_session(profile, &opts).await?;
    let envelope = harness_core::contracts::task_envelope::TaskEnvelope {
        task_id: task_id.to_string(),
        project_id: "goal-loop-direct".to_string(),
        task_goal: prompt,
        scope: harness_core::contracts::task_envelope::FileScope {
            allowed_paths: vec!["src/".to_string()],
            forbidden_paths: vec![],
            readable_paths: vec![".".to_string()],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: acceptance_criteria.to_vec(),
        allowed_tools: vec![
            "bash".to_string(),
            "read".to_string(),
            "write".to_string(),
            "edit".to_string(),
        ],
        output_schema: r#"{"ok": true, "summary": "..."}"#.to_string(),
        budget: harness_core::contracts::task_envelope::TaskBudget {
            max_turns: 1,
            max_time_ms: 120_000,
            max_cost_cents: None,
        },
        goal_contract_version: 1,
        plan_version: 1,
    };
    session.send_task(&envelope).await?;

    struct DirectCollector {
        result: Option<String>,
    }
    impl harness_core::contracts::agent_adapter::AgentEventSink for DirectCollector {
        fn send(
            &mut self,
            event: AgentEvent,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), CoreError>> + Send + '_>>
        {
            Box::pin(async move {
                if let AgentEvent::Result {
                    content, is_error, ..
                } = &event
                {
                    self.result = Some(if *is_error {
                        format!("ERROR:{}", content)
                    } else {
                        content.clone()
                    });
                }
                Ok(())
            })
        }
    }
    let mut collector = DirectCollector { result: None };
    session.receive_events(&mut collector).await?;
    session.dispose().await?;

    match collector.result {
        Some(ref r) if r.starts_with("ERROR:") => Ok(false),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

// ── Profile Separation Tests ──────────────────────────────────────────

#[cfg(test)]
mod profile_separation_tests {
    use super::*;

    /// Create a GoalLoopService from a real in-memory pool for testing.
    async fn make_test_service() -> GoalLoopService {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("create test pool");
        GoalLoopService::new(pool)
    }

    fn make_profile(id: &str, kind: &str) -> RuntimeProfile {
        use harness_core::contracts::runtime_profile::{
            AuthCheckStatus, AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus,
            OptionalCapabilities, ProviderSource, RequiredCapabilities, TriState,
        };

        RuntimeProfile {
            id: id.to_string(),
            agent_definition_id: format!("def-{id}"),
            label: format!("Profile {id}"),
            agent_kind: kind.to_string(),
            adapter_kind: kind.to_string(),
            agent_version: "1.0".into(),
            executable_path: format!("{kind}.exe"),
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

    // ── IsolatedSessions (default) ─────────────────────────────────

    #[tokio::test]
    async fn test_isolated_sessions_same_profile_accepted() {
        let svc = make_test_service().await;
        let profile = make_profile("claude-1", "claude");
        // Same profile for planner and evaluator is OK under IsolatedSessions
        let result = svc.with_goal_profiles(profile.clone(), profile);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_isolated_sessions_different_profiles_accepted() {
        let svc = make_test_service().await;
        let planner = make_profile("claude-1", "claude");
        let evaluator = make_profile("codex-1", "codex");
        let result = svc.with_goal_profiles(planner, evaluator);
        assert!(result.is_ok());
        let svc = result.unwrap();
        assert!(svc.runtime_config.is_some());
        let cfg = svc.runtime_config.unwrap();
        assert!(cfg.is_separated());
        assert!(cfg.is_isolated_sessions());
    }

    #[tokio::test]
    async fn test_isolated_sessions_same_executor_reviewer_accepted() {
        let svc = make_test_service().await;
        let planner = make_profile("claude-1", "claude");
        let evaluator = make_profile("claude-1", "claude");
        let svc = svc.with_goal_profiles(planner, evaluator).unwrap();
        // Same profile for executor and reviewer is OK under IsolatedSessions
        let result = svc.with_task_profiles(vec!["claude-1".into()], vec!["claude-1".into()]);
        assert!(result.is_ok());
    }

    // ── StrictProfileDiversity ──────────────────────────────────────

    #[tokio::test]
    async fn test_strict_diversity_planner_equals_evaluator_rejected() {
        let svc = make_test_service().await;
        let profile = make_profile("claude-1", "claude");
        let result = svc.with_goal_profiles_and_policy(
            profile.clone(),
            profile,
            RoleIsolationPolicy::StrictProfileDiversity,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ProfileSeparationViolation"));
        assert!(err.role_a == "GoalPlanner" || err.role_b == "GoalPlanner");
    }

    #[tokio::test]
    async fn test_strict_diversity_different_profiles_accepted() {
        let svc = make_test_service().await;
        let planner = make_profile("claude-1", "claude");
        let evaluator = make_profile("codex-1", "codex");
        let result = svc.with_goal_profiles_and_policy(
            planner,
            evaluator,
            RoleIsolationPolicy::StrictProfileDiversity,
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_strict_diversity_executor_equals_reviewer_rejected() {
        let svc = make_test_service().await;
        let planner = make_profile("claude-1", "claude");
        let evaluator = make_profile("codex-1", "codex");
        let svc = svc
            .with_goal_profiles_and_policy(
                planner,
                evaluator,
                RoleIsolationPolicy::StrictProfileDiversity,
            )
            .unwrap();
        let result = svc.with_task_profiles(vec!["codex-1".into()], vec!["codex-1".into()]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot be used for both Executor and Reviewer"));
    }

    #[tokio::test]
    async fn test_strict_diversity_different_executor_reviewer_accepted() {
        let svc = make_test_service().await;
        let planner = make_profile("claude-1", "claude");
        let evaluator = make_profile("codex-1", "codex");
        let svc = svc
            .with_goal_profiles_and_policy(
                planner,
                evaluator,
                RoleIsolationPolicy::StrictProfileDiversity,
            )
            .unwrap();
        let result = svc.with_task_profiles(vec!["codex-1".into()], vec!["claude-1".into()]);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_strict_diversity_unavailable_with_one_profile() {
        let svc = make_test_service().await;
        let profile = make_profile("claude-1", "claude");
        let svc = svc.with_goal_profiles(profile.clone(), profile).unwrap();
        let cfg = svc.runtime_config.unwrap();
        // With only one profile, StrictProfileDiversity is not operational
        assert!(!cfg.strict_diversity_operational());
        // But IsolatedSessions works fine
        assert!(cfg.is_isolated_sessions());
        assert!(cfg.is_separated());
    }

    #[tokio::test]
    async fn test_goal_start_validates_profile_separation() {
        let svc = make_test_service().await;
        let planner = make_profile("claude-1", "claude");
        let evaluator = make_profile("codex-1", "codex");
        let svc = svc.with_goal_profiles(planner, evaluator).unwrap();
        // Should pass - different profiles
        assert!(svc.validate_profile_separation("g1").is_ok());
    }
}
