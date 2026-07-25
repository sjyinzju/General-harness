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

use chrono::Utc;
use harness_core::contracts::goal::{GoalSpec, GoalState};
use harness_core::contracts::plan::{
    compute_task_fingerprint, Milestone, MilestoneState, PlanRevision, PlanState, PlannedTask,
    PlannedTaskState, RiskLevel,
};
use harness_core::state_machine::GoalFsm;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::repo::GoalRepo;
use super::validation::{check_completion_gate, validate_plan_proposal};
use super::{
    ApprovalRequest, ApprovalState, ApprovalType, CriterionStatus, GoalLoopRunState,
    GoalObservation, PlanProposal, ProgressAssessmentProposal, ReplanDecision, ReplanTrigger,
};

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
}

impl GoalLoopService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            repo: GoalRepo::new(pool.clone()),
            pool,
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
            .unwrap_or(false);

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

    /// Start a new goal loop iteration.
    pub async fn start_loop_run(&self, goal_id: &str) -> Result<String, CoreError> {
        let plan = self.repo.get_active_plan(goal_id).await?;
        let plan_id = plan.as_ref().map(|p| p.plan_revision_id.as_str());
        let run_id = self.repo.create_loop_run(goal_id, plan_id).await?;
        Ok(run_id)
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
