//! Goal repository — persistent storage for Goals, PlanRevisions, Milestones,
//! PlannedTasks, Observations, Assessments, Approvals, and Invocations.
//!
//! All writes go through the Supervisor's database pool. All state changes
//! are transactional (state update + event append).

use chrono::Utc;
use harness_core::contracts::goal::{
    GoalConstraint, GoalCreator, GoalSpec, GoalState, SuccessCriterion,
};
use harness_core::contracts::plan::{
    Milestone, MilestoneState, PlanRevision, PlanState, PlannedTask, PlannedTaskState, RiskLevel,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::{
    ApprovalRequest, ApprovalState, ApprovalType, GoalLoopRunState, GoalObservation,
    InterventionClassification, InterventionState, ProgressAssessmentProposal, UserIntervention,
};

// ── Goal Repo ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct GoalRepo {
    pool: SqlitePool,
}

impl GoalRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // ── Goals ──────────────────────────────────────────────────────

    pub async fn insert_goal(&self, goal: &GoalSpec) -> Result<(), CoreError> {
        let budget_json = serde_json::to_string(&goal.budget).unwrap_or_default();
        let policy_json = serde_json::to_string(&goal.approval_policy).unwrap_or_default();
        let creator_json = serde_json::to_string(&goal.created_by).unwrap_or_default();
        let non_goals_json = serde_json::to_string(&goal.non_goals).unwrap_or_default();

        sqlx::query(
            r#"INSERT INTO goals (goal_id, revision, title, objective, repository_id, target_ref,
               initial_base_head, state, budget_json, approval_policy_json, created_by_json,
               non_goals_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&goal.goal_id)
        .bind(goal.revision)
        .bind(&goal.title)
        .bind(&goal.objective)
        .bind(&goal.repository_id)
        .bind(&goal.target_ref)
        .bind(&goal.initial_base_head)
        .bind("draft")
        .bind(&budget_json)
        .bind(&policy_json)
        .bind(&creator_json)
        .bind(&non_goals_json)
        .bind(now_sql())
        .bind(now_sql())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        // Insert success criteria
        for c in &goal.success_criteria {
            self.insert_success_criterion(&goal.goal_id, c).await?;
        }

        // Insert constraints
        for c in &goal.constraints {
            self.insert_constraint(&goal.goal_id, c).await?;
        }

        Ok(())
    }

    async fn insert_success_criterion(
        &self,
        goal_id: &str,
        c: &SuccessCriterion,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO goal_success_criteria (criterion_id, goal_id, description,
               evidence_policy, evidence_policy_config, verification_policy,
               verification_policy_config, subjectivity, required)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&c.criterion_id)
        .bind(goal_id)
        .bind(&c.description)
        .bind(evidence_policy_str(&c.evidence_policy))
        .bind(serde_json::to_string(&c.evidence_policy).unwrap_or_default())
        .bind(verification_policy_str(&c.verification_policy))
        .bind(serde_json::to_string(&c.verification_policy).unwrap_or_default())
        .bind(if c.subjectivity.requires_human_approval() {
            "subjective"
        } else {
            "objective"
        })
        .bind(c.required as i32)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn insert_constraint(&self, goal_id: &str, c: &GoalConstraint) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO goal_constraints (constraint_id, goal_id, description,
               constraint_type, constraint_config, blocking)
               VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&c.constraint_id)
        .bind(goal_id)
        .bind(&c.description)
        .bind(constraint_type_str(&c.constraint_type))
        .bind(serde_json::to_string(&c.constraint_type).unwrap_or_default())
        .bind(c.blocking as i32)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn update_goal_state(
        &self,
        goal_id: &str,
        new_state: GoalState,
    ) -> Result<(), CoreError> {
        let r = sqlx::query(
            r#"UPDATE goals SET state = ?, updated_at = ?,
               completed_at = CASE WHEN ? IN ('succeeded','failed','cancelled') THEN ? ELSE completed_at END
               WHERE goal_id = ? AND state NOT IN ('succeeded','failed','cancelled')"#,
        )
        .bind(new_state.as_str())
        .bind(now_sql())
        .bind(new_state.as_str())
        .bind(now_sql())
        .bind(goal_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        if r.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "goal is terminal or not found",
                ErrorSource::Harness,
            ));
        }
        Ok(())
    }

    pub async fn get_goal(&self, goal_id: &str) -> Result<Option<GoalSpec>, CoreError> {
        let row: Option<GoalRow> = sqlx::query_as(
            r#"SELECT goal_id, revision, title, objective, repository_id, target_ref,
               initial_base_head, state, budget_json, approval_policy_json, created_by_json,
               non_goals_json, created_at FROM goals WHERE goal_id = ?"#,
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        match row {
            Some(r) => {
                let criteria = self.get_success_criteria(goal_id).await?;
                let constraints = self.get_constraints(goal_id).await?;
                Ok(Some(r.into_goal_spec(criteria, constraints)))
            }
            None => Ok(None),
        }
    }

    async fn get_success_criteria(
        &self,
        goal_id: &str,
    ) -> Result<Vec<SuccessCriterion>, CoreError> {
        let rows: Vec<CriterionRow> = sqlx::query_as(
            r#"SELECT criterion_id, description, evidence_policy, evidence_policy_config,
               verification_policy, verification_policy_config, subjectivity, required
               FROM goal_success_criteria WHERE goal_id = ?"#,
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows.into_iter().map(|r| r.into_criterion()).collect())
    }

    async fn get_constraints(&self, goal_id: &str) -> Result<Vec<GoalConstraint>, CoreError> {
        let rows: Vec<ConstraintRow> = sqlx::query_as(
            r#"SELECT constraint_id, description, constraint_type, constraint_config, blocking
               FROM goal_constraints WHERE goal_id = ?"#,
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows.into_iter().map(|r| r.into_constraint()).collect())
    }

    pub async fn list_goals_by_state(
        &self,
        state: Option<&str>,
    ) -> Result<Vec<GoalSpec>, CoreError> {
        let rows: Vec<GoalRow> = if let Some(s) = state {
            sqlx::query_as(
                r#"SELECT goal_id, revision, title, objective, repository_id, target_ref,
                   initial_base_head, state, budget_json, approval_policy_json, created_by_json,
                   non_goals_json, created_at FROM goals WHERE state = ? ORDER BY created_at DESC"#,
            )
            .bind(s)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?
        } else {
            sqlx::query_as(
                r#"SELECT goal_id, revision, title, objective, repository_id, target_ref,
                   initial_base_head, state, budget_json, approval_policy_json, created_by_json,
                   non_goals_json, created_at FROM goals ORDER BY created_at DESC"#,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?
        };

        let mut goals = Vec::new();
        for r in rows {
            let criteria = self.get_success_criteria(&r.goal_id).await?;
            let constraints = self.get_constraints(&r.goal_id).await?;
            goals.push(r.into_goal_spec(criteria, constraints));
        }
        Ok(goals)
    }

    // ── Goal Revisions ──────────────────────────────────────────────

    pub async fn insert_goal_revision(
        &self,
        goal_id: &str,
        revision_number: i64,
        spec_snapshot: &serde_json::Value,
        spec_digest: &str,
        created_by: &str,
        reason: &str,
    ) -> Result<String, CoreError> {
        let revision_id = format!("gr-{}", uuid::Uuid::new_v4());
        sqlx::query(
            r#"INSERT INTO goal_revisions (goal_revision_id, goal_id, revision_number,
               spec_snapshot_json, spec_digest, created_by, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&revision_id)
        .bind(goal_id)
        .bind(revision_number)
        .bind(serde_json::to_string(spec_snapshot).unwrap_or_default())
        .bind(spec_digest)
        .bind(created_by)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(revision_id)
    }

    // ── Plan Revisions ──────────────────────────────────────────────

    pub async fn insert_plan_revision(&self, plan: &PlanRevision) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO plan_revisions (plan_revision_id, goal_id, goal_revision,
               revision_number, base_repository_head, planner_profile_id,
               planner_invocation_id, proposal_digest, validation_digest, state,
               created_at, activated_at, superseded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&plan.plan_revision_id)
        .bind(&plan.goal_id)
        .bind(plan.goal_revision)
        .bind(plan.revision_number)
        .bind(&plan.base_repository_head)
        .bind(&plan.planner_profile_id)
        .bind(&plan.planner_invocation_id)
        .bind(&plan.proposal_digest)
        .bind(&plan.validation_digest)
        .bind(plan.state.as_str())
        .bind(now_sql())
        .bind(plan.activated_at.map(|t| t.to_rfc3339()))
        .bind(plan.superseded_at.map(|t| t.to_rfc3339()))
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn update_plan_state(
        &self,
        plan_revision_id: &str,
        new_state: PlanState,
        validation_digest: Option<&str>,
    ) -> Result<(), CoreError> {
        let r = sqlx::query(
            r#"UPDATE plan_revisions SET state = ?,
               validation_digest = COALESCE(?, validation_digest),
               activated_at = CASE WHEN ? = 'active' THEN ? ELSE activated_at END,
               superseded_at = CASE WHEN ? = 'superseded' THEN ? ELSE superseded_at END
               WHERE plan_revision_id = ? AND state NOT IN ('superseded','completed','rejected','invalid','cancelled')"#,
        )
        .bind(new_state.as_str())
        .bind(validation_digest)
        .bind(new_state.as_str())
        .bind(now_sql())
        .bind(new_state.as_str())
        .bind(now_sql())
        .bind(plan_revision_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        if r.rows_affected() == 0 {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "plan revision is terminal or not found",
                ErrorSource::Harness,
            ));
        }
        Ok(())
    }

    pub async fn supersede_active_plans(
        &self,
        goal_id: &str,
        except_plan_id: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"UPDATE plan_revisions SET state = 'superseded', superseded_at = ?
               WHERE goal_id = ? AND state = 'active' AND plan_revision_id != ?"#,
        )
        .bind(now_sql())
        .bind(goal_id)
        .bind(except_plan_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn get_active_plan(&self, goal_id: &str) -> Result<Option<PlanRevision>, CoreError> {
        let row: Option<PlanRow> = sqlx::query_as(
            r#"SELECT plan_revision_id, goal_id, goal_revision, revision_number,
               base_repository_head, planner_profile_id, planner_invocation_id,
               proposal_digest, validation_digest, state, created_at, activated_at, superseded_at
               FROM plan_revisions WHERE goal_id = ? AND state = 'active'"#,
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| r.into_plan_revision()))
    }

    /// Fetch a single plan revision by id (any state).
    pub async fn get_plan_revision(
        &self,
        plan_revision_id: &str,
    ) -> Result<Option<PlanRevision>, CoreError> {
        let row: Option<PlanRow> = sqlx::query_as(
            r#"SELECT plan_revision_id, goal_id, goal_revision, revision_number,
               base_repository_head, planner_profile_id, planner_invocation_id,
               proposal_digest, validation_digest, state, created_at, activated_at, superseded_at
               FROM plan_revisions WHERE plan_revision_id = ?"#,
        )
        .bind(plan_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| r.into_plan_revision()))
    }

    // ── Milestones ──────────────────────────────────────────────────

    pub async fn insert_milestone(&self, m: &Milestone) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO plan_milestones (milestone_id, plan_revision_id, client_ref,
               title, objective, success_criteria_refs_json, dependencies_json, priority, state)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&m.milestone_id)
        .bind(&m.plan_revision_id)
        .bind(&m.client_ref)
        .bind(&m.title)
        .bind(&m.objective)
        .bind(serde_json::to_string(&m.success_criteria_refs).unwrap_or_default())
        .bind(serde_json::to_string(&m.dependencies).unwrap_or_default())
        .bind(m.priority)
        .bind(m.state.as_str())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn update_milestone_state(
        &self,
        milestone_id: &str,
        new_state: MilestoneState,
    ) -> Result<(), CoreError> {
        sqlx::query(r#"UPDATE plan_milestones SET state = ? WHERE milestone_id = ?"#)
            .bind(new_state.as_str())
            .bind(milestone_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    // ── Planned Tasks ───────────────────────────────────────────────

    pub async fn insert_planned_task(&self, pt: &PlannedTask) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO planned_tasks (planned_task_id, plan_revision_id, milestone_id,
               client_ref, title, objective, acceptance_criteria_json, dependency_refs_json,
               expected_evidence_json, expected_resource_scope_json, risk_level,
               requires_approval, task_fingerprint, state, materialized_task_id, materialized_loop_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&pt.planned_task_id)
        .bind(&pt.plan_revision_id)
        .bind(&pt.milestone_id)
        .bind(&pt.client_ref)
        .bind(&pt.title)
        .bind(&pt.objective)
        .bind(serde_json::to_string(&pt.acceptance_criteria).unwrap_or_default())
        .bind(serde_json::to_string(&pt.dependency_refs).unwrap_or_default())
        .bind(serde_json::to_string(&pt.expected_evidence).unwrap_or_default())
        .bind(serde_json::to_string(&pt.expected_resource_scope).unwrap_or_default())
        .bind(pt.risk_level.as_str())
        .bind(pt.requires_approval as i32)
        .bind(&pt.task_fingerprint)
        .bind(pt.state.as_str())
        .bind(&pt.materialized_task_id)
        .bind(&pt.materialized_loop_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn update_planned_task_state(
        &self,
        planned_task_id: &str,
        new_state: PlannedTaskState,
        materialized_task_id: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"UPDATE planned_tasks SET state = ?,
               materialized_task_id = COALESCE(?, materialized_task_id),
               updated_at = ?
               WHERE planned_task_id = ?"#,
        )
        .bind(new_state.as_str())
        .bind(materialized_task_id)
        .bind(now_sql())
        .bind(planned_task_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn find_duplicate_fingerprint(
        &self,
        goal_id: &str,
        fingerprint: &str,
    ) -> Result<Option<String>, CoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"SELECT pt.planned_task_id FROM planned_tasks pt
               JOIN plan_revisions pr ON pt.plan_revision_id = pr.plan_revision_id
               WHERE pr.goal_id = ? AND pt.task_fingerprint = ? AND pt.state != 'superseded'
               LIMIT 1"#,
        )
        .bind(goal_id)
        .bind(fingerprint)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(row.map(|r| r.0))
    }

    pub async fn count_completed_tasks(&self, plan_revision_id: &str) -> Result<i64, CoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM planned_tasks WHERE plan_revision_id = ? AND state = 'completed'",
        )
        .bind(plan_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.0).unwrap_or(0))
    }

    pub async fn count_total_tasks(&self, plan_revision_id: &str) -> Result<i64, CoreError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM planned_tasks WHERE plan_revision_id = ? AND state != 'superseded'",
        )
        .bind(plan_revision_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.0).unwrap_or(0))
    }

    pub async fn get_pending_tasks_ordered(
        &self,
        plan_revision_id: &str,
    ) -> Result<Vec<PlannedTask>, CoreError> {
        let rows: Vec<PlannedTaskRow> = sqlx::query_as(
            r#"SELECT planned_task_id, plan_revision_id, milestone_id, client_ref, title,
               objective, acceptance_criteria_json, dependency_refs_json, expected_evidence_json,
               expected_resource_scope_json, risk_level, requires_approval, task_fingerprint,
               state, materialized_task_id, materialized_loop_id
               FROM planned_tasks
               WHERE plan_revision_id = ? AND state = 'pending'
               ORDER BY client_ref ASC"#,
        )
        .bind(plan_revision_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows.into_iter().map(|r| r.into_planned_task()).collect())
    }

    // ── Goal Loop Runs ──────────────────────────────────────────────

    /// Create or reuse a goal loop run. If an active run already exists
    /// (from a crashed predecessor supervisor), returns its run_id instead
    /// of creating a new one. This handles the partial UNIQUE index:
    /// `idx_goal_loop_one_active_per_goal` WHERE state NOT IN terminal.
    pub async fn create_loop_run(
        &self,
        goal_id: &str,
        plan_revision_id: Option<&str>,
    ) -> Result<String, CoreError> {
        // Check for existing active run (from a crashed predecessor)
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT run_id FROM goal_loop_runs WHERE goal_id = ? AND state NOT IN ('completed','failed','cancelled') LIMIT 1",
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        if let Some((existing_run_id,)) = existing {
            return Ok(existing_run_id);
        }

        let run_id = format!("glr-{}", uuid::Uuid::new_v4());
        sqlx::query(
            r#"INSERT INTO goal_loop_runs (run_id, goal_id, plan_revision_id, state,
               iteration_number, created_at)
               VALUES (?, ?, ?, 'created', 0, ?)"#,
        )
        .bind(&run_id)
        .bind(goal_id)
        .bind(plan_revision_id)
        .bind(now_sql())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(run_id)
    }

    pub async fn update_loop_run_state(
        &self,
        run_id: &str,
        new_state: GoalLoopRunState,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"UPDATE goal_loop_runs SET state = ?,
               completed_at = CASE WHEN ? IN ('completed','failed','cancelled') THEN ? ELSE completed_at END
               WHERE run_id = ?"#,
        )
        .bind(format_state(&new_state))
        .bind(format_state(&new_state))
        .bind(now_sql())
        .bind(run_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Observations ────────────────────────────────────────────────

    /// Insert an observation. Returns `true` if created, `false` if already
    /// exists (idempotent duplicate suppressed by UNIQUE index).
    pub async fn insert_observation(&self, obs: &GoalObservation) -> Result<bool, CoreError> {
        let result = sqlx::query(
            r#"INSERT OR IGNORE INTO goal_observations (observation_id, goal_id,
               plan_revision_id, planned_task_id, source_aggregate_type, source_aggregate_id,
               source_event_id, source_digest, repository_head, claim, evidence_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&obs.observation_id)
        .bind(&obs.goal_id)
        .bind(&obs.plan_revision_id)
        .bind(&obs.planned_task_id)
        .bind(&obs.source_aggregate_type)
        .bind(&obs.source_aggregate_id)
        .bind(&obs.source_event_id)
        .bind(&obs.source_digest)
        .bind(&obs.repository_head)
        .bind(&obs.claim)
        .bind(&obs.evidence_type)
        .bind(now_sql())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(result.rows_affected() > 0)
    }

    // ── Assessments ─────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_assessment(
        &self,
        assessment_id: &str,
        goal_id: &str,
        plan_revision_id: Option<&str>,
        loop_run_id: Option<&str>,
        evaluator_profile_id: &str,
        evaluator_invocation_id: &str,
        proposal: &ProgressAssessmentProposal,
        rust_validation: &serde_json::Value,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO goal_progress_assessments (assessment_id, goal_id,
               plan_revision_id, goal_loop_run_id, evaluator_profile_id,
               evaluator_invocation_id, proposed_assessment_json,
               rust_validation_result_json, completion_recommended)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(assessment_id)
        .bind(goal_id)
        .bind(plan_revision_id)
        .bind(loop_run_id)
        .bind(evaluator_profile_id)
        .bind(evaluator_invocation_id)
        .bind(serde_json::to_string(proposal).unwrap_or_default())
        .bind(serde_json::to_string(rust_validation).unwrap_or_default())
        .bind(proposal.completion_recommended as i32)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Approvals ───────────────────────────────────────────────────

    pub async fn create_approval(&self, approval: &ApprovalRequest) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO approval_requests (approval_id, goal_id, plan_revision_id,
               approval_type, requested_action_json, payload_digest, reason, state, created_at,
               request_id, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&approval.approval_id)
        .bind(&approval.goal_id)
        .bind(&approval.plan_revision_id)
        .bind(approval.approval_type.as_str())
        .bind(serde_json::to_string(&approval.requested_action).unwrap_or_default())
        .bind(&approval.payload_digest)
        .bind(&approval.reason)
        .bind("pending")
        .bind(now_sql())
        .bind(&approval.request_id)
        .bind(&approval.source)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn resolve_approval(
        &self,
        approval_id: &str,
        new_state: &str,
        resolved_by: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"UPDATE approval_requests SET state = ?, resolved_at = ?, resolved_by = ?
               WHERE approval_id = ? AND state = 'pending'"#,
        )
        .bind(new_state)
        .bind(now_sql())
        .bind(resolved_by)
        .bind(approval_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Resolve a pending approval, storing the user's response payload.
    /// Returns `true` if the row was transitioned (was still pending),
    /// `false` if the approval was already resolved — callers use this for
    /// idempotent replay / conflict reporting.
    pub async fn resolve_approval_with_response(
        &self,
        approval_id: &str,
        new_state: &str,
        resolved_by: &str,
        response_json: Option<&str>,
    ) -> Result<bool, CoreError> {
        let result = sqlx::query(
            r#"UPDATE approval_requests
               SET state = ?, resolved_at = ?, resolved_by = ?, response_json = ?
               WHERE approval_id = ? AND state = 'pending'"#,
        )
        .bind(new_state)
        .bind(now_sql())
        .bind(resolved_by)
        .bind(response_json)
        .bind(approval_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(result.rows_affected() > 0)
    }

    /// Fetch a single approval request by id.
    pub async fn get_approval(
        &self,
        approval_id: &str,
    ) -> Result<Option<ApprovalRequest>, CoreError> {
        let row: Option<ApprovalRow> = sqlx::query_as(
            r#"SELECT approval_id, goal_id, plan_revision_id, approval_type,
               requested_action_json, payload_digest, reason, state, created_at,
               resolved_at, resolved_by, response_json, request_id, source
               FROM approval_requests WHERE approval_id = ?"#,
        )
        .bind(approval_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.into_approval()))
    }

    pub async fn list_pending_approvals(
        &self,
        goal_id: &str,
    ) -> Result<Vec<ApprovalRequest>, CoreError> {
        let rows: Vec<ApprovalRow> = sqlx::query_as(
            r#"SELECT approval_id, goal_id, plan_revision_id, approval_type,
               requested_action_json, payload_digest, reason, state, created_at,
               resolved_at, resolved_by, response_json, request_id, source
               FROM approval_requests WHERE goal_id = ? AND state = 'pending'
               ORDER BY created_at ASC"#,
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(rows.into_iter().map(|r| r.into_approval()).collect())
    }

    /// Cancel all pending approvals of a given type for a goal (e.g. when a
    /// newer plan revision supersedes an outstanding plan-approval request).
    /// Returns the number of approvals cancelled.
    pub async fn cancel_pending_approvals(
        &self,
        goal_id: &str,
        approval_type: &str,
        resolved_by: &str,
    ) -> Result<u64, CoreError> {
        let result = sqlx::query(
            r#"UPDATE approval_requests SET state = 'cancelled', resolved_at = ?, resolved_by = ?
               WHERE goal_id = ? AND approval_type = ? AND state = 'pending'"#,
        )
        .bind(now_sql())
        .bind(resolved_by)
        .bind(goal_id)
        .bind(approval_type)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(result.rows_affected())
    }

    // ── User Interventions (I8A) ────────────────────────────────────

    pub async fn insert_intervention(
        &self,
        intervention: &UserIntervention,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO user_interventions (intervention_id, goal_id, request_id,
               source, message, classification, state, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&intervention.intervention_id)
        .bind(&intervention.goal_id)
        .bind(&intervention.request_id)
        .bind(&intervention.source)
        .bind(&intervention.message)
        .bind(intervention.classification.as_str())
        .bind(intervention.state.as_str())
        .bind(now_sql())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// List interventions for a goal, optionally filtered by state,
    /// ordered by insertion order.
    pub async fn list_interventions(
        &self,
        goal_id: &str,
        state: Option<&str>,
    ) -> Result<Vec<UserIntervention>, CoreError> {
        let rows: Vec<InterventionRow> = match state {
            Some(s) => {
                sqlx::query_as(
                    r#"SELECT intervention_id, goal_id, request_id, source, message,
                       classification, state, created_at, processed_at, applied_plan_revision_id
                       FROM user_interventions WHERE goal_id = ? AND state = ?
                       ORDER BY rowid ASC"#,
                )
                .bind(goal_id)
                .bind(s)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query_as(
                    r#"SELECT intervention_id, goal_id, request_id, source, message,
                       classification, state, created_at, processed_at, applied_plan_revision_id
                       FROM user_interventions WHERE goal_id = ?
                       ORDER BY rowid ASC"#,
                )
                .bind(goal_id)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into_intervention()).collect())
    }

    /// Mark all `received` interventions for a goal as applied to a plan
    /// revision. Returns the number of interventions marked.
    pub async fn mark_interventions_applied(
        &self,
        goal_id: &str,
        plan_revision_id: &str,
    ) -> Result<u64, CoreError> {
        let result = sqlx::query(
            r#"UPDATE user_interventions
               SET state = 'applied', processed_at = ?, applied_plan_revision_id = ?
               WHERE goal_id = ? AND state = 'received'"#,
        )
        .bind(now_sql())
        .bind(plan_revision_id)
        .bind(goal_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(result.rows_affected())
    }

    /// Latest plan revision id for a goal (highest revision_number), used as
    /// the stale-approval guard reference.
    pub async fn get_latest_plan_revision_id(
        &self,
        goal_id: &str,
    ) -> Result<Option<String>, CoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"SELECT plan_revision_id FROM plan_revisions
               WHERE goal_id = ? ORDER BY revision_number DESC LIMIT 1"#,
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|r| r.0))
    }

    // ── Invocations ─────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_invocation(
        &self,
        invocation_id: &str,
        goal_id: &str,
        plan_revision_id: Option<&str>,
        kind: &str,
        profile_id: &str,
        idempotency_key: &str,
        input_digest: &str,
    ) -> Result<(), CoreError> {
        sqlx::query(
            r#"INSERT INTO planner_invocations (invocation_id, goal_id, plan_revision_id,
               invocation_kind, profile_id, idempotency_key, input_digest, state, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)"#,
        )
        .bind(invocation_id)
        .bind(goal_id)
        .bind(plan_revision_id)
        .bind(kind)
        .bind(profile_id)
        .bind(idempotency_key)
        .bind(input_digest)
        .bind(now_sql())
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // ── Observations ──────────────────────────────────────────────────

    /// List all observations for a goal, ordered by creation time.
    pub async fn list_goal_observations(
        &self,
        goal_id: &str,
    ) -> Result<Vec<GoalObservation>, CoreError> {
        let rows: Vec<GoalObservationRow> = sqlx::query_as(
            r#"SELECT observation_id, goal_id, plan_revision_id, planned_task_id,
               source_aggregate_type, source_aggregate_id, source_event_id, source_digest,
               repository_head, claim, evidence_type, created_at
               FROM goal_observations WHERE goal_id = ?
               ORDER BY created_at ASC"#,
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into_observation()).collect())
    }

    /// Update the materialized task and loop IDs for a planned task.
    pub async fn update_planned_task_materialization(
        &self,
        planned_task_id: &str,
        materialized_task_id: Option<&str>,
        materialized_loop_id: Option<&str>,
    ) -> Result<(), CoreError> {
        sqlx::query(
            "UPDATE planned_tasks SET materialized_task_id = ?, materialized_loop_id = ? WHERE planned_task_id = ?",
        )
        .bind(materialized_task_id)
        .bind(materialized_loop_id)
        .bind(planned_task_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    /// Get all planned tasks for a plan revision (all states).
    pub async fn get_all_planned_tasks(
        &self,
        plan_revision_id: &str,
    ) -> Result<Vec<PlannedTask>, CoreError> {
        let rows: Vec<PlannedTaskRow> = sqlx::query_as(
            r#"SELECT planned_task_id, plan_revision_id, milestone_id, client_ref, title,
               objective, acceptance_criteria_json, dependency_refs_json, expected_evidence_json,
               expected_resource_scope_json, risk_level, requires_approval, task_fingerprint,
               state, materialized_task_id, materialized_loop_id
               FROM planned_tasks
               WHERE plan_revision_id = ?
               ORDER BY client_ref ASC"#,
        )
        .bind(plan_revision_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows.into_iter().map(|r| r.into_planned_task()).collect())
    }

    // ── Events ──────────────────────────────────────────────────────

    pub async fn append_goal_event(
        &self,
        goal_id: &str,
        event_type: &str,
        payload_json: &str,
    ) -> Result<(), CoreError> {
        // Atomic sequence allocation: a single INSERT…SELECT avoids the
        // read-then-insert race when IPC handlers and the goal loop append
        // concurrently. UNIQUE (goal_id, sequence_num) (migration 030) makes
        // any residual collision a hard error instead of a silent duplicate,
        // and busy_timeout retries cover contention between writers.
        sqlx::query(
            r#"INSERT INTO goal_events (goal_id, event_type, payload_json, sequence_num)
               SELECT ?, ?, ?, COALESCE(MAX(sequence_num), 0) + 1
               FROM goal_events WHERE goal_id = ?"#,
        )
        .bind(goal_id)
        .bind(event_type)
        .bind(payload_json)
        .bind(goal_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn append_plan_event(
        &self,
        plan_revision_id: &str,
        goal_id: &str,
        event_type: &str,
        payload_json: &str,
    ) -> Result<(), CoreError> {
        let seq: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence_num), 0) + 1 FROM plan_events WHERE plan_revision_id = ?",
        )
        .bind(plan_revision_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;

        sqlx::query(
            r#"INSERT INTO plan_events (plan_revision_id, goal_id, event_type, payload_json, sequence_num)
               VALUES (?, ?, ?, ?, ?)"#,
        )
        .bind(plan_revision_id)
        .bind(goal_id)
        .bind(event_type)
        .bind(payload_json)
        .bind(seq)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }
}

// ── Row types for sqlx::query_as ────────────────────────────────────

#[allow(dead_code)]
#[derive(sqlx::FromRow)]
struct GoalRow {
    goal_id: String,
    revision: i64,
    title: String,
    objective: String,
    repository_id: String,
    target_ref: String,
    initial_base_head: String,
    state: String,
    budget_json: String,
    approval_policy_json: String,
    created_by_json: String,
    non_goals_json: String,
    created_at: String,
}

impl GoalRow {
    fn into_goal_spec(
        self,
        criteria: Vec<SuccessCriterion>,
        constraints: Vec<GoalConstraint>,
    ) -> GoalSpec {
        GoalSpec {
            goal_id: self.goal_id,
            revision: self.revision,
            title: self.title,
            objective: self.objective,
            repository_id: self.repository_id,
            target_ref: self.target_ref,
            initial_base_head: self.initial_base_head,
            success_criteria: criteria,
            constraints,
            non_goals: serde_json::from_str(&self.non_goals_json).unwrap_or_default(),
            budget: serde_json::from_str(&self.budget_json).unwrap_or_default(),
            approval_policy: serde_json::from_str(&self.approval_policy_json).unwrap_or_default(),
            created_by: serde_json::from_str(&self.created_by_json).unwrap_or(
                GoalCreator::System {
                    component: "unknown".into(),
                    reason: "deserialized".into(),
                },
            ),
            created_at: parse_dt(&self.created_at),
        }
    }
}

#[allow(dead_code)]
#[derive(sqlx::FromRow)]
struct CriterionRow {
    criterion_id: String,
    description: String,
    evidence_policy: String,
    evidence_policy_config: String,
    verification_policy: String,
    verification_policy_config: String,
    subjectivity: String,
    required: i32,
}

impl CriterionRow {
    fn into_criterion(self) -> SuccessCriterion {
        use harness_core::contracts::goal::{
            CriterionSubjectivity, EvidencePolicy, VerificationPolicy,
        };
        SuccessCriterion {
            criterion_id: self.criterion_id,
            description: self.description,
            evidence_policy: serde_json::from_str(&self.evidence_policy_config)
                .unwrap_or(EvidencePolicy::TaskTerminalResult),
            verification_policy: serde_json::from_str(&self.verification_policy_config)
                .unwrap_or(VerificationPolicy::ExistenceOnly),
            subjectivity: if self.subjectivity == "subjective" {
                CriterionSubjectivity::Subjective
            } else {
                CriterionSubjectivity::Objective
            },
            required: self.required != 0,
        }
    }
}

#[allow(dead_code)]
#[derive(sqlx::FromRow)]
struct ConstraintRow {
    constraint_id: String,
    description: String,
    constraint_type: String,
    constraint_config: String,
    blocking: i32,
}

impl ConstraintRow {
    fn into_constraint(self) -> GoalConstraint {
        use harness_core::contracts::goal::ConstraintType;
        GoalConstraint {
            constraint_id: self.constraint_id,
            description: self.description,
            constraint_type: serde_json::from_str(&self.constraint_config).unwrap_or(
                ConstraintType::Custom {
                    name: "unknown".into(),
                    spec: "{}".into(),
                },
            ),
            blocking: self.blocking != 0,
        }
    }
}

#[derive(sqlx::FromRow)]
struct PlanRow {
    plan_revision_id: String,
    goal_id: String,
    goal_revision: i64,
    revision_number: i64,
    base_repository_head: String,
    planner_profile_id: String,
    planner_invocation_id: String,
    proposal_digest: String,
    validation_digest: Option<String>,
    state: String,
    created_at: String,
    activated_at: Option<String>,
    superseded_at: Option<String>,
}

impl PlanRow {
    fn into_plan_revision(self) -> PlanRevision {
        PlanRevision {
            plan_revision_id: self.plan_revision_id,
            goal_id: self.goal_id,
            goal_revision: self.goal_revision,
            revision_number: self.revision_number,
            base_repository_head: self.base_repository_head,
            planner_profile_id: self.planner_profile_id,
            planner_invocation_id: self.planner_invocation_id,
            proposal_digest: self.proposal_digest,
            validation_digest: self.validation_digest,
            state: parse_plan_state(&self.state),
            created_at: parse_dt(&self.created_at),
            activated_at: self.activated_at.map(|s| parse_dt(&s)),
            superseded_at: self.superseded_at.map(|s| parse_dt(&s)),
        }
    }
}

#[derive(sqlx::FromRow)]
struct PlannedTaskRow {
    planned_task_id: String,
    plan_revision_id: String,
    milestone_id: String,
    client_ref: String,
    title: String,
    objective: String,
    acceptance_criteria_json: String,
    dependency_refs_json: String,
    expected_evidence_json: String,
    expected_resource_scope_json: String,
    risk_level: String,
    requires_approval: i32,
    task_fingerprint: String,
    state: String,
    materialized_task_id: Option<String>,
    materialized_loop_id: Option<String>,
}

impl PlannedTaskRow {
    fn into_planned_task(self) -> PlannedTask {
        PlannedTask {
            planned_task_id: self.planned_task_id,
            plan_revision_id: self.plan_revision_id,
            milestone_id: self.milestone_id,
            client_ref: self.client_ref,
            title: self.title,
            objective: self.objective,
            acceptance_criteria: serde_json::from_str(&self.acceptance_criteria_json)
                .unwrap_or_default(),
            dependency_refs: serde_json::from_str(&self.dependency_refs_json).unwrap_or_default(),
            expected_evidence: serde_json::from_str(&self.expected_evidence_json)
                .unwrap_or_default(),
            expected_resource_scope: serde_json::from_str(&self.expected_resource_scope_json)
                .unwrap_or_default(),
            risk_level: RiskLevel::parse(&self.risk_level).unwrap_or(RiskLevel::Low),
            requires_approval: self.requires_approval != 0,
            task_fingerprint: self.task_fingerprint,
            state: parse_planned_state(&self.state),
            materialized_task_id: self.materialized_task_id,
            materialized_loop_id: self.materialized_loop_id,
        }
    }
}

#[derive(sqlx::FromRow)]
struct GoalObservationRow {
    observation_id: String,
    goal_id: String,
    plan_revision_id: Option<String>,
    planned_task_id: Option<String>,
    source_aggregate_type: String,
    source_aggregate_id: String,
    source_event_id: String,
    source_digest: String,
    repository_head: String,
    claim: String,
    evidence_type: String,
    created_at: String,
}

impl GoalObservationRow {
    fn into_observation(self) -> GoalObservation {
        GoalObservation {
            observation_id: self.observation_id,
            goal_id: self.goal_id,
            plan_revision_id: self.plan_revision_id,
            planned_task_id: self.planned_task_id,
            source_aggregate_type: self.source_aggregate_type,
            source_aggregate_id: self.source_aggregate_id,
            source_event_id: self.source_event_id,
            source_digest: self.source_digest,
            repository_head: self.repository_head,
            claim: self.claim,
            evidence_type: self.evidence_type,
            created_at: parse_dt(&self.created_at),
        }
    }
}

#[derive(sqlx::FromRow)]
struct ApprovalRow {
    approval_id: String,
    goal_id: String,
    plan_revision_id: Option<String>,
    approval_type: String,
    requested_action_json: String,
    payload_digest: String,
    reason: String,
    state: String,
    created_at: String,
    resolved_at: Option<String>,
    resolved_by: Option<String>,
    response_json: Option<String>,
    request_id: Option<String>,
    source: String,
}

impl ApprovalRow {
    fn into_approval(self) -> ApprovalRequest {
        ApprovalRequest {
            approval_id: self.approval_id,
            goal_id: self.goal_id,
            plan_revision_id: self.plan_revision_id,
            approval_type: ApprovalType::parse(&self.approval_type)
                .unwrap_or(ApprovalType::ApproveInitialPlan),
            requested_action: serde_json::from_str(&self.requested_action_json).unwrap_or_default(),
            payload_digest: self.payload_digest,
            reason: self.reason,
            state: parse_approval_state(&self.state),
            created_at: parse_dt(&self.created_at),
            resolved_at: self.resolved_at.map(|s| parse_dt(&s)),
            resolved_by: self.resolved_by,
            response: self
                .response_json
                .and_then(|s| serde_json::from_str(&s).ok()),
            request_id: self.request_id,
            source: self.source,
        }
    }
}

#[derive(sqlx::FromRow)]
struct InterventionRow {
    intervention_id: String,
    goal_id: String,
    request_id: Option<String>,
    source: String,
    message: String,
    classification: String,
    state: String,
    created_at: String,
    processed_at: Option<String>,
    applied_plan_revision_id: Option<String>,
}

impl InterventionRow {
    fn into_intervention(self) -> UserIntervention {
        UserIntervention {
            intervention_id: self.intervention_id,
            goal_id: self.goal_id,
            request_id: self.request_id,
            source: self.source,
            message: self.message,
            classification: InterventionClassification::parse(&self.classification)
                .unwrap_or(InterventionClassification::ConstraintAddition),
            state: InterventionState::parse(&self.state).unwrap_or(InterventionState::Received),
            created_at: parse_dt(&self.created_at),
            processed_at: self.processed_at.map(|s| parse_dt(&s)),
            applied_plan_revision_id: self.applied_plan_revision_id,
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn now_sql() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_dt(s: &str) -> chrono::DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn db_err(e: sqlx::Error) -> CoreError {
    CoreError::new(
        ErrorCode::PersistenceError,
        e.to_string(),
        ErrorSource::System,
    )
}

fn evidence_policy_str(ep: &harness_core::contracts::goal::EvidencePolicy) -> &'static str {
    use harness_core::contracts::goal::EvidencePolicy::*;
    match ep {
        TaskTerminalResult => "task_terminal_result",
        VerificationCommand { .. } => "verification_command",
        ReviewDecision => "review_decision",
        CommitOid => "commit_oid",
        IntegratedTargetHead => "integrated_target_head",
        FileDigest { .. } => "file_digest",
        StructuredUserApproval => "structured_user_approval",
        ExternalEvidence { .. } => "external_evidence",
    }
}

fn verification_policy_str(vp: &harness_core::contracts::goal::VerificationPolicy) -> &'static str {
    use harness_core::contracts::goal::VerificationPolicy::*;
    match vp {
        ExistenceOnly => "existence_only",
        DigestMatch => "digest_match",
        DeterministicScript { .. } => "deterministic_script",
        HumanReview => "human_review",
    }
}

fn constraint_type_str(ct: &harness_core::contracts::goal::ConstraintType) -> &'static str {
    use harness_core::contracts::goal::ConstraintType::*;
    match ct {
        FileExclusion { .. } => "file_exclusion",
        ResourceLimit { .. } => "resource_limit",
        ToolExclusion { .. } => "tool_exclusion",
        BehaviorConstraint { .. } => "behavior_constraint",
        Custom { .. } => "custom",
    }
}

fn format_state(state: &GoalLoopRunState) -> &'static str {
    match state {
        GoalLoopRunState::Created => "created",
        GoalLoopRunState::Planning => "planning",
        GoalLoopRunState::ActivatingPlan => "activating_plan",
        GoalLoopRunState::SelectingWork => "selecting_work",
        GoalLoopRunState::DispatchingTasks => "dispatching_tasks",
        GoalLoopRunState::WaitingForResults => "waiting_for_results",
        GoalLoopRunState::CollectingEvidence => "collecting_evidence",
        GoalLoopRunState::AssessingProgress => "assessing_progress",
        GoalLoopRunState::Replanning => "replanning",
        GoalLoopRunState::WaitingForApproval => "waiting_for_approval",
        GoalLoopRunState::Paused => "paused",
        GoalLoopRunState::Completed => "completed",
        GoalLoopRunState::Blocked => "blocked",
        GoalLoopRunState::Failed => "failed",
        GoalLoopRunState::Cancelled => "cancelled",
    }
}

fn parse_plan_state(s: &str) -> PlanState {
    match s {
        "proposed" => PlanState::Proposed,
        "validating" => PlanState::Validating,
        "validated" => PlanState::Validated,
        "active" => PlanState::Active,
        "superseded" => PlanState::Superseded,
        "completed" => PlanState::Completed,
        "rejected" => PlanState::Rejected,
        "invalid" => PlanState::Invalid,
        "cancelled" => PlanState::Cancelled,
        _ => PlanState::Proposed,
    }
}

fn parse_planned_state(s: &str) -> PlannedTaskState {
    match s {
        "pending" => PlannedTaskState::Pending,
        "materialized" => PlannedTaskState::Materialized,
        "running" => PlannedTaskState::Running,
        "completed" => PlannedTaskState::Completed,
        "failed" => PlannedTaskState::Failed,
        "cancelled" => PlannedTaskState::Cancelled,
        "superseded" => PlannedTaskState::Superseded,
        _ => PlannedTaskState::Pending,
    }
}

fn parse_approval_state(s: &str) -> ApprovalState {
    match s {
        "pending" => ApprovalState::Pending,
        "approved" => ApprovalState::Approved,
        "rejected" => ApprovalState::Rejected,
        "expired" => ApprovalState::Expired,
        "cancelled" => ApprovalState::Cancelled,
        _ => ApprovalState::Pending,
    }
}
