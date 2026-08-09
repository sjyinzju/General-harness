//! I8A Human Interaction Protocol — production writer methods.
//!
//! All user-facing interaction mutations (clarification answers, plan
//! approvals, plan-change requests, interventions, pause/resume) go through
//! this impl block on `GoalLoopService`. The TUI/CLI never writes business
//! tables directly: TUI → IPC → Supervisor (Request Ledger) → these methods
//! → Repository.
//!
//! User natural-language input is DATA, never a command: interventions and
//! answers are stored and routed into planner context; nothing here ever
//! reaches a shell.

use harness_core::contracts::goal::GoalState;
use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::service::{get_goal_state, GoalLoopService};
use super::{
    ApprovalRequest, ApprovalType, ClarificationQuestion, InterventionClassification,
    InterventionState, PlanProposal, UserIntervention,
};
use harness_core::contracts::plan::{PlanRevision, PlanState};

/// Outcome of an idempotent interaction transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionOutcome {
    /// The transition was applied by this call.
    Applied,
    /// The target state already held — replay-safe no-op.
    AlreadyInState,
}

impl InteractionOutcome {
    pub fn applied(&self) -> bool {
        matches!(self, Self::Applied)
    }
}

impl GoalLoopService {
    // ── Clarification protocol ──────────────────────────────────────

    /// Park the goal in WaitingForApproval with a clarification request.
    /// Called by the goal loop when the Planner returns ClarificationNeeded.
    pub async fn request_clarification(
        &self,
        goal_id: &str,
        questions: &[ClarificationQuestion],
    ) -> Result<ApprovalRequest, CoreError> {
        if questions.is_empty() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "clarification request must carry at least one question",
                ErrorSource::Harness,
            ));
        }

        // Draft goals must pass through Planning before they can wait.
        if get_goal_state(&self.pool, goal_id).await? == GoalState::Draft {
            self.transition_goal(goal_id, GoalState::Planning).await?;
        }

        let approval = self
            .request_approval(
                goal_id,
                None,
                ApprovalType::ProvideMissingInformation,
                serde_json::json!({ "questions": questions }),
                "planner needs missing information before it can plan",
            )
            .await?;

        if get_goal_state(&self.pool, goal_id).await? != GoalState::WaitingForApproval {
            self.transition_goal(goal_id, GoalState::WaitingForApproval)
                .await?;
        }

        self.repo
            .append_goal_event(
                goal_id,
                "clarification_requested",
                &serde_json::json!({
                    "approval_id": approval.approval_id,
                    "question_count": questions.len(),
                })
                .to_string(),
            )
            .await?;

        Ok(approval)
    }

    /// Record the user's answers to a clarification request and return the
    /// goal to Planning. Replay-safe: an already-resolved approval returns
    /// AlreadyInState without side effects.
    pub async fn answer_clarification(
        &self,
        goal_id: &str,
        approval_id: &str,
        answers: &serde_json::Value,
        resolved_by: &str,
    ) -> Result<InteractionOutcome, CoreError> {
        let approval = self.repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                ErrorCode::NotFound,
                "approval request not found",
                ErrorSource::Harness,
            )
        })?;
        if approval.goal_id != goal_id {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval does not belong to this goal",
                ErrorSource::Harness,
            ));
        }
        if approval.approval_type != ApprovalType::ProvideMissingInformation {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval is not a clarification request",
                ErrorSource::Harness,
            ));
        }

        let response_json = serde_json::to_string(answers).unwrap_or_default();
        let transitioned = self
            .repo
            .resolve_approval_with_response(
                approval_id,
                "approved",
                resolved_by,
                Some(&response_json),
            )
            .await?;
        if !transitioned {
            // Already resolved — replay of a completed answer.
            return Ok(InteractionOutcome::AlreadyInState);
        }

        // WaitingForApproval → Planning so the loop replans with the answers.
        if get_goal_state(&self.pool, goal_id).await? == GoalState::WaitingForApproval {
            self.transition_goal(goal_id, GoalState::Planning).await?;
        }

        self.repo
            .append_goal_event(
                goal_id,
                "clarification_answered",
                &serde_json::json!({
                    "approval_id": approval_id,
                    "resolved_by": resolved_by,
                })
                .to_string(),
            )
            .await?;

        self.ensure_loop_run(goal_id).await;
        Ok(InteractionOutcome::Applied)
    }

    // ── Plan approval protocol ──────────────────────────────────────

    /// Create a plan-approval request bound to a concrete plan revision and
    /// park the goal in WaitingForApproval. Any older pending plan-approval
    /// requests are cancelled (superseded by this revision).
    pub async fn request_plan_approval(
        &self,
        goal_id: &str,
        plan: &PlanRevision,
        proposal: &PlanProposal,
    ) -> Result<ApprovalRequest, CoreError> {
        // Supersede older pending plan approvals for this goal.
        let _ = self
            .repo
            .cancel_pending_approvals(goal_id, "approve_initial_plan", "system:superseded")
            .await?;

        let tasks_summary: Vec<serde_json::Value> = proposal
            .tasks
            .iter()
            .map(|t| {
                serde_json::json!({
                    "client_ref": t.client_ref,
                    "title": t.title,
                    "objective": t.objective,
                    "dependencies": t.dependencies,
                    "risk_level": t.risk_level,
                    "requires_approval": t.requires_approval,
                })
            })
            .collect();

        if get_goal_state(&self.pool, goal_id).await? == GoalState::Draft {
            self.transition_goal(goal_id, GoalState::Planning).await?;
        }

        let approval = self
            .request_approval(
                goal_id,
                Some(&plan.plan_revision_id),
                ApprovalType::ApproveInitialPlan,
                serde_json::json!({
                    "goal_summary": proposal.goal_summary,
                    "revision_number": plan.revision_number,
                    "milestone_count": proposal.milestones.len(),
                    "task_count": proposal.tasks.len(),
                    "tasks": tasks_summary,
                }),
                "plan revision requires user approval before activation",
            )
            .await?;

        // Idempotent park: the goal may already be waiting (e.g. a newer
        // revision superseding a still-pending approval).
        if get_goal_state(&self.pool, goal_id).await? != GoalState::WaitingForApproval {
            self.transition_goal(goal_id, GoalState::WaitingForApproval)
                .await?;
        }

        self.repo
            .append_goal_event(
                goal_id,
                "plan_approval_requested",
                &serde_json::json!({
                    "approval_id": approval.approval_id,
                    "plan_revision_id": plan.plan_revision_id,
                    "revision_number": plan.revision_number,
                })
                .to_string(),
            )
            .await?;

        Ok(approval)
    }

    /// Approve a plan revision: stale-guarded activation + goal → Active.
    /// `expected_plan_revision_id` (when provided by the client) must match
    /// the approval's bound revision — a mismatch is a stale decision.
    pub async fn approve_plan(
        &self,
        goal_id: &str,
        approval_id: &str,
        resolved_by: &str,
        expected_plan_revision_id: Option<&str>,
    ) -> Result<InteractionOutcome, CoreError> {
        let approval = self.repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                ErrorCode::NotFound,
                "approval request not found",
                ErrorSource::Harness,
            )
        })?;
        if approval.goal_id != goal_id {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval does not belong to this goal",
                ErrorSource::Harness,
            ));
        }
        if approval.approval_type != ApprovalType::ApproveInitialPlan {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval is not a plan-approval request",
                ErrorSource::Harness,
            ));
        }
        let bound_revision = approval.plan_revision_id.clone().ok_or_else(|| {
            CoreError::new(
                ErrorCode::InvalidState,
                "plan-approval request has no bound plan revision",
                ErrorSource::Harness,
            )
        })?;

        // ── Stale guards ────────────────────────────────────────────
        if let Some(expected) = expected_plan_revision_id {
            if expected != bound_revision {
                return Err(CoreError::new(
                    ErrorCode::InvalidState,
                    format!(
                        "stale approval decision: expected revision {expected}, approval is bound to {bound_revision}"
                    ),
                    ErrorSource::Harness,
                ));
            }
        }
        let latest = self.repo.get_latest_plan_revision_id(goal_id).await?;
        if latest.as_deref() != Some(bound_revision.as_str()) {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                format!(
                    "stale approval: revision {bound_revision} has been superseded by a newer revision"
                ),
                ErrorSource::Harness,
            ));
        }

        let transitioned = self
            .repo
            .resolve_approval_with_response(
                approval_id,
                "approved",
                resolved_by,
                Some(&serde_json::json!({"decision": "approve"}).to_string()),
            )
            .await?;
        if !transitioned {
            return Ok(InteractionOutcome::AlreadyInState);
        }

        // Activate the Validated revision and resume the goal.
        self.activate_validated_plan(goal_id, &bound_revision)
            .await?;
        if get_goal_state(&self.pool, goal_id).await? == GoalState::WaitingForApproval {
            self.transition_goal(goal_id, GoalState::Active).await?;
        }

        self.repo
            .append_goal_event(
                goal_id,
                "plan_approved",
                &serde_json::json!({
                    "approval_id": approval_id,
                    "plan_revision_id": bound_revision,
                    "resolved_by": resolved_by,
                })
                .to_string(),
            )
            .await?;

        self.ensure_loop_run(goal_id).await;
        Ok(InteractionOutcome::Applied)
    }

    /// Reject a proposed plan with feedback: the revision goes to Rejected,
    /// the feedback becomes a plan_change_required intervention, and the goal
    /// returns to Planning for a new revision.
    pub async fn request_plan_changes(
        &self,
        goal_id: &str,
        approval_id: &str,
        feedback: &str,
        resolved_by: &str,
    ) -> Result<InteractionOutcome, CoreError> {
        let approval = self.repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                ErrorCode::NotFound,
                "approval request not found",
                ErrorSource::Harness,
            )
        })?;
        if approval.goal_id != goal_id {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval does not belong to this goal",
                ErrorSource::Harness,
            ));
        }
        if approval.approval_type != ApprovalType::ApproveInitialPlan {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval is not a plan-approval request",
                ErrorSource::Harness,
            ));
        }

        let transitioned = self
            .repo
            .resolve_approval_with_response(
                approval_id,
                "rejected",
                resolved_by,
                Some(
                    &serde_json::json!({"decision": "request_changes", "feedback": feedback})
                        .to_string(),
                ),
            )
            .await?;
        if !transitioned {
            return Ok(InteractionOutcome::AlreadyInState);
        }

        // The rejected revision is terminal — a replan creates a NEW revision.
        if let Some(ref revision_id) = approval.plan_revision_id {
            let _ = self
                .repo
                .update_plan_state(revision_id, PlanState::Rejected, None)
                .await;
        }

        // Feedback is planner input for the next revision.
        self.insert_intervention_row(
            goal_id,
            feedback,
            InterventionClassification::PlanChangeRequired,
            approval.request_id.as_deref(),
            "user",
        )
        .await?;

        if get_goal_state(&self.pool, goal_id).await? == GoalState::WaitingForApproval {
            self.transition_goal(goal_id, GoalState::Planning).await?;
        }

        self.repo
            .append_goal_event(
                goal_id,
                "plan_changes_requested",
                &serde_json::json!({
                    "approval_id": approval_id,
                    "plan_revision_id": approval.plan_revision_id,
                    "resolved_by": resolved_by,
                })
                .to_string(),
            )
            .await?;

        self.ensure_loop_run(goal_id).await;
        Ok(InteractionOutcome::Applied)
    }

    /// Terminally reject a proposed plan: the revision goes to Rejected and
    /// the goal is cancelled. Unlike `request_plan_changes` there is no
    /// replan — the user has decided the goal should not proceed.
    pub async fn reject_plan(
        &self,
        goal_id: &str,
        approval_id: &str,
        resolved_by: &str,
        expected_plan_revision_id: Option<&str>,
    ) -> Result<InteractionOutcome, CoreError> {
        let approval = self.repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                ErrorCode::NotFound,
                "approval request not found",
                ErrorSource::Harness,
            )
        })?;
        if approval.goal_id != goal_id {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval does not belong to this goal",
                ErrorSource::Harness,
            ));
        }
        if approval.approval_type != ApprovalType::ApproveInitialPlan {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "approval is not a plan-approval request",
                ErrorSource::Harness,
            ));
        }

        // Stale guard: a reject decision made against a superseded revision
        // must not silently kill the goal.
        if let (Some(expected), Some(bound)) = (
            expected_plan_revision_id,
            approval.plan_revision_id.as_deref(),
        ) {
            if expected != bound {
                return Err(CoreError::new(
                    ErrorCode::InvalidState,
                    format!(
                        "stale reject decision: expected revision {expected}, approval is bound to {bound}"
                    ),
                    ErrorSource::Harness,
                ));
            }
        }

        let transitioned = self
            .repo
            .resolve_approval_with_response(
                approval_id,
                "rejected",
                resolved_by,
                Some(&serde_json::json!({"decision": "reject"}).to_string()),
            )
            .await?;
        if !transitioned {
            return Ok(InteractionOutcome::AlreadyInState);
        }

        if let Some(ref revision_id) = approval.plan_revision_id {
            let _ = self
                .repo
                .update_plan_state(revision_id, PlanState::Rejected, None)
                .await;
        }

        if !get_goal_state(&self.pool, goal_id).await?.is_terminal() {
            self.transition_goal(goal_id, GoalState::Cancelled).await?;
        }

        self.repo
            .append_goal_event(
                goal_id,
                "plan_rejected",
                &serde_json::json!({
                    "approval_id": approval_id,
                    "plan_revision_id": approval.plan_revision_id,
                    "resolved_by": resolved_by,
                })
                .to_string(),
            )
            .await?;

        Ok(InteractionOutcome::Applied)
    }

    // ── User interventions ──────────────────────────────────────────

    /// Record a user→harness message. It never blocks progress by itself;
    /// it is consumed by the next planning iteration. I8A uses a
    /// deterministic classifier: every message defaults to
    /// constraint_addition (pause/cancel intents go through their dedicated
    /// commands, not free text).
    pub async fn record_intervention(
        &self,
        goal_id: &str,
        message: &str,
        request_id: Option<&str>,
        source: &str,
    ) -> Result<UserIntervention, CoreError> {
        if message.trim().is_empty() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "intervention message must not be empty",
                ErrorSource::Harness,
            ));
        }
        // Goal must exist and not be terminal.
        let state = get_goal_state(&self.pool, goal_id).await?;
        if state.is_terminal() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                "cannot record an intervention on a terminal goal",
                ErrorSource::Harness,
            ));
        }

        self.insert_intervention_row(
            goal_id,
            message,
            InterventionClassification::ConstraintAddition,
            request_id,
            source,
        )
        .await
    }

    async fn insert_intervention_row(
        &self,
        goal_id: &str,
        message: &str,
        classification: InterventionClassification,
        request_id: Option<&str>,
        source: &str,
    ) -> Result<UserIntervention, CoreError> {
        let intervention = UserIntervention {
            intervention_id: format!("uiv-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            request_id: request_id.map(|s| s.to_string()),
            source: source.to_string(),
            message: message.to_string(),
            classification,
            state: InterventionState::Received,
            created_at: chrono::Utc::now(),
            processed_at: None,
            applied_plan_revision_id: None,
        };
        self.repo.insert_intervention(&intervention).await?;
        self.repo
            .append_goal_event(
                goal_id,
                "user_intervention_recorded",
                &serde_json::json!({
                    "intervention_id": intervention.intervention_id,
                    "classification": classification.as_str(),
                })
                .to_string(),
            )
            .await?;
        Ok(intervention)
    }

    // ── Pause / Resume ──────────────────────────────────────────────

    /// Pause a goal. Idempotent: pausing an already-paused goal is a no-op.
    /// Only Active and Blocked goals can pause (FSM); other states error.
    pub async fn pause_goal(&self, goal_id: &str) -> Result<InteractionOutcome, CoreError> {
        let state = get_goal_state(&self.pool, goal_id).await?;
        match state {
            GoalState::Paused => Ok(InteractionOutcome::AlreadyInState),
            GoalState::Active | GoalState::Blocked => {
                self.transition_goal(goal_id, GoalState::Paused).await?;
                self.repo
                    .append_goal_event(
                        goal_id,
                        "goal_paused",
                        &serde_json::json!({"from": format!("{state:?}").to_lowercase()})
                            .to_string(),
                    )
                    .await?;
                Ok(InteractionOutcome::Applied)
            }
            other => Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: other.as_str().to_string(),
                    to: GoalState::Paused.as_str().to_string(),
                },
                "goal cannot be paused in its current state",
                ErrorSource::Harness,
            )),
        }
    }

    /// Resume a paused goal. Idempotent: resuming an Active goal is a no-op.
    pub async fn resume_goal(&self, goal_id: &str) -> Result<InteractionOutcome, CoreError> {
        let state = get_goal_state(&self.pool, goal_id).await?;
        match state {
            GoalState::Active => Ok(InteractionOutcome::AlreadyInState),
            GoalState::Paused => {
                self.transition_goal(goal_id, GoalState::Active).await?;
                self.repo
                    .append_goal_event(goal_id, "goal_resumed", "{}")
                    .await?;
                self.ensure_loop_run(goal_id).await;
                Ok(InteractionOutcome::Applied)
            }
            other => Err(CoreError::new(
                ErrorCode::InvalidStateTransition {
                    from: other.as_str().to_string(),
                    to: GoalState::Active.as_str().to_string(),
                },
                "goal cannot be resumed in its current state",
                ErrorSource::Harness,
            )),
        }
    }

    // ── Loop restart ────────────────────────────────────────────────

    /// (Re)start the background goal loop after an interaction resolution.
    /// The loop self-terminates while a goal waits for a human, so every
    /// resolution that returns the goal to Planning/Active restarts it.
    /// Safe: at most one active run per goal (partial unique index), and
    /// dispatch is fingerprint/saga-idempotent.
    pub async fn ensure_loop_run(&self, goal_id: &str) {
        if let Err(e) = self.start_loop_run(goal_id).await {
            tracing::warn!(
                goal_id = %goal_id,
                error = %e,
                "failed to restart goal loop after interaction resolution"
            );
        }
    }
}
