//! Goal module — I7 Goal Loop: durable goal persistence, planning, orchestration,
//! evidence collection, progress assessment, replanning, and completion gating.
//!
//! Reuses existing I4.5 (TaskEngineeringLoop), I4.6 (Review), I5 (Commit/Integration),
//! and I6 (Supervisor/OperationIntent) production paths.

pub mod repo;
pub mod service;
pub mod validation;

use chrono::{DateTime, Utc};
use harness_core::contracts::goal::GoalId;
use serde::{Deserialize, Serialize};

// ── Plan Proposal (from Planner LLM) ──────────────────────────────────

/// Structured output that the Planner LLM must produce.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanProposal {
    pub schema_version: String,
    pub goal_summary: String,
    pub assumptions: Vec<String>,
    pub milestones: Vec<ProposedMilestone>,
    pub tasks: Vec<ProposedTask>,
    pub risks: Vec<ProposedRisk>,
    pub completion_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedMilestone {
    pub client_ref: String,
    pub title: String,
    pub objective: String,
    #[serde(default)]
    pub success_criteria_refs: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedTask {
    pub client_ref: String,
    pub milestone_ref: String,
    pub title: String,
    pub objective: String,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub expected_evidence: Vec<String>,
    #[serde(default)]
    pub expected_resource_scope: Vec<String>,
    #[serde(default)]
    pub risk_level: String,
    #[serde(default)]
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedRisk {
    pub description: String,
    pub severity: String,
    pub mitigation: String,
}

// ── Progress Assessment ───────────────────────────────────────────────

/// Structured output from the GoalEvaluator LLM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressAssessmentProposal {
    pub schema_version: String,
    pub overall_assessment: String,
    pub criteria_assessments: Vec<CriterionAssessment>,
    pub plan_sufficient: bool,
    pub replan_recommended: bool,
    pub completion_recommended: bool,
    pub blockers: Vec<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CriterionAssessment {
    pub criterion_id: String,
    pub status: CriterionStatus,
    pub evidence_refs: Vec<String>,
    pub reason: String,
    #[serde(default)]
    pub confidence: f64,
    #[serde(default)]
    pub requires_human_confirmation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriterionStatus {
    Satisfied,
    PartiallySatisfied,
    Unsatisfied,
    Unknown,
    Blocked,
}

// ── GoalObservation ────────────────────────────────────────────────────

/// A durable observation bound to a specific source event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalObservation {
    pub observation_id: String,
    pub goal_id: GoalId,
    pub plan_revision_id: Option<String>,
    pub planned_task_id: Option<String>,
    pub source_aggregate_type: String,
    pub source_aggregate_id: String,
    pub source_event_id: String,
    pub source_digest: String,
    pub repository_head: String,
    pub claim: String,
    pub evidence_type: String,
    pub created_at: DateTime<Utc>,
}

// ── GoalLoopRun ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalLoopRunState {
    Created,
    Planning,
    ActivatingPlan,
    SelectingWork,
    DispatchingTasks,
    WaitingForResults,
    CollectingEvidence,
    AssessingProgress,
    Replanning,
    WaitingForApproval,
    Paused,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

impl GoalLoopRunState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::Planning
                | Self::ActivatingPlan
                | Self::SelectingWork
                | Self::DispatchingTasks
                | Self::WaitingForResults
                | Self::CollectingEvidence
                | Self::AssessingProgress
                | Self::Replanning
                | Self::WaitingForApproval
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalLoopRun {
    pub run_id: String,
    pub goal_id: GoalId,
    pub plan_revision_id: Option<String>,
    pub state: GoalLoopRunState,
    pub iteration_number: i64,
    pub tasks_dispatched_this_run: i64,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

// ── ReplanDecision ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplanDecision {
    ContinueCurrentPlan,
    CreatePlanRevision,
    WaitForApproval,
    Pause,
    Block,
    RecommendCompletion,
    FailGoal,
}

// ── ApprovalRequest ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub approval_id: String,
    pub goal_id: GoalId,
    pub plan_revision_id: Option<String>,
    pub approval_type: ApprovalType,
    pub requested_action: serde_json::Value,
    pub payload_digest: String,
    pub reason: String,
    pub state: ApprovalState,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalType {
    ApproveInitialPlan,
    ApproveHighRiskTask,
    ApproveScopeChange,
    ApproveBudgetIncrease,
    ProvideMissingInformation,
    ApproveGoalCompletion,
    ApproveResumeAfterNoProgress,
}

impl ApprovalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ApproveInitialPlan => "approve_initial_plan",
            Self::ApproveHighRiskTask => "approve_high_risk_task",
            Self::ApproveScopeChange => "approve_scope_change",
            Self::ApproveBudgetIncrease => "approve_budget_increase",
            Self::ProvideMissingInformation => "provide_missing_information",
            Self::ApproveGoalCompletion => "approve_goal_completion",
            Self::ApproveResumeAfterNoProgress => "approve_resume_after_no_progress",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "approve_initial_plan" => Some(Self::ApproveInitialPlan),
            "approve_high_risk_task" => Some(Self::ApproveHighRiskTask),
            "approve_scope_change" => Some(Self::ApproveScopeChange),
            "approve_budget_increase" => Some(Self::ApproveBudgetIncrease),
            "provide_missing_information" => Some(Self::ProvideMissingInformation),
            "approve_goal_completion" => Some(Self::ApproveGoalCompletion),
            "approve_resume_after_no_progress" => Some(Self::ApproveResumeAfterNoProgress),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    Pending,
    Approved,
    Rejected,
    Expired,
    Cancelled,
}

impl ApprovalState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Approved | Self::Rejected | Self::Expired | Self::Cancelled
        )
    }
}

// ── Replan Trigger ─────────────────────────────────────────────────────

/// Deterministic triggers for replanning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplanTrigger {
    TaskFailed { task_id: String, reason: String },
    TaskBlocked { task_id: String, reason: String },
    IntegrationConflict { integration_id: String },
    CandidateStale { candidate_id: String },
    TargetHeadAdvanced { old_head: String, new_head: String },
    ConsecutiveFailures { count: u32 },
    NoProgress { iterations: u32 },
    PlanInvalidated { reason: String },
    UserRequestedReplan { reason: String },
    EvaluatorRecommendation { reason: String },
}

// ── Completion Gate ────────────────────────────────────────────────────

/// Result of running the Completion Gate checks.
#[derive(Debug, Clone)]
pub struct CompletionGateResult {
    pub can_complete: bool,
    pub blocking_reasons: Vec<String>,
    pub criteria_results: Vec<CriterionCompletionStatus>,
    pub requires_human_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CriterionCompletionStatus {
    pub criterion_id: String,
    pub satisfied: bool,
    pub evidence_refs: Vec<String>,
    pub missing_evidence: Vec<String>,
}
