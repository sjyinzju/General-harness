//! Goal module — I7 Goal Loop: durable goal persistence, planning, orchestration,
//! evidence collection, progress assessment, replanning, and completion gating.
//!
//! Reuses existing I4.5 (TaskEngineeringLoop), I4.6 (Review), I5 (Commit/Integration),
//! and I6 (Supervisor/OperationIntent) production paths.

pub mod evaluator;
pub mod failpoint;
pub mod interaction;
pub mod planner;
pub mod repo;
pub mod service;
pub mod validation;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use harness_core::contracts::agent_adapter::AgentAdapter;
use harness_core::contracts::goal::GoalId;
use harness_core::contracts::runtime_profile::RuntimeProfile;
use serde::{Deserialize, Serialize};

/// Default schema version for structured LLM output when the LLM omits it.
fn default_schema_version() -> String {
    "1.0".to_string()
}

// ── Plan Proposal (from Planner LLM) ──────────────────────────────────

/// Structured output that the Planner LLM must produce.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanProposal {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub goal_summary: String,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub milestones: Vec<ProposedMilestone>,
    #[serde(default)]
    pub tasks: Vec<ProposedTask>,
    #[serde(default)]
    pub risks: Vec<ProposedRisk>,
    #[serde(default)]
    pub completion_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedMilestone {
    #[serde(default)]
    pub client_ref: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
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
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    pub overall_assessment: String,
    pub criteria_assessments: Vec<CriterionAssessment>,
    pub plan_sufficient: bool,
    pub replan_recommended: bool,
    pub completion_recommended: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Human-readable recommendation (advisory — Rust CompletionPolicy is authoritative).
    /// Mirrors the schema's `recommendation` enum field.
    #[serde(default)]
    pub recommendation: String,
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

/// Outcome of an `import_observation` call.
///
/// `Created(id)` — a new observation was inserted.
/// `AlreadyExists(id)` — the observation already existed (idempotent duplicate,
///   rejected by the UNIQUE index on source_aggregate_type/source_aggregate_id/
///   source_event_id).
#[derive(Debug, Clone)]
pub enum ObservationOutcome {
    Created(String),
    AlreadyExists(String),
}

impl std::fmt::Display for ObservationOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created(id) => write!(f, "created({id})"),
            Self::AlreadyExists(id) => write!(f, "already_exists({id})"),
        }
    }
}

impl ObservationOutcome {
    pub fn observation_id(&self) -> &str {
        match self {
            Self::Created(id) | Self::AlreadyExists(id) => id,
        }
    }

    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }
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
    /// User's answer or decision detail (I8A, migration 030).
    #[serde(default)]
    pub response: Option<serde_json::Value>,
    /// Originating IPC request id, when created via IPC.
    #[serde(default)]
    pub request_id: Option<String>,
    /// Who created the request: "system" | "user" | "ipc".
    #[serde(default = "default_approval_source")]
    pub source: String,
}

fn default_approval_source() -> String {
    "system".to_string()
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

// ── User Intervention (I8A) ────────────────────────────────────────────

/// A user→harness message that does not block progress by itself.
/// Stored durably and consumed by future planning iterations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIntervention {
    pub intervention_id: String,
    pub goal_id: GoalId,
    pub request_id: Option<String>,
    pub source: String,
    pub message: String,
    pub classification: InterventionClassification,
    pub state: InterventionState,
    pub created_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub applied_plan_revision_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionClassification {
    Informational,
    ConstraintAddition,
    PlanChangeRequired,
    PauseRequested,
    CancelRequested,
}

impl InterventionClassification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Informational => "informational",
            Self::ConstraintAddition => "constraint_addition",
            Self::PlanChangeRequired => "plan_change_required",
            Self::PauseRequested => "pause_requested",
            Self::CancelRequested => "cancel_requested",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "informational" => Some(Self::Informational),
            "constraint_addition" => Some(Self::ConstraintAddition),
            "plan_change_required" => Some(Self::PlanChangeRequired),
            "pause_requested" => Some(Self::PauseRequested),
            "cancel_requested" => Some(Self::CancelRequested),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionState {
    Received,
    Applied,
    Superseded,
}

impl InterventionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Applied => "applied",
            Self::Superseded => "superseded",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "received" => Some(Self::Received),
            "applied" => Some(Self::Applied),
            "superseded" => Some(Self::Superseded),
            _ => None,
        }
    }
}

// ── Planner Outcome (I8A) ──────────────────────────────────────────────

/// One clarifying question the Planner asks the user before it can plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClarificationQuestion {
    #[serde(default)]
    pub question_id: String,
    pub prompt: String,
    #[serde(default)]
    pub choices: Vec<String>,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub reason: String,
}

fn default_true() -> bool {
    true
}

/// Structured Planner result: either a full plan proposal or a request for
/// missing information (interactive mode only).
#[derive(Debug, Clone)]
pub enum PlannerOutcome {
    Plan(Box<PlanProposal>),
    ClarificationNeeded(Vec<ClarificationQuestion>),
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

// ── Profile Separation ──────────────────────────────────────────────────

/// Role isolation policy — governs which profile separation rules apply.
///
/// # Production default
///
/// `IsolatedSessions` — a single operational RuntimeProfile can drive all four
/// roles (Planner, Executor, Reviewer, Evaluator) provided each role runs in a
/// *fresh, independent* Agent session with role-appropriate permissions and
/// context isolation.
///
/// # Optional high-assurance mode
///
/// `StrictProfileDiversity` — the legacy rule requiring different profiles for
/// Planner/Evaluator and Executor/Reviewer. Only usable when two or more
/// operational profiles are available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RoleIsolationPolicy {
    /// Default: same profile allowed; distinct sessions required.
    #[default]
    IsolatedSessions,
    /// High-assurance: different profiles required for Planner/Evaluator
    /// and Executor/Reviewer.
    StrictProfileDiversity,
}

impl RoleIsolationPolicy {
    /// Human-readable label.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IsolatedSessions => "isolated_sessions",
            Self::StrictProfileDiversity => "strict_profile_diversity",
        }
    }
}

/// Structured error when profile separation rules are violated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSeparationError {
    pub role_a: String,
    pub profile_a: String,
    pub role_b: String,
    pub profile_b: String,
    pub goal_id: Option<String>,
    pub message: String,
}

impl std::fmt::Display for ProfileSeparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProfileSeparationViolation: {} (role={}) and {} (role={}) must use different profiles{}. {}",
            self.profile_a,
            self.role_a,
            self.profile_b,
            self.role_b,
            self.goal_id
                .as_ref()
                .map(|id| format!(" for goal {id}"))
                .unwrap_or_default(),
            self.message,
        )
    }
}

/// Runtime configuration for goal services, enforcing profile separation.
#[derive(Debug, Clone)]
pub struct GoalRuntimeConfig {
    /// Which isolation policy governs this goal run.
    pub role_isolation_policy: RoleIsolationPolicy,
    pub planner_profile_id: String,
    pub evaluator_profile_id: String,
    pub executor_profile_ids: Vec<String>,
    pub reviewer_profile_ids: Vec<String>,
}

impl GoalRuntimeConfig {
    /// Validate profile separation rules according to the active policy.
    ///
    /// - `IsolatedSessions`: checks that at least one profile is configured;
    ///   same-profile is allowed. Session isolation is enforced at runtime.
    /// - `StrictProfileDiversity`: Planner != Evaluator AND Executor ∩ Reviewer = ∅.
    pub fn validate(&self, goal_id: Option<&str>) -> Result<(), Box<ProfileSeparationError>> {
        match self.role_isolation_policy {
            RoleIsolationPolicy::IsolatedSessions => {
                // At least one operational profile must be configured
                if self.planner_profile_id.is_empty() || self.evaluator_profile_id.is_empty() {
                    return Err(Box::new(ProfileSeparationError {
                        role_a: "GoalPlanner".into(),
                        profile_a: self.planner_profile_id.clone(),
                        role_b: "GoalEvaluator".into(),
                        profile_b: self.evaluator_profile_id.clone(),
                        goal_id: goal_id.map(|s| s.to_string()),
                        message: "At least one operational RuntimeProfile is required for IsolatedSessions mode".into(),
                    }));
                }
                // Same profile is acceptable under IsolatedSessions
                Ok(())
            }
            RoleIsolationPolicy::StrictProfileDiversity => {
                // R3a: Planner != Evaluator
                if self.planner_profile_id == self.evaluator_profile_id {
                    return Err(Box::new(ProfileSeparationError {
                        role_a: "GoalPlanner".into(),
                        profile_a: self.planner_profile_id.clone(),
                        role_b: "GoalEvaluator".into(),
                        profile_b: self.evaluator_profile_id.clone(),
                        goal_id: goal_id.map(|s| s.to_string()),
                        message: "Planner and Evaluator must use different profiles under StrictProfileDiversity".into(),
                    }));
                }

                // R3b: Executor != Reviewer (any overlap)
                let exec_set: std::collections::HashSet<&str> = self
                    .executor_profile_ids
                    .iter()
                    .map(|s| s.as_str())
                    .collect();
                for rp in &self.reviewer_profile_ids {
                    if exec_set.contains(rp.as_str()) {
                        return Err(Box::new(ProfileSeparationError {
                            role_a: "TaskExecutor".into(),
                            profile_a: rp.clone(),
                            role_b: "TaskReviewer".into(),
                            profile_b: rp.clone(),
                            goal_id: goal_id.map(|s| s.to_string()),
                            message: format!(
                                "profile '{}' cannot be used for both Executor and Reviewer roles under StrictProfileDiversity",
                                rp
                            ),
                        }));
                    }
                }

                Ok(())
            }
        }
    }

    /// Check if profile separation is satisfied without returning an error.
    pub fn is_separated(&self) -> bool {
        self.validate(None).is_ok()
    }

    /// Returns true if the config uses IsolatedSessions (single-profile OK).
    pub fn is_isolated_sessions(&self) -> bool {
        self.role_isolation_policy == RoleIsolationPolicy::IsolatedSessions
    }

    /// Returns true if the environment supports StrictProfileDiversity
    /// (i.e., at least two distinct profiles are configured).
    pub fn strict_diversity_operational(&self) -> bool {
        let profiles: std::collections::HashSet<&str> = std::collections::HashSet::from_iter([
            self.planner_profile_id.as_str(),
            self.evaluator_profile_id.as_str(),
        ]);
        profiles.len() >= 2
    }
}

// ── Invocation Record ───────────────────────────────────────────────────

/// Durable record of a real Agent invocation for a goal role.
///
/// Records session provenance: each invocation gets a unique
/// `harness_session_id` and `invocation_id`, with `session_mode = fresh`
/// and `resume_requested = false` — proving session independence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleInvocation {
    /// Unique invocation identifier (one per role call).
    pub invocation_id: String,
    /// Role: GoalPlanner, GoalEvaluator, TaskExecutor, TaskReviewer, Certification.
    pub role: String,
    /// RuntimeProfile ID used for this invocation.
    pub profile_id: String,
    /// Adapter kind (claude-cli, codex-cli, etc.).
    pub adapter_kind: String,
    /// Path to the agent binary.
    pub binary_path: String,
    /// Agent version string.
    pub binary_version: String,
    /// SHA-256 of the input context.
    pub input_digest: String,
    /// SHA-256 of the rendered prompt.
    pub prompt_digest: String,
    /// SHA-256 of the output (None if failed).
    pub output_digest: Option<String>,

    // ── Session provenance (RC-C) ──────────────────────────
    /// Harness-assigned session ID (distinct from vendor session_id).
    /// Always unique per invocation; never resumed.
    pub harness_session_id: String,
    /// Vendor/provider session ID (e.g., Claude's session_id).
    /// May be None if the provider doesn't expose it.
    pub vendor_session_id: Option<String>,
    /// Session mode: "fresh" or "resume".
    /// Must be "fresh" for all acceptance invocations.
    pub session_mode: String,
    /// Whether resume was requested (must be false for acceptance).
    pub resume_requested: bool,
    /// OS process identity (pid + start time fingerprint).
    pub process_identity: String,

    /// When the invocation started.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When the invocation completed (None if still running/crashed).
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Terminal state: "completed", "failed", "timeout", "cancelled".
    pub terminal_state: Option<String>,
}

// ── RoleRuntimeRouter ───────────────────────────────────────────────────

/// Typed runtime routing for the four production roles.
///
/// Each role routes through the production `AgentAdapter` abstraction.
/// The ONLY difference between deterministic and real runtime is the
/// Adapter implementation — there are NOT two separate business state
/// machines or orchestration chains.
///
/// # Role routing
///
/// | Role     | Adapter       | Profile         | Session Mode |
/// |----------|---------------|-----------------|--------------|
/// | Planner  | AgentAdapter  | planner_profile | fresh        |
/// | Executor | AgentAdapter  | executor_profile| fresh        |
/// | Reviewer | AgentAdapter  | reviewer_profile| fresh        |
/// | Evaluator| AgentAdapter  | evaluator_profile| fresh       |
///
/// All four roles use `session_mode = fresh` with `resume_requested = false`.
#[derive(Clone)]
pub struct RoleRuntimeRouter {
    /// Adapter shared across all roles (fresh session per invocation).
    pub adapter: Arc<dyn AgentAdapter>,
    /// Profile for the Planner role.
    pub planner_profile: RuntimeProfile,
    /// Profile for the Executor role.
    pub executor_profile: RuntimeProfile,
    /// Profile for the Reviewer role.
    pub reviewer_profile: RuntimeProfile,
    /// Profile for the Evaluator role.
    pub evaluator_profile: RuntimeProfile,
}

impl std::fmt::Debug for RoleRuntimeRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoleRuntimeRouter")
            .field("adapter_kind", &self.adapter.kind())
            .field("planner_profile", &self.planner_profile.id)
            .field("executor_profile", &self.executor_profile.id)
            .field("reviewer_profile", &self.reviewer_profile.id)
            .field("evaluator_profile", &self.evaluator_profile.id)
            .finish()
    }
}

impl RoleRuntimeRouter {
    /// Create a new router with distinct profiles per role.
    /// All four roles share the same adapter but use independent sessions.
    pub fn new(
        adapter: Arc<dyn AgentAdapter>,
        planner_profile: RuntimeProfile,
        executor_profile: RuntimeProfile,
        reviewer_profile: RuntimeProfile,
        evaluator_profile: RuntimeProfile,
    ) -> Self {
        Self {
            adapter,
            planner_profile,
            executor_profile,
            reviewer_profile,
            evaluator_profile,
        }
    }

    /// Create a single-profile router (all roles share one profile).
    /// Sessions are still independent — this is the IsolatedSessions policy.
    pub fn single_profile(adapter: Arc<dyn AgentAdapter>, profile: RuntimeProfile) -> Self {
        Self {
            adapter,
            planner_profile: profile.clone(),
            executor_profile: profile.clone(),
            reviewer_profile: profile.clone(),
            evaluator_profile: profile,
        }
    }

    /// Check if this router satisfies StrictProfileDiversity (all four profiles distinct).
    pub fn is_strictly_diverse(&self) -> bool {
        let ids = [
            &self.planner_profile.id,
            &self.executor_profile.id,
            &self.reviewer_profile.id,
            &self.evaluator_profile.id,
        ];
        let set: std::collections::HashSet<_> = ids.iter().collect();
        set.len() == ids.len()
    }
}
