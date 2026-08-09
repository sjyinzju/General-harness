//! Presentation DTOs for the goal interaction surface (I8A).
//!
//! These are pure projection types consumed by IPC clients (CLI today,
//! TUI in I8B). They carry ZERO I/O dependencies and never expose
//! repository or scheduler internals — only what a renderer needs.
//!
//! Client contract: fetch `GoalSnapshot` (authoritative at
//! `last_event_sequence`), then fold `PresentationEvent`s obtained from
//! `goal.events(after_sequence = last_event_sequence)`.

use serde::{Deserialize, Serialize};

/// Goal header fields of a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotGoal {
    pub goal_id: String,
    pub revision: i64,
    pub title: String,
    pub objective: String,
    /// Goal FSM state string (`GoalState::as_str` values).
    pub state: String,
    /// Budget as stored (opaque to the presentation layer).
    pub budget: serde_json::Value,
    /// Approval policy as stored.
    pub approval_policy: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

/// A plan revision reference inside a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPlan {
    pub plan_revision_id: String,
    pub revision_number: i64,
    pub state: String,
}

/// A planned task projection inside a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTask {
    pub planned_task_id: String,
    pub milestone_id: String,
    pub client_ref: String,
    pub title: String,
    pub state: String,
    /// Client refs of dependency tasks.
    pub dependencies: Vec<String>,
    pub risk_level: String,
    pub requires_approval: bool,
    /// Verification strategy: what evidence this task must produce.
    pub expected_evidence: Vec<String>,
    /// Set once the planned task is materialized as a real Task.
    pub materialized_task_id: Option<String>,
    pub materialized_loop_id: Option<String>,
}

/// A pending interaction (clarification or plan approval) awaiting the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingInteraction {
    pub approval_id: String,
    /// Approval type string (e.g. "approve_initial_plan",
    /// "provide_missing_information").
    pub kind: String,
    /// Bound plan revision, when the interaction concerns a plan.
    pub plan_revision_id: Option<String>,
    pub reason: String,
    /// Questions or plan summary — the payload the user must act on.
    pub requested_action: serde_json::Value,
    pub created_at: String,
}

/// A recorded user intervention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotIntervention {
    pub intervention_id: String,
    pub message: String,
    pub classification: String,
    pub state: String,
    pub created_at: String,
    pub applied_plan_revision_id: Option<String>,
}

/// An active goal loop run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningActivity {
    pub run_id: String,
    pub state: String,
    pub iteration_number: i64,
    pub plan_revision_id: Option<String>,
}

/// Optional usage totals. Provider-absent metrics stay `None` — never
/// fabricated.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageTotals {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub tool_calls: Option<i64>,
    pub wall_time_ms: Option<i64>,
    pub estimated_cost_micros: Option<i64>,
}

/// Usage grouped by runtime profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileUsage {
    pub profile_id: String,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub totals: UsageTotals,
}

/// Boundary-only usage projection from the task usage ledger.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageSummary {
    pub totals: UsageTotals,
    /// AND over all ledger rows; absent rows → false.
    pub usage_known: bool,
    /// Distinct usage sources observed
    /// (`provider_reported` | `estimated` | `unknown`).
    pub sources: Vec<String>,
    pub per_profile: Vec<ProfileUsage>,
}

/// Full goal state projection returned by `goal.snapshot`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalSnapshot {
    pub goal: SnapshotGoal,
    /// The currently active plan revision, if any.
    pub active_plan: Option<SnapshotPlan>,
    /// The newest plan revision regardless of state (approval target).
    pub latest_plan: Option<SnapshotPlan>,
    /// Tasks of the active (or latest) plan revision.
    pub tasks: Vec<SnapshotTask>,
    pub pending_interactions: Vec<PendingInteraction>,
    /// Recent interventions, newest first.
    pub interventions: Vec<SnapshotIntervention>,
    pub running_activities: Vec<RunningActivity>,
    pub usage: UsageSummary,
    /// Resume cursor for `goal.events(after_sequence = …)`.
    pub last_event_sequence: i64,
}

/// A single goal event projected for presentation.
///
/// Sequence numbers are per-goal, gapless-monotonic; events are never
/// mutated, so folding them over a snapshot is deterministic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresentationEvent {
    pub sequence: i64,
    pub goal_id: String,
    pub event_type: String,
    pub occurred_at: String,
    pub payload: serde_json::Value,
}
