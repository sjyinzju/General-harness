//! Plan — PlanRevision, Milestone, PlannedTask, and DAG types.
//!
//! I7.1: Plans are proposed by a Planner (LLM), validated by Rust, and
//! activated by the Supervisor. Each replan creates a new immutable
//! PlanRevision — old revisions are NEVER overwritten.
//!
//! All types are pure data. No I/O, no Agent dependencies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::goal::GoalId;

// ── Typed IDs ──────────────────────────────────────────────────────────

pub type PlanRevisionId = String;
pub type MilestoneId = String;
pub type PlannedTaskId = String;

// ── Plan State ─────────────────────────────────────────────────────────

/// PlanRevision lifecycle.
///
/// Terminal: Superseded, Completed, Rejected, Invalid, Cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanState {
    /// Plan proposed by Planner, not yet validated.
    Proposed,
    /// Rust validator is running.
    Validating,
    /// Validation passed; ready to activate.
    Validated,
    /// Plan is active — tasks may be materialized.
    Active,
    // ── Terminal ──
    /// A newer PlanRevision has replaced this one.
    Superseded,
    /// All milestones complete → Plan completed.
    Completed,
    /// Plan was rejected (by validator or user).
    Rejected,
    /// Validation failed permanently.
    Invalid,
    /// Plan was explicitly cancelled.
    Cancelled,
}

impl PlanState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Superseded | Self::Completed | Self::Rejected | Self::Invalid | Self::Cancelled
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    pub fn can_transition_to(&self, target: PlanState) -> bool {
        if self.is_terminal() {
            return false;
        }
        use PlanState::*;
        matches!(
            (self, target),
            (Proposed, Validating)
                | (Proposed, Rejected)
                | (Proposed, Cancelled)
                | (Validating, Validated)
                | (Validating, Invalid)
                | (Validating, Rejected)
                | (Validated, Active)
                | (Validated, Rejected)
                | (Validated, Cancelled)
                | (Active, Superseded)
                | (Active, Completed)
                | (Active, Cancelled)
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Validating => "validating",
            Self::Validated => "validated",
            Self::Active => "active",
            Self::Superseded => "superseded",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Invalid => "invalid",
            Self::Cancelled => "cancelled",
        }
    }
}

// ── PlanRevision ───────────────────────────────────────────────────────

/// An immutable plan revision produced by a Planner invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRevision {
    /// System-generated unique identifier.
    pub plan_revision_id: PlanRevisionId,
    /// The Goal this plan belongs to.
    pub goal_id: GoalId,
    /// The Goal revision this plan was based on.
    pub goal_revision: i64,
    /// Monotonic revision number (1-based across the Goal).
    pub revision_number: i64,

    /// HEAD commit of target repository when plan was created.
    pub base_repository_head: String,

    /// Planner profile used to generate this plan.
    pub planner_profile_id: String,
    /// Durable invocation record for the Planner call.
    pub planner_invocation_id: String,

    /// Digest of the raw PlanProposal from the Planner.
    pub proposal_digest: String,
    /// Digest of the validation result.
    pub validation_digest: Option<String>,

    pub state: PlanState,

    pub created_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
    pub superseded_at: Option<DateTime<Utc>>,
}

// ── Milestone ──────────────────────────────────────────────────────────

/// A named milestone within a PlanRevision. Groups related tasks and
/// maps to success criteria.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Milestone {
    /// System-generated unique identifier.
    pub milestone_id: MilestoneId,
    pub plan_revision_id: PlanRevisionId,

    /// Planner-provided client reference (must be unique within the plan).
    pub client_ref: String,
    pub title: String,
    pub objective: String,

    /// References to Goal success criteria this milestone contributes to.
    pub success_criteria_refs: Vec<String>,

    /// Client refs of dependent milestones within this plan.
    pub dependencies: Vec<String>,

    /// Priority (higher = more important).
    pub priority: i32,

    pub state: MilestoneState,
}

/// Milestone lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MilestoneState {
    Pending,
    InProgress,
    Completed,
    Blocked,
    Cancelled,
}

impl MilestoneState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
        }
    }
}

// ── PlannedTask ────────────────────────────────────────────────────────

/// A task planned by the Planner. When selected, it is materialized into
/// a real Task via the existing TaskEngineeringLoopService.
///
/// The Planner may only specify client_ref; the real ID is generated by
/// the Harness at materialization time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedTask {
    /// System-generated unique identifier.
    pub planned_task_id: PlannedTaskId,
    pub plan_revision_id: PlanRevisionId,
    pub milestone_id: MilestoneId,

    /// Planner-provided client reference (must be unique within the plan).
    pub client_ref: String,
    pub title: String,
    pub objective: String,

    /// What must be true for this task to be considered complete.
    pub acceptance_criteria: Vec<String>,

    /// Client refs of dependent planned tasks.
    pub dependency_refs: Vec<String>,

    /// What evidence this task is expected to produce.
    pub expected_evidence: Vec<String>,

    /// Expected resource scope (file paths, directories, etc.).
    pub expected_resource_scope: Vec<String>,

    /// Risk level for approval gating.
    pub risk_level: RiskLevel,

    /// Whether this task requires explicit approval before dispatch.
    pub requires_approval: bool,

    /// Stable fingerprint for duplicate detection across plan revisions.
    pub task_fingerprint: String,

    pub state: PlannedTaskState,

    /// When this task was materialized into a real Task (None if not yet).
    pub materialized_task_id: Option<String>,
}

/// Risk level for approval gating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl RiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "critical" => Some(Self::Critical),
            _ => None,
        }
    }
}

/// PlannedTask lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannedTaskState {
    /// Task planned but not yet materialized.
    Pending,
    /// Materialized as a real Task and dispatched.
    Materialized,
    /// The real Task is actively executing.
    Running,
    /// Task completed (terminal states from Task aggregate).
    Completed,
    /// Task failed.
    Failed,
    /// Explicitly cancelled.
    Cancelled,
    /// Superseded by a newer plan revision.
    Superseded,
}

impl PlannedTaskState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Superseded
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Materialized | Self::Running)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Materialized => "materialized",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Superseded => "superseded",
        }
    }
}

// ── Task dependency helpers ────────────────────────────────────────────

/// Validate that a DAG of dependencies has no cycles.
/// `deps` maps each client_ref to its dependency client_refs.
pub fn validate_dag_no_cycles(deps: &std::collections::HashMap<String, Vec<String>>) -> Result<(), Vec<String>> {
    let mut cycle_paths: Vec<String> = Vec::new();

    // DFS-based cycle detection with recursion stack coloring.
    // 0 = unvisited, 1 = in current path (gray), 2 = fully processed (black).
    let mut color: std::collections::HashMap<String, u8> = std::collections::HashMap::new();

    fn dfs(
        node: &str,
        deps: &std::collections::HashMap<String, Vec<String>>,
        color: &mut std::collections::HashMap<String, u8>,
        path: &mut Vec<String>,
        cycle_paths: &mut Vec<String>,
    ) {
        let c = *color.get(node).unwrap_or(&0);
        if c == 2 {
            return;
        }
        if c == 1 {
            // Found a cycle
            let cycle_start = path.iter().position(|n| n == node).unwrap_or(0);
            let cycle: Vec<&str> = path[cycle_start..].iter().map(|s| s.as_str()).collect();
            cycle_paths.push(format!("cycle: {} -> {}", cycle.join(" -> "), node));
            return;
        }
        color.insert(node.to_string(), 1);
        path.push(node.to_string());
        if let Some(dep_list) = deps.get(node) {
            for dep in dep_list {
                dfs(dep, deps, color, path, cycle_paths);
            }
        }
        path.pop();
        color.insert(node.to_string(), 2);
    }

    for node in deps.keys() {
        let mut path: Vec<String> = Vec::new();
        dfs(node, deps, &mut color, &mut path, &mut cycle_paths);
    }

    if cycle_paths.is_empty() {
        Ok(())
    } else {
        Err(cycle_paths)
    }
}

/// Compute a stable fingerprint for a planned task.
pub fn compute_task_fingerprint(
    goal_revision: i64,
    normalized_objective: &str,
    acceptance_criteria: &[String],
    dependency_refs: &[String],
    repository_id: &str,
    target_ref: &str,
    expected_evidence: &[String],
) -> String {
    let input = format!(
        "{}|{}|{}|{:?}|{}|{}|{:?}",
        goal_revision,
        normalized_objective.to_lowercase().trim(),
        acceptance_criteria.join(",").to_lowercase(),
        dependency_refs,
        repository_id,
        target_ref,
        expected_evidence.join(",").to_lowercase(),
    );
    sha256_hex(&input)
}

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_plan_state_terminal() {
        assert!(PlanState::Superseded.is_terminal());
        assert!(PlanState::Completed.is_terminal());
        assert!(PlanState::Rejected.is_terminal());
        assert!(PlanState::Invalid.is_terminal());
        assert!(PlanState::Cancelled.is_terminal());
        assert!(!PlanState::Proposed.is_terminal());
        assert!(!PlanState::Active.is_terminal());
    }

    #[test]
    fn test_plan_state_transitions() {
        assert!(PlanState::Proposed.can_transition_to(PlanState::Validating));
        assert!(PlanState::Validating.can_transition_to(PlanState::Validated));
        assert!(PlanState::Validated.can_transition_to(PlanState::Active));
        assert!(PlanState::Active.can_transition_to(PlanState::Superseded));
        assert!(PlanState::Active.can_transition_to(PlanState::Completed));
        // Terminal cannot transition
        assert!(!PlanState::Superseded.can_transition_to(PlanState::Active));
        // Invalid transitions
        assert!(!PlanState::Proposed.can_transition_to(PlanState::Active));
        assert!(!PlanState::Validated.can_transition_to(PlanState::Validating));
    }

    #[test]
    fn test_dag_no_cycles_simple() {
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        deps.insert("T1".into(), vec![]);
        deps.insert("T2".into(), vec!["T1".into()]);
        deps.insert("T3".into(), vec!["T1".into(), "T2".into()]);
        assert!(validate_dag_no_cycles(&deps).is_ok());
    }

    #[test]
    fn test_dag_cycle_detected() {
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        deps.insert("T1".into(), vec!["T3".into()]);
        deps.insert("T2".into(), vec!["T1".into()]);
        deps.insert("T3".into(), vec!["T2".into()]);
        let result = validate_dag_no_cycles(&deps);
        assert!(result.is_err());
    }

    #[test]
    fn test_dag_self_loop() {
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        deps.insert("T1".into(), vec!["T1".into()]);
        let result = validate_dag_no_cycles(&deps);
        assert!(result.is_err());
    }

    #[test]
    fn test_dag_empty() {
        let deps: HashMap<String, Vec<String>> = HashMap::new();
        assert!(validate_dag_no_cycles(&deps).is_ok());
    }

    #[test]
    fn test_task_fingerprint_deterministic() {
        let fp1 = compute_task_fingerprint(
            1,
            "Add a function",
            &["test passes".into()],
            &[],
            "repo-1",
            "refs/heads/main",
            &["test output".into()],
        );
        let fp2 = compute_task_fingerprint(
            1,
            "Add a function",
            &["test passes".into()],
            &[],
            "repo-1",
            "refs/heads/main",
            &["test output".into()],
        );
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_task_fingerprint_different_objective() {
        let fp1 = compute_task_fingerprint(
            1,
            "Add a function",
            &["test passes".into()],
            &[],
            "repo-1",
            "refs/heads/main",
            &[],
        );
        let fp2 = compute_task_fingerprint(
            1,
            "Add a DIFFERENT function",
            &["test passes".into()],
            &[],
            "repo-1",
            "refs/heads/main",
            &[],
        );
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_risk_level_from_str() {
        assert_eq!(RiskLevel::from_str("low"), Some(RiskLevel::Low));
        assert_eq!(RiskLevel::from_str("high"), Some(RiskLevel::High));
        assert_eq!(RiskLevel::from_str("unknown"), None);
    }
}
