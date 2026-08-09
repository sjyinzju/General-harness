//! Interactive GoalSpec construction — pure, deterministic, unit-testable.
//!
//! The TUI submits *interactive* goals: the initial plan requires explicit
//! user approval, so the console can always pause the user for decisions.
//! User text is data — it lands in `title`/`objective`, never in commands.

use chrono::Utc;
use harness_core::contracts::goal::{
    ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator, GoalSpec,
    SuccessCriterion, VerificationPolicy,
};

/// Deterministic goal id generation: `goal-<ulid-ish>` from a monotonic
/// counter + wall clock. The client owns the id so `goal.create` retries
/// after a timeout are safe (PK collision → the Supervisor treats the
/// original as authoritative).
pub fn new_goal_id(now: chrono::DateTime<Utc>) -> String {
    format!("goal-tui-{}", now.format("%Y%m%d%H%M%S%f"))
}

/// Derive a short title from the objective text (first line, capped).
pub fn title_from_objective(objective: &str) -> String {
    let first = objective.lines().next().unwrap_or("").trim();
    let capped: String = first.chars().take(72).collect();
    if capped.is_empty() {
        "Untitled goal".to_string()
    } else {
        capped
    }
}

/// Context the runner supplies from its environment (never from shell
/// output — the runner only forwards what the Supervisor already knows).
#[derive(Debug, Clone)]
pub struct RepoContext {
    pub repository_id: String,
    pub target_ref: String,
    pub initial_base_head: String,
}

impl Default for RepoContext {
    fn default() -> Self {
        Self {
            repository_id: "local-repo".to_string(),
            target_ref: "refs/heads/main".to_string(),
            initial_base_head: "HEAD".to_string(),
        }
    }
}

/// Build an interactive GoalSpec from user text.
pub fn build_interactive_goal_spec(objective: &str, repo: &RepoContext) -> GoalSpec {
    let now = Utc::now();
    let goal_id = new_goal_id(now);
    build_interactive_goal_spec_with_id(objective, repo, goal_id, now)
}

/// Same as [`build_interactive_goal_spec`] but with explicit id/clock —
/// keeps tests deterministic.
pub fn build_interactive_goal_spec_with_id(
    objective: &str,
    repo: &RepoContext,
    goal_id: String,
    now: chrono::DateTime<Utc>,
) -> GoalSpec {
    GoalSpec {
        goal_id,
        revision: 1,
        title: title_from_objective(objective),
        objective: objective.trim().to_string(),
        repository_id: repo.repository_id.clone(),
        target_ref: repo.target_ref.clone(),
        initial_base_head: repo.initial_base_head.clone(),
        success_criteria: vec![SuccessCriterion {
            criterion_id: "c1".to_string(),
            description: "All planned tasks complete successfully".to_string(),
            evidence_policy: EvidencePolicy::TaskTerminalResult,
            verification_policy: VerificationPolicy::ExistenceOnly,
            subjectivity: CriterionSubjectivity::Objective,
            required: true,
        }],
        constraints: Vec::new(),
        non_goals: Vec::new(),
        budget: GoalBudget::default(),
        approval_policy: ApprovalPolicy {
            require_initial_plan_approval: true,
            ..ApprovalPolicy::default()
        },
        created_by: GoalCreator::User {
            user_id: "tui-user".to_string(),
            user_name: Some("TUI Console".to_string()),
        },
        created_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_interactive_spec_with_plan_approval() {
        let spec = build_interactive_goal_spec_with_id(
            "Add a README\nwith details",
            &RepoContext::default(),
            "goal-test-1".into(),
            Utc::now(),
        );
        assert_eq!(spec.goal_id, "goal-test-1");
        assert_eq!(spec.title, "Add a README");
        assert!(spec.approval_policy.require_initial_plan_approval);
        assert_eq!(spec.success_criteria.len(), 1);
        assert!(spec.success_criteria[0].required);
        // Round-trips through the same JSON the IPC carries.
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["goal_id"], "goal-test-1");
        assert_eq!(json["repository_id"], "local-repo");
    }

    #[test]
    fn title_is_capped_and_non_empty() {
        let long = "x".repeat(200);
        assert_eq!(title_from_objective(&long).len(), 72);
        assert_eq!(title_from_objective("   "), "Untitled goal");
    }
}
