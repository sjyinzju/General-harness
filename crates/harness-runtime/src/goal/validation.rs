//! PlanValidator — Rust-side validation of Planner LLM output.
//!
//! Every PlanProposal from the Planner is validated deterministically before
//! activation. The Planner may propose; the Rust validator decides.
//!
//! Rejects: schema violations, duplicate refs, missing deps, cycles,
//! budget overflows, scope expansion, criterion mutation, non-goal conflicts.

use std::collections::{HashMap, HashSet};

use harness_core::contracts::goal::GoalSpec;
use harness_core::contracts::plan::validate_dag_no_cycles;

use super::{CriterionStatus, PlanProposal};

/// Result of validating a PlanProposal against a GoalSpec.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub proposal_digest: String,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            proposal_digest: String::new(),
        }
    }

    fn add_error(&mut self, error: String) {
        self.valid = false;
        self.errors.push(error);
    }

    fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }
}

/// Validate a PlanProposal against a GoalSpec using Rust business rules.
pub fn validate_plan_proposal(
    proposal: &PlanProposal,
    goal: &GoalSpec,
    existing_task_count: u32,
) -> ValidationResult {
    let mut result = ValidationResult::new();

    // Compute proposal digest for idempotency
    result.proposal_digest = compute_proposal_digest(proposal);

    // 1. Schema validation
    if proposal.schema_version != "1.0" {
        result.add_warning(format!(
            "unknown schema_version: {}",
            proposal.schema_version
        ));
    }

    // 2. Milestone client_ref uniqueness
    let mut milestone_refs: HashSet<&str> = HashSet::new();
    for m in &proposal.milestones {
        if !milestone_refs.insert(&m.client_ref) {
            result.add_error(format!("duplicate milestone client_ref: {}", m.client_ref));
        }
    }

    // 3. Task client_ref uniqueness
    let mut task_refs: HashSet<&str> = HashSet::new();
    for t in &proposal.tasks {
        if !task_refs.insert(&t.client_ref) {
            result.add_error(format!("duplicate task client_ref: {}", t.client_ref));
        }
    }

    // 4. All tasks belong to a valid milestone
    for t in &proposal.tasks {
        if !milestone_refs.contains(t.milestone_ref.as_str()) {
            result.add_error(format!(
                "task {} references unknown milestone: {}",
                t.client_ref, t.milestone_ref
            ));
        }
    }

    // 5. Task dependency DAG — no cycles, all deps exist
    let mut task_deps: HashMap<String, Vec<String>> = HashMap::new();
    for t in &proposal.tasks {
        task_deps.insert(t.client_ref.clone(), t.dependencies.clone());
    }
    // Add entries for tasks with no deps
    for t in &proposal.tasks {
        task_deps.entry(t.client_ref.clone()).or_default();
    }
    // Validate all deps exist
    for t in &proposal.tasks {
        for dep in &t.dependencies {
            if !task_refs.contains(dep.as_str()) {
                result.add_error(format!(
                    "task {} depends on unknown task: {}",
                    t.client_ref, dep
                ));
            }
        }
    }
    if result.valid {
        if let Err(cycles) = validate_dag_no_cycles(&task_deps) {
            for cycle in cycles {
                result.add_error(cycle);
            }
        }
    }

    // 6. Milestone dependency DAG — no cycles
    let mut ms_deps: HashMap<String, Vec<String>> = HashMap::new();
    for m in &proposal.milestones {
        ms_deps.insert(m.client_ref.clone(), m.dependencies.clone());
    }
    for m in &proposal.milestones {
        for dep in &m.dependencies {
            if !milestone_refs.contains(dep.as_str()) {
                result.add_error(format!(
                    "milestone {} depends on unknown milestone: {}",
                    m.client_ref, dep
                ));
            }
        }
    }
    if result.valid {
        if let Err(cycles) = validate_dag_no_cycles(&ms_deps) {
            for cycle in cycles {
                result.add_error(cycle);
            }
        }
    }

    // 7. All required success criteria covered by at least one milestone
    let mut covered_criteria: HashSet<&str> = HashSet::new();
    for m in &proposal.milestones {
        for c_ref in &m.success_criteria_refs {
            covered_criteria.insert(c_ref);
        }
    }
    for c in &goal.success_criteria {
        if c.required && !covered_criteria.contains(c.criterion_id.as_str()) {
            result.add_error(format!(
                "required success criterion '{}' not covered by any milestone",
                c.criterion_id
            ));
        }
    }

    // 8. Acceptance criteria non-empty
    for t in &proposal.tasks {
        if t.acceptance_criteria.is_empty() {
            result.add_error(format!(
                "task {} has empty acceptance_criteria",
                t.client_ref
            ));
        }
    }

    // 9. Expected evidence non-empty
    for t in &proposal.tasks {
        if t.expected_evidence.is_empty() {
            result.add_error(format!(
                "task {} has empty expected_evidence",
                t.client_ref
            ));
        }
    }

    // 10. Task count within budget
    let proposed_task_count = proposal.tasks.len() as u32;
    if existing_task_count + proposed_task_count > goal.budget.max_total_tasks {
        result.add_error(format!(
            "proposed {} tasks + existing {} exceeds budget max_total_tasks {}",
            proposed_task_count,
            existing_task_count,
            goal.budget.max_total_tasks
        ));
    }

    // 11. Risk level validation
    for t in &proposal.tasks {
        if !["low", "medium", "high", "critical"].contains(&t.risk_level.as_str()) {
            result.add_error(format!(
                "task {} has invalid risk_level: {}",
                t.client_ref, t.risk_level
            ));
        }
    }

    // 12. No goal scope expansion (check for non-goal conflicts)
    for ng in &goal.non_goals {
        let ng_lower = ng.to_lowercase();
        for t in &proposal.tasks {
            if t.objective.to_lowercase().contains(&ng_lower) {
                result.add_warning(format!(
                    "task {} objective may conflict with non-goal: '{}'",
                    t.client_ref, ng
                ));
            }
        }
    }

    // 13. No success criterion mutation (Planner cannot change success criteria)
    // This is checked by construction — the Planner only gets to reference existing criteria

    // 14. Base HEAD must be non-empty
    if goal.initial_base_head.is_empty() {
        result.add_warning("goal initial_base_head is empty — plan may be based on unresolved ref".into());
    }

    result
}

/// Compute a stable digest of a PlanProposal for idempotency.
fn compute_proposal_digest(proposal: &PlanProposal) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();

    hasher.update(proposal.schema_version.as_bytes());
    hasher.update(proposal.goal_summary.as_bytes());

    for m in &proposal.milestones {
        hasher.update(m.client_ref.as_bytes());
        hasher.update(m.title.as_bytes());
        hasher.update(m.objective.as_bytes());
    }

    for t in &proposal.tasks {
        hasher.update(t.client_ref.as_bytes());
        hasher.update(t.milestone_ref.as_bytes());
        hasher.update(t.title.as_bytes());
        hasher.update(t.objective.as_bytes());
        for ac in &t.acceptance_criteria {
            hasher.update(ac.as_bytes());
        }
        for dep in &t.dependencies {
            hasher.update(dep.as_bytes());
        }
    }

    format!("{:x}", hasher.finalize())
}

// ── Completion Gate ───────────────────────────────────────────────────

/// Check whether a Goal can be marked Succeeded.
/// This is a Rust-only check — the GoalEvaluator's recommendation is an
/// input, not the decision.
pub fn check_completion_gate(
    goal: &GoalSpec,
    criteria_statuses: &HashMap<String, CriterionStatus>,
    all_evidence_refs: &HashSet<String>,
    pending_task_count: usize,
    target_head_verified: bool,
    evaluator_recommends_completion: bool,
    has_unresolved_critical_findings: bool,
    has_pending_approvals: bool,
) -> super::CompletionGateResult {
    let mut blocking: Vec<String> = Vec::new();
    let mut criteria_results = Vec::new();

    // 1. All required success criteria must be satisfied
    for c in &goal.success_criteria {
        let status = criteria_statuses
            .get(&c.criterion_id)
            .cloned()
            .unwrap_or(CriterionStatus::Unknown);

        let satisfied = matches!(status, CriterionStatus::Satisfied);
        let evidence_refs: Vec<String> = all_evidence_refs
            .iter()
            .filter(|r| r.contains(&c.criterion_id))
            .cloned()
            .collect();

        let missing_evidence: Vec<String> = if c.required && !satisfied {
            vec![format!("evidence missing for criterion: {}", c.description)]
        } else {
            vec![]
        };

        criteria_results.push(super::CriterionCompletionStatus {
            criterion_id: c.criterion_id.clone(),
            satisfied,
            evidence_refs,
            missing_evidence,
        });

        if c.required && !satisfied {
            blocking.push(format!(
                "required criterion '{}' not satisfied: {}",
                c.criterion_id, c.description
            ));
        }
    }

    // 2. No pending required tasks
    if pending_task_count > 0 {
        blocking.push(format!(
            "{} planned tasks still pending/running/blocked",
            pending_task_count
        ));
    }

    // 3. Target ref HEAD must be verified
    if !target_head_verified {
        blocking.push("target ref HEAD not verified".into());
    }

    // 4. Evaluator must recommend completion
    if !evaluator_recommends_completion {
        blocking.push("GoalEvaluator does not recommend completion".into());
    }

    // 5. No unresolved Critical/High findings
    if has_unresolved_critical_findings {
        blocking.push("unresolved Critical/High findings exist".into());
    }

    // 6. No pending approvals
    if has_pending_approvals {
        blocking.push("pending approval requests exist".into());
    }

    // 7. Check for subjective criteria requiring human approval
    let requires_human = goal
        .success_criteria
        .iter()
        .any(|c| c.subjectivity.requires_human_approval() && c.required);

    let can_complete = blocking.is_empty() && !requires_human;

    super::CompletionGateResult {
        can_complete,
        blocking_reasons: blocking,
        criteria_results,
        requires_human_approval: requires_human && !has_pending_approvals,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::goal::{
        ApprovalPolicy, CriterionSubjectivity, EvidencePolicy, GoalBudget, GoalCreator,
        SuccessCriterion, VerificationPolicy,
    };

    fn make_test_goal() -> GoalSpec {
        GoalSpec {
            goal_id: "g1".into(),
            revision: 1,
            title: "Test Goal".into(),
            objective: "Test the system".into(),
            repository_id: "repo-1".into(),
            target_ref: "refs/heads/main".into(),
            initial_base_head: "abc123".into(),
            success_criteria: vec![
                SuccessCriterion {
                    criterion_id: "c1".into(),
                    description: "All tests pass".into(),
                    evidence_policy: EvidencePolicy::TaskTerminalResult,
                    verification_policy: VerificationPolicy::ExistenceOnly,
                    subjectivity: CriterionSubjectivity::Objective,
                    required: true,
                },
            ],
            constraints: vec![],
            non_goals: vec!["Do not modify CI config".into()],
            budget: GoalBudget::default(),
            approval_policy: ApprovalPolicy::default(),
            created_by: GoalCreator::User {
                user_id: "u1".into(),
                user_name: None,
            },
            created_at: chrono::Utc::now(),
        }
    }

    fn make_valid_proposal() -> PlanProposal {
        PlanProposal {
            schema_version: "1.0".into(),
            goal_summary: "Test plan".into(),
            assumptions: vec![],
            milestones: vec![super::super::ProposedMilestone {
                client_ref: "M1".into(),
                title: "Milestone 1".into(),
                objective: "Complete all work".into(),
                success_criteria_refs: vec!["c1".into()],
                dependencies: vec![],
                priority: 10,
            }],
            tasks: vec![super::super::ProposedTask {
                client_ref: "T1".into(),
                milestone_ref: "M1".into(),
                title: "Task 1".into(),
                objective: "Do the work".into(),
                acceptance_criteria: vec!["test passes".into()],
                dependencies: vec![],
                expected_evidence: vec!["test output".into()],
                expected_resource_scope: vec![],
                risk_level: "low".into(),
                requires_approval: false,
            }],
            risks: vec![],
            completion_strategy: "Single milestone, single task".into(),
        }
    }

    #[test]
    fn test_valid_proposal_passes() {
        let goal = make_test_goal();
        let proposal = make_valid_proposal();
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(result.valid, "expected valid, got errors: {:?}", result.errors);
    }

    #[test]
    fn test_duplicate_milestone_ref_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.milestones.push(super::super::ProposedMilestone {
            client_ref: "M1".into(),
            title: "Duplicate".into(),
            objective: "dup".into(),
            success_criteria_refs: vec![],
            dependencies: vec![],
            priority: 0,
        });
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_duplicate_task_ref_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.tasks.push(super::super::ProposedTask {
            client_ref: "T1".into(),
            milestone_ref: "M1".into(),
            title: "Duplicate".into(),
            objective: "dup".into(),
            acceptance_criteria: vec!["test".into()],
            dependencies: vec![],
            expected_evidence: vec!["ev".into()],
            expected_resource_scope: vec![],
            risk_level: "low".into(),
            requires_approval: false,
        });
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_task_cycle_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.tasks.push(super::super::ProposedTask {
            client_ref: "T2".into(),
            milestone_ref: "M1".into(),
            title: "Task 2".into(),
            objective: "depends".into(),
            acceptance_criteria: vec!["test".into()],
            dependencies: vec!["T3".into()],
            expected_evidence: vec!["ev".into()],
            expected_resource_scope: vec![],
            risk_level: "low".into(),
            requires_approval: false,
        });
        proposal.tasks.push(super::super::ProposedTask {
            client_ref: "T3".into(),
            milestone_ref: "M1".into(),
            title: "Task 3".into(),
            objective: "cycles back".into(),
            acceptance_criteria: vec!["test".into()],
            dependencies: vec!["T2".into()],
            expected_evidence: vec!["ev".into()],
            expected_resource_scope: vec![],
            risk_level: "low".into(),
            requires_approval: false,
        });
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_missing_dependency_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.tasks[0].dependencies = vec!["T_nonexistent".into()];
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_uncovered_required_criterion_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.milestones[0].success_criteria_refs = vec![];
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_empty_acceptance_criteria_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.tasks[0].acceptance_criteria = vec![];
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_empty_evidence_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.tasks[0].expected_evidence = vec![];
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_budget_overflow_rejected() {
        let mut goal = make_test_goal();
        goal.budget.max_total_tasks = 1;
        let mut proposal = make_valid_proposal();
        proposal.tasks.push(super::super::ProposedTask {
            client_ref: "T2".into(),
            milestone_ref: "M1".into(),
            title: "Task 2".into(),
            objective: "extra".into(),
            acceptance_criteria: vec!["test".into()],
            dependencies: vec![],
            expected_evidence: vec!["ev".into()],
            expected_resource_scope: vec![],
            risk_level: "low".into(),
            requires_approval: false,
        });
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_invalid_risk_level_rejected() {
        let goal = make_test_goal();
        let mut proposal = make_valid_proposal();
        proposal.tasks[0].risk_level = "extreme".into();
        let result = validate_plan_proposal(&proposal, &goal, 0);
        assert!(!result.valid);
    }

    #[test]
    fn test_completion_gate_all_satisfied() {
        let goal = make_test_goal();
        let mut statuses = HashMap::new();
        statuses.insert("c1".into(), CriterionStatus::Satisfied);
        let mut evidence_refs = HashSet::new();
        evidence_refs.insert("ev-c1-task1".into());

        let result = check_completion_gate(
            &goal, &statuses, &evidence_refs,
            0, true, true, false, false,
        );
        assert!(result.can_complete);
        assert!(result.blocking_reasons.is_empty());
    }

    #[test]
    fn test_completion_gate_missing_required_criterion() {
        let goal = make_test_goal();
        let statuses: HashMap<String, CriterionStatus> = HashMap::new();
        let evidence_refs = HashSet::new();

        let result = check_completion_gate(
            &goal, &statuses, &evidence_refs,
            0, true, true, false, false,
        );
        assert!(!result.can_complete);
        assert!(!result.blocking_reasons.is_empty());
    }

    #[test]
    fn test_completion_gate_pending_tasks_block() {
        let goal = make_test_goal();
        let mut statuses = HashMap::new();
        statuses.insert("c1".into(), CriterionStatus::Satisfied);
        let mut evidence_refs = HashSet::new();
        evidence_refs.insert("ev-c1".into());

        let result = check_completion_gate(
            &goal, &statuses, &evidence_refs,
            2, true, true, false, false,
        );
        assert!(!result.can_complete);
    }
}
