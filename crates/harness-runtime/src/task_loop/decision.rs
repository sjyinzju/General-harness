//! Decision engine for I4.5 Task Engineering Loop.
//!
//! Reads immutable I4 facts (VerificationOutcome, Dossier, StepResults,
//! Evidence) and deterministically classifies the next loop action.
//! All mappings are configuration-driven; no LLM or Agent self-report.

use super::types::*;

/// Stable decision precedence (lower = higher priority).
#[allow(dead_code)]
const PRECEDENCE: &[(DecisionClassification, u32)] = &[
    (DecisionClassification::Cancelled, 1),
    (DecisionClassification::AwaitingReconciliation, 2),
    (DecisionClassification::InfrastructureBlocked, 3),
    (DecisionClassification::AwaitingHuman, 4),
    (DecisionClassification::CompleteCandidate, 5),
    (DecisionClassification::NonRetryable, 8),
    (DecisionClassification::BudgetExhausted, 9),
    (DecisionClassification::NoProgress, 10),
    (DecisionClassification::EscalateToProjectPlanner, 11),
    (DecisionClassification::ContinueRepair, 15),
];

/// Input facts the decision engine reads (all from I4 certified outputs).
#[derive(Debug, Clone, Default)]
pub struct DecisionInput {
    pub cancellation_requested: bool,
    pub i4_reconciliation_required: bool,
    pub active_process: bool,
    pub active_scanner: bool,
    pub ownership_fencing_ok: bool,
    pub worktree_identity_ok: bool,
    pub outcome_result: Option<String>,
    pub next_action: Option<String>,
    pub all_required_steps_passed: bool,
    pub evidence_complete: bool,
    pub dossier_present: bool,
    pub dossier_fingerprint_matches: bool,
    pub security_blocker: bool,
    pub budget_exhausted_hard: bool,
    pub no_progress: bool,
    pub cycle_detected: bool,
    pub infrastructure_blocked: bool,
    pub repairable: bool,
    pub task_scope_insufficient: bool,
    pub primary_failure: Option<String>,
}

impl DecisionInput {
    /// Classify the next loop action. Returns exactly one decision.
    pub fn classify(&self) -> DecisionClassification {
        // 1. Cancellation
        if self.cancellation_requested {
            return DecisionClassification::Cancelled;
        }

        // 2. Reconciliation / active process
        if self.i4_reconciliation_required || self.active_process || self.active_scanner {
            return DecisionClassification::AwaitingReconciliation;
        }

        // 3. Ownership / security ambiguity
        if !self.ownership_fencing_ok {
            return DecisionClassification::AwaitingReconciliation;
        }
        if self.security_blocker {
            return DecisionClassification::AwaitingHuman;
        }
        if !self.worktree_identity_ok {
            return DecisionClassification::AwaitingHuman;
        }

        // 4. CompleteCandidate
        if self.is_complete_candidate() {
            return DecisionClassification::CompleteCandidate;
        }

        // 5. Non-retryable outcomes
        if self.is_non_retryable() {
            return DecisionClassification::NonRetryable;
        }

        // 6. Budget exhausted (but not if already CompleteCandidate)
        if self.budget_exhausted_hard {
            return DecisionClassification::BudgetExhausted;
        }

        // 7. No progress / cycle
        if self.cycle_detected || self.no_progress {
            return DecisionClassification::NoProgress;
        }

        // 8. Infrastructure blocked
        if self.infrastructure_blocked {
            return DecisionClassification::InfrastructureBlocked;
        }

        // 9. Repairable
        if self.repairable {
            return DecisionClassification::ContinueRepair;
        }

        // 10. Escalation
        if self.task_scope_insufficient {
            return DecisionClassification::EscalateToProjectPlanner;
        }

        // Default: escalate.
        DecisionClassification::EscalateToProjectPlanner
    }

    fn is_complete_candidate(&self) -> bool {
        self.outcome_result.as_deref() == Some("passed")
            && self.next_action.as_deref() == Some("CompleteCandidate")
            && self.all_required_steps_passed
            && self.evidence_complete
            && self.dossier_present
            && self.dossier_fingerprint_matches
            && self.ownership_fencing_ok
            && self.worktree_identity_ok
            && !self.security_blocker
            && !self.i4_reconciliation_required
            && !self.active_process
            && !self.active_scanner
    }

    fn is_non_retryable(&self) -> bool {
        matches!(
            self.next_action.as_deref(),
            Some("NonRetryable") | Some("IrrecoverableAmbiguity")
        ) || self.outcome_result.as_deref() == Some("OutcomeConflict")
    }
}

/// Default repairable classification mapping.
/// All entries are configurable via policy; these are safe defaults.
pub fn is_default_repairable(failure_classification: &str) -> bool {
    matches!(
        failure_classification,
        "BuildFailure"
            | "TestFailure"
            | "LintFailure"
            | "TypecheckFailure"
            | "CommandFailure"
            | "OutputMismatch"
            | "ScopeViolation"
            | "ForbiddenChange"
            | "PolicyViolation"
            | "RequiredFileMissing"
            | "ArtifactMissing"
            | "ArtifactCorruption"
    )
}

/// Classifications that ALWAYS block automatic retry (security / ownership).
pub fn is_always_blocking(failure_classification: &str) -> bool {
    matches!(
        failure_classification,
        "SecretExposure" | "OwnershipLost" | "StaleFencing" | "WorktreeMissing"
    )
}

/// Compute a decision fingerprint from input facts.
pub fn decision_fingerprint(input: &DecisionInput) -> String {
    let s = format!(
        "cancel={}|rec={}|proc={}|scan={}|owner={}|wt={}|outcome={}|next={}|steps={}|ev={}|dos={}|dospel={}|sec={}|budget={}|noprog={}|cycle={}|infra={}|repair={}|scope={}|fail={}",
        input.cancellation_requested,
        input.i4_reconciliation_required,
        input.active_process,
        input.active_scanner,
        input.ownership_fencing_ok,
        input.worktree_identity_ok,
        input.outcome_result.as_deref().unwrap_or("-"),
        input.next_action.as_deref().unwrap_or("-"),
        input.all_required_steps_passed,
        input.evidence_complete,
        input.dossier_present,
        input.dossier_fingerprint_matches,
        input.security_blocker,
        input.budget_exhausted_hard,
        input.no_progress,
        input.cycle_detected,
        input.infrastructure_blocked,
        input.repairable,
        input.task_scope_insufficient,
        input.primary_failure.as_deref().unwrap_or("-"),
    );
    fingerprint_hex(&s)
}
