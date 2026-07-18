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

// ── Completion Eligibility (Production Hard Gate) ────────────────────

/// Production guard: eligibility for CompleteCandidate MUST be derived from
/// durable I4 facts, not from caller-constructed DecisionInput fields.
/// Every field must be independently verified against the database before
/// CompleteCandidate can be accepted.
#[derive(Debug, Clone, Default)]
pub struct CompletionEligibility {
    pub execution_terminal: bool,
    pub outcome_passed: bool,
    pub verification_terminal: bool,
    pub required_steps_complete: bool,
    pub evidence_complete: bool,
    pub dossier_fingerprint_valid: bool,
    pub process_inactive: bool,
    pub reconciliation_clear: bool,
    pub workspace_valid: bool,
    pub ownership_valid: bool,
}

impl CompletionEligibility {
    /// All gates must pass for CompleteCandidate to be valid.
    pub fn all_passed(&self) -> bool {
        self.execution_terminal
            && self.outcome_passed
            && self.verification_terminal
            && self.required_steps_complete
            && self.evidence_complete
            && self.dossier_fingerprint_valid
            && self.process_inactive
            && self.reconciliation_clear
            && self.workspace_valid
            && self.ownership_valid
    }

    /// Return the list of failed gates for diagnostics.
    pub fn failed_gates(&self) -> Vec<&'static str> {
        let mut gates = Vec::new();
        if !self.execution_terminal {
            gates.push("execution_terminal");
        }
        if !self.outcome_passed {
            gates.push("outcome_passed");
        }
        if !self.verification_terminal {
            gates.push("verification_terminal");
        }
        if !self.required_steps_complete {
            gates.push("required_steps_complete");
        }
        if !self.evidence_complete {
            gates.push("evidence_complete");
        }
        if !self.dossier_fingerprint_valid {
            gates.push("dossier_fingerprint_valid");
        }
        if !self.process_inactive {
            gates.push("process_inactive");
        }
        if !self.reconciliation_clear {
            gates.push("reconciliation_clear");
        }
        if !self.workspace_valid {
            gates.push("workspace_valid");
        }
        if !self.ownership_valid {
            gates.push("ownership_valid");
        }
        gates
    }
}

/// Validate completion eligibility from durable I4 state.
/// This is the PRODUCTION HARD GATE — it reads actual database state,
/// not caller-provided DecisionInput fields.
pub async fn validate_completion_eligibility(
    pool: &sqlx::SqlitePool,
    execution_id: &str,
) -> Result<CompletionEligibility, String> {
    let mut eligibility = CompletionEligibility::default();

    // 1. Execution must be terminal.
    let exec_row: Option<(String,)> =
        sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id=?")
            .bind(execution_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("eligibility query: {e}"))?;
    if let Some((lc,)) = exec_row {
        eligibility.execution_terminal = lc == "completed" || lc == "failed" || lc == "cancelled";
    }

    // 2. Verification must be terminal with passed outcome.
    let ver_row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT lifecycle, outcome_json FROM verification_runs WHERE execution_id=?",
    )
    .bind(execution_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("eligibility ver query: {e}"))?;

    if let Some((v_lc, outcome_json)) = ver_row {
        eligibility.verification_terminal =
            v_lc == "finalized" || v_lc == "completed" || v_lc == "blocked";

        // Parse outcome JSON for "result": "passed"
        if let Some(ref oj) = outcome_json {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(oj) {
                eligibility.outcome_passed = v
                    .get("result")
                    .and_then(|r| r.as_str())
                    .map(|s| s == "passed")
                    .unwrap_or(false);
                eligibility.required_steps_complete = v
                    .get("all_required_steps_passed")
                    .and_then(|s| s.as_bool())
                    .unwrap_or(false);
                eligibility.evidence_complete = v
                    .get("evidence_complete")
                    .and_then(|s| s.as_bool())
                    .unwrap_or(false);
            }
        }
    }

    // Dossier fingerprint: verify finalization operation has dossier_json and
    // is in a terminal lifecycle.
    let dossier_row: Option<(String,)> = sqlx::query_as(
        "SELECT dossier_json FROM verification_finalization_operations \
         WHERE verification_run_id=(SELECT run_id FROM verification_runs WHERE execution_id=? LIMIT 1) \
         AND dossier_json IS NOT NULL \
         AND lifecycle IN ('completed','outcome_persisted') \
         LIMIT 1",
    )
    .bind(execution_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("eligibility dossier query: {e}"))?;
    eligibility.dossier_fingerprint_valid = dossier_row.is_some();

    // 3. No active process for this execution.
    let proc_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM verification_step_operations WHERE execution_id=? AND status='running'")
            .bind(execution_id)
            .fetch_one(pool)
            .await
            .map_err(|e| format!("eligibility proc query: {e}"))?;
    eligibility.process_inactive = proc_count.0 == 0;

    // 4. No reconciliation required.
    let rec_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM verification_reconciliation_operations WHERE verification_run_id=(SELECT run_id FROM verification_runs WHERE execution_id=? LIMIT 1) AND lifecycle NOT IN ('completed','noop')",
    )
    .bind(execution_id)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("eligibility rec query: {e}"))?;
    eligibility.reconciliation_clear = rec_count.0 == 0;

    // 5. Workspace / worktree must exist and be valid.
    let wt_row: Option<(String,)> = sqlx::query_as("SELECT id FROM worktrees WHERE execution_id=?")
        .bind(execution_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("eligibility wt query: {e}"))?;
    eligibility.workspace_valid = wt_row.is_some();

    // 6. Ownership / handoff must be valid.
    let ho_row: Option<(String,)> = sqlx::query_as(
        "SELECT handoff_id FROM resource_handoffs WHERE execution_id=? AND status NOT IN ('released','failed')",
    )
    .bind(execution_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("eligibility ho query: {e}"))?;
    eligibility.ownership_valid = ho_row.is_some();

    Ok(eligibility)
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
