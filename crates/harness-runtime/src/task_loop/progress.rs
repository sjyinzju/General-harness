//! Progress detector and budget enforcement for I4.5.
//!
//! Deterministically detects no-progress loops and cycle patterns from
//! immutable I4 facts. Never relies on LLM or Agent self-report.

use super::types::*;

/// A compact fingerprint of one Attempt's outcome for progress comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct AttemptProgressFingerprint {
    pub primary_failure: String,
    pub blocker_set: Vec<String>,
    pub failed_required_steps: Vec<String>,
    pub required_passed_count: i64,
    pub diff_fingerprint: String,
    pub changed_files: Vec<String>,
    pub worktree_head: String,
    pub outcome_fingerprint: String,
    pub evidence_fingerprint: String,
    pub profile_id: String,
    pub context_fingerprint: String,
}

impl AttemptProgressFingerprint {
    /// Canonical string for fingerprint hashing.
    pub fn canonical_string(&self) -> String {
        let mut blockers = self.blocker_set.clone();
        blockers.sort();
        blockers.dedup();
        let mut failed = self.failed_required_steps.clone();
        failed.sort();
        let mut files = self.changed_files.clone();
        files.sort();
        format!(
            "fail={}|blockers=[{}]|failed_steps=[{}]|passed={}|diff={}|files=[{}]|wt_head={}|outcome={}|evidence={}|profile={}|ctx={}",
            self.primary_failure,
            blockers.join(","),
            failed.join(","),
            self.required_passed_count,
            self.diff_fingerprint,
            files.join(","),
            self.worktree_head,
            self.outcome_fingerprint,
            self.evidence_fingerprint,
            self.profile_id,
            self.context_fingerprint,
        )
    }

    pub fn fingerprint(&self) -> String {
        fingerprint_hex(&self.canonical_string())
    }
}

/// Compare two Attempt fingerprints and classify progress.
pub fn classify_progress(
    prev: &AttemptProgressFingerprint,
    current: &AttemptProgressFingerprint,
) -> ProgressVerdict {
    // Same failure + same blockers + same/empty diff + no improvement → NoProgress.
    let same_failure = prev.primary_failure == current.primary_failure;
    let same_blockers = {
        let mut pb = prev.blocker_set.clone();
        pb.sort();
        let mut cb = current.blocker_set.clone();
        cb.sort();
        pb == cb
    };
    let no_diff_change =
        prev.diff_fingerprint == current.diff_fingerprint || current.diff_fingerprint.is_empty();
    let no_improvement = current.required_passed_count <= prev.required_passed_count;

    if same_failure && same_blockers && no_diff_change && no_improvement {
        return ProgressVerdict::NoProgress;
    }

    // Some improvement but same primary failure → PartialProgress.
    if same_failure && !no_improvement {
        return ProgressVerdict::PartialProgress;
    }

    // Regression: worse than before.
    if current.required_passed_count < prev.required_passed_count {
        return ProgressVerdict::Regression;
    }

    ProgressVerdict::Progress
}

/// Detect a cycle: fingerprint A appears, then B, then A again.
pub fn detect_cycle(history: &[AttemptProgressFingerprint]) -> bool {
    if history.len() < 3 {
        return false;
    }
    let fps: Vec<String> = history.iter().map(|h| h.fingerprint()).collect();
    // Look for A ... A pattern with at least one different entry between.
    for i in 0..fps.len().saturating_sub(2) {
        for j in (i + 2)..fps.len() {
            if fps[i] == fps[j] {
                // Check there's at least one different between.
                let mid: Vec<&String> = fps[i + 1..j].iter().filter(|f| *f != &fps[i]).collect();
                if !mid.is_empty() {
                    return true;
                }
            }
        }
    }
    false
}

// ── Budget enforcement ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BudgetPolicy {
    pub max_attempts: Option<u32>,
    pub max_attempts_mode: BudgetMode,
    pub max_wall_time_ms: Option<u64>,
    pub max_wall_time_mode: BudgetMode,
    pub max_tool_calls: Option<u64>,
    pub max_tool_calls_mode: BudgetMode,
    pub max_input_tokens: Option<u64>,
    pub max_input_tokens_mode: BudgetMode,
    pub max_output_tokens: Option<u64>,
    pub max_output_tokens_mode: BudgetMode,
    pub max_total_tokens: Option<u64>,
    pub max_total_tokens_mode: BudgetMode,
    pub max_estimated_cost_micros: Option<u64>,
    pub max_estimated_cost_micros_mode: BudgetMode,
    pub max_no_progress_streak: Option<u32>,
    pub max_no_progress_mode: BudgetMode,
    pub max_same_failure_streak: Option<u32>,
    pub max_same_failure_mode: BudgetMode,
    pub max_profile_switches: Option<u32>,
    pub max_profile_switches_mode: BudgetMode,
    pub unknown_usage_policy: UnknownUsagePolicy,
}

#[derive(Debug, Clone)]
pub enum BudgetCheckResult {
    Ok,
    Exhausted {
        limit_name: &'static str,
        current: i64,
        max: i64,
    },
    Unknown {
        limit_name: &'static str,
        policy: UnknownUsagePolicy,
    },
}

impl BudgetPolicy {
    /// Check if the loop can create one more Attempt.
    #[allow(clippy::too_many_arguments)]
    pub fn check_can_attempt(
        &self,
        attempt_count: i64,
        no_progress_streak: i64,
        same_failure_streak: i64,
        profile_switch_count: i64,
        total_input_tokens: Option<i64>,
        total_output_tokens: Option<i64>,
        total_tokens: Option<i64>,
        total_tool_calls: Option<i64>,
        total_wall_time_ms: Option<i64>,
        total_cost_micros: Option<i64>,
        usage_known: bool,
    ) -> BudgetCheckResult {
        let check_hard =
            |val: Option<i64>, max_val: Option<u64>, mode: BudgetMode, name: &'static str| {
                if mode != BudgetMode::Hard {
                    return None;
                }
                let max = max_val?;
                match val {
                    Some(v) if v >= max as i64 => Some(BudgetCheckResult::Exhausted {
                        limit_name: name,
                        current: v,
                        max: max as i64,
                    }),
                    None if !usage_known => Some(BudgetCheckResult::Unknown {
                        limit_name: name,
                        policy: self.unknown_usage_policy,
                    }),
                    _ => None,
                }
            };

        // Hard attempt limit.
        if let Some(max) = self.max_attempts {
            if self.max_attempts_mode == BudgetMode::Hard && attempt_count >= max as i64 {
                return BudgetCheckResult::Exhausted {
                    limit_name: "max_attempts",
                    current: attempt_count,
                    max: max as i64,
                };
            }
        }

        // Hard no-progress streak.
        if let Some(max) = self.max_no_progress_streak {
            if self.max_no_progress_mode == BudgetMode::Hard && no_progress_streak >= max as i64 {
                return BudgetCheckResult::Exhausted {
                    limit_name: "max_no_progress_streak",
                    current: no_progress_streak,
                    max: max as i64,
                };
            }
        }

        // Hard same-failure streak.
        if let Some(max) = self.max_same_failure_streak {
            if self.max_same_failure_mode == BudgetMode::Hard && same_failure_streak >= max as i64 {
                return BudgetCheckResult::Exhausted {
                    limit_name: "max_same_failure_streak",
                    current: same_failure_streak,
                    max: max as i64,
                };
            }
        }

        // Hard profile switch limit.
        if let Some(max) = self.max_profile_switches {
            if self.max_profile_switches_mode == BudgetMode::Hard
                && profile_switch_count >= max as i64
            {
                return BudgetCheckResult::Exhausted {
                    limit_name: "max_profile_switches",
                    current: profile_switch_count,
                    max: max as i64,
                };
            }
        }

        // Per-dimension hard limits.
        if let Some(r) = check_hard(
            total_input_tokens,
            self.max_input_tokens,
            self.max_input_tokens_mode,
            "max_input_tokens",
        ) {
            return r;
        }
        if let Some(r) = check_hard(
            total_output_tokens,
            self.max_output_tokens,
            self.max_output_tokens_mode,
            "max_output_tokens",
        ) {
            return r;
        }
        if let Some(r) = check_hard(
            total_tokens,
            self.max_total_tokens,
            self.max_total_tokens_mode,
            "max_total_tokens",
        ) {
            return r;
        }
        if let Some(r) = check_hard(
            total_tool_calls,
            self.max_tool_calls,
            self.max_tool_calls_mode,
            "max_tool_calls",
        ) {
            return r;
        }
        if let Some(r) = check_hard(
            total_cost_micros,
            self.max_estimated_cost_micros,
            self.max_estimated_cost_micros_mode,
            "max_estimated_cost_micros",
        ) {
            return r;
        }
        if let Some(r) = check_hard(
            total_wall_time_ms,
            self.max_wall_time_ms,
            self.max_wall_time_mode,
            "max_wall_time",
        ) {
            return r;
        }

        BudgetCheckResult::Ok
    }
}

impl Default for BudgetPolicy {
    fn default() -> Self {
        Self {
            max_attempts: Some(10),
            max_attempts_mode: BudgetMode::Hard,
            max_wall_time_ms: Some(3_600_000),
            max_wall_time_mode: BudgetMode::Hard,
            max_tool_calls: None,
            max_tool_calls_mode: BudgetMode::ObserveOnly,
            max_input_tokens: None,
            max_input_tokens_mode: BudgetMode::ObserveOnly,
            max_output_tokens: None,
            max_output_tokens_mode: BudgetMode::ObserveOnly,
            max_total_tokens: None,
            max_total_tokens_mode: BudgetMode::ObserveOnly,
            max_estimated_cost_micros: None,
            max_estimated_cost_micros_mode: BudgetMode::ObserveOnly,
            max_no_progress_streak: Some(3),
            max_no_progress_mode: BudgetMode::Hard,
            max_same_failure_streak: Some(5),
            max_same_failure_mode: BudgetMode::Hard,
            max_profile_switches: Some(2),
            max_profile_switches_mode: BudgetMode::Hard,
            unknown_usage_policy: UnknownUsagePolicy::AllowWithWarning,
        }
    }
}
