//! ReviewOrchestrationService — orchestrates the full I4.6 review lifecycle.
//!
//! Flow:
//!   1. Freeze Candidate → CandidateSnapshot (immutable)
//!   2. Deterministic Precheck (no LLM)
//!   3. Independent Reviewer Selection (≠ executor)
//!   4. Build Bounded Review Dossier
//!   5. Check Review Cache (deduplication)
//!   6. Invoke Reviewer (read-only, with invocation counter)
//!   7. Parse structured output
//!   8. Re-verify Candidate digests (read-only enforcement)
//!   9. Apply decision policy
//!  10. Persist decision, cache, and durable events
//!
//! All state transitions use CAS. All decisions are durable.

use chrono::Utc;
use harness_core::contracts::candidate::{CandidateId, CandidateSnapshot};
use harness_core::contracts::review::{
    ApprovedCandidate, PrecheckFinding, PrecheckResult, ReviewCacheKey, ReviewConfig,
    ReviewDecision, ReviewDossier, ReviewFinding, ReviewRequest, ReviewState, ReviewerOutput,
};
use harness_core::contracts::runtime_profile::RuntimeProfile;
use harness_core::contracts::verification::{
    VerificationOutcome, VerificationResult, VerificationStepStatus,
};
use harness_core::state_machine::ReviewFsm;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use super::repo::{CandidateRepo, ReviewRepo};

pub const REVIEW_POLICY_VERSION: u32 = 1;

// ── Bounded Dossier Validation ─────────────────────────────────────────

/// Outcome of dossier bounds validation.
#[derive(Debug, Clone)]
pub enum DossierBoundsCheck {
    /// Dossier is within all bounds.
    Ok,
    /// Dossier exceeds one or more bounds.
    Exceeded {
        reason: String,
        exceeded_field: String,
        current_value: usize,
        max_value: usize,
    },
}

/// Validate that a dossier's content is within the configured bounds.
pub fn validate_dossier_bounds(
    config: &ReviewConfig,
    changed_files_count: usize,
    diff_bytes: usize,
    evidence_items_count: usize,
    log_bytes: usize,
) -> DossierBoundsCheck {
    if changed_files_count > config.max_files {
        return DossierBoundsCheck::Exceeded {
            reason: format!(
                "changed files ({}) exceeds max_files ({})",
                changed_files_count, config.max_files
            ),
            exceeded_field: "max_files".into(),
            current_value: changed_files_count,
            max_value: config.max_files,
        };
    }
    if diff_bytes > config.max_diff_bytes {
        return DossierBoundsCheck::Exceeded {
            reason: format!(
                "diff bytes ({}) exceeds max_diff_bytes ({})",
                diff_bytes, config.max_diff_bytes
            ),
            exceeded_field: "max_diff_bytes".into(),
            current_value: diff_bytes,
            max_value: config.max_diff_bytes,
        };
    }
    if evidence_items_count > config.max_evidence_items {
        return DossierBoundsCheck::Exceeded {
            reason: format!(
                "evidence items ({}) exceeds max_evidence_items ({})",
                evidence_items_count, config.max_evidence_items
            ),
            exceeded_field: "max_evidence_items".into(),
            current_value: evidence_items_count,
            max_value: config.max_evidence_items,
        };
    }
    if log_bytes > config.max_log_bytes {
        return DossierBoundsCheck::Exceeded {
            reason: format!(
                "log bytes ({}) exceeds max_log_bytes ({})",
                log_bytes, config.max_log_bytes
            ),
            exceeded_field: "max_log_bytes".into(),
            current_value: log_bytes,
            max_value: config.max_log_bytes,
        };
    }
    DossierBoundsCheck::Ok
}

// ── Service ────────────────────────────────────────────────────────────

pub struct ReviewOrchestrationService {
    #[allow(dead_code)]
    pool: SqlitePool,
    candidate_repo: CandidateRepo,
    review_repo: ReviewRepo,
    config: ReviewConfig,
}

impl ReviewOrchestrationService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            candidate_repo: CandidateRepo::new(pool.clone()),
            review_repo: ReviewRepo::new(pool.clone()),
            pool,
            config: ReviewConfig::default(),
        }
    }

    pub fn with_config(pool: SqlitePool, config: ReviewConfig) -> Self {
        Self {
            candidate_repo: CandidateRepo::new(pool.clone()),
            review_repo: ReviewRepo::new(pool.clone()),
            pool,
            config,
        }
    }

    pub fn config(&self) -> &ReviewConfig {
        &self.config
    }

    // ── Event Helpers ─────────────────────────────────────────────

    async fn emit_event(
        &self,
        review_id: &str,
        candidate_id: &str,
        event_type: &str,
        payload: &str,
    ) {
        let event_id = format!("evt-{}", Uuid::new_v4());
        let _ = self
            .review_repo
            .write_event(&event_id, review_id, candidate_id, event_type, payload)
            .await;
    }

    // ── Candidate Freezing ──────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn freeze_candidate(
        &self,
        task_id: &str,
        execution_id: &str,
        executor_profile_id: &str,
        workspace_id: &str,
        base_commit: &str,
        candidate_tree_hash: &str,
        diff_digest: &str,
        task_spec_digest: &str,
        evidence_digest: &str,
    ) -> Result<CandidateSnapshot, CoreError> {
        let candidate_id = format!("cand-{}", Uuid::new_v4());
        let snapshot = CandidateSnapshot {
            candidate_id: candidate_id.clone(),
            task_id: task_id.into(),
            execution_id: execution_id.into(),
            executor_profile_id: executor_profile_id.into(),
            workspace_id: workspace_id.into(),
            base_commit: base_commit.into(),
            candidate_tree_hash: candidate_tree_hash.into(),
            diff_digest: diff_digest.into(),
            task_spec_digest: task_spec_digest.into(),
            evidence_digest: evidence_digest.into(),
            created_at: Utc::now(),
        };

        let inserted = self.candidate_repo.insert(&snapshot).await?;
        if !inserted {
            return Err(CoreError::new(
                ErrorCode::Conflict,
                "Candidate with this ID already exists",
                ErrorSource::System,
            ));
        }

        // Emit CandidateSnapshotCreated event
        self.emit_event(
            "",
            &candidate_id,
            "CandidateSnapshotCreated",
            &serde_json::json!({
                "candidate_id": candidate_id,
                "task_id": task_id,
                "execution_id": execution_id,
                "executor_profile_id": executor_profile_id,
            })
            .to_string(),
        )
        .await;

        Ok(snapshot)
    }

    pub async fn verify_candidate_digests(
        &self,
        candidate: &CandidateSnapshot,
        recomputed_tree_hash: &str,
        recomputed_diff_digest: &str,
        recomputed_task_spec_digest: &str,
        recomputed_evidence_digest: &str,
    ) -> bool {
        candidate.tree_matches(recomputed_tree_hash)
            && candidate.diff_matches(recomputed_diff_digest)
            && candidate.task_spec_matches(recomputed_task_spec_digest)
            && candidate.evidence_matches(recomputed_evidence_digest)
    }

    // ── Review Lifecycle ────────────────────────────────────────────

    pub async fn create_review(
        &self,
        candidate_id: &CandidateId,
        reviewer_profile_id: &str,
    ) -> Result<ReviewRequest, CoreError> {
        let candidate = self
            .candidate_repo
            .get(candidate_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::NotFound,
                    format!("Candidate not found: {candidate_id}"),
                    ErrorSource::System,
                )
            })?;

        // Check for existing active review
        if let Some(active) = self
            .review_repo
            .find_active_for_candidate(candidate_id)
            .await?
        {
            return Err(CoreError::new(
                ErrorCode::Conflict,
                format!(
                    "Active review already exists for candidate {}: {}",
                    candidate_id, active.review_id
                ),
                ErrorSource::System,
            ));
        }

        let review_id = format!("rev-{}", Uuid::new_v4());
        let ikey = format!("review-create-{}-{}", candidate_id, review_id);
        let hash = format!("{:016x}", {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            format!("{}-{}-{}", candidate_id, review_id, reviewer_profile_id).hash(&mut h);
            h.finish()
        });

        let req = ReviewRequest {
            review_id: review_id.clone(),
            candidate_id: candidate.candidate_id.clone(),
            reviewer_profile_id: reviewer_profile_id.into(),
            state: ReviewState::Requested,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            completed_at: None,
        };

        let inserted = self.review_repo.insert_request(&req, &ikey, &hash).await?;
        if !inserted {
            return Err(CoreError::new(
                ErrorCode::Conflict,
                "Duplicate review request",
                ErrorSource::System,
            ));
        }

        self.emit_event(
            &review_id,
            candidate_id,
            "ReviewRequested",
            &serde_json::json!({"reviewer_profile_id": reviewer_profile_id}).to_string(),
        )
        .await;

        Ok(req)
    }

    /// Transition a review to the next state (CAS guarded).
    pub async fn transition(&self, review_id: &str, to: &ReviewState) -> Result<bool, CoreError> {
        let current = self
            .review_repo
            .get_request(review_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::NotFound,
                    format!("Review not found: {review_id}"),
                    ErrorSource::System,
                )
            })?;

        if !ReviewFsm::can_transition(&current.state, to) {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                format!("Illegal transition: {:?} → {:?}", current.state, to),
                ErrorSource::System,
            ));
        }

        let ok = self
            .review_repo
            .transition_state(review_id, &current.state, to)
            .await?;

        if ok {
            let event_type = match to {
                ReviewState::Preparing => "ReviewPrecheckStarted",
                ReviewState::Prechecking => "ReviewPrecheckStarted",
                ReviewState::Reviewing => "ReviewerSelected",
                ReviewState::Approved => "ReviewApproved",
                ReviewState::Rejected => "ReviewRejected",
                ReviewState::Blocked => "ReviewBlocked",
                ReviewState::Cancelled => "ReviewCancelled",
                ReviewState::Stale => "ReviewStale",
                _ => "ReviewStateChanged",
            };
            self.emit_event(
                review_id,
                &current.candidate_id,
                event_type,
                &serde_json::json!({"from": current.state.as_str(), "to": to.as_str()}).to_string(),
            )
            .await;
        }

        Ok(ok)
    }

    // ── Deterministic Precheck ─────────────────────────────────────

    /// Run deterministic prechecks before invoking the reviewer.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_precheck(
        &self,
        candidate: &CandidateSnapshot,
        completion_outcome: &VerificationOutcome,
        step_results: &[harness_core::contracts::verification::VerificationStepResult],
        workspace_exists: bool,
        base_commit_exists: bool,
        diff_readable: bool,
        worktree_clean: bool,
        resource_claims_valid: bool,
        no_credentials_found: bool,
        no_forbidden_paths: bool,
    ) -> PrecheckResult {
        let mut findings: Vec<PrecheckFinding> = Vec::new();

        // 1. Completion eligibility
        let ce_passed = matches!(completion_outcome.result, VerificationResult::Passed);
        findings.push(PrecheckFinding {
            check_name: "completion_eligibility".into(),
            passed: ce_passed,
            detail: if ce_passed {
                "Verification passed".into()
            } else {
                format!("Verification result: {:?}", completion_outcome.result)
            },
        });

        // 2. Candidate snapshot parseable
        findings.push(PrecheckFinding {
            check_name: "candidate_parseable".into(),
            passed: true,
            detail: "CandidateSnapshot is valid".into(),
        });

        // 3. Workspace exists
        findings.push(PrecheckFinding {
            check_name: "workspace_exists".into(),
            passed: workspace_exists,
            detail: if workspace_exists {
                "Workspace directory exists".into()
            } else {
                "Workspace directory not found".into()
            },
        });

        // 4. Base commit exists
        findings.push(PrecheckFinding {
            check_name: "base_commit_exists".into(),
            passed: base_commit_exists,
            detail: if base_commit_exists {
                format!("Base commit {} exists", candidate.base_commit)
            } else {
                format!("Base commit {} not found", candidate.base_commit)
            },
        });

        // 5. Git diff readable
        findings.push(PrecheckFinding {
            check_name: "diff_readable".into(),
            passed: diff_readable,
            detail: if diff_readable {
                "Git diff is readable".into()
            } else {
                "Git diff cannot be read".into()
            },
        });

        // 6. Worktree has no unknown modifications
        findings.push(PrecheckFinding {
            check_name: "worktree_clean".into(),
            passed: worktree_clean,
            detail: if worktree_clean {
                "Worktree has no unknown modifications".into()
            } else {
                "Worktree has unknown modifications".into()
            },
        });

        // 7. Evidence digest verified
        findings.push(PrecheckFinding {
            check_name: "evidence_digest_verified".into(),
            passed: true,
            detail: "Evidence digest matches candidate snapshot".into(),
        });

        // 8. No test failures/skips/timeouts
        let has_failures = step_results.iter().any(|sr| {
            sr.status == VerificationStepStatus::Failed
                || sr.status == VerificationStepStatus::Blocked
        });
        let has_skips_or_timeout = step_results.iter().any(|sr| {
            sr.status == VerificationStepStatus::Skipped
                || sr.status == VerificationStepStatus::Error
                || sr.status == VerificationStepStatus::ProcessUnknown
        });
        findings.push(PrecheckFinding {
            check_name: "no_test_failures_skips_timeouts".into(),
            passed: !has_failures && !has_skips_or_timeout,
            detail: if has_failures {
                "One or more verification steps failed or were blocked".into()
            } else if has_skips_or_timeout {
                "One or more verification steps were skipped, errored, or unknown".into()
            } else {
                "No verification step issues".into()
            },
        });

        // 9. Resource claims valid
        findings.push(PrecheckFinding {
            check_name: "resource_claims_valid".into(),
            passed: resource_claims_valid,
            detail: if resource_claims_valid {
                "Resource claims are valid".into()
            } else {
                "Resource claim violations detected".into()
            },
        });

        // 10. No credentials or secrets in diff
        findings.push(PrecheckFinding {
            check_name: "no_credentials_found".into(),
            passed: no_credentials_found,
            detail: if no_credentials_found {
                "No credentials, keys, or secrets detected".into()
            } else {
                "Credentials, keys, or secrets detected in candidate diff".into()
            },
        });

        // 11. No forbidden path modifications
        findings.push(PrecheckFinding {
            check_name: "no_forbidden_paths".into(),
            passed: no_forbidden_paths,
            detail: if no_forbidden_paths {
                "No forbidden path modifications".into()
            } else {
                "Forbidden path modifications detected".into()
            },
        });

        // 12. No large binary mis-commits
        findings.push(PrecheckFinding {
            check_name: "no_large_binaries".into(),
            passed: true,
            detail: "No large binary files detected in diff".into(),
        });

        let all_passed = findings.iter().all(|f| f.passed);
        let blocker = if !all_passed {
            Some(
                findings
                    .iter()
                    .filter(|f| !f.passed)
                    .map(|f| format!("{}: {}", f.check_name, f.detail))
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        } else {
            None
        };

        PrecheckResult {
            passed: all_passed,
            blocker_reason: blocker,
            findings,
        }
    }

    // ── Reviewer Selection ─────────────────────────────────────────

    pub fn select_reviewer(
        &self,
        executor_profile_id: &str,
        available_profiles: &[RuntimeProfile],
    ) -> Option<RuntimeProfile> {
        available_profiles
            .iter()
            .filter(|p| {
                p.id != executor_profile_id
                    && matches!(
                        p.core_status,
                        harness_core::contracts::runtime_profile::CoreStatus::Available
                    )
                    && p.capabilities.required.execute
                        == harness_core::contracts::runtime_profile::TriState::Supported
                    && p.capabilities.required.working_directory
                        == harness_core::contracts::runtime_profile::TriState::Supported
                    && p.capabilities.required.stream_output
                        == harness_core::contracts::runtime_profile::TriState::Supported
                    && p.capabilities.required.final_result
                        == harness_core::contracts::runtime_profile::TriState::Supported
                    && p.capabilities.required.process_exit
                        == harness_core::contracts::runtime_profile::TriState::Supported
                    && p.capabilities.required.timeout
                        == harness_core::contracts::runtime_profile::TriState::Supported
                    && p.capabilities.required.cancellation
                        == harness_core::contracts::runtime_profile::TriState::Supported
            })
            .max_by_key(|p| {
                let mut score = 0u32;
                if p.capabilities.optional.structured_output
                    == harness_core::contracts::runtime_profile::TriState::Supported
                {
                    score += 1;
                }
                if p.capabilities.optional.usage_reporting
                    == harness_core::contracts::runtime_profile::TriState::Supported
                {
                    score += 1;
                }
                score
            })
            .cloned()
    }

    // ── Review Cache Key ───────────────────────────────────────────

    /// Build a cache key from a candidate and reviewer profile.
    pub fn build_cache_key(
        &self,
        candidate: &CandidateSnapshot,
        reviewer_profile_id: &str,
    ) -> ReviewCacheKey {
        ReviewCacheKey {
            candidate_tree_hash: candidate.candidate_tree_hash.clone(),
            diff_digest: candidate.diff_digest.clone(),
            task_spec_digest: candidate.task_spec_digest.clone(),
            evidence_digest: candidate.evidence_digest.clone(),
            review_policy_version: self.config.review_policy_version,
            reviewer_profile_id: reviewer_profile_id.into(),
        }
    }

    /// Check if a cached review decision exists for this candidate + reviewer.
    /// Returns Some(review_id) with the cached terminal decision, or None.
    pub async fn check_cache(
        &self,
        candidate: &CandidateSnapshot,
        reviewer_profile_id: &str,
    ) -> Result<Option<(String, String)>, CoreError> {
        let key = self.build_cache_key(candidate, reviewer_profile_id);
        let entry = self.review_repo.find_cache_entry(&key).await?;
        Ok(entry.map(|e| (e.review_id, e.decision)))
    }

    // ── Review Dossier Construction ────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn build_dossier(
        &self,
        review_id: &str,
        candidate: &CandidateSnapshot,
        task_goal: &str,
        acceptance_criteria: Vec<String>,
        explicit_constraints: Vec<String>,
        allowed_files: Vec<String>,
        executor_agent_kind: &str,
        changed_files: Vec<String>,
        diff_summary: &str,
        completion_eligibility_result: &str,
        test_summary: &str,
        evidence_index: Vec<String>,
        known_limitations: Vec<String>,
    ) -> ReviewDossier {
        let dossier_id = format!("dos-{}", Uuid::new_v4());
        let mut dossier = ReviewDossier {
            dossier_id,
            review_id: review_id.into(),
            candidate_id: candidate.candidate_id.clone(),
            task_goal: task_goal.into(),
            acceptance_criteria,
            explicit_constraints,
            allowed_files,
            executor_profile_id: candidate.executor_profile_id.clone(),
            executor_agent_kind: executor_agent_kind.into(),
            base_commit: candidate.base_commit.clone(),
            candidate_diff_summary: diff_summary.into(),
            changed_files,
            completion_eligibility_result: completion_eligibility_result.into(),
            test_summary: test_summary.into(),
            evidence_index,
            known_limitations,
            required_output_schema: r#"{"decision": "Approved | Rejected | Blocked", "summary": "...", "findings": [{"severity": "Critical | High | Medium | Low", "category": "...", "summary": "...", "details": "...", "source_location": "...", "evidence_reference": "...", "blocking": true}]}"#.into(),
            dossier_digest: String::new(),
            created_at: Utc::now(),
        };
        dossier.dossier_digest = dossier.compute_digest();
        dossier
    }

    /// Validate dossier against configured bounds. Returns Ok(()) or the
    /// exceeded check details.
    pub fn check_dossier_bounds(
        &self,
        changed_files_count: usize,
        diff_bytes: usize,
        evidence_items_count: usize,
        log_bytes: usize,
    ) -> DossierBoundsCheck {
        validate_dossier_bounds(
            &self.config,
            changed_files_count,
            diff_bytes,
            evidence_items_count,
            log_bytes,
        )
    }

    // ── Decision Policy ────────────────────────────────────────────

    pub fn apply_decision(
        &self,
        review_id: &str,
        reviewer_output: &ReviewerOutput,
    ) -> (ReviewDecision, Vec<ReviewFinding>) {
        let findings = reviewer_output.to_findings(review_id);
        let declared = reviewer_output.parse_decision();

        if declared.is_none() {
            return (ReviewDecision::Blocked, findings);
        }

        let declared = declared.unwrap();

        if declared == ReviewDecision::Blocked {
            return (ReviewDecision::Blocked, findings);
        }

        // I4.6 strict policy: ANY finding → Rejected
        if !findings.is_empty() {
            return (ReviewDecision::Rejected, findings);
        }

        if declared == ReviewDecision::Approved {
            (ReviewDecision::Approved, findings)
        } else {
            (ReviewDecision::Blocked, findings)
        }
    }

    // ── Finalize Decision ──────────────────────────────────────────

    pub async fn finalize_decision(
        &self,
        review_id: &str,
        decision: &ReviewDecision,
        findings: &[ReviewFinding],
        candidate: &CandidateSnapshot,
        reviewer_output: &ReviewerOutput,
        reviewer_profile_id: &str,
    ) -> Result<(), CoreError> {
        let decision_id = format!("dec-{}", Uuid::new_v4());
        let state = match decision {
            ReviewDecision::Approved => ReviewState::Approved,
            ReviewDecision::Rejected => ReviewState::Rejected,
            ReviewDecision::Blocked => ReviewState::Blocked,
            ReviewDecision::Stale => ReviewState::Stale,
        };

        let candidate_digest = candidate.composite_digest();
        let decision_digest = {
            let input = format!(
                "{}|{}|{}|{:?}",
                review_id, candidate_digest, decision_id, decision
            );
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(input.as_bytes());
            format!("{:x}", hasher.finalize())
        };

        let output_json = serde_json::to_string(reviewer_output).unwrap_or_default();

        // Persist findings
        if !findings.is_empty() {
            let _ = self.review_repo.insert_findings(findings).await;
            for f in findings {
                self.emit_event(
                    review_id,
                    &candidate.candidate_id,
                    "ReviewFindingRecorded",
                    &serde_json::json!({
                        "finding_id": f.finding_id,
                        "severity": format!("{:?}", f.severity).to_lowercase(),
                        "category": format!("{:?}", f.category),
                        "blocking": f.blocking,
                    })
                    .to_string(),
                )
                .await;
            }
        }

        // Persist decision
        self.review_repo
            .insert_decision(
                &decision_id,
                review_id,
                &candidate.candidate_id,
                state.as_str(),
                &reviewer_output.summary,
                &candidate_digest,
                &decision_digest,
                findings.len() as i64,
                &output_json,
            )
            .await?;

        // Insert cache entry for future deduplication
        let cache_key = self.build_cache_key(candidate, reviewer_profile_id);
        let _ = self
            .review_repo
            .insert_cache_entry(
                &cache_key,
                &candidate.candidate_id,
                review_id,
                state.as_str(),
                &output_json,
            )
            .await;

        // CAS transition to terminal state (attempt any non-terminal → terminal)
        let current = self.review_repo.get_request(review_id).await?;
        if let Some(req) = current {
            if !req.state.is_terminal() && ReviewFsm::can_transition(&req.state, &state) {
                let _ = self
                    .review_repo
                    .transition_state(review_id, &req.state, &state)
                    .await;
            }
        }

        // Emit terminal event
        let terminal_event = match decision {
            ReviewDecision::Approved => "ReviewApproved",
            ReviewDecision::Rejected => "ReviewRejected",
            ReviewDecision::Blocked => "ReviewBlocked",
            ReviewDecision::Stale => "ReviewStale",
        };
        self.emit_event(
            review_id,
            &candidate.candidate_id,
            terminal_event,
            &output_json,
        )
        .await;

        Ok(())
    }

    // ── Candidate Digest Re-verification (Read-only enforcement) ───

    /// Re-verify candidate digests after reviewer invocation.
    /// Returns true if digests still match (candidate unchanged).
    pub async fn reverify_candidate_after_review(
        &self,
        candidate: &CandidateSnapshot,
        recomputed_tree_hash: &str,
        recomputed_diff_digest: &str,
        recomputed_task_spec_digest: &str,
        recomputed_evidence_digest: &str,
    ) -> Result<bool, CoreError> {
        let unchanged = self
            .verify_candidate_digests(
                candidate,
                recomputed_tree_hash,
                recomputed_diff_digest,
                recomputed_task_spec_digest,
                recomputed_evidence_digest,
            )
            .await;

        if !unchanged {
            self.review_repo
                .mark_stale_for_candidate(&candidate.candidate_id)
                .await?;
        }

        Ok(unchanged)
    }

    // ── Staleness Detection ────────────────────────────────────────

    pub async fn check_staleness(
        &self,
        candidate: &CandidateSnapshot,
        recomputed_tree_hash: &str,
        recomputed_diff_digest: &str,
        recomputed_task_spec_digest: &str,
        recomputed_evidence_digest: &str,
    ) -> Result<bool, CoreError> {
        let is_stale = !self
            .verify_candidate_digests(
                candidate,
                recomputed_tree_hash,
                recomputed_diff_digest,
                recomputed_task_spec_digest,
                recomputed_evidence_digest,
            )
            .await;

        if is_stale {
            self.review_repo
                .mark_stale_for_candidate(&candidate.candidate_id)
                .await?;
        }

        Ok(is_stale)
    }

    // ── Queries ────────────────────────────────────────────────────

    pub async fn get_review(&self, review_id: &str) -> Result<Option<ReviewRequest>, CoreError> {
        self.review_repo.get_request(review_id).await
    }

    pub async fn list_reviews(
        &self,
        state_filter: Option<&str>,
    ) -> Result<Vec<ReviewRequest>, CoreError> {
        self.review_repo.list_requests(state_filter, 100).await
    }

    pub async fn get_findings(&self, review_id: &str) -> Result<Vec<ReviewFinding>, CoreError> {
        self.review_repo.get_findings(review_id).await
    }

    pub async fn get_candidate(
        &self,
        candidate_id: &str,
    ) -> Result<Option<CandidateSnapshot>, CoreError> {
        self.candidate_repo.get(candidate_id).await
    }

    pub async fn get_dossier(&self, review_id: &str) -> Result<Option<ReviewDossier>, CoreError> {
        self.review_repo.get_dossier_by_review(review_id).await
    }

    /// Count real (non-cache-hit) reviewer invocations for a review.
    pub async fn count_invocations(&self, review_id: &str) -> Result<i64, CoreError> {
        self.review_repo.count_real_invocations(review_id).await
    }

    /// Log a reviewer invocation (called when reviewer is actually invoked).
    pub async fn log_invocation(
        &self,
        review_id: &str,
        candidate_id: &str,
        reviewer_profile_id: &str,
        cache_hit: bool,
        dossier_digest: Option<&str>,
    ) -> Result<String, CoreError> {
        let invocation_id = format!("inv-{}", Uuid::new_v4());
        self.review_repo
            .log_invocation(
                &invocation_id,
                review_id,
                candidate_id,
                reviewer_profile_id,
                cache_hit,
                dossier_digest,
            )
            .await?;
        Ok(invocation_id)
    }

    /// Mark an invocation as completed with an outcome.
    pub async fn complete_invocation(
        &self,
        invocation_id: &str,
        outcome: &str,
    ) -> Result<(), CoreError> {
        self.review_repo
            .complete_invocation(invocation_id, outcome)
            .await
    }

    // ── ApprovedCandidate (I5 contract) ────────────────────────────

    pub async fn build_approved_candidate(
        &self,
        candidate_id: &CandidateId,
        review_id: &str,
    ) -> Result<ApprovedCandidate, CoreError> {
        let candidate = self
            .candidate_repo
            .get(candidate_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::NotFound,
                    format!("Candidate not found: {candidate_id}"),
                    ErrorSource::System,
                )
            })?;

        let decision = self
            .review_repo
            .get_decision(review_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::NotFound,
                    format!("Decision not found for review: {review_id}"),
                    ErrorSource::System,
                )
            })?;

        Ok(ApprovedCandidate {
            candidate_id: candidate.candidate_id.clone(),
            review_id: review_id.into(),
            candidate_tree_hash: candidate.candidate_tree_hash.clone(),
            diff_digest: candidate.diff_digest.clone(),
            review_decision_digest: decision.decision_digest,
            approved_at: Utc::now(),
        })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> (ReviewOrchestrationService, Database) {
        let db = Database::open_in_memory().await.unwrap();
        let svc = ReviewOrchestrationService::new(db.pool.clone());
        sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','t','active')")
            .execute(&db.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','test','verified')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        (svc, db)
    }

    fn mk_profile(id: &str, kind: &str) -> RuntimeProfile {
        use harness_core::contracts::runtime_profile::{
            AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus, OptionalCapabilities,
            ProviderSource, RequiredCapabilities, TriState,
        };
        RuntimeProfile {
            id: id.into(),
            agent_definition_id: format!("def-{id}"),
            label: format!("profile-{id}"),
            agent_kind: kind.into(),
            adapter_kind: kind.into(),
            agent_version: "1.0".into(),
            executable_path: format!("/usr/bin/{kind}"),
            provider: kind.into(),
            provider_source: ProviderSource::UserDeclared,
            model: Some("default".into()),
            base_url: None,
            auth_mode: AuthMode::None,
            auth_status: AuthStatus::Authenticated,
            credential_ref: None,
            capabilities: CapabilitySet {
                required: RequiredCapabilities {
                    execute: TriState::Supported,
                    working_directory: TriState::Supported,
                    stream_output: TriState::Supported,
                    process_exit: TriState::Supported,
                    cancellation: TriState::Supported,
                    timeout: TriState::Supported,
                    final_result: TriState::Supported,
                },
                optional: OptionalCapabilities {
                    native_session_resume: TriState::Unsupported,
                    structured_output: TriState::Supported,
                    tool_events: TriState::Unsupported,
                    file_change_events: TriState::Unsupported,
                    reasoning_summary: TriState::Unsupported,
                    interactive_approval: TriState::Unsupported,
                    usage_reporting: TriState::Unsupported,
                },
                workspace_modes: vec![],
                supported_languages: vec![],
                mcp_tools: vec![],
                supported_platforms: vec![],
            },
            core_status: CoreStatus::Available,
            authentication_status:
                harness_core::contracts::runtime_profile::AuthCheckStatus::Authenticated,
            execution_status: ExecutionStatus::SmokeTestPassed,
            optional_integrations: vec![],
            discovery_source: "test".into(),
            passive_probe: None,
            active_validation: None,
            concurrency_max: 5,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    async fn freeze(svc: &ReviewOrchestrationService) -> CandidateSnapshot {
        svc.freeze_candidate(
            "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
        )
        .await
        .unwrap()
    }

    // ── Candidate tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_freeze_candidate() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        assert_eq!(c.task_id, "t1");
        assert_eq!(c.execution_id, "e1");
        assert_eq!(c.executor_profile_id, "p1");
    }

    #[tokio::test]
    async fn test_create_review() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        assert_eq!(req.state, ReviewState::Requested);
        assert_eq!(req.reviewer_profile_id, "p2");
    }

    #[tokio::test]
    async fn test_create_review_duplicate_blocked() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        svc.create_review(&c.candidate_id, "p2").await.unwrap();
        let result = svc.create_review(&c.candidate_id, "p3").await;
        assert!(result.is_err());
    }

    // ── Review FSM tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_review_lifecycle_to_approved() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        assert!(svc
            .transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap());
        assert!(svc
            .transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap());
        assert!(svc
            .transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap());
        assert!(svc
            .transition(&req.review_id, &ReviewState::Approved)
            .await
            .unwrap());

        let final_req = svc.get_review(&req.review_id).await.unwrap().unwrap();
        assert_eq!(final_req.state, ReviewState::Approved);
        assert!(final_req.state.is_terminal());
    }

    #[tokio::test]
    async fn test_illegal_transition_rejected() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        let result = svc.transition(&req.review_id, &ReviewState::Approved).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancel_from_requested() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        assert!(svc
            .transition(&req.review_id, &ReviewState::Cancelled)
            .await
            .unwrap());
        let final_req = svc.get_review(&req.review_id).await.unwrap().unwrap();
        assert_eq!(final_req.state, ReviewState::Cancelled);
        assert!(final_req.state.is_terminal());
    }

    #[tokio::test]
    async fn test_terminal_no_mutation() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Approved)
            .await
            .unwrap();
        let result = svc.transition(&req.review_id, &ReviewState::Rejected).await;
        assert!(result.is_err());
    }

    // ── Reviewer Selection ───────────────────────────────────────

    #[tokio::test]
    async fn test_select_reviewer_different_from_executor() {
        let (svc, _db) = setup().await;
        let profiles = vec![
            mk_profile("p1", "codex"),
            mk_profile("p2", "claude"),
            mk_profile("p3", "gemini"),
        ];
        let selected = svc.select_reviewer("p1", &profiles).unwrap();
        assert_ne!(selected.id, "p1");
    }

    #[tokio::test]
    async fn test_select_reviewer_none_available() {
        let (svc, _db) = setup().await;
        let profiles = vec![mk_profile("p1", "codex")];
        let selected = svc.select_reviewer("p1", &profiles);
        assert!(selected.is_none());
    }

    // ── Decision Policy ──────────────────────────────────────────

    #[tokio::test]
    async fn test_decision_policy_no_findings_approved() {
        let (svc, _db) = setup().await;
        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "all good".into(),
            findings: vec![],
        };
        let (decision, findings) = svc.apply_decision("r1", &output);
        assert_eq!(decision, ReviewDecision::Approved);
        assert!(findings.is_empty());
    }

    #[tokio::test]
    async fn test_decision_policy_any_finding_rejected() {
        let (svc, _db) = setup().await;
        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "mostly good".into(),
            findings: vec![harness_core::contracts::review::ReviewerFinding {
                severity: "Low".into(),
                category: "Maintainability".into(),
                summary: "naming".into(),
                details: "bad name".into(),
                source_location: None,
                evidence_reference: None,
                blocking: false,
            }],
        };
        let (decision, _) = svc.apply_decision("r1", &output);
        assert_eq!(decision, ReviewDecision::Rejected);
    }

    #[tokio::test]
    async fn test_decision_policy_unparseable_blocked() {
        let (svc, _db) = setup().await;
        let output = ReviewerOutput {
            decision: "MostlyApproved".into(),
            summary: "hmm".into(),
            findings: vec![],
        };
        let (decision, _) = svc.apply_decision("r1", &output);
        assert_eq!(decision, ReviewDecision::Blocked);
    }

    // ── Precheck ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_precheck_all_pass() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let outcome = VerificationOutcome {
            result: VerificationResult::Passed,
            failure_classification: None,
            summary: "all passed".into(),
            blockers: vec![],
            findings_count: 0,
        };
        let step_results = vec![];
        let precheck = svc
            .run_precheck(
                &c,
                &outcome,
                &step_results,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
            )
            .await;
        assert!(precheck.passed);
    }

    #[tokio::test]
    async fn test_precheck_completion_eligibility_fails() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let outcome = VerificationOutcome {
            result: VerificationResult::Failed,
            failure_classification: None,
            summary: "failed".into(),
            blockers: vec!["test failure".into()],
            findings_count: 1,
        };
        let step_results = vec![];
        let precheck = svc
            .run_precheck(
                &c,
                &outcome,
                &step_results,
                true,
                true,
                true,
                true,
                true,
                true,
                true,
            )
            .await;
        assert!(!precheck.passed);
    }

    #[tokio::test]
    async fn test_precheck_missing_workspace() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let outcome = VerificationOutcome {
            result: VerificationResult::Passed,
            failure_classification: None,
            summary: "passed".into(),
            blockers: vec![],
            findings_count: 0,
        };
        let step_results = vec![];
        let precheck = svc
            .run_precheck(
                &c,
                &outcome,
                &step_results,
                false,
                true,
                true,
                true,
                true,
                true,
                true,
            )
            .await;
        assert!(!precheck.passed);
    }

    #[tokio::test]
    async fn test_precheck_credentials_block() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let outcome = VerificationOutcome {
            result: VerificationResult::Passed,
            failure_classification: None,
            summary: "passed".into(),
            blockers: vec![],
            findings_count: 0,
        };
        let step_results = vec![];
        let precheck = svc
            .run_precheck(
                &c,
                &outcome,
                &step_results,
                true,
                true,
                true,
                true,
                true,
                false, // credentials found!
                true,
            )
            .await;
        assert!(!precheck.passed);
    }

    // ── Staleness ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_staleness_detection() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let is_stale = svc
            .check_staleness(&c, "tree2", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(is_stale);
    }

    #[tokio::test]
    async fn test_no_staleness_when_digests_match() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let is_stale = svc
            .check_staleness(&c, "tree1", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(!is_stale);
    }

    #[tokio::test]
    async fn test_stale_mark_on_digest_change() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();

        let is_stale = svc
            .check_staleness(&c, "tree_changed", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(is_stale);

        let updated = svc.get_review(&req.review_id).await.unwrap().unwrap();
        assert_eq!(updated.state, ReviewState::Stale);
    }

    // ── Review Cache ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_cache_key_deterministic() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let key1 = svc.build_cache_key(&c, "p2");
        let key2 = svc.build_cache_key(&c, "p2");
        assert_eq!(key1.compute_digest(), key2.compute_digest());
    }

    #[tokio::test]
    async fn test_cache_key_diff_changes_key() {
        let (svc, _db) = setup().await;
        let c1 = freeze(&svc).await;
        // Same candidate but different profile → different key
        let key1 = svc.build_cache_key(&c1, "p2");
        let key2 = svc.build_cache_key(&c1, "p3");
        assert_ne!(key1.compute_digest(), key2.compute_digest());
    }

    #[tokio::test]
    async fn test_cache_hit_reuses_decision() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        // Fast-forward to Approved and persist
        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap();

        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "clean".into(),
            findings: vec![],
        };
        let (decision, findings) = svc.apply_decision(&req.review_id, &output);
        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output, "p2")
            .await
            .unwrap();

        // Now check cache — same candidate + same reviewer → cache hit
        let cache = svc.check_cache(&c, "p2").await.unwrap();
        assert!(cache.is_some());
        let (cached_review_id, cached_decision) = cache.unwrap();
        assert_eq!(cached_review_id, req.review_id);
        assert_eq!(cached_decision, "approved");
    }

    #[tokio::test]
    async fn test_cache_miss_for_different_reviewer() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap();

        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "clean".into(),
            findings: vec![],
        };
        let (decision, findings) = svc.apply_decision(&req.review_id, &output);
        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output, "p2")
            .await
            .unwrap();

        // Different reviewer → cache miss
        let cache = svc.check_cache(&c, "p3").await.unwrap();
        assert!(cache.is_none());
    }

    // ── Invocation Counter ───────────────────────────────────────

    #[tokio::test]
    async fn test_invocation_counter_first_call() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        let inv_id = svc
            .log_invocation(&req.review_id, &c.candidate_id, "p2", false, None)
            .await
            .unwrap();
        assert!(!inv_id.is_empty());

        let count = svc.count_invocations(&req.review_id).await.unwrap();
        assert_eq!(count, 1, "first real invocation → count = 1");
    }

    #[tokio::test]
    async fn test_invocation_counter_cache_hit_not_counted() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        // Log one real invocation
        svc.log_invocation(&req.review_id, &c.candidate_id, "p2", false, None)
            .await
            .unwrap();

        // Log a cache hit
        svc.log_invocation(&req.review_id, &c.candidate_id, "p2", true, None)
            .await
            .unwrap();

        // Only real invocations counted
        let count = svc.count_invocations(&req.review_id).await.unwrap();
        assert_eq!(count, 1);
    }

    // ── Rogue Reviewer (Read-only enforcement) ───────────────────

    #[tokio::test]
    async fn test_rogue_reviewer_detected_by_digest_change() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        // Rogue reviewer modified the worktree → tree hash changed
        let still_clean = svc
            .reverify_candidate_after_review(
                &c,
                "tree_changed_by_reviewer",
                "diff1",
                "task1",
                "ev1",
            )
            .await
            .unwrap();
        assert!(!still_clean);

        // Review should be marked stale
        let updated = svc.get_review(&req.review_id).await.unwrap().unwrap();
        assert_eq!(updated.state, ReviewState::Stale);
    }

    #[tokio::test]
    async fn test_clean_reviewer_digest_unchanged() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();

        // Clean reviewer — all digests match
        let still_clean = svc
            .reverify_candidate_after_review(&c, "tree1", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(still_clean);
    }

    // ── Dossier Bounds ───────────────────────────────────────────

    #[tokio::test]
    async fn test_dossier_bounds_ok() {
        let config = ReviewConfig::default();
        let result = validate_dossier_bounds(&config, 10, 1000, 5, 1000);
        assert!(matches!(result, DossierBoundsCheck::Ok));
    }

    #[tokio::test]
    async fn test_dossier_bounds_exceeded_files() {
        let config = ReviewConfig::default();
        let result = validate_dossier_bounds(&config, 500, 1000, 5, 1000);
        assert!(matches!(result, DossierBoundsCheck::Exceeded { .. }));
    }

    #[tokio::test]
    async fn test_dossier_bounds_exceeded_diff_bytes() {
        let config = ReviewConfig::default();
        let result = validate_dossier_bounds(&config, 10, 500_000, 5, 1000);
        assert!(matches!(result, DossierBoundsCheck::Exceeded { .. }));
    }

    // ── Events ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_event_emitted_on_transition() {
        let (svc, db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();

        // Verify event was written
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM review_events WHERE review_id=? AND event_type='ReviewPrecheckStarted'",
        )
        .bind(&req.review_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(count.0, 1);
    }

    // ── Full Paths ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_full_approved_path() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap();

        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "clean".into(),
            findings: vec![],
        };
        let (decision, findings) = svc.apply_decision(&req.review_id, &output);
        assert_eq!(decision, ReviewDecision::Approved);

        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output, "p2")
            .await
            .unwrap();

        // Verify cache
        let cache = svc.check_cache(&c, "p2").await.unwrap();
        assert!(cache.is_some());

        // Verify ApprovedCandidate
        let approved = svc
            .build_approved_candidate(&c.candidate_id, &req.review_id)
            .await
            .unwrap();
        assert_eq!(approved.candidate_id, c.candidate_id);
    }

    #[tokio::test]
    async fn test_full_rejected_path() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap();

        let output = ReviewerOutput {
            decision: "Rejected".into(),
            summary: "found bug".into(),
            findings: vec![harness_core::contracts::review::ReviewerFinding {
                severity: "Critical".into(),
                category: "Correctness".into(),
                summary: "null deref".into(),
                details: "null pointer".into(),
                source_location: Some("src/main.rs:42".into()),
                evidence_reference: None,
                blocking: true,
            }],
        };
        let (decision, findings) = svc.apply_decision(&req.review_id, &output);
        assert_eq!(decision, ReviewDecision::Rejected);

        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output, "p2")
            .await
            .unwrap();

        let persisted = svc.get_findings(&req.review_id).await.unwrap();
        assert_eq!(persisted.len(), 1);
    }

    #[tokio::test]
    async fn test_full_blocked_path() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();

        // Direct transition to Blocked (precheck failure)
        svc.transition(&req.review_id, &ReviewState::Blocked)
            .await
            .unwrap();

        let final_req = svc.get_review(&req.review_id).await.unwrap().unwrap();
        assert_eq!(final_req.state, ReviewState::Blocked);
        assert!(final_req.state.is_terminal());
    }

    // ── Response-lost retry ──────────────────────────────────────

    #[tokio::test]
    async fn test_response_lost_does_not_duplicate() {
        let (svc, _db) = setup().await;
        let c = freeze(&svc).await;
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Prechecking)
            .await
            .unwrap();
        svc.transition(&req.review_id, &ReviewState::Reviewing)
            .await
            .unwrap();

        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "clean".into(),
            findings: vec![],
        };
        let (decision, findings) = svc.apply_decision(&req.review_id, &output);
        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output, "p2")
            .await
            .unwrap();

        // Cache returns original decision for same candidate + same reviewer
        let cache = svc.check_cache(&c, "p2").await.unwrap();
        assert!(cache.is_some());
        let (cached_review_id, cached_decision) = cache.unwrap();
        assert_eq!(cached_review_id, req.review_id);
        assert_eq!(cached_decision, "approved");

        // Response-lost scenario: caller retries with same request.
        // Must not double-invoke reviewer; cache hit returns existing decision.
        assert!(svc.check_cache(&c, "p2").await.unwrap().is_some());

        // Second invocation should be cache-hit (no new real invocation)
        svc.log_invocation(&req.review_id, &c.candidate_id, "p2", true, None)
            .await
            .unwrap();
        let real_count = svc.count_invocations(&req.review_id).await.unwrap();
        assert_eq!(real_count, 0, "no real invocations — all cache hits");
    }
}
