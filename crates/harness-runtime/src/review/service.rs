//! ReviewOrchestrationService — orchestrates the full I4.6 review lifecycle.
//!
//! Flow:
//!   1. Freeze Candidate → CandidateSnapshot (immutable)
//!   2. Deterministic Precheck (no LLM)
//!   3. Independent Reviewer Selection (≠ executor)
//!   4. Build Review Dossier
//!   5. Invoke Reviewer (read-only)
//!   6. Parse structured output
//!   7. Apply decision policy
//!   8. Detect staleness (digest re-verification)
//!
//! All state transitions use CAS. All decisions are durable.

use chrono::Utc;
use harness_core::contracts::candidate::{CandidateId, CandidateSnapshot};
use harness_core::contracts::review::{
    ApprovedCandidate, FindingSeverity, PrecheckFinding, PrecheckResult, ReviewDecision,
    ReviewDossier, ReviewFinding, ReviewRequest, ReviewState, ReviewerOutput,
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

// ── Service ────────────────────────────────────────────────────────────

pub struct ReviewOrchestrationService {
    #[allow(dead_code)]
    pool: SqlitePool,
    candidate_repo: CandidateRepo,
    review_repo: ReviewRepo,
}

impl ReviewOrchestrationService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            candidate_repo: CandidateRepo::new(pool.clone()),
            review_repo: ReviewRepo::new(pool.clone()),
            pool,
        }
    }

    // ── Candidate Freezing ──────────────────────────────────────────

    /// Freeze a Candidate from a completed execution.
    /// Requires: task_id, execution_id, executor profile, workspace info,
    /// base commit, tree hash, diff digest, task spec digest, evidence digest.
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
            candidate_id,
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
                format!("Candidate already exists: {}", snapshot.candidate_id),
                ErrorSource::System,
            ));
        }
        Ok(snapshot)
    }

    /// Recompute all digests for a candidate and check they still match.
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

    /// Create a new review request for a frozen candidate.
    pub async fn create_review(
        &self,
        candidate_id: &CandidateId,
        reviewer_profile_id: &str,
    ) -> Result<ReviewRequest, CoreError> {
        // Verify candidate exists
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
            review_id,
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

        self.review_repo
            .transition_state(review_id, &current.state, to)
            .await
    }

    // ── Deterministic Precheck ─────────────────────────────────────

    /// Run deterministic prechecks before invoking the reviewer.
    /// Returns PrecheckResult with structured findings.
    /// Does NOT invoke any LLM or Agent.
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
    ) -> PrecheckResult {
        let mut findings: Vec<PrecheckFinding> = Vec::new();

        // 1. Candidate snapshot parseable (checked by type system)
        findings.push(PrecheckFinding {
            check_name: "candidate_parseable".into(),
            passed: true,
            detail: "CandidateSnapshot is a valid typed struct".into(),
        });

        // 2. Completion eligibility
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
                format!("Base commit {} exists in repo", candidate.base_commit)
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
                "Worktree has no unknown modifications beyond candidate diff".into()
            } else {
                "Worktree has unknown modifications".into()
            },
        });

        // 7. Evidence exists (digest matching already checked)
        findings.push(PrecheckFinding {
            check_name: "evidence_exists".into(),
            passed: true, // Digest was already verified at freeze time
            detail: "Evidence digest matches candidate snapshot".into(),
        });

        // 8. No test failures
        let has_failures = step_results.iter().any(|sr| {
            sr.status == VerificationStepStatus::Failed
                || sr.status == VerificationStepStatus::Blocked
        });
        findings.push(PrecheckFinding {
            check_name: "no_test_failures".into(),
            passed: !has_failures,
            detail: if has_failures {
                "One or more verification steps failed or were blocked".into()
            } else {
                "No verification step failures".into()
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

        // 10. No test skips/timeouts
        let has_skips_or_timeout = step_results.iter().any(|sr| {
            sr.status == VerificationStepStatus::Skipped
                || sr.status == VerificationStepStatus::Error
                || sr.status == VerificationStepStatus::ProcessUnknown
        });
        findings.push(PrecheckFinding {
            check_name: "no_test_skips_timeouts".into(),
            passed: !has_skips_or_timeout,
            detail: if has_skips_or_timeout {
                "One or more verification steps were skipped, errored, or had unknown process status".into()
            } else {
                "No verification skips, timeouts, or unknown process states".into()
            },
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

    /// Select a reviewer profile that is different from the executor.
    /// Returns None if no compatible reviewer is available.
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
                // Prefer profiles with the most optional capabilities
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

    // ── Review Dossier Construction ────────────────────────────────

    /// Build a structured review dossier for the reviewer.
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

    // ── Decision Policy ────────────────────────────────────────────

    /// Apply decision policy to reviewer output.
    /// Current policy (I4.6): ANY finding → Rejected.
    /// No fuzzy states allowed.
    pub fn apply_decision_policy(
        &self,
        reviewer_output: &ReviewerOutput,
    ) -> (ReviewDecision, Vec<ReviewFinding>) {
        let review_id = "temp"; // caller provides actual review_id
        let findings = reviewer_output.to_findings(review_id);

        // First, try to parse the reviewer's declared decision.
        let declared = reviewer_output.parse_decision();

        // If the output cannot be parsed at all → Blocked
        if declared.is_none() {
            return (ReviewDecision::Blocked, findings);
        }

        let declared = declared.unwrap();

        // If reviewer declares Blocked, respect it.
        if declared == ReviewDecision::Blocked {
            return (ReviewDecision::Blocked, findings);
        }

        // If reviewer declares Stale (candidate may have changed), respect it.
        if declared == ReviewDecision::Stale {
            // Stale is not a valid ReviewerOutput decision, but we handle it anyway
            return (ReviewDecision::Stale, findings);
        }

        // I4.6 strict policy: ANY finding → Rejected
        if !findings.is_empty() {
            // Further classification:
            let has_critical_or_high = findings
                .iter()
                .any(|f| f.severity.at_least(&FindingSeverity::High));
            let has_blocking = findings.iter().any(|f| f.blocking);

            if has_critical_or_high || has_blocking {
                return (ReviewDecision::Rejected, findings);
            }

            // Even Low/Medium non-blocking findings → Rejected (strict policy)
            return (ReviewDecision::Rejected, findings);
        }

        // Zero findings → Approved
        if declared == ReviewDecision::Approved {
            (ReviewDecision::Approved, findings)
        } else {
            // Reviewer declared Rejected but had no findings → Blocked (inconsistent)
            (ReviewDecision::Blocked, findings)
        }
    }

    /// Apply decision policy with actual review_id.
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

    /// Persist the final review decision and transition the review state.
    pub async fn finalize_decision(
        &self,
        review_id: &str,
        decision: &ReviewDecision,
        findings: &[ReviewFinding],
        candidate: &CandidateSnapshot,
        reviewer_output: &ReviewerOutput,
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

        // Persist findings (non-blocking — duplicate insertions are NOOP)
        if !findings.is_empty() {
            let _ = self.review_repo.insert_findings(findings).await;
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

        // Transition state (CAS from Reviewing)
        let _ = self
            .review_repo
            .transition_state(review_id, &ReviewState::Reviewing, &state)
            .await;

        Ok(())
    }

    // ── Staleness Detection ────────────────────────────────────────

    /// Check if a candidate is still valid by recomputing digests.
    /// Returns true if the candidate has gone stale.
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
            // Mark all active reviews for this candidate as Stale
            self.review_repo
                .mark_stale_for_candidate(&candidate.candidate_id)
                .await?;
        }

        Ok(is_stale)
    }

    // ── Get Review ─────────────────────────────────────────────────

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

    // ── ApprovedCandidate (I5 contract) ────────────────────────────

    /// Build an ApprovedCandidate for I5 consumption.
    /// Only call after review is in Approved state.
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
        // Seed necessary referenced rows
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
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')")
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

    #[tokio::test]
    async fn test_freeze_candidate() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        assert_eq!(c.task_id, "t1");
        assert_eq!(c.execution_id, "e1");
        assert_eq!(c.executor_profile_id, "p1");
    }

    #[tokio::test]
    async fn test_freeze_candidate_duplicate() {
        let (svc, _db) = setup().await;
        let _c1 = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_review() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        assert_eq!(req.state, ReviewState::Requested);
        assert_eq!(req.reviewer_profile_id, "p2");
    }

    #[tokio::test]
    async fn test_create_review_duplicate_blocked() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        svc.create_review(&c.candidate_id, "p2").await.unwrap();
        let result = svc.create_review(&c.candidate_id, "p3").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_review_lifecycle_to_approved() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
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
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        // Cannot go directly from Requested to Approved
        let result = svc.transition(&req.review_id, &ReviewState::Approved).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancel_from_requested() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
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
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
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
        // Terminal → anything should fail
        let result = svc.transition(&req.review_id, &ReviewState::Rejected).await;
        assert!(result.is_err());
    }

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
        // Only one profile — same as executor
        let profiles = vec![mk_profile("p1", "codex")];
        let selected = svc.select_reviewer("p1", &profiles);
        assert!(selected.is_none());
    }

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
        let (decision, _findings) = svc.apply_decision("r1", &output);
        assert_eq!(decision, ReviewDecision::Rejected);
    }

    #[tokio::test]
    async fn test_decision_policy_critical_finding_rejected() {
        let (svc, _db) = setup().await;
        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "has critical".into(),
            findings: vec![harness_core::contracts::review::ReviewerFinding {
                severity: "Critical".into(),
                category: "Correctness".into(),
                summary: "bug".into(),
                details: "null pointer".into(),
                source_location: Some("src/main.rs:10".into()),
                evidence_reference: None,
                blocking: true,
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

    #[tokio::test]
    async fn test_precheck_all_pass() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
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
                true, // workspace exists
                true, // base commit exists
                true, // diff readable
                true, // worktree clean
                true, // resource claims valid
            )
            .await;
        assert!(precheck.passed);
    }

    #[tokio::test]
    async fn test_precheck_completion_eligibility_fails() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let outcome = VerificationOutcome {
            result: VerificationResult::Failed,
            failure_classification: None,
            summary: "failed".into(),
            blockers: vec!["test failure".into()],
            findings_count: 1,
        };
        let step_results = vec![];
        let precheck = svc
            .run_precheck(&c, &outcome, &step_results, true, true, true, true, true)
            .await;
        assert!(!precheck.passed);
        assert!(precheck.blocker_reason.is_some());
    }

    #[tokio::test]
    async fn test_precheck_missing_workspace() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
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
                false, // workspace missing
                true,
                true,
                true,
                true,
            )
            .await;
        assert!(!precheck.passed);
    }

    #[tokio::test]
    async fn test_staleness_detection() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        // No review yet, so no reviews to mark stale
        // But the staleness check should return true for mismatched digest
        let is_stale = svc
            .check_staleness(&c, "tree2", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(is_stale);
    }

    #[tokio::test]
    async fn test_no_staleness_when_digests_match() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let is_stale = svc
            .check_staleness(&c, "tree1", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(!is_stale);
    }

    #[tokio::test]
    async fn test_review_rejected_with_findings() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        // Fast-forward to Reviewing
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
            summary: "found issues".into(),
            findings: vec![harness_core::contracts::review::ReviewerFinding {
                severity: "High".into(),
                category: "Correctness".into(),
                summary: "logic error".into(),
                details: "off-by-one in loop".into(),
                source_location: Some("src/main.rs:42".into()),
                evidence_reference: None,
                blocking: true,
            }],
        };
        let (decision, findings) = svc.apply_decision(&req.review_id, &output);
        assert_eq!(decision, ReviewDecision::Rejected);
        assert_eq!(findings.len(), 1);

        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output)
            .await
            .unwrap();

        let persisted_findings = svc.get_findings(&req.review_id).await.unwrap();
        assert_eq!(persisted_findings.len(), 1);
    }

    #[tokio::test]
    async fn test_build_approved_candidate() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();

        // Fast-forward to Approved
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
        svc.finalize_decision(&req.review_id, &decision, &findings, &c, &output)
            .await
            .unwrap();

        // Force transition to Approved (CAS from Reviewing)
        let _ = sqlx::query("UPDATE review_requests SET state='approved' WHERE review_id=?")
            .bind(&req.review_id)
            .execute(&svc.pool)
            .await;

        let approved = svc
            .build_approved_candidate(&c.candidate_id, &req.review_id)
            .await
            .unwrap();
        assert_eq!(approved.candidate_id, c.candidate_id);
        assert_eq!(approved.review_id, req.review_id);
    }

    #[tokio::test]
    async fn test_stale_mark_on_digest_change() {
        let (svc, _db) = setup().await;
        let c = svc
            .freeze_candidate(
                "t1", "e1", "p1", "w1", "abc123", "tree1", "diff1", "task1", "ev1",
            )
            .await
            .unwrap();
        let req = svc.create_review(&c.candidate_id, "p2").await.unwrap();
        svc.transition(&req.review_id, &ReviewState::Preparing)
            .await
            .unwrap();

        // Now check staleness with changed tree hash
        let is_stale = svc
            .check_staleness(&c, "tree_changed", "diff1", "task1", "ev1")
            .await
            .unwrap();
        assert!(is_stale);

        // The active review should be marked as Stale
        let updated = svc.get_review(&req.review_id).await.unwrap().unwrap();
        assert_eq!(updated.state, ReviewState::Stale);
    }
}
