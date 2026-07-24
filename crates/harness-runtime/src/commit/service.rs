//! ControlledCommitService — admission validation + controlled Git commit creation.
//!
//! Flow:
//!   1. Validate ApprovedCandidate (admission)
//!   2. Create CommitRequest and persist
//!   3. Create Git commit via plumbing (write-tree + commit-tree)
//!   4. Verify commit object exists and tree/parent match
//!   5. Persist CommitCandidate and transition to Created
//!
//! Idempotency: same candidate + review + target_ref yields same commit OID.
//! Recovery: Git object existing before DB write is detected and reconciled.

use chrono::Utc;
use harness_core::contracts::candidate::{CandidateId, CandidateSnapshot};
use harness_core::contracts::commit::{
    CommitAdmission, CommitCandidate, CommitRequest, CommitState, GitIdentity,
};
use harness_core::contracts::review::ApprovedCandidate;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use std::path::Path;
use uuid::Uuid;

use super::repo::CommitRepo;

/// Result of commit creation, including any recovery information.
#[derive(Debug, Clone)]
pub struct CommitOutcome {
    pub commit_candidate: CommitCandidate,
    pub recovered: bool,
}

pub struct ControlledCommitService {
    pool: SqlitePool,
    commit_repo: CommitRepo,
}

impl ControlledCommitService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            commit_repo: CommitRepo::new(pool.clone()),
            pool,
        }
    }

    // ── Admission ────────────────────────────────────────────────────

    /// Validate that an ApprovedCandidate is eligible for commit creation.
    /// Must be called from the database — never trusts caller-provided fields.
    pub async fn validate_admission(
        &self,
        approved: &ApprovedCandidate,
    ) -> Result<CommitAdmission, CoreError> {
        let mut reasons: Vec<String> = Vec::new();

        // 1. Candidate exists
        let candidate = match self.get_candidate(&approved.candidate_id).await? {
            Some(c) => c,
            None => {
                return Ok(CommitAdmission::Blocked {
                    reasons: vec![format!("Candidate not found: {}", approved.candidate_id)],
                });
            }
        };

        // 2. Review exists and is terminal Approved
        let review = match self.get_review(&approved.review_id).await? {
            Some(r) => r,
            None => {
                return Ok(CommitAdmission::Blocked {
                    reasons: vec![format!("Review not found: {}", approved.review_id)],
                });
            }
        };

        if review.state != "approved" {
            reasons.push(format!(
                "Review {} is not Approved (state: {})",
                approved.review_id, review.state
            ));
        }

        // 3. candidate_id matches review
        if review.candidate_id != approved.candidate_id {
            reasons.push(format!(
                "Review candidate_id {} does not match approved candidate_id {}",
                review.candidate_id, approved.candidate_id
            ));
        }

        // 4. review_decision_digest matches
        if let Some(decision) = self.get_decision(&approved.review_id).await? {
            if decision.decision_digest != approved.review_decision_digest {
                reasons.push("review_decision_digest mismatch".into());
            }
        } else {
            reasons.push("No review decision found".into());
        }

        // 5. candidate_tree_hash matches
        if candidate.candidate_tree_hash != approved.candidate_tree_hash {
            return Ok(CommitAdmission::Stale {
                reason: format!(
                    "candidate_tree_hash mismatch: stored={}, approved={}",
                    candidate.candidate_tree_hash, approved.candidate_tree_hash
                ),
            });
        }

        // 6. diff_digest matches
        if candidate.diff_digest != approved.diff_digest {
            return Ok(CommitAdmission::Stale {
                reason: format!(
                    "diff_digest mismatch: stored={}, approved={}",
                    candidate.diff_digest, approved.diff_digest
                ),
            });
        }

        // 7. executor_profile_id != reviewer_profile_id
        if review.reviewer_profile_id == candidate.executor_profile_id {
            reasons.push(format!(
                "Reviewer {} is the same as executor {}",
                review.reviewer_profile_id, candidate.executor_profile_id
            ));
        }

        // 8. Base commit exists in the repository (will be verified at commit time)

        if !reasons.is_empty() {
            return Ok(CommitAdmission::Blocked { reasons });
        }

        Ok(CommitAdmission::Admitted)
    }

    // ── Create Commit ────────────────────────────────────────────────

    /// Create or recover a controlled commit from an ApprovedCandidate.
    ///
    /// Idempotent: same inputs → same CommitCandidate.
    /// Handles: concurrent creators, response-lost retries, Git object before DB.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_commit(
        &self,
        approved: &ApprovedCandidate,
        repository_id: &str,
        target_ref: &str,
        author: &GitIdentity,
        committer: &GitIdentity,
        message: &str,
        repo_path: &Path,
    ) -> Result<CommitOutcome, CoreError> {
        // 1. Admission
        let admission = self.validate_admission(approved).await?;
        match admission {
            CommitAdmission::Blocked { reasons } => {
                return Err(CoreError::new(
                    ErrorCode::InvalidState,
                    format!("commit admission blocked: {}", reasons.join("; ")),
                    ErrorSource::System,
                ));
            }
            CommitAdmission::Stale { reason } => {
                return Err(CoreError::new(
                    ErrorCode::InvalidState,
                    format!("candidate stale: {reason}"),
                    ErrorSource::System,
                ));
            }
            CommitAdmission::Admitted => {}
        }

        // 2. Get candidate for tree/parent info
        let candidate = self
            .get_candidate(&approved.candidate_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::NotFound,
                    "candidate vanished after admission",
                    ErrorSource::System,
                )
            })?;

        // 3. Idempotency check: existing commit request?
        let ikey = format!(
            "commit-{}-{}-{}",
            approved.candidate_id, approved.review_id, target_ref
        );
        if let Some(existing) = self.commit_repo.find_by_idempotency_key(&ikey).await? {
            // Check if commit already created
            if let Some(cc) = self
                .commit_repo
                .get_candidate(&existing.commit_request_id)
                .await?
            {
                return Ok(CommitOutcome {
                    commit_candidate: cc,
                    recovered: true,
                });
            }
            // Request exists but no candidate yet — attempt recovery
            return self
                .recover_or_create(approved, &candidate, &existing, repo_path)
                .await;
        }

        // 4. Create CommitRequest
        let commit_request_id = format!("cr-{}", Uuid::new_v4());
        let ts = approved.approved_at;
        let req = CommitRequest {
            commit_request_id: commit_request_id.clone(),
            candidate_id: approved.candidate_id.clone(),
            review_id: approved.review_id.clone(),
            repository_id: repository_id.into(),
            target_ref: target_ref.into(),
            expected_base_commit: candidate.base_commit.clone(),
            author_identity: author.clone(),
            committer_identity: committer.clone(),
            commit_timestamp: ts,
            message: message.into(),
            idempotency_key: ikey.clone(),
            created_at: Utc::now(),
        };

        let inserted = self.commit_repo.insert_request(&req).await?;
        if !inserted {
            // Race: another caller inserted first
            if let Some(existing) = self.commit_repo.find_by_idempotency_key(&ikey).await? {
                return self
                    .recover_or_create(approved, &candidate, &existing, repo_path)
                    .await;
            }
            return Err(CoreError::new(
                ErrorCode::Conflict,
                "commit request insert failed but no existing found",
                ErrorSource::System,
            ));
        }

        // Emit event
        self.emit_event(
            &commit_request_id,
            &approved.candidate_id,
            "CommitRequested",
            "{}",
        )
        .await;

        // 5. Transition to Materializing
        self.commit_repo
            .transition_state(
                &commit_request_id,
                &CommitState::Requested,
                &CommitState::Materializing,
            )
            .await?;
        self.emit_event(
            &commit_request_id,
            &approved.candidate_id,
            "CommitMaterializationStarted",
            "{}",
        )
        .await;

        // 6. Log attempt
        let attempt_id = format!("catt-{}", Uuid::new_v4());
        self.commit_repo
            .log_attempt(&attempt_id, &commit_request_id, 1)
            .await?;

        // 7. Create Git commit via plumbing
        let full_message = req.build_message(
            &candidate.task_id,
            &candidate.execution_id,
            &candidate.diff_digest,
        );

        let tree_oid = &candidate.candidate_tree_hash;
        let parent_oid = &candidate.base_commit;

        let commit_oid = self
            .git_commit_tree(
                repo_path,
                tree_oid,
                parent_oid,
                &full_message,
                author,
                committer,
                &ts,
            )
            .await?;

        // 8. Verify commit object
        self.verify_commit_object(repo_path, &commit_oid, tree_oid, parent_oid)
            .await?;

        // 9. Persist CommitCandidate
        let cc = CommitCandidate {
            commit_request_id: commit_request_id.clone(),
            candidate_id: approved.candidate_id.clone(),
            review_id: approved.review_id.clone(),
            repository_id: repository_id.into(),
            commit_oid: commit_oid.clone(),
            parent_oid: parent_oid.clone(),
            tree_oid: tree_oid.clone(),
            diff_digest: approved.diff_digest.clone(),
            created_at: Utc::now(),
        };

        self.commit_repo.insert_candidate(&cc).await?;

        // 10. Transition to Created
        self.commit_repo
            .transition_state(
                &commit_request_id,
                &CommitState::Materializing,
                &CommitState::Created,
            )
            .await?;

        // Complete attempt
        self.commit_repo
            .complete_attempt(&attempt_id, "created", Some(&commit_oid), None)
            .await?;

        // Emit event
        self.emit_event(
            &commit_request_id,
            &approved.candidate_id,
            "CommitCreated",
            &serde_json::json!({"commit_oid": commit_oid, "tree_oid": tree_oid, "parent_oid": parent_oid}).to_string(),
        )
        .await;

        Ok(CommitOutcome {
            commit_candidate: cc,
            recovered: false,
        })
    }

    // ── Recovery ──────────────────────────────────────────────────────

    async fn recover_or_create(
        &self,
        approved: &ApprovedCandidate,
        candidate: &CandidateSnapshot,
        existing: &CommitRequest,
        repo_path: &Path,
    ) -> Result<CommitOutcome, CoreError> {
        // Check if Git object already exists (created before DB write)
        let tree_oid = &candidate.candidate_tree_hash;
        let parent_oid = &candidate.base_commit;

        let full_message = existing.build_message(
            &candidate.task_id,
            &candidate.execution_id,
            &candidate.diff_digest,
        );

        // Try to compute the expected commit OID
        let expected_oid = self
            .git_commit_tree(
                repo_path,
                tree_oid,
                parent_oid,
                &full_message,
                &existing.author_identity,
                &existing.committer_identity,
                &existing.commit_timestamp,
            )
            .await?;

        // Check if this OID exists in Git
        if self.git_object_exists(repo_path, &expected_oid).await? {
            // Git object exists — reconcile DB
            let current_state = self
                .commit_repo
                .get_state(&existing.commit_request_id)
                .await?
                .unwrap_or_default();
            if current_state != "created" {
                let cc = CommitCandidate {
                    commit_request_id: existing.commit_request_id.clone(),
                    candidate_id: approved.candidate_id.clone(),
                    review_id: approved.review_id.clone(),
                    repository_id: existing.repository_id.clone(),
                    commit_oid: expected_oid.clone(),
                    parent_oid: parent_oid.clone(),
                    tree_oid: tree_oid.clone(),
                    diff_digest: approved.diff_digest.clone(),
                    created_at: Utc::now(),
                };
                self.commit_repo.insert_candidate(&cc).await?;
                let _ = self
                    .commit_repo
                    .transition_state(
                        &existing.commit_request_id,
                        &CommitState::Materializing,
                        &CommitState::Created,
                    )
                    .await;
                self.emit_event(
                    &existing.commit_request_id,
                    &approved.candidate_id,
                    "CommitCreationRecovered",
                    &serde_json::json!({"commit_oid": expected_oid}).to_string(),
                )
                .await;
                return Ok(CommitOutcome {
                    commit_candidate: cc,
                    recovered: true,
                });
            }
        }

        // Request exists but commit not created yet — create it now
        let commit_oid = self
            .git_commit_tree(
                repo_path,
                tree_oid,
                parent_oid,
                &full_message,
                &existing.author_identity,
                &existing.committer_identity,
                &existing.commit_timestamp,
            )
            .await?;

        self.verify_commit_object(repo_path, &commit_oid, tree_oid, parent_oid)
            .await?;

        let cc = CommitCandidate {
            commit_request_id: existing.commit_request_id.clone(),
            candidate_id: approved.candidate_id.clone(),
            review_id: approved.review_id.clone(),
            repository_id: existing.repository_id.clone(),
            commit_oid: commit_oid.clone(),
            parent_oid: parent_oid.clone(),
            tree_oid: tree_oid.clone(),
            diff_digest: approved.diff_digest.clone(),
            created_at: Utc::now(),
        };

        self.commit_repo.insert_candidate(&cc).await?;
        let _ = self
            .commit_repo
            .transition_state(
                &existing.commit_request_id,
                &CommitState::Materializing,
                &CommitState::Created,
            )
            .await;
        self.emit_event(
            &existing.commit_request_id,
            &approved.candidate_id,
            "CommitCreationRecovered",
            &serde_json::json!({"commit_oid": commit_oid}).to_string(),
        )
        .await;

        Ok(CommitOutcome {
            commit_candidate: cc,
            recovered: true,
        })
    }

    // ── Git Plumbing ──────────────────────────────────────────────────

    /// Create a commit object using git commit-tree.
    /// Uses environment variables for author/committer identity — never modifies global config.
    #[allow(clippy::too_many_arguments)]
    async fn git_commit_tree(
        &self,
        repo_path: &Path,
        tree_oid: &str,
        parent_oid: &str,
        message: &str,
        author: &GitIdentity,
        committer: &GitIdentity,
        timestamp: &chrono::DateTime<Utc>,
    ) -> Result<String, CoreError> {
        let ts_str = timestamp.format("%s %z").to_string();

        let author_env = format!("{} <{}> {}", author.name, author.email, ts_str);
        let committer_env = format!("{} <{}> {}", committer.name, committer.email, ts_str);

        let output = std::process::Command::new("git")
            .args(["commit-tree", tree_oid, "-p", parent_oid, "-m", message])
            .env("GIT_AUTHOR_NAME", &author.name)
            .env("GIT_AUTHOR_EMAIL", &author.email)
            .env("GIT_AUTHOR_DATE", &author_env)
            .env("GIT_COMMITTER_NAME", &committer.name)
            .env("GIT_COMMITTER_EMAIL", &committer.email)
            .env("GIT_COMMITTER_DATE", &committer_env)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .current_dir(repo_path)
            .output()
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::ProcessSpawnFailed,
                    format!("git commit-tree: {e}"),
                    ErrorSource::System,
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CoreError::new(
                ErrorCode::WorkspaceError,
                format!("git commit-tree failed: {}", stderr.trim()),
                ErrorSource::System,
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Verify that a commit object exists and has the expected tree and parent.
    async fn verify_commit_object(
        &self,
        repo_path: &Path,
        commit_oid: &str,
        expected_tree: &str,
        expected_parent: &str,
    ) -> Result<(), CoreError> {
        // Verify tree
        let tree_out = std::process::Command::new("git")
            .args(["cat-file", "-p", commit_oid])
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .current_dir(repo_path)
            .output()
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::ProcessSpawnFailed,
                    format!("git cat-file: {e}"),
                    ErrorSource::System,
                )
            })?;

        if !tree_out.status.success() {
            return Err(CoreError::new(
                ErrorCode::WorkspaceError,
                format!("commit object {} not found", commit_oid),
                ErrorSource::System,
            ));
        }

        let output = String::from_utf8_lossy(&tree_out.stdout);
        let first_line = output.lines().next().unwrap_or("");

        if !first_line.starts_with(&format!("tree {}", expected_tree)) {
            return Err(CoreError::new(
                ErrorCode::WorkspaceError,
                format!(
                    "commit tree mismatch: expected tree {}, got line: {}",
                    expected_tree, first_line
                ),
                ErrorSource::System,
            ));
        }

        // Verify parent in output
        if !output.contains(&format!("parent {}", expected_parent)) {
            return Err(CoreError::new(
                ErrorCode::WorkspaceError,
                format!(
                    "commit parent mismatch: expected parent {}",
                    expected_parent
                ),
                ErrorSource::System,
            ));
        }

        Ok(())
    }

    /// Check if a Git object exists in the repository.
    async fn git_object_exists(&self, repo_path: &Path, oid: &str) -> Result<bool, CoreError> {
        let output = std::process::Command::new("git")
            .args(["cat-file", "-e", oid])
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .current_dir(repo_path)
            .output()
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::ProcessSpawnFailed,
                    format!("git cat-file -e: {e}"),
                    ErrorSource::System,
                )
            })?;
        Ok(output.status.success())
    }

    // ── DB Helpers ────────────────────────────────────────────────────

    async fn get_candidate(
        &self,
        id: &CandidateId,
    ) -> Result<Option<CandidateSnapshot>, CoreError> {
        let row: Option<CandidateSnapshot> = sqlx::query_as::<_, (String, String, String, String, String, String, String, String, String, String, String)>(
            "SELECT candidate_id, task_id, execution_id, executor_profile_id, workspace_id, base_commit, candidate_tree_hash, diff_digest, task_spec_digest, evidence_digest, created_at FROM candidate_snapshots WHERE candidate_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?
        .map(|(cid, tid, eid, epid, wid, bc, cth, dd, tsd, ed, cat)| CandidateSnapshot {
            candidate_id: cid,
            task_id: tid,
            execution_id: eid,
            executor_profile_id: epid,
            workspace_id: wid,
            base_commit: bc,
            candidate_tree_hash: cth,
            diff_digest: dd,
            task_spec_digest: tsd,
            evidence_digest: ed,
            created_at: parse_dt_opt(&cat),
        });
        Ok(row)
    }

    async fn get_review(&self, id: &str) -> Result<Option<ReviewInfo>, CoreError> {
        let row = sqlx::query_as::<_, (String, String, String, String)>(
            "SELECT review_id, candidate_id, reviewer_profile_id, state FROM review_requests WHERE review_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(row.map(|(rid, cid, rpid, state)| ReviewInfo {
            review_id: rid,
            candidate_id: cid,
            reviewer_profile_id: rpid,
            state,
        }))
    }

    async fn get_decision(&self, review_id: &str) -> Result<Option<DecisionInfo>, CoreError> {
        let row = sqlx::query_as::<_, (String, String, String)>(
            "SELECT decision_id, review_id, decision_digest FROM review_decisions WHERE review_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(review_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(row.map(|(did, rid, dd)| DecisionInfo {
            decision_id: did,
            review_id: rid,
            decision_digest: dd,
        }))
    }

    // ── Events ────────────────────────────────────────────────────────

    async fn emit_event(
        &self,
        commit_request_id: &str,
        candidate_id: &str,
        event_type: &str,
        payload_json: &str,
    ) {
        let event_id = format!("evt-{}", Uuid::new_v4());
        let _ = self
            .commit_repo
            .write_event(
                &event_id,
                commit_request_id,
                candidate_id,
                event_type,
                payload_json,
            )
            .await;
    }

    // ── Queries ───────────────────────────────────────────────────────

    pub async fn get_commit_request(&self, id: &str) -> Result<Option<CommitRequest>, CoreError> {
        self.commit_repo.get_request(id).await
    }

    pub async fn get_commit_candidate(
        &self,
        id: &str,
    ) -> Result<Option<CommitCandidate>, CoreError> {
        self.commit_repo.get_candidate(id).await
    }
}

// ── Helper types ──────────────────────────────────────────────────────

struct ReviewInfo {
    #[allow(dead_code)]
    review_id: String,
    candidate_id: String,
    reviewer_profile_id: String,
    state: String,
}

struct DecisionInfo {
    #[allow(dead_code)]
    decision_id: String,
    #[allow(dead_code)]
    review_id: String,
    decision_digest: String,
}

fn parse_dt_opt(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().into())
        .unwrap_or_else(chrono::Utc::now)
}
