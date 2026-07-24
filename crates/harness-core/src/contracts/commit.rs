//! Commit — controlled Git commit creation from an ApprovedCandidate.
//!
//! I5.1: After admission validation, a controlled commit is created via Git
//! plumbing (write-tree + commit-tree). The commit is idempotent: same
//! candidate + review + target_ref always produces the same commit OID.
//!
//! All types are pure data. No I/O, no Git dependencies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::candidate::CandidateId;
use super::review::ReviewId;

// ── Typed IDs ──────────────────────────────────────────────────────────

pub type CommitRequestId = String;
pub type RepositoryId = String;

// ── Git Identity ───────────────────────────────────────────────────────

/// Author or committer identity for a Git commit.
/// Must be provided explicitly — never derived from global git config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitIdentity {
    pub name: String,
    pub email: String,
}

impl GitIdentity {
    pub fn new(name: &str, email: &str) -> Self {
        Self {
            name: name.to_string(),
            email: email.to_string(),
        }
    }
}

// ── Commit Request ─────────────────────────────────────────────────────

/// A request to create a controlled Git commit from an ApprovedCandidate.
/// All fields necessary for deterministic commit OID are captured at creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    pub commit_request_id: CommitRequestId,
    pub candidate_id: CandidateId,
    pub review_id: ReviewId,
    pub repository_id: RepositoryId,
    pub target_ref: String,
    pub expected_base_commit: String,
    pub author_identity: GitIdentity,
    pub committer_identity: GitIdentity,
    pub commit_timestamp: DateTime<Utc>,
    pub message: String,
    pub idempotency_key: String,
    pub created_at: DateTime<Utc>,
}

impl CommitRequest {
    /// Compute a deterministic idempotency digest for this request.
    /// Same inputs → same digest → same commit OID.
    pub fn idempotency_digest(&self) -> String {
        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.candidate_id,
            self.review_id,
            self.repository_id,
            self.target_ref,
            self.expected_base_commit,
            self.author_identity.name,
            self.author_identity.email,
            self.committer_identity.name,
            self.committer_identity.email,
            self.commit_timestamp.to_rfc3339(),
        );
        sha256_hex(&input)
    }

    /// Build the commit message with Harness trailers.
    pub fn build_message(&self, task_id: &str, execution_id: &str, diff_digest: &str) -> String {
        format!(
            "{}\n\n\
             Harness-Candidate: {}\n\
             Harness-Review: {}\n\
             Harness-Task: {}\n\
             Harness-Execution: {}\n\
             Harness-Diff-Digest: {}",
            self.message,
            self.candidate_id,
            self.review_id,
            task_id,
            execution_id,
            diff_digest
        )
    }
}

// ── Commit Candidate ───────────────────────────────────────────────────

/// A successfully created Git commit from a CommitRequest.
/// The commit OID is deterministically derived from the request fields + tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitCandidate {
    pub commit_request_id: CommitRequestId,
    pub candidate_id: CandidateId,
    pub review_id: ReviewId,
    pub repository_id: RepositoryId,
    pub commit_oid: String,
    pub parent_oid: String,
    pub tree_oid: String,
    pub diff_digest: String,
    pub created_at: DateTime<Utc>,
}

// ── Commit State ───────────────────────────────────────────────────────

/// Lifecycle states for a CommitRequest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitState {
    /// Request created, not yet materialized.
    Requested,
    /// Git commit object is being created.
    Materializing,
    /// Commit object exists in Git and is recorded in the database.
    Created,
    /// Admission blocked the commit.
    Blocked,
    /// Commit creation failed with an error.
    Failed,
    /// Commit was explicitly cancelled.
    Cancelled,
}

impl CommitState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Created | Self::Blocked | Self::Failed | Self::Cancelled)
    }

    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Materializing => "materializing",
            Self::Created => "created",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

// ── Commit Admission ───────────────────────────────────────────────────

/// Result of admission validation for a CommitRequest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitAdmission {
    /// All checks passed; commit may proceed.
    Admitted,
    /// One or more checks blocked admission.
    Blocked { reasons: Vec<String> },
    /// Candidate is stale (digests no longer match).
    Stale { reason: String },
}

// ── Helpers ────────────────────────────────────────────────────────────

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_commit_request_idempotency_digest_deterministic() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let r1 = CommitRequest {
            commit_request_id: "cr-1".into(),
            candidate_id: "c1".into(),
            review_id: "r1".into(),
            repository_id: "repo-1".into(),
            target_ref: "refs/heads/main".into(),
            expected_base_commit: "abc123".into(),
            author_identity: GitIdentity::new("Author", "author@test.com"),
            committer_identity: GitIdentity::new("Committer", "committer@test.com"),
            commit_timestamp: ts,
            message: "test commit".into(),
            idempotency_key: "ik-1".into(),
            created_at: ts,
        };
        let r2 = CommitRequest {
            commit_request_id: "cr-2".into(), // different ID
            ..r1.clone()
        };
        // Same input fields → same digest (commit_request_id not part of digest)
        assert_eq!(r1.idempotency_digest(), r2.idempotency_digest());
    }

    #[test]
    fn test_commit_request_different_message_different_digest() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let r1 = CommitRequest {
            commit_request_id: "cr-1".into(),
            candidate_id: "c1".into(),
            review_id: "r1".into(),
            repository_id: "repo-1".into(),
            target_ref: "refs/heads/main".into(),
            expected_base_commit: "abc123".into(),
            author_identity: GitIdentity::new("Author", "author@test.com"),
            committer_identity: GitIdentity::new("Committer", "committer@test.com"),
            commit_timestamp: ts,
            message: "msg A".into(),
            idempotency_key: "ik-1".into(),
            created_at: ts,
        };
        let r2 = CommitRequest {
            message: "msg B".into(),
            ..r1.clone()
        };
        assert_ne!(r1.idempotency_digest(), r2.idempotency_digest());
    }

    #[test]
    fn test_build_message_with_trailers() {
        let ts = Utc::now();
        let req = CommitRequest {
            commit_request_id: "cr-1".into(),
            candidate_id: "c1".into(),
            review_id: "r1".into(),
            repository_id: "repo-1".into(),
            target_ref: "refs/heads/main".into(),
            expected_base_commit: "abc123".into(),
            author_identity: GitIdentity::new("A", "a@test.com"),
            committer_identity: GitIdentity::new("C", "c@test.com"),
            commit_timestamp: ts,
            message: "feat: add X".into(),
            idempotency_key: "ik-1".into(),
            created_at: ts,
        };
        let msg = req.build_message("t1", "e1", "diff-digest-1");
        assert!(msg.contains("feat: add X"));
        assert!(msg.contains("Harness-Candidate: c1"));
        assert!(msg.contains("Harness-Review: r1"));
        assert!(msg.contains("Harness-Task: t1"));
        assert!(msg.contains("Harness-Execution: e1"));
        assert!(msg.contains("Harness-Diff-Digest: diff-digest-1"));
    }

    #[test]
    fn test_commit_state_terminal() {
        assert!(CommitState::Created.is_terminal());
        assert!(CommitState::Blocked.is_terminal());
        assert!(CommitState::Failed.is_terminal());
        assert!(CommitState::Cancelled.is_terminal());
        assert!(!CommitState::Requested.is_terminal());
        assert!(!CommitState::Materializing.is_terminal());
    }
}
