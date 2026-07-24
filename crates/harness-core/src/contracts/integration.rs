//! Integration — durable integration queue, sandboxed integration, and atomic publish.
//!
//! I5.2/I5.3: After a controlled commit is created, it is enqueued for integration
//! against the target ref. Integration is serialized per (repo, target_ref), uses
//! lease/fencing for mutual exclusion, and publishes via atomic git update-ref.
//!
//! All types are pure data. No I/O, no Git dependencies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::candidate::CandidateId;
use super::commit::{CommitRequestId, RepositoryId};
use super::review::ReviewId;

// ── Typed IDs ──────────────────────────────────────────────────────────

pub type IntegrationId = String;
pub type IntegrationAttemptId = String;

// ── Integration State ──────────────────────────────────────────────────

/// Lifecycle of an IntegrationRequest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationState {
    /// Enqueued, waiting to be picked up.
    Queued,
    /// Waiting for the lease on (repo, target_ref).
    WaitingForLease,
    /// Preparing integration worktree and context.
    Preparing,
    /// Applying the candidate commit to the target ref.
    Applying,
    /// Running integration verification.
    Verifying,
    /// Verification passed; awaiting atomic publish.
    ReadyToPublish,
    /// Successfully published — terminal.
    Integrated,
    // ── Terminal branches ──
    /// Merge conflict detected.
    Conflict,
    /// Admission or precondition blocked integration.
    Blocked,
    /// Integration failed (verification failed, infrastructure, etc.).
    Failed,
    /// Explicitly cancelled.
    Cancelled,
    /// Underlying candidate or commit became stale.
    Stale,
}

impl IntegrationState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Integrated
                | Self::Conflict
                | Self::Blocked
                | Self::Failed
                | Self::Cancelled
                | Self::Stale
        )
    }

    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// Non-terminal states that can be recovered (resumed).
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::WaitingForLease
                | Self::Preparing
                | Self::Applying
                | Self::Verifying
                | Self::ReadyToPublish
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::WaitingForLease => "waiting_for_lease",
            Self::Preparing => "preparing",
            Self::Applying => "applying",
            Self::Verifying => "verifying",
            Self::ReadyToPublish => "ready_to_publish",
            Self::Integrated => "integrated",
            Self::Conflict => "conflict",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Stale => "stale",
        }
    }
}

// ── Integration Request ────────────────────────────────────────────────

/// A request to integrate a commit candidate into a target ref.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationRequest {
    pub integration_id: IntegrationId,
    pub commit_request_id: CommitRequestId,
    pub candidate_id: CandidateId,
    pub review_id: ReviewId,
    pub repository_id: RepositoryId,
    pub target_ref: String,
    pub expected_target_head: String,
    pub priority: i32,
    pub idempotency_key: String,
    pub created_at: DateTime<Utc>,
}

impl IntegrationRequest {
    /// Validate that the target ref is a well-formed ref.
    pub fn validate_target_ref(ref_name: &str) -> Result<(), String> {
        if ref_name.is_empty() {
            return Err("target ref must not be empty".into());
        }
        if ref_name == "HEAD" {
            return Err("HEAD is not a valid target ref for integration".into());
        }
        if ref_name.starts_with("refs/remotes/") {
            return Err("remote refs are not valid integration targets".into());
        }
        // Must start with refs/ (heads, tags, etc.)
        if !ref_name.starts_with("refs/") {
            return Err(format!(
                "target ref must be a full ref (e.g. refs/heads/main), got: {ref_name}"
            ));
        }
        Ok(())
    }

    /// Build a queue ordering key: (repo, target_ref, priority DESC, created_at ASC, integration_id ASC).
    pub fn queue_scope(&self) -> (String, String) {
        (self.repository_id.clone(), self.target_ref.clone())
    }
}

// ── Integration Attempt ────────────────────────────────────────────────

/// A single attempt to integrate. Multiple attempts may exist for one
/// IntegrationRequest (e.g., after a CAS failure on publish).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationAttempt {
    pub attempt_id: IntegrationAttemptId,
    pub integration_id: IntegrationId,
    pub attempt_number: u32,
    pub state: IntegrationState,
    pub commit_oid: String,
    pub parent_oid: String,
    pub target_head_at_start: String,
    pub integration_tree_oid: Option<String>,
    pub integration_commit_oid: Option<String>,
    pub lease_id: Option<String>,
    pub fencing_token: Option<i64>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

// ── Integration Strategy ───────────────────────────────────────────────

/// The strategy used for integration. Determined by whether target_head
/// equals the candidate's parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStrategy {
    /// Target has not advanced: candidate commit can be fast-forwarded directly.
    FastForward,
    /// Target has advanced but patch applies cleanly.
    CherryPick,
    /// Patch cannot be applied cleanly.
    Conflict,
}

// ── Conflict Info ──────────────────────────────────────────────────────

/// Structured conflict information when integration fails with Conflict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictInfo {
    pub conflicting_files: Vec<String>,
    pub candidate_base: String,
    pub candidate_commit: String,
    pub target_head: String,
    pub conflict_type: String,
    pub git_diagnostic: String,
}

// ── Integration Verification ───────────────────────────────────────────

/// A single verification command to run in the integration worktree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationCommand {
    /// The command to execute (e.g., "cargo", "test", etc.).
    pub program: String,
    /// Arguments to the command.
    pub args: Vec<String>,
    /// Working directory relative to integration worktree root.
    pub working_dir: Option<String>,
}

/// Policy for integration verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationVerificationPolicy {
    pub commands: Vec<VerificationCommand>,
    pub timeout_secs: u64,
    pub max_output_bytes: u64,
    /// If true, verification failure blocks publish.
    pub required: bool,
}

impl Default for IntegrationVerificationPolicy {
    fn default() -> Self {
        Self {
            commands: vec![
                VerificationCommand {
                    program: "cargo".into(),
                    args: vec!["fmt".into(), "--all".into(), "--check".into()],
                    working_dir: None,
                },
                VerificationCommand {
                    program: "cargo".into(),
                    args: vec![
                        "clippy".into(),
                        "--workspace".into(),
                        "--all-targets".into(),
                        "--".into(),
                        "-D".into(),
                        "warnings".into(),
                    ],
                    working_dir: None,
                },
            ],
            timeout_secs: 600,
            max_output_bytes: 256_000,
            required: true,
        }
    }
}

// ── Integration Result ─────────────────────────────────────────────────

/// The durable result of an integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationResult {
    pub integration_id: IntegrationId,
    pub attempt_id: IntegrationAttemptId,
    pub state: IntegrationState,
    pub previous_target_head: String,
    pub new_target_head: Option<String>,
    pub commit_oid: String,
    pub strategy: Option<IntegrationStrategy>,
    pub verification_status: Option<String>,
    pub conflicts: Option<ConflictInfo>,
    pub created_at: DateTime<Utc>,
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_target_ref_valid() {
        assert!(IntegrationRequest::validate_target_ref("refs/heads/main").is_ok());
        assert!(IntegrationRequest::validate_target_ref("refs/heads/feature/xyz").is_ok());
        assert!(IntegrationRequest::validate_target_ref("refs/tags/v1.0").is_ok());
    }

    #[test]
    fn test_validate_target_ref_invalid() {
        assert!(IntegrationRequest::validate_target_ref("").is_err());
        assert!(IntegrationRequest::validate_target_ref("HEAD").is_err());
        assert!(IntegrationRequest::validate_target_ref("refs/remotes/origin/main").is_err());
        assert!(IntegrationRequest::validate_target_ref("main").is_err());
    }

    #[test]
    fn test_integration_state_terminal() {
        assert!(IntegrationState::Integrated.is_terminal());
        assert!(IntegrationState::Conflict.is_terminal());
        assert!(IntegrationState::Blocked.is_terminal());
        assert!(IntegrationState::Failed.is_terminal());
        assert!(IntegrationState::Cancelled.is_terminal());
        assert!(IntegrationState::Stale.is_terminal());
        assert!(!IntegrationState::Queued.is_terminal());
        assert!(!IntegrationState::Applying.is_terminal());
    }

    #[test]
    fn test_recoverable_states() {
        assert!(IntegrationState::WaitingForLease.is_recoverable());
        assert!(IntegrationState::Preparing.is_recoverable());
        assert!(IntegrationState::Applying.is_recoverable());
        assert!(IntegrationState::Verifying.is_recoverable());
        assert!(IntegrationState::ReadyToPublish.is_recoverable());
        assert!(!IntegrationState::Queued.is_recoverable());
        assert!(!IntegrationState::Integrated.is_recoverable());
    }

    #[test]
    fn test_queue_scope() {
        let req = IntegrationRequest {
            integration_id: "i1".into(),
            commit_request_id: "cr1".into(),
            candidate_id: "c1".into(),
            review_id: "r1".into(),
            repository_id: "repo-a".into(),
            target_ref: "refs/heads/main".into(),
            expected_target_head: "abc123".into(),
            priority: 10,
            idempotency_key: "ik1".into(),
            created_at: Utc::now(),
        };
        assert_eq!(
            req.queue_scope(),
            ("repo-a".to_string(), "refs/heads/main".to_string())
        );
    }
}
