//! Review — independent read-only review of a frozen Candidate.
//!
//! Reviews are performed by a Reviewer Agent that is DIFFERENT from the
//! Executor. Reviews are immutable once terminal. Re-review requires a new
//! ReviewRequest.
//!
//! All types are pure data. No I/O, no Agent dependencies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::candidate::CandidateId;

// ── Typed IDs ──────────────────────────────────────────────────────────

pub type ReviewId = String;
pub type FindingId = String;

// ── Review State ───────────────────────────────────────────────────────

/// The lifecycle of a single Review.
///
/// Terminal states: Approved, Rejected, Blocked, Cancelled, Stale.
/// Non-terminal states can transition to Cancelled.
/// Candidate change → Stale (from any non-terminal state).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewState {
    /// Review created but not yet started.
    Requested,
    /// Preparing the review dossier.
    Preparing,
    /// Running deterministic prechecks.
    Prechecking,
    /// Reviewer is actively reviewing.
    Reviewing,
    // ── Terminal ──
    /// All checks passed, zero findings.
    Approved,
    /// One or more findings block approval.
    Rejected,
    /// Precheck or infrastructure blocked the review.
    Blocked,
    /// Review was explicitly cancelled.
    Cancelled,
    /// Candidate changed during review.
    Stale,
}

impl ReviewState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Approved | Self::Rejected | Self::Blocked | Self::Cancelled | Self::Stale
        )
    }

    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// Returns the canonical state name as a static string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Preparing => "preparing",
            Self::Prechecking => "prechecking",
            Self::Reviewing => "reviewing",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Stale => "stale",
        }
    }
}

// ── Review Request ─────────────────────────────────────────────────────

/// A request to review a frozen Candidate.
/// Exactly one active ReviewRequest should exist per Candidate at a time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewRequest {
    pub review_id: ReviewId,
    pub candidate_id: CandidateId,
    pub reviewer_profile_id: String,
    pub state: ReviewState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

// ── Review Finding ─────────────────────────────────────────────────────

/// Severity of a ReviewFinding.
/// Higher numeric level = more severe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Critical,
    High,
    Medium,
    Low,
}

impl FindingSeverity {
    /// Numeric severity level: 4=Critical, 3=High, 2=Medium, 1=Low.
    pub fn level(&self) -> u8 {
        match self {
            Self::Critical => 4,
            Self::High => 3,
            Self::Medium => 2,
            Self::Low => 1,
        }
    }

    /// Returns true if this severity is at least as severe as `other`.
    pub fn at_least(&self, other: &FindingSeverity) -> bool {
        self.level() >= other.level()
    }
}

/// Category of a ReviewFinding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingCategory {
    RequirementMismatch,
    ScopeViolation,
    Correctness,
    Safety,
    Security,
    EvidenceGap,
    TestGap,
    ArchitectureViolation,
    Maintainability,
}

/// A single finding produced during review.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewFinding {
    pub finding_id: FindingId,
    pub review_id: ReviewId,
    pub severity: FindingSeverity,
    pub category: FindingCategory,
    pub summary: String,
    pub details: String,
    pub source_location: Option<String>,
    pub evidence_reference: Option<String>,
    pub blocking: bool,
}

// ── Review Decision ────────────────────────────────────────────────────

/// The terminal decision of a review.
/// No fuzzy states allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecision {
    Approved,
    Rejected,
    Blocked,
    Stale,
}

/// Structured output from the Reviewer.
/// The Reviewer MUST produce parseable JSON — natural-language-only
/// output is rejected (→ Blocked).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewerOutput {
    pub decision: String, // "Approved" | "Rejected" | "Blocked"
    pub summary: String,
    pub findings: Vec<ReviewerFinding>,
}

/// A single finding in the Reviewer's structured output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewerFinding {
    pub severity: String, // "Critical" | "High" | "Medium" | "Low"
    pub category: String,
    pub summary: String,
    pub details: String,
    pub source_location: Option<String>,
    pub evidence_reference: Option<String>,
    pub blocking: bool,
}

impl ReviewerOutput {
    /// Parse the Reviewer's decision string into a ReviewDecision.
    pub fn parse_decision(&self) -> Option<ReviewDecision> {
        match self.decision.as_str() {
            "Approved" => Some(ReviewDecision::Approved),
            "Rejected" => Some(ReviewDecision::Rejected),
            "Blocked" => Some(ReviewDecision::Blocked),
            _ => None,
        }
    }

    /// Convert Reviewer findings into domain ReviewFindings.
    pub fn to_findings(&self, review_id: &str) -> Vec<ReviewFinding> {
        self.findings
            .iter()
            .map(|f| ReviewFinding {
                finding_id: format!("f-{}", uuid::Uuid::new_v4()),
                review_id: review_id.into(),
                severity: parse_severity(&f.severity),
                category: parse_category(&f.category),
                summary: f.summary.clone(),
                details: f.details.clone(),
                source_location: f.source_location.clone(),
                evidence_reference: f.evidence_reference.clone(),
                blocking: f.blocking,
            })
            .collect()
    }

    /// Check if this output represents zero findings.
    pub fn has_no_findings(&self) -> bool {
        self.findings.is_empty()
    }
}

fn parse_severity(s: &str) -> FindingSeverity {
    match s {
        "Critical" => FindingSeverity::Critical,
        "High" => FindingSeverity::High,
        "Medium" => FindingSeverity::Medium,
        "Low" => FindingSeverity::Low,
        _ => FindingSeverity::Medium, // conservative default
    }
}

fn parse_category(s: &str) -> FindingCategory {
    match s {
        "RequirementMismatch" => FindingCategory::RequirementMismatch,
        "ScopeViolation" => FindingCategory::ScopeViolation,
        "Correctness" => FindingCategory::Correctness,
        "Safety" => FindingCategory::Safety,
        "Security" => FindingCategory::Security,
        "EvidenceGap" => FindingCategory::EvidenceGap,
        "TestGap" => FindingCategory::TestGap,
        "ArchitectureViolation" => FindingCategory::ArchitectureViolation,
        "Maintainability" => FindingCategory::Maintainability,
        _ => FindingCategory::Correctness, // conservative default
    }
}

// ── Review Dossier ─────────────────────────────────────────────────────

/// A bounded, structured brief for the Reviewer.
/// Contains everything the Reviewer needs, and NOTHING more.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewDossier {
    pub dossier_id: String,
    pub review_id: ReviewId,
    pub candidate_id: CandidateId,

    // Task context
    pub task_goal: String,
    pub acceptance_criteria: Vec<String>,
    pub explicit_constraints: Vec<String>,
    pub allowed_files: Vec<String>,

    // Execution context
    pub executor_profile_id: String,
    pub executor_agent_kind: String,
    pub base_commit: String,
    pub candidate_diff_summary: String,
    pub changed_files: Vec<String>,

    // I4.5 context
    pub completion_eligibility_result: String,
    pub test_summary: String,
    pub evidence_index: Vec<String>,

    // Constraints
    pub known_limitations: Vec<String>,
    pub required_output_schema: String,

    // Metadata
    pub dossier_digest: String,
    pub created_at: DateTime<Utc>,
}

impl ReviewDossier {
    /// Compute a digest of the dossier content (for idempotency and integrity).
    pub fn compute_digest(&self) -> String {
        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{}",
            self.dossier_id,
            self.candidate_id,
            self.task_goal,
            self.executor_profile_id,
            self.base_commit,
            self.candidate_diff_summary,
            self.completion_eligibility_result,
            self.test_summary,
            self.required_output_schema,
            self.acceptance_criteria,
            self.changed_files,
            self.evidence_index,
            self.known_limitations,
            self.explicit_constraints,
            sha256_hex(&serde_json::to_string(&self.allowed_files).unwrap_or_default()),
        );
        sha256_hex(&input)
    }
}

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

// ── ApprovedCandidate (I5 contract) ────────────────────────────────────

/// The ONLY output of I4.6 that I5 is allowed to consume.
/// An ApprovedCandidate certifies that:
/// - A Candidate was frozen
/// - A Review was completed with terminal Approved state
/// - All digests matched at review time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovedCandidate {
    pub candidate_id: CandidateId,
    pub review_id: ReviewId,
    pub candidate_tree_hash: String,
    pub diff_digest: String,
    pub review_decision_digest: String,
    pub approved_at: DateTime<Utc>,
}

impl ApprovedCandidate {
    /// Verify this ApprovedCandidate is still valid given current digests.
    pub fn is_still_valid(&self, current_tree_hash: &str, current_diff_digest: &str) -> bool {
        self.candidate_tree_hash == current_tree_hash && self.diff_digest == current_diff_digest
    }
}

// ── Precheck Result ────────────────────────────────────────────────────

/// Result of deterministic precheck before reviewer invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecheckResult {
    pub passed: bool,
    pub blocker_reason: Option<String>,
    pub findings: Vec<PrecheckFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecheckFinding {
    pub check_name: String,
    pub passed: bool,
    pub detail: String,
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_review_state_terminal() {
        assert!(ReviewState::Approved.is_terminal());
        assert!(ReviewState::Rejected.is_terminal());
        assert!(ReviewState::Blocked.is_terminal());
        assert!(ReviewState::Cancelled.is_terminal());
        assert!(ReviewState::Stale.is_terminal());
        assert!(!ReviewState::Requested.is_terminal());
        assert!(!ReviewState::Preparing.is_terminal());
        assert!(!ReviewState::Prechecking.is_terminal());
        assert!(!ReviewState::Reviewing.is_terminal());
    }

    #[test]
    fn test_reviewer_output_parse_approved() {
        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "all good".into(),
            findings: vec![],
        };
        assert_eq!(output.parse_decision(), Some(ReviewDecision::Approved));
    }

    #[test]
    fn test_reviewer_output_parse_rejected() {
        let output = ReviewerOutput {
            decision: "Rejected".into(),
            summary: "bad".into(),
            findings: vec![ReviewerFinding {
                severity: "Critical".into(),
                category: "Correctness".into(),
                summary: "bug".into(),
                details: "a bug".into(),
                source_location: None,
                evidence_reference: None,
                blocking: true,
            }],
        };
        assert_eq!(output.parse_decision(), Some(ReviewDecision::Rejected));
    }

    #[test]
    fn test_reviewer_output_parse_bad_decision() {
        let output = ReviewerOutput {
            decision: "MostlyApproved".into(),
            summary: "hmm".into(),
            findings: vec![],
        };
        assert_eq!(output.parse_decision(), None);
    }

    #[test]
    fn test_reviewer_output_no_findings() {
        let output = ReviewerOutput {
            decision: "Approved".into(),
            summary: "clean".into(),
            findings: vec![],
        };
        assert!(output.has_no_findings());
    }

    #[test]
    fn test_reviewer_output_has_findings() {
        let output = ReviewerOutput {
            decision: "Rejected".into(),
            summary: "issues".into(),
            findings: vec![ReviewerFinding {
                severity: "Low".into(),
                category: "Maintainability".into(),
                summary: "naming".into(),
                details: "bad name".into(),
                source_location: None,
                evidence_reference: None,
                blocking: false,
            }],
        };
        assert!(!output.has_no_findings());
    }

    #[test]
    fn test_finding_severity_ordering() {
        assert_eq!(FindingSeverity::Critical.level(), 4);
        assert_eq!(FindingSeverity::High.level(), 3);
        assert_eq!(FindingSeverity::Medium.level(), 2);
        assert_eq!(FindingSeverity::Low.level(), 1);
        assert!(FindingSeverity::Critical.at_least(&FindingSeverity::High));
        assert!(FindingSeverity::High.at_least(&FindingSeverity::Medium));
        assert!(!FindingSeverity::Low.at_least(&FindingSeverity::Medium));
    }

    #[test]
    fn test_approved_candidate_validity() {
        let ac = ApprovedCandidate {
            candidate_id: "c1".into(),
            review_id: "r1".into(),
            candidate_tree_hash: "tree1".into(),
            diff_digest: "diff1".into(),
            review_decision_digest: "dec1".into(),
            approved_at: Utc::now(),
        };
        assert!(ac.is_still_valid("tree1", "diff1"));
        assert!(!ac.is_still_valid("tree2", "diff1"));
        assert!(!ac.is_still_valid("tree1", "diff2"));
    }

    #[test]
    fn test_dossier_digest_deterministic() {
        let mut d1 = ReviewDossier {
            dossier_id: "d1".into(),
            review_id: "r1".into(),
            candidate_id: "c1".into(),
            task_goal: "fix bug".into(),
            acceptance_criteria: vec!["test passes".into()],
            explicit_constraints: vec![],
            allowed_files: vec!["src/".into()],
            executor_profile_id: "p1".into(),
            executor_agent_kind: "claude".into(),
            base_commit: "abc123".into(),
            candidate_diff_summary: "1 file changed".into(),
            changed_files: vec!["src/main.rs".into()],
            completion_eligibility_result: "CompleteCandidate".into(),
            test_summary: "all passed".into(),
            evidence_index: vec!["ev1".into()],
            known_limitations: vec![],
            required_output_schema: r#"{"decision":"string"}"#.into(),
            dossier_digest: "".into(),
            created_at: Utc::now(),
        };
        let mut d2 = d1.clone();
        d1.dossier_digest = d1.compute_digest();
        d2.dossier_digest = d2.compute_digest();
        assert_eq!(d1.dossier_digest, d2.dossier_digest);
    }
}
