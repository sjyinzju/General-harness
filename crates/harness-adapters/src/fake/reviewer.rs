//! FakeReviewer — scriptable reviewer for testing the I4.6 review gate.
//!
//! Produces structured ReviewerOutput from a pre-configured script.
//! Never modifies files. Always produces parseable output.

use harness_core::contracts::review::{ReviewDossier, ReviewerFinding, ReviewerOutput};

/// Pre-configured review script for deterministic testing.
#[derive(Debug, Clone)]
pub struct FakeReviewScript {
    /// The decision string: "Approved", "Rejected", or "Blocked"
    pub decision: String,
    /// The summary text
    pub summary: String,
    /// Findings to return
    pub findings: Vec<FakeReviewFinding>,
    /// If true, simulate a crash/timeout (no output produced)
    pub simulate_crash: bool,
    /// If true, produce unparseable output
    pub produce_garbage: bool,
    /// If true, modify a file in the worktree (to test read-only detection)
    pub rogue_modification: bool,
}

#[derive(Debug, Clone)]
pub struct FakeReviewFinding {
    pub severity: String,
    pub category: String,
    pub summary: String,
    pub details: String,
    pub source_location: Option<String>,
    pub blocking: bool,
}

impl Default for FakeReviewScript {
    fn default() -> Self {
        Self {
            decision: "Approved".into(),
            summary: "All checks passed. No findings.".into(),
            findings: vec![],
            simulate_crash: false,
            produce_garbage: false,
            rogue_modification: false,
        }
    }
}

/// A fake reviewer that executes a pre-configured script.
pub struct FakeReviewer {
    script: FakeReviewScript,
}

impl FakeReviewer {
    pub fn new(script: FakeReviewScript) -> Self {
        Self { script }
    }

    /// Create an "Approved" reviewer (zero findings).
    pub fn approved() -> Self {
        Self::new(FakeReviewScript::default())
    }

    /// Create a "Rejected" reviewer with a single finding.
    pub fn rejected(severity: &str, category: &str, summary: &str) -> Self {
        Self::new(FakeReviewScript {
            decision: "Rejected".into(),
            summary: format!("Rejected: {summary}"),
            findings: vec![FakeReviewFinding {
                severity: severity.into(),
                category: category.into(),
                summary: summary.into(),
                details: format!("Details for: {summary}"),
                source_location: None,
                blocking: true,
            }],
            ..Default::default()
        })
    }

    /// Create a "Blocked" reviewer.
    pub fn blocked(reason: &str) -> Self {
        Self::new(FakeReviewScript {
            decision: "Blocked".into(),
            summary: format!("Blocked: {reason}"),
            findings: vec![],
            ..Default::default()
        })
    }

    /// Create a reviewer that crashes (no output).
    pub fn crashing() -> Self {
        Self::new(FakeReviewScript {
            simulate_crash: true,
            ..Default::default()
        })
    }

    /// Create a reviewer that produces unparseable output.
    pub fn garbage() -> Self {
        Self::new(FakeReviewScript {
            produce_garbage: true,
            ..Default::default()
        })
    }

    /// Create a reviewer that modifies files (rogue).
    pub fn rogue() -> Self {
        Self::new(FakeReviewScript {
            rogue_modification: true,
            ..Default::default()
        })
    }

    /// Review a dossier and produce output (or simulate crash/garbage/rogue).
    pub async fn review(&self, _dossier: &ReviewDossier) -> Result<ReviewerOutput, String> {
        if self.script.simulate_crash {
            return Err("FakeReviewer: simulated crash".into());
        }

        if self.script.produce_garbage {
            return Err("FakeReviewer: garbage output".into());
        }

        if self.script.rogue_modification {
            // Rogue reviewer modifies worktree — detected by digest change
            // In real env, this would modify files. Here we just signal it.
            // The harness should detect the digest change post-review.
            return Ok(ReviewerOutput {
                decision: "Approved".into(),
                summary: "Looks good (rogue modification hidden)".into(),
                findings: vec![],
            });
        }

        let findings: Vec<ReviewerFinding> = self
            .script
            .findings
            .iter()
            .map(|f| ReviewerFinding {
                severity: f.severity.clone(),
                category: f.category.clone(),
                summary: f.summary.clone(),
                details: f.details.clone(),
                source_location: f.source_location.clone(),
                evidence_reference: None,
                blocking: f.blocking,
            })
            .collect();

        Ok(ReviewerOutput {
            decision: self.script.decision.clone(),
            summary: self.script.summary.clone(),
            findings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_dossier() -> ReviewDossier {
        ReviewDossier {
            dossier_id: "d1".into(),
            review_id: "r1".into(),
            candidate_id: "c1".into(),
            task_goal: "test".into(),
            acceptance_criteria: vec![],
            explicit_constraints: vec![],
            allowed_files: vec![],
            executor_profile_id: "p1".into(),
            executor_agent_kind: "fake".into(),
            base_commit: "abc".into(),
            candidate_diff_summary: "none".into(),
            changed_files: vec![],
            completion_eligibility_result: "CompleteCandidate".into(),
            test_summary: "all passed".into(),
            evidence_index: vec![],
            known_limitations: vec![],
            required_output_schema: "{}".into(),
            dossier_digest: "digest".into(),
            created_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_fake_reviewer_approved() {
        let reviewer = FakeReviewer::approved();
        let output = reviewer.review(&mk_dossier()).await.unwrap();
        assert_eq!(output.decision, "Approved");
        assert!(output.findings.is_empty());
    }

    #[tokio::test]
    async fn test_fake_reviewer_rejected() {
        let reviewer = FakeReviewer::rejected("Critical", "Correctness", "null pointer bug");
        let output = reviewer.review(&mk_dossier()).await.unwrap();
        assert_eq!(output.decision, "Rejected");
        assert_eq!(output.findings.len(), 1);
        assert_eq!(output.findings[0].severity, "Critical");
    }

    #[tokio::test]
    async fn test_fake_reviewer_blocked() {
        let reviewer = FakeReviewer::blocked("infrastructure error");
        let output = reviewer.review(&mk_dossier()).await.unwrap();
        assert_eq!(output.decision, "Blocked");
    }

    #[tokio::test]
    async fn test_fake_reviewer_crash() {
        let reviewer = FakeReviewer::crashing();
        let result = reviewer.review(&mk_dossier()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fake_reviewer_garbage() {
        let reviewer = FakeReviewer::garbage();
        let result = reviewer.review(&mk_dossier()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fake_reviewer_rogue() {
        let reviewer = FakeReviewer::rogue();
        let output = reviewer.review(&mk_dossier()).await.unwrap();
        // Rogue reviewer still produces output, but the harness should
        // detect the file modification via digest change.
        assert_eq!(output.decision, "Approved");
    }
}
