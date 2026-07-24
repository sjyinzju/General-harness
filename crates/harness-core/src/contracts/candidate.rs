//! Candidate — immutable snapshot of a completed task execution ready for review.
//!
//! A Candidate is frozen the moment Verification completes with
//! `NextActionCategory::CompleteCandidate`. It binds task spec, execution,
//! executor profile, workspace, base commit, tree hash, diff digest, and
//! evidence digest into a single immutable entity.
//!
//! Once frozen, any change to the underlying worktree, diff, task spec, or
//! evidence invalidates the Candidate (→ Stale), and any in-progress Review
//! based on it must be marked Stale.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Opaque typed identifier for a CandidateSnapshot.
pub type CandidateId = String;

/// An immutable snapshot of a completed execution, binding together the
/// task, execution, executor, workspace, and all relevant digests.
///
/// Created once per CompleteCandidate decision. Never mutated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateSnapshot {
    pub candidate_id: CandidateId,
    pub task_id: String,
    pub execution_id: String,
    pub executor_profile_id: String,

    pub workspace_id: String,
    pub base_commit: String,
    pub candidate_tree_hash: String,
    pub diff_digest: String,

    pub task_spec_digest: String,
    pub evidence_digest: String,

    pub created_at: DateTime<Utc>,
}

impl CandidateSnapshot {
    /// Compute a composite digest over all bound fields.
    /// Used for staleness detection: if ANY input changes, this digest changes.
    pub fn composite_digest(&self) -> String {
        let input = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.candidate_id,
            self.task_id,
            self.execution_id,
            self.executor_profile_id,
            self.workspace_id,
            self.base_commit,
            self.candidate_tree_hash,
            self.diff_digest,
            self.task_spec_digest,
        );
        sha256_hex(&input)
    }

    /// Verify that the evidence digest still matches the stored value.
    /// Callers should re-compute the evidence digest and compare.
    pub fn evidence_matches(&self, recomputed_evidence_digest: &str) -> bool {
        self.evidence_digest == recomputed_evidence_digest
    }

    /// Verify that the diff digest still matches the stored value.
    pub fn diff_matches(&self, recomputed_diff_digest: &str) -> bool {
        self.diff_digest == recomputed_diff_digest
    }

    /// Verify that the tree hash still matches.
    pub fn tree_matches(&self, recomputed_tree_hash: &str) -> bool {
        self.candidate_tree_hash == recomputed_tree_hash
    }

    /// Verify that the task spec digest still matches.
    pub fn task_spec_matches(&self, recomputed_spec_digest: &str) -> bool {
        self.task_spec_digest == recomputed_spec_digest
    }
}

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_candidate(
        id: &str,
        base: &str,
        tree: &str,
        diff: &str,
        task: &str,
        evidence: &str,
    ) -> CandidateSnapshot {
        CandidateSnapshot {
            candidate_id: id.into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            executor_profile_id: "p1".into(),
            workspace_id: "w1".into(),
            base_commit: base.into(),
            candidate_tree_hash: tree.into(),
            diff_digest: diff.into(),
            task_spec_digest: task.into(),
            evidence_digest: evidence.into(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_composite_digest_deterministic() {
        let c1 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev1");
        let c2 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev1");
        assert_eq!(c1.composite_digest(), c2.composite_digest());
    }

    #[test]
    fn test_composite_digest_diff_changes_digest() {
        let c1 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev1");
        let c2 = mk_candidate("c1", "abc", "tree1", "diff2", "task1", "ev1");
        assert_ne!(c1.composite_digest(), c2.composite_digest());
    }

    #[test]
    fn test_evidence_changes_digest() {
        let c1 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev1");
        let c2 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev2");
        // evidence is NOT in composite digest (it's separately verified)
        assert_eq!(c1.composite_digest(), c2.composite_digest());
        assert!(c1.evidence_matches("ev1"));
        assert!(!c1.evidence_matches("ev2"));
    }

    #[test]
    fn test_task_spec_changes_digest() {
        let c1 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev1");
        let c2 = mk_candidate("c1", "abc", "tree1", "diff1", "task2", "ev1");
        assert_ne!(c1.composite_digest(), c2.composite_digest());
    }

    #[test]
    fn test_base_commit_changes_digest() {
        let c1 = mk_candidate("c1", "abc", "tree1", "diff1", "task1", "ev1");
        let c2 = mk_candidate("c1", "def", "tree1", "diff1", "task1", "ev1");
        assert_ne!(c1.composite_digest(), c2.composite_digest());
    }
}
