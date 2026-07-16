//! PolicyReconciler — cross-check PolicyEvidence against the live worktree,
//! lease fencing epoch, policy version, artifact reference, and input
//! fingerprint. Stale or invalid evidence is marked so it can no longer
//! serve as a basis for commit/verification.

use std::path::Path;

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::evidence::{PolicyEvaluationRecord, PolicyEvidenceStore};

/// Why a piece of evidence was marked stale/invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileReason {
    StaleFencing {
        evidence_fencing: Option<i64>,
        current: i64,
    },
    OldPolicyVersion {
        evidence_version: i64,
        current: u32,
    },
    WorktreeMissing,
    ArtifactLost(String),
    AlreadyInvalid,
}

#[derive(Debug, Clone)]
pub struct ReconciliationFinding {
    pub evaluation_id: String,
    pub reason: ReconcileReason,
    pub previous_decision: String,
}

#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    pub findings: Vec<ReconciliationFinding>,
    pub marked_invalid: usize,
}

pub struct PolicyReconciler {
    store: PolicyEvidenceStore,
}

impl PolicyReconciler {
    pub fn new(store: PolicyEvidenceStore) -> Self {
        Self { store }
    }

    /// Reconcile all evidence for `worktree_id` against the current epoch /
    /// policy version / worktree existence. Invalidated rows are marked
    /// `invalid` in the DB so subsequent idempotency lookups skip them.
    pub async fn reconcile(
        &self,
        worktree_id: &str,
        current_fencing: i64,
        current_policy_version: u32,
        worktree_exists: bool,
    ) -> Result<ReconciliationReport, CoreError> {
        let rows = self.store.find_all_for_worktree(worktree_id).await?;
        let mut findings = Vec::new();
        let mut marked = 0usize;
        for row in rows {
            let prev = row.decision.clone();
            match self.classify(
                &row,
                current_fencing,
                current_policy_version,
                worktree_exists,
            ) {
                None => {} // clean — no drift.
                Some(ReconcileReason::AlreadyInvalid) => {
                    findings.push(ReconciliationFinding {
                        evaluation_id: row.id.clone(),
                        reason: ReconcileReason::AlreadyInvalid,
                        previous_decision: prev,
                    });
                }
                Some(reason) => {
                    let reason_str = format!("{reason:?}");
                    self.store.mark_invalid(&row.id, &reason_str).await?;
                    marked += 1;
                    findings.push(ReconciliationFinding {
                        evaluation_id: row.id.clone(),
                        reason,
                        previous_decision: prev,
                    });
                }
            }
        }
        Ok(ReconciliationReport {
            findings,
            marked_invalid: marked,
        })
    }

    /// Classify a row. `None` means the row is still valid (no drift).
    fn classify(
        &self,
        row: &PolicyEvaluationRecord,
        current_fencing: i64,
        current_policy_version: u32,
        worktree_exists: bool,
    ) -> Option<ReconcileReason> {
        if row.decision == "invalid" {
            return Some(ReconcileReason::AlreadyInvalid);
        }
        if !worktree_exists {
            return Some(ReconcileReason::WorktreeMissing);
        }
        if row.fencing_token != Some(current_fencing) {
            return Some(ReconcileReason::StaleFencing {
                evidence_fencing: row.fencing_token,
                current: current_fencing,
            });
        }
        if (row.policy_version as i64) < current_policy_version as i64 {
            return Some(ReconcileReason::OldPolicyVersion {
                evidence_version: row.policy_version as i64,
                current: current_policy_version,
            });
        }
        if let Some(ref artifact) = row.artifact_reference {
            if !artifact.is_empty() && !Path::new(artifact).exists() {
                return Some(ReconcileReason::ArtifactLost(artifact.clone()));
            }
        }
        None
    }
}

#[allow(dead_code)]
fn r_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
