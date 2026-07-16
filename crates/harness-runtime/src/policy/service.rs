//! WorkspacePolicyService — unified entry point for policy evaluation.
//! Validates the caller's lease credential (fencing token) in real time,
//! delegates to FileScopeValidator / CommandPolicyEngine / SecretScanner,
//! and persists structured PolicyEvidence.
//!
//! Fail-closed invariant: no PolicyEvidence is ever produced without a
//! live `LeaseFencingValidator` confirming that the caller's lease is
//! Active and its fencing token equals the current worktree epoch. The
//! raw lease token is NEVER persisted, logged, or rendered in Debug/Display.

use std::sync::Arc;

use harness_core::{CoreError, ErrorCode, ErrorSource};
use uuid::Uuid;

use super::command::{ApprovalRequest, CommandFingerprint, CommandPolicyEngine, PolicyDecision};
use super::evidence::{PolicyEvaluationRecord, PolicyEvidence, PolicyEvidenceStore, PolicyFinding};
use super::file_scope::{FileScopeValidator, ScopeDecision, ScopeViolation};
use super::scanner::{SecretScanReport, SecretScanner};

/// Live lease credential carried by a guard. The `lease_token` is a secret:
/// it must never be persisted, logged, or rendered. `fencing_token` is the
/// public monotonic epoch and is safe to store in evidence.
pub struct WorkspaceAccessGuard {
    pub lease_id: String,
    pub lease_token: String,
    pub fencing_token: i64,
    pub worktree_id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub evaluator_identity: String,
}

// Custom Debug: the lease token is redacted and must never leak.
impl std::fmt::Debug for WorkspaceAccessGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceAccessGuard")
            .field("lease_id", &self.lease_id)
            .field("lease_token", &"[REDACTED]")
            .field("fencing_token", &self.fencing_token)
            .field("worktree_id", &self.worktree_id)
            .field("project_id", &self.project_id)
            .field("task_id", &self.task_id)
            .field("execution_id", &self.execution_id)
            .field("evaluator_identity", &self.evaluator_identity)
            .finish()
    }
}

/// Trait injected into WorkspacePolicyService so it can verify that the
/// caller still holds an Active lease whose fencing token equals the
/// current worktree epoch. Implemented by `WorkspaceLeaseService` in
/// production; tests supply a mock.
#[async_trait::async_trait]
pub trait LeaseFencingValidator: Send + Sync {
    /// Return Ok(()) only when `lease_id` is Active and `lease_token` +
    /// `fencing_token` match the live lease record (and therefore the
    /// current worktree epoch).
    async fn validate_active_fencing(
        &self,
        lease_id: &str,
        lease_token: &str,
        fencing_token: i64,
    ) -> Result<(), CoreError>;
}

/// No-op validator for tests that do not exercise lease fencing. Explicitly
/// named to prevent accidental production use — production must inject a
/// real `WorkspaceLeaseService`-backed validator via [`WorkspacePolicyService::new`].
pub struct NoOpLeaseFencingValidator;

#[async_trait::async_trait]
impl LeaseFencingValidator for NoOpLeaseFencingValidator {
    async fn validate_active_fencing(
        &self,
        _lease_id: &str,
        _lease_token: &str,
        _fencing_token: i64,
    ) -> Result<(), CoreError> {
        Ok(())
    }
}

pub struct WorkspacePolicyService {
    evidence_store: PolicyEvidenceStore,
    command_engine: CommandPolicyEngine,
    lease_validator: Arc<dyn LeaseFencingValidator>,
}

impl WorkspacePolicyService {
    /// Production constructor: a `LeaseFencingValidator` is MANDATORY.
    pub fn new(
        evidence_store: PolicyEvidenceStore,
        lease_validator: Arc<dyn LeaseFencingValidator>,
    ) -> Self {
        Self {
            evidence_store,
            command_engine: CommandPolicyEngine::new(),
            lease_validator,
        }
    }

    /// Test-only constructor that skips live lease fencing verification.
    /// Explicitly named to prevent accidental production use.
    pub fn new_unverified_for_tests(evidence_store: PolicyEvidenceStore) -> Self {
        Self::new(evidence_store, Arc::new(NoOpLeaseFencingValidator))
    }

    /// Verify the caller's lease credential is live and matches the current
    /// fencing epoch. Called at every evidence-producing entry point.
    async fn enforce_fencing(&self, guard: &WorkspaceAccessGuard) -> Result<(), CoreError> {
        self.lease_validator
            .validate_active_fencing(&guard.lease_id, &guard.lease_token, guard.fencing_token)
            .await
            .map_err(|_| {
                policy_err(format!(
                    "lease fencing verification failed for {} (fencing={})",
                    guard.lease_id, guard.fencing_token
                ))
            })
    }

    /// Evaluate a command against policy, producing evidence.
    pub async fn evaluate_command(
        &self,
        guard: &WorkspaceAccessGuard,
        executable: &str,
        args: &[String],
        cwd: &str,
        env_names: &[String],
    ) -> Result<(PolicyDecision, PolicyEvidence), CoreError> {
        // Fail-closed: no evidence without a live lease.
        self.enforce_fencing(guard).await?;

        let fp =
            self.command_engine
                .fingerprint(executable, args, std::path::Path::new(cwd), env_names);
        let composite = fp.composite_key();

        // Idempotency: same FULL fingerprint (exec+args+cwd+env) → existing
        // result. A different executable/cwd/env with the same args cannot
        // collide because the composite key binds all four dimensions.
        if let Some(existing) = self.evidence_store.find_by_fingerprint(&composite).await? {
            if existing.fencing_token == Some(guard.fencing_token) {
                let decision = match existing.decision.as_str() {
                    "allowed" => PolicyDecision::Allow,
                    "denied" => PolicyDecision::Deny {
                        reason: existing.reasons_json.clone(),
                    },
                    _ => PolicyDecision::RequireApproval {
                        reason: existing.reasons_json.clone(),
                        fingerprint: fp.clone(),
                    },
                };
                return Ok((
                    decision,
                    PolicyEvidence {
                        evaluation: existing,
                        findings: vec![],
                    },
                ));
            }
            // Stale: another owner now holds the lease.
            return Err(policy_err("evidence is stale — lease owner changed".into()));
        }

        let decision = self.command_engine.evaluate_command(
            executable,
            args,
            std::path::Path::new(cwd),
            env_names,
        )?;

        let eval_id = format!("pe-{}", Uuid::new_v4());
        let (dec_str, reasons) = match &decision {
            PolicyDecision::Allow => ("allowed", vec!["default_allow".to_string()]),
            PolicyDecision::Deny { reason } => ("denied", vec![reason.clone()]),
            PolicyDecision::RequireApproval { reason, .. } => {
                ("require_approval", vec![reason.clone()])
            }
        };

        // Store the COMPOSITE fingerprint so future lookups bind all four
        // dimensions. Only the public fencing_token (epoch) is persisted —
        // never the lease token.
        let record = PolicyEvaluationRecord {
            id: eval_id.clone(),
            evaluation_type: "command".into(),
            project_id: guard.project_id.clone(),
            task_id: guard.task_id.clone(),
            execution_id: guard.execution_id.clone(),
            worktree_id: Some(guard.worktree_id.clone()),
            fencing_token: Some(guard.fencing_token),
            policy_version: 1,
            input_fingerprint: Some(composite),
            decision: dec_str.into(),
            reasons_json: serde_json::to_string(&reasons).unwrap_or_default(),
            changed_path_count: None,
            finding_count: None,
            artifact_reference: None,
            evaluator_identity: guard.evaluator_identity.clone(),
            created_at: String::new(),
        };
        self.evidence_store.insert_evaluation(&record).await?;

        Ok((
            decision,
            PolicyEvidence {
                evaluation: record,
                findings: vec![],
            },
        ))
    }

    /// Validate file scope for a set of paths.
    pub fn validate_file_scope(
        &self,
        validator: &FileScopeValidator,
        paths: &[String],
    ) -> Vec<(String, ScopeDecision)> {
        paths
            .iter()
            .map(|p| {
                (
                    p.clone(),
                    validator
                        .validate(p)
                        .map(|(d, _)| d)
                        .unwrap_or(ScopeDecision::Denied(ScopeViolation::AmbiguousPath(
                            p.clone(),
                        ))),
                )
            })
            .collect()
    }

    /// Scan a diff for secrets.
    pub fn scan_diff(
        &self,
        scanner: &SecretScanner,
        files: &[(String, Vec<u8>)],
    ) -> SecretScanReport {
        scanner.scan_diff(files)
    }

    /// Persist a scan report as policy evidence.
    pub async fn persist_scan_evidence(
        &self,
        guard: &WorkspaceAccessGuard,
        report: &SecretScanReport,
    ) -> Result<PolicyEvidence, CoreError> {
        // Fail-closed: no evidence without a live lease.
        self.enforce_fencing(guard).await?;

        let eval_id = format!("pe-{}", Uuid::new_v4());
        let record = PolicyEvaluationRecord {
            id: eval_id.clone(),
            evaluation_type: "secret_scan".into(),
            project_id: guard.project_id.clone(),
            task_id: guard.task_id.clone(),
            execution_id: guard.execution_id.clone(),
            worktree_id: Some(guard.worktree_id.clone()),
            fencing_token: Some(guard.fencing_token),
            policy_version: 1,
            input_fingerprint: None,
            decision: if report.clean {
                "allowed".into()
            } else {
                "denied".into()
            },
            reasons_json: serde_json::to_string(
                &report
                    .findings
                    .iter()
                    .map(|f| format!("{:?}", f.kind))
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_default(),
            changed_path_count: Some(report.files_scanned as i64),
            finding_count: Some(report.findings.len() as i64),
            artifact_reference: None,
            evaluator_identity: guard.evaluator_identity.clone(),
            created_at: String::new(),
        };
        self.evidence_store.insert_evaluation(&record).await?;

        let mut findings = Vec::new();
        for f in &report.findings {
            let fid = format!("pf-{}", Uuid::new_v4());
            let pf = PolicyFinding {
                id: fid,
                evaluation_id: eval_id.clone(),
                finding_type: format!("{:?}", f.kind),
                file_path: Some(f.file_path.clone()),
                line_number: f.line_number.map(|n| n as i64),
                byte_range_start: f.byte_range.map(|(s, _)| s as i64),
                byte_range_end: f.byte_range.map(|(_, e)| e as i64),
                redacted_preview: f.redacted_preview.clone(),
                fingerprint: None,
            };
            self.evidence_store.insert_finding(&pf).await?;
            findings.push(pf);
        }

        Ok(PolicyEvidence {
            evaluation: record,
            findings,
        })
    }

    /// Invalidate stale evidence for a worktree (new lease owner).
    pub async fn invalidate_stale_evidence(
        &self,
        worktree_id: &str,
        current_fencing_token: i64,
    ) -> Result<Vec<PolicyEvaluationRecord>, CoreError> {
        let stale = self
            .evidence_store
            .find_stale_evidence(worktree_id, current_fencing_token)
            .await?;
        for s in &stale {
            self.evidence_store.invalidate_stale(&s.id).await?;
        }
        Ok(stale)
    }

    /// Validate an approval request against a stored fingerprint.
    pub fn validate_approval(
        &self,
        request: &ApprovalRequest,
        fingerprint: &CommandFingerprint,
    ) -> Result<ApprovalOutcome, CoreError> {
        if request.command_fingerprint != *fingerprint {
            return Ok(ApprovalOutcome::FingerprintMismatch);
        }
        if let Some(ref expiry) = request.expiry {
            let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
            if expiry < &now {
                return Ok(ApprovalOutcome::Expired);
            }
        }
        Ok(ApprovalOutcome::Approved)
    }
}

/// Outcome of `validate_approval`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalOutcome {
    Approved,
    Expired,
    FingerprintMismatch,
}

fn policy_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
