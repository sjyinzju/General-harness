//! Workspace Policy v1 — file scope, path safety, command policy, secret
//! scanning, diff scope validation, policy reconciliation, structured
//! policy evidence, and approval persistence.

pub mod command;
pub mod diff;
pub mod evidence;
pub mod file_scope;
pub mod reconciler;
pub mod scanner;
pub mod service;

pub use command::{
    ApprovalDecision, ApprovalRequest, CommandFingerprint, CommandPolicyEngine, PolicyDecision,
};
pub use diff::{
    ChangeKind, ChangedPath, DiffArea, DiffIncludes, GitDiffScopeValidator, RenameEvidence,
    ScopeValidationReport, UntrackedEvidence,
};
pub use evidence::{ApprovalRecord, PolicyEvaluationRecord, PolicyEvidence, PolicyFinding};
pub use file_scope::{
    FileScopeValidator, NormalizedWorkspacePath, ScopeDecision, ScopeRule, ScopeViolation,
};
pub use reconciler::{
    PolicyReconciler, ReconcileReason, ReconciliationFinding, ReconciliationReport,
};
pub use scanner::{SecretFinding, SecretKind, SecretScanReport};
pub use service::{
    ApprovalOutcome, LeaseFencingValidator, NoOpLeaseFencingValidator, WorkspaceAccessGuard,
    WorkspacePolicyService,
};
