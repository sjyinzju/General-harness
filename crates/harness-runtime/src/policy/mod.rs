//! Workspace Policy v1 — file scope, path safety, command policy, secret
//! scanning, and structured policy evidence.

pub mod command;
pub mod evidence;
pub mod file_scope;
pub mod scanner;
pub mod service;

pub use command::{
    ApprovalDecision, ApprovalRequest, CommandFingerprint, CommandPolicyEngine, PolicyDecision,
};
pub use evidence::{PolicyEvaluationRecord, PolicyEvidence, PolicyFinding};
pub use file_scope::{
    FileScopeValidator, NormalizedWorkspacePath, ScopeDecision, ScopeRule, ScopeViolation,
};
pub use scanner::{SecretFinding, SecretKind, SecretScanReport};
pub use service::WorkspacePolicyService;
