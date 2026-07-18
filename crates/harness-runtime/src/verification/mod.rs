//! Verification — deterministic post-execution quality checks.
//!
//! Batch 1: Model, persistence, idempotency.
//! Later batches: plan execution, diff/secret/policy checks, finalization.

pub mod approval_validator;
pub mod content_validator;
pub mod evidence_repo;
pub mod execution_service;
pub mod finalization;
pub mod ownership_service;
pub mod plan_repo;
pub mod policy_evidence;
pub mod reconciler;
pub mod run_repo;

pub use content_validator::VerificationContentValidator;
pub use evidence_repo::VerificationEvidenceRepo;
pub use execution_service::{
    FakeProcessExecutor, ProcessExecutor, ProcessManagerAdapter, ProcessResult,
    StepExecutionOutcome, StepExecutionRequest, VerificationExecutionService,
};
pub use finalization::{
    FinalizationDossier, FinalizationOutcome, FinalizationRequest, NextActionCategory,
    VerificationFinalizationService, VerificationOutcomeAggregator,
};
pub use ownership_service::{
    OwnershipTakeoverResult, TakeoverRequest, VerificationOwnershipService,
};
pub use plan_repo::VerificationPlanRepo;
pub use policy_evidence::{
    PolicyScanner, PolicyStepOutcome, PolicyStepRequest, VerificationPolicyEvidenceService,
};
pub use run_repo::VerificationRunRepo;
