//! Verification — deterministic post-execution quality checks.
//!
//! Batch 1: Model, persistence, idempotency.
//! Later batches: plan execution, diff/secret/policy checks, finalization.

pub mod content_validator;
pub mod evidence_repo;
pub mod plan_repo;
pub mod run_repo;

pub use content_validator::VerificationContentValidator;
pub use evidence_repo::VerificationEvidenceRepo;
pub use plan_repo::VerificationPlanRepo;
pub use run_repo::VerificationRunRepo;
