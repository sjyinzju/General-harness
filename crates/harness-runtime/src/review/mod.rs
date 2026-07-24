//! Candidate Review Gate (I4.6) — candidate freezing, deterministic precheck,
//! independent reviewer selection, read-only review execution, structured findings,
//! decision policy, staleness detection, and I5 ApprovedCandidate contract.

pub mod repo;
pub mod service;

pub use repo::{CacheEntry, CandidateRepo, ReviewRepo};
pub use service::{
    validate_dossier_bounds, DossierBoundsCheck, ReviewOrchestrationService, REVIEW_POLICY_VERSION,
};
