//! Resource Claim kernel — persistence, lease integration, reconciliation,
//! and TaskEnvelope adaptation.
//!
//! - [`ResourceClaimRepo`] — persistence with atomic claim groups.
//! - [`ResourceClaimService`] — production service with lease/fencing validation.
//! - [`ResourceClaimReconciler`] — detects and repairs claim anomalies.
//! - [`derive_claims_from_envelope`] — TaskEnvelope adapter.

pub mod adapter;
mod reconciler;
mod repo;
pub mod service;

pub use adapter::derive_claims_from_envelope;
pub use reconciler::{ClaimAnomaly, ReconciliationReport, ResourceClaimReconciler};
pub use repo::{AcquireOutcome, ClaimGroupRecord, ClaimGuard, ClaimRowRecord, ResourceClaimRepo};
pub use service::{ResourceClaimLeaseValidator, ResourceClaimService};
