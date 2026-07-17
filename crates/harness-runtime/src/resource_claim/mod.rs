//! Resource Claim kernel — persistence, lease integration, reconciliation,
//! and TaskEnvelope adaptation.
//!
//! - [`ResourceClaimRepo`] — persistence with atomic claim groups.
//! - [`ResourceClaimService`] — production service with lease/fencing validation.
//! - [`ResourceClaimReconciler`] — detects and repairs claim anomalies.
//! - [`derive_claims_from_envelope`] — TaskEnvelope adapter.
//!
//! # Note on explicit_auto_deref
//!
//! sqlx 0.8 implements `Executor` for `&mut SqliteConnection` but not for
//! `&mut PoolConnection<Sqlite>`. Code in this module must explicitly deref
//! `PoolConnection` to `SqliteConnection` with `&mut *conn`, which triggers
//! a false-positive clippy warning. We allow it at module level.

#![allow(clippy::explicit_auto_deref)]

pub mod adapter;
mod reconciler;
mod repo;
pub mod service;

pub use adapter::derive_claims_from_envelope;
pub use reconciler::{ClaimAnomaly, ReconciliationReport, ResourceClaimReconciler};
pub use repo::{AcquireOutcome, ClaimGroupRecord, ClaimGuard, ClaimRowRecord, ResourceClaimRepo};
pub use service::{ResourceClaimLeaseValidator, ResourceClaimService};
