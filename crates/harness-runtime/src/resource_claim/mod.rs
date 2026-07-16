//! Resource Claim persistence — atomic claim group acquisition,
//! cross-connection serialization, and DomainEvent emission.
//!
//! The [`ResourceClaimRepo`] is the persistence layer. A higher-level
//! [`super::ResourceClaimService`] (in `service.rs`) will inject
//! lease/fencing validation and reconciliation.

mod repo;

pub use repo::{AcquireOutcome, ClaimGroupRecord, ClaimGuard, ClaimRowRecord, ResourceClaimRepo};
