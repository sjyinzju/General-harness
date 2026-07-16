//! Resource Claim model — pure domain types and conflict detection.
//!
//! This module has ZERO dependencies on I/O, databases, subprocesses,
//! or any specific Agent. It defines the canonical representation of
//! resource claims and the deterministic overlap/conflict engine used
//! by the persistence layer and scheduler.
//!
//! # Resource Kinds (v1)
//!
//! - [`ResourceKind::ExactFile`] — a single file by repo-relative path.
//! - [`ResourceKind::DirectoryPrefix`] — a directory and everything under it.
//! - [`ResourceKind::RepositoryWide`] — the entire repository.
//! - [`ResourceKind::Logical`] — a named logical resource (e.g. "database-schema").
//!
//! # Access Modes
//!
//! - [`AccessMode::Read`] — shared; compatible with other readers.
//! - [`AccessMode::Write`] — exclusive; conflicts with readers and writers.
//!
//! # Claim Groups
//!
//! A [`ClaimGroupSpec`] bundles multiple [`ResourceClaimSpec`] values that
//! must be acquired atomically: all succeed or none succeed. The engine
//! normalizes duplicates, upgrades weaker modes, and produces a stable,
//! hash-able representation before conflict checking.

mod engine;
mod normalize;
mod spec;
mod types;

pub use engine::{ExistingClaim, ResourceOverlapEngine};
pub use normalize::NormalizedResourcePath;
pub use spec::{ClaimGroupIdentity, ClaimGroupSpec, ResourceClaimRecord, ResourceClaimSpec};
pub use types::{
    AccessMode, ClaimConflict, ClaimDecision, ClaimLifecycle, ConflictReason, LogicalResourceKey,
    ResourceIdentity, ResourceKind,
};
