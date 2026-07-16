//! WorktreeManager v1 — git repository inspection, worktree lifecycle
//! (create/inspect/remove via Operation/Saga), ownership metadata, and
//! reconciliation. System `git` CLI is the primary implementation.

pub mod git;
pub mod git_verifier;
pub mod inspector;
pub mod lock;
pub mod manager;
pub mod metadata;
pub mod naming;
pub mod reconciler;
pub mod types;

pub use git::{GitOutput, GitRunner};
pub use git_verifier::{GitVerificationResult, NoOpGitVerifier, WorktreeGitVerifier};
pub use inspector::{RepositoryFacts, RepositoryInspector, WorktreeListEntry};
pub use lock::RepositoryLocks;
pub use manager::WorktreeManager;
pub use metadata::WorktreeMetadata;
pub use reconciler::{WorktreeDrift, WorktreeDriftKind, WorktreeReconciler};
pub use types::*;
