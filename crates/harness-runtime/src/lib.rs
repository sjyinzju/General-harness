//! harness-runtime: Persistence, repositories, transition, process management.
pub mod artifact;
pub mod db;
pub mod discovery;
pub mod event_log;
pub mod idempotency;
pub mod lease;
pub mod operation;
pub mod policy;
pub mod process;
pub mod repo;
pub mod resource_claim;
pub mod transition;
pub mod worktree;

pub use artifact::{ArtifactRoot, RuntimeArtifactDirectory};
pub use db::Database;
pub use lease::WorkspaceLeaseService;
pub use process::ProcessManager;
pub use transition::TransitionService;
pub use worktree::WorktreeManager;
