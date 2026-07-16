//! harness-runtime: Persistence, repositories, transition, process management.
pub mod artifact;
pub mod db;
pub mod event_log;
pub mod idempotency;
pub mod lease;
pub mod operation;
pub mod process;
pub mod repo;
pub mod transition;
pub mod worktree;

pub use artifact::{ArtifactRoot, RuntimeArtifactDirectory};
pub use db::Database;
pub use lease::WorkspaceLeaseService;
pub use process::ProcessManager;
pub use transition::TransitionService;
pub use worktree::WorktreeManager;
