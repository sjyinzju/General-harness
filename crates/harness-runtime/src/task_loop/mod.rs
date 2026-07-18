//! I4.5 Task Engineering Loop — evidence-gated, bounded Attempt sequencing
//! for a single Task.
//!
//! I4 owns one Execution Attempt end-to-end. I4.5 owns the loop across
//! Attempts: it creates new immutable Executions, reads certified I4
//! outcomes, and deterministically decides whether to stop or create the
//! next Attempt with a Repair Context Pack.
//!
//! NEVER: modifies I4 outcomes, calls Agent/LLM directly, commits/merges,
//! deletes Worktrees, or creates Tasks.

pub mod events;
pub mod repo;
pub mod types;

pub use events::TaskLoopEventWriter;
pub use repo::TaskLoopRepo;
pub use types::*;
