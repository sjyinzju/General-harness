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

pub mod decision;
pub mod events;
pub mod faults;
pub mod gateway;
pub mod progress;
pub mod reconciler;
pub mod repo;
pub mod service;
pub mod types;

pub use decision::DecisionInput;
pub use events::TaskLoopEventWriter;
pub use gateway::{
    CreateExecutionRequest, ExecutionCreated, ExecutionObservation, FixtureI4Gateway, I4Gateway,
    ProductionI4Gateway,
};
pub use progress::{
    classify_progress, detect_cycle, AttemptProgressFingerprint, BudgetCheckResult, BudgetPolicy,
};
pub use reconciler::{ReconcileOutcome, TaskLoopReconciler};
pub use repo::TaskLoopRepo;
pub use service::{
    CancelLoopOutcome, LoopInspection, LoopStartOutcome, ObserveOutcome, PrepareAttemptOutcome,
    TaskEngineeringLoopService,
};
pub use types::*;
