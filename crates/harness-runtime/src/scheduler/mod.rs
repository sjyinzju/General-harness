//! Task DAG Scheduler — deterministic orchestration of Agent dispatch.
//!
//! Batch 1: Task readiness evaluation + profile selection + concurrency arbitration.
//! Batch 2: Dispatch saga (worktree → lease → claim → adapter → events).
//! Batch 3: Scheduler reconciler (anomaly detection and repair).

pub mod concurrency;
pub mod dispatch;
pub mod event_sink;
pub mod profile_selector;
pub mod readiness;
pub mod reconciler;

pub use concurrency::ConcurrencyManager;
pub use dispatch::SchedulerOrchestrator;
pub use event_sink::SchedulerEventSink;
pub use profile_selector::RuntimeProfileSelector;
pub use readiness::TaskReadinessEvaluator;
pub use reconciler::SchedulerReconciler;
