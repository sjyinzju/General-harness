//! ProcessManager v1 — cross-platform subprocess management.
pub mod job_object;
pub mod manager;
pub mod reconciler;
pub mod registry;
pub mod types;

pub use manager::ProcessManager;
pub use reconciler::ProcessReconciler;
pub use registry::ProcessRegistry;
pub use types::*;
