//! ProcessManager v1 — cross-platform subprocess management.
pub mod capture;
pub mod job_object;
pub mod manager;
pub mod reconciler;
pub mod redactor;
pub mod registry;
pub mod types;

pub use capture::{StreamCaptureConfig, StreamCaptureResult};
pub use manager::ProcessManager;
pub use reconciler::ProcessReconciler;
pub use redactor::ProcessEventRedactor;
pub use registry::ProcessRegistry;
pub use types::*;
