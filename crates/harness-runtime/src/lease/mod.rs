//! WorkspaceLeaseService v1 — lease ownership, fencing, heartbeat, and
//! reconciliation. Integrated with WorktreeManager for safety gating.

pub mod access_validator;
pub mod clock;
pub mod guard;
pub mod reconciler;
pub mod runner;
pub mod service;
pub mod transition;
pub mod types;

pub use access_validator::ServiceLeaseAccessValidator;
pub use clock::{Clock, SystemClock, TestClock};
pub use guard::{
    LeaseAccessResult, LeaseCredential, NoOpAccessValidator, WorkspaceLeaseAccessValidator,
    WorktreeAccessRequest,
};
pub use reconciler::{LeaseDrift, LeaseDriftKind, WorkspaceLeaseReconciler};
pub use runner::{HeartbeatResult, LeaseHeartbeatRunner};
pub use service::WorkspaceLeaseService;
pub use types::*;
