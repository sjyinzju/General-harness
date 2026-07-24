//! Liveness subsystem — unified temporary artifact lifecycle management.
//!
//! This module provides:
//! - **DeletionGuard** — multi-layer safety checks before any automated deletion.
//! - **HarnessTempDir** — managed runtime temp directories with ownership markers.
//! - **HarnessEvidenceDir** — managed evidence directories with retention policies.
//! - **ManagedCargoRunDir** — isolated Cargo target directories with cleanup.
//! - **LivenessOrchestrator** — startup/completion/CLI janitor coordination.
//! - **RunContext** — per-run managed temp provider with env redirection.
//!
//! # Safety invariants
//! - Every managed directory carries a `.harness-owned.json` marker.
//! - No automated deletion without DeletionGuard approval.
//! - Existing unmarked target artifacts are NEVER deleted by automation.
//! - `target/debug` (shared Cargo cache) is NEVER deleted.
//! - Repo root, target root, user home, system TEMP root are PROTECTED.
//!
//! # Entry points
//! - `RunContext::create()` — per-run managed temp + env redirection.
//! - `LivenessOrchestrator::startup_janitor()` — reclaim stale owned dirs at boot.
//! - `LivenessOrchestrator::completion_janitor()` — clean current-run temp at exit.
//! - `LivenessOrchestrator::cli_cleanup()` — manual `harness cleanup` command.

pub mod cargo_target;
pub mod evidence_dir;
pub mod guard;
pub mod orchestrator;
pub mod run_context;
pub mod temp_dir;
pub mod types;

// Re-exports for convenience.
pub use cargo_target::ManagedCargoRunDir;
pub use evidence_dir::HarnessEvidenceDir;
pub use guard::DeletionGuard;
pub use orchestrator::LivenessOrchestrator;
pub use run_context::{RunContext, TempPathProvider};
pub use temp_dir::HarnessTempDir;
pub use types::{
    CleanupAction, CleanupEntry, CleanupResult, EvidenceRetention, LivenessConfig, ManagedDirKind,
    MarkerState, OwnershipMarker, ProtectedPaths, SafetyVerdict,
};
