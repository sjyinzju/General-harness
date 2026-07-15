//! harness-core: Domain contracts, state machines, and policy types.
//!
//! This crate has ZERO dependencies on I/O, databases, subprocesses,
//! or any specific Agent (Claude, Codex, etc.).

pub mod contracts;
pub mod policies;
pub mod state_machine;

/// Common error type for harness-core.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("Invalid state transition: {current} -> {target}: {reason}")]
    InvalidTransition {
        current: String,
        target: String,
        reason: String,
    },
    #[error("Validation error: {0}")]
    Validation(String),
}
