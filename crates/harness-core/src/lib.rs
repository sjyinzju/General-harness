//! harness-core: Domain contracts, state machines, and policy types.
//!
//! This crate has ZERO dependencies on I/O, databases, subprocesses,
//! or any specific Agent (Claude, Codex, etc.).

pub mod contracts;
pub mod error;
pub mod policies;
pub mod state_machine;

pub use error::{CoreError, ErrorCode, ErrorSource};
