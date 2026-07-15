//! FakeAgentAdapter — fully scriptable adapter for testing.
pub mod adapter;
pub mod script;

pub use adapter::FakeAgentAdapter;
pub use script::{FakeExecutionScript, FakeFailure, FakeFileOp};
