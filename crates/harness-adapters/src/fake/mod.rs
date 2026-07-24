//! FakeAgentAdapter — fully scriptable adapter for testing.
pub mod adapter;
pub mod reviewer;
pub mod script;

pub use adapter::FakeAgentAdapter;
pub use reviewer::{FakeReviewScript, FakeReviewer};
pub use script::{FakeExecutionScript, FakeFailure, FakeFileOp};
