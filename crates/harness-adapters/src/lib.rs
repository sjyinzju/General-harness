//! harness-adapters: AgentAdapter implementations + Contract Test Suite.

pub mod claude;
pub mod codex;
pub mod contract_test;
pub mod fake;

pub use claude::ClaudeCliAdapter;
pub use codex::CodexCliAdapter;
pub use contract_test::AdapterContractTest;
pub use fake::FakeAgentAdapter;
