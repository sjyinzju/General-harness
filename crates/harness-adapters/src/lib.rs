//! harness-adapters: AgentAdapter implementations + Contract Test Suite.

pub mod contract_test;
pub mod fake;

pub use contract_test::AdapterContractTest;
pub use fake::FakeAgentAdapter;
