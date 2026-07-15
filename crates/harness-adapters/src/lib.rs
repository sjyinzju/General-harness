//! harness-adapters: AgentAdapter implementations + Contract Test Suite.

pub mod fake;
pub mod contract_test;

pub use fake::FakeAgentAdapter;
pub use contract_test::AdapterContractTest;
