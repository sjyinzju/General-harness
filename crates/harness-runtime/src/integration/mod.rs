//! Integration Queue — I5.2/I5.3 durable integration queue, sandboxed integration, atomic publish.

pub mod executor;
pub mod repo;
pub mod service;

pub use executor::IntegrationExecutor;
pub use service::IntegrationQueueService;
