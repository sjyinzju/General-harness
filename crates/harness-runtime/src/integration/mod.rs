//! Integration Queue — I5.2/I5.3 durable integration queue, sandboxed integration, atomic publish.

pub mod repo;
pub mod service;

pub use service::IntegrationQueueService;
