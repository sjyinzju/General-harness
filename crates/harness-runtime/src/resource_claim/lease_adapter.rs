//! Production adapter: bridges `WorkspaceLeaseService` into the
//! `ResourceClaimLeaseValidator` trait so claim acquisition can validate
//! lease identity, token, fencing, and expiry through the real lease
//! service (not a test stub).

use std::sync::Arc;

use crate::lease::service::WorkspaceLeaseService;

/// Production adapter that delegates lease validation to the real
/// `WorkspaceLeaseService`.
pub struct LeaseServiceAdapter {
    lease_service: Arc<WorkspaceLeaseService>,
}

impl LeaseServiceAdapter {
    pub fn new(lease_service: Arc<WorkspaceLeaseService>) -> Self {
        Self { lease_service }
    }
}

#[async_trait::async_trait]
impl crate::resource_claim::service::ResourceClaimLeaseValidator for LeaseServiceAdapter {
    async fn validate_lease(
        &self,
        lease_id: &str,
        lease_token: &str,
        fencing_token: i64,
    ) -> Result<(), harness_core::CoreError> {
        self.lease_service
            .validate_lease(lease_id, lease_token, fencing_token)
            .await
    }

    async fn get_lease_expires_at(
        &self,
        lease_id: &str,
    ) -> Result<Option<String>, harness_core::CoreError> {
        self.lease_service
            .get_lease(lease_id)
            .await
            .map(|r| r.map(|lr| lr.expires_at))
    }
}
