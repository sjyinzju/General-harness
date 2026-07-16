//! Concrete `WorkspaceLeaseAccessValidator` backed by `WorkspaceLeaseService`.

use std::sync::Arc;

use super::guard::{
    LeaseAccessResult, LeaseCredential, WorkspaceLeaseAccessValidator, WorktreeAccessRequest,
};
use super::service::WorkspaceLeaseService;
use harness_core::{CoreError, ErrorCode, ErrorSource};

pub struct ServiceLeaseAccessValidator {
    service: Arc<WorkspaceLeaseService>,
}

impl ServiceLeaseAccessValidator {
    pub fn new(service: Arc<WorkspaceLeaseService>) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl WorkspaceLeaseAccessValidator for ServiceLeaseAccessValidator {
    async fn can_remove_worktree(
        &self,
        request: &WorktreeAccessRequest,
    ) -> Result<LeaseAccessResult, CoreError> {
        let active = self
            .service
            .get_active_for_worktree(&request.worktree_id)
            .await?;
        let Some(lease) = active else {
            return Ok(LeaseAccessResult::Allowed);
        };

        // If the caller presents a valid credential with matching fencing
        // token, allow even an active lease (administrative force path).
        if let Some(ref cred) = request.lease_credential {
            if cred.lease_id == lease.lease_id
                && cred.fencing_token == lease.fencing_token
                && self
                    .service
                    .validate_lease(&request.worktree_id, &cred.lease_token, cred.fencing_token)
                    .await
                    .is_ok()
            {
                return Ok(LeaseAccessResult::Allowed);
            }
            return Ok(LeaseAccessResult::StaleFencingToken);
        }

        Ok(LeaseAccessResult::BlockedByActiveLease {
            lease_id: lease.lease_id,
            owner_supervisor_id: lease.owner_supervisor_id,
        })
    }

    async fn validate_force_credential(
        &self,
        worktree_id: &str,
        credential: &LeaseCredential,
    ) -> Result<bool, CoreError> {
        Ok(self
            .service
            .validate_lease(
                worktree_id,
                &credential.lease_token,
                credential.fencing_token,
            )
            .await
            .is_ok())
    }
}

fn _e(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
