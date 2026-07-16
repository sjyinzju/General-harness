//! LeaseHeartbeatRunner — runtime-owned, cancelable background heartbeat.
//!
//! Call `run()` once; it loops at the configured interval until the token is
//! cancelled. When the runner stops it never creates a new lease, never
//! retries an execution, and never hides a database error.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::service::WorkspaceLeaseService;
use crate::lease::types::LeaseHeartbeatOutcome;

/// Structured result produced after each heartbeat attempt.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    pub ok: bool,
    pub outcome: Option<LeaseHeartbeatOutcome>,
    pub error: Option<String>,
}

pub struct LeaseHeartbeatRunner {
    service: Arc<WorkspaceLeaseService>,
    lease_id: String,
    lease_token: String,
    fencing_token: i64,
    /// After this many consecutive failures, the lease is definitively at
    /// risk and the runner emits `LeaseHeartbeatOutcome::AtRisk`.
    max_consecutive_failures: u32,
}

impl LeaseHeartbeatRunner {
    pub fn new(
        service: Arc<WorkspaceLeaseService>,
        lease_id: String,
        lease_token: String,
        fencing_token: i64,
    ) -> Self {
        Self {
            service,
            lease_id,
            lease_token,
            fencing_token,
            max_consecutive_failures: 3,
        }
    }

    /// Run heartbeat at the configured interval until `cancel` is signalled.
    /// Each iteration reports its result via the `on_result` callback.
    pub async fn run(&self, cancel: CancellationToken, mut on_result: impl FnMut(HeartbeatResult)) {
        let interval = self.service.config().heartbeat_interval;
        let mut consecutive_failures: u32 = 0;

        loop {
            if cancel.is_cancelled() {
                break;
            }
            let delay = interval;
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(delay) => {}
            }
            if cancel.is_cancelled() {
                break;
            }

            let result = self
                .service
                .heartbeat(&self.lease_id, &self.lease_token, self.fencing_token)
                .await;

            match &result {
                Ok(LeaseHeartbeatOutcome::Ok) => {
                    consecutive_failures = 0;
                    on_result(HeartbeatResult {
                        ok: true,
                        outcome: Some(LeaseHeartbeatOutcome::Ok),
                        error: None,
                    });
                }
                Ok(LeaseHeartbeatOutcome::AtRisk { .. }) => {
                    on_result(HeartbeatResult {
                        ok: true,
                        outcome: result.ok(),
                        error: None,
                    });
                }
                Ok(
                    s @ (LeaseHeartbeatOutcome::TokenMismatch
                    | LeaseHeartbeatOutcome::FencingMismatch
                    | LeaseHeartbeatOutcome::Expired
                    | LeaseHeartbeatOutcome::NotActive),
                ) => {
                    on_result(HeartbeatResult {
                        ok: false,
                        outcome: Some(s.clone()),
                        error: Some(format!("heartbeat stopped: {s:?}")),
                    });
                    break;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    let at_risk = consecutive_failures >= self.max_consecutive_failures;
                    on_result(HeartbeatResult {
                        ok: false,
                        outcome: at_risk.then_some(LeaseHeartbeatOutcome::AtRisk {
                            expires_at: String::new(),
                        }),
                        error: Some(e.message.clone()),
                    });
                    if at_risk {
                        break;
                    }
                }
            }
        }
    }
}
