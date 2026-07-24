//! Supervisor heartbeat — periodic lease renewal with CAS fencing.
//!
//! The heartbeat task runs on a configurable interval and updates
//! the lease expiry time in the database. Each heartbeat uses CAS
//! (compare-and-swap) to ensure the fencing token hasn't changed.
//! If CAS fails, the supervisor immediately enters Failed state.

use chrono::Utc;
use harness_core::contracts::supervisor::{
    SupervisorInstance, SupervisorInstanceId, SupervisorState,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing;

use super::repo::SupervisorRepo;

/// Handle to a running heartbeat task.
pub struct HeartbeatHandle {
    cancel: CancellationToken,
}

impl HeartbeatHandle {
    /// Start a background heartbeat task.
    pub async fn start(
        repo: SupervisorRepo,
        instance_id: SupervisorInstanceId,
        fencing_token: i64,
        heartbeat_interval: Duration,
        lease_duration: Duration,
        instance_state: Arc<RwLock<Option<SupervisorInstance>>>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            heartbeat_loop(
                repo,
                instance_id,
                fencing_token,
                heartbeat_interval,
                lease_duration,
                instance_state,
                cancel_clone,
            )
            .await;
        });

        Self { cancel }
    }

    /// Stop the heartbeat task.
    pub async fn stop(self) {
        self.cancel.cancel();
    }
}

async fn heartbeat_loop(
    repo: SupervisorRepo,
    instance_id: SupervisorInstanceId,
    initial_fencing_token: i64,
    heartbeat_interval: Duration,
    lease_duration: Duration,
    instance_state: Arc<RwLock<Option<SupervisorInstance>>>,
    cancel: CancellationToken,
) {
    let current_fencing_token = initial_fencing_token;
    let mut consecutive_failures: u32 = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 3;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!(%instance_id, "heartbeat loop cancelled");
                break;
            }
            _ = tokio::time::sleep(heartbeat_interval) => {}
        }

        let new_expires_at = Utc::now() + lease_duration;

        match repo
            .heartbeat_cas(&instance_id, current_fencing_token, new_expires_at)
            .await
        {
            Ok(true) => {
                // CAS succeeded — lease renewed
                consecutive_failures = 0;

                // Update in-memory instance
                let mut guard = instance_state.write().await;
                if let Some(ref mut inst) = *guard {
                    inst.heartbeat_at = Utc::now();
                    inst.lease_expires_at = new_expires_at;
                }

                tracing::debug!(
                    %instance_id,
                    fencing_token = current_fencing_token,
                    "heartbeat succeeded"
                );
            }
            Ok(false) => {
                // CAS failed — fencing token changed or state is no longer active
                consecutive_failures += 1;
                tracing::warn!(
                    %instance_id,
                    expected_fencing_token = current_fencing_token,
                    consecutive_failures,
                    "heartbeat CAS failed — lease may have been taken over"
                );

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    tracing::error!(
                        %instance_id,
                        consecutive_failures,
                        "heartbeat CAS failed repeatedly — entering Failed state"
                    );

                    // Update in-memory state to Failed
                    let mut guard = instance_state.write().await;
                    if let Some(ref mut inst) = *guard {
                        inst.state = SupervisorState::Failed;
                    }

                    // Stop further heartbeats
                    break;
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                tracing::error!(
                    %instance_id,
                    error = %e,
                    consecutive_failures,
                    "heartbeat error"
                );

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    let mut guard = instance_state.write().await;
                    if let Some(ref mut inst) = *guard {
                        inst.state = SupervisorState::Failed;
                    }
                    break;
                }
            }
        }
    }
}
