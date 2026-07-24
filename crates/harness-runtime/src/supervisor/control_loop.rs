//! Supervisor deterministic control loop.
//!
//! The main loop runs on each tick (periodic or IPC wakeup):
//! 1. Renew supervisor lease
//! 2. Persist pending operation intents
//! 3. Select runnable operations
//! 4. Acquire claims/leases
//! 5. Launch bounded operations
//! 6. Consume events/results
//! 7. Persist transitions
//! 8. Clean up completed resources

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use harness_core::contracts::supervisor::SupervisorInstanceId;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing;

use super::repo::SupervisorRepo;

/// Configuration for the control loop.
#[derive(Debug, Clone)]
pub struct ControlLoopConfig {
    /// Maximum number of concurrent operations.
    pub max_concurrency: usize,
    /// Tick interval when idle.
    pub tick_interval: Duration,
    /// How long to wait for operation completion.
    pub operation_timeout: Duration,
}

impl Default for ControlLoopConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            tick_interval: Duration::from_secs(1),
            operation_timeout: Duration::from_secs(300),
        }
    }
}

/// Wake-up signal for the control loop.
#[derive(Debug, Clone)]
pub enum LoopWakeup {
    /// Periodic tick.
    Tick,
    /// An IPC command was received.
    Command,
    /// Shutdown requested.
    Shutdown,
}

/// The supervisor control loop.
pub struct ControlLoop {
    config: ControlLoopConfig,
    pool: SqlitePool,
    repo: SupervisorRepo,
    instance_id: SupervisorInstanceId,
    fencing_token: i64,
    /// Active operation count (for concurrency limiting).
    active_operations: Arc<RwLock<usize>>,
    /// Wakeup channel sender.
    wakeup_tx: mpsc::Sender<LoopWakeup>,
    /// Wakeup channel receiver.
    wakeup_rx: Arc<Mutex<mpsc::Receiver<LoopWakeup>>>,
}

impl ControlLoop {
    /// Create a new control loop.
    pub fn new(
        config: ControlLoopConfig,
        pool: SqlitePool,
        repo: SupervisorRepo,
        instance_id: SupervisorInstanceId,
        fencing_token: i64,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            config,
            pool,
            repo,
            instance_id,
            fencing_token,
            active_operations: Arc::new(RwLock::new(0)),
            wakeup_tx: tx,
            wakeup_rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Get a sender to wake up the control loop.
    pub fn wakeup_sender(&self) -> mpsc::Sender<LoopWakeup> {
        self.wakeup_tx.clone()
    }

    /// Run the control loop until shutdown.
    pub async fn run(&self, cancel: CancellationToken) {
        tracing::info!(
            instance_id = %self.instance_id,
            "control loop starting"
        );

        let mut tick_interval = tokio::time::interval(self.config.tick_interval);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(instance_id = %self.instance_id, "control loop cancelled");
                    break;
                }
                wakeup = async {
                    let mut rx = self.wakeup_rx.lock().await;
                    rx.recv().await
                } => {
                    match wakeup {
                        Some(LoopWakeup::Shutdown) => {
                            tracing::info!("control loop shutdown requested");
                            break;
                        }
                        Some(LoopWakeup::Command | LoopWakeup::Tick) => {
                            self.handle_tick().await;
                        }
                        None => {
                            // Channel closed
                            tracing::info!("wakeup channel closed");
                            break;
                        }
                    }
                }
                _ = tick_interval.tick() => {
                    self.handle_tick().await;
                }
            }
        }

        tracing::info!(instance_id = %self.instance_id, "control loop stopped");
    }

    /// Handle a single control loop tick.
    async fn handle_tick(&self) {
        // 1. Renew supervisor lease (heartbeat check)
        // Heartbeat is handled by a separate background task

        // 2. Check active operation count
        let active = *self.active_operations.read().await;
        if active >= self.config.max_concurrency {
            tracing::debug!(
                active,
                max = self.config.max_concurrency,
                "at concurrency limit"
            );
            return;
        }

        // 3. Scan for pending operation intents
        match self.scan_pending_operations().await {
            Ok(pending) => {
                if pending.is_empty() {
                    return; // Nothing to do
                }
                tracing::debug!(count = pending.len(), "pending operations found");
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to scan pending operations");
                return;
            }
        }

        // 4. Launch operations (up to concurrency limit)
        // Operation execution is handled asynchronously via the command handler.
        // The control loop's job is to ensure bounded concurrency and fair scheduling.
    }

    /// Scan for pending operation intents in the database.
    async fn scan_pending_operations(&self) -> Result<Vec<String>, String> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"SELECT operation_id FROM operation_intents
               WHERE state = 'pending'
                 AND (owner_instance_id IS NULL OR owner_fencing_token <= ?)
               ORDER BY created_at ASC
               LIMIT 16"#,
        )
        .bind(self.fencing_token)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("scan operations: {e}"))?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Increment the active operation count.
    pub async fn operation_started(&self) {
        let mut count = self.active_operations.write().await;
        *count += 1;
    }

    /// Decrement the active operation count.
    pub async fn operation_completed(&self) {
        let mut count = self.active_operations.write().await;
        *count = count.saturating_sub(1);
    }

    /// Get current active operation count.
    pub async fn active_count(&self) -> usize {
        *self.active_operations.read().await
    }
}
