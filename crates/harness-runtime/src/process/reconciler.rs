//! ProcessReconciler — uses TransitionService for atomic lifecycle changes.

use std::sync::Arc;

use harness_core::state_machine::ExecutionLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

use super::registry::ProcessRegistry;
use crate::transition::TransitionService;

pub struct ProcessReconciler {
    pool: SqlitePool,
    registry: Arc<ProcessRegistry>,
    supervisor_instance_id: String,
}

impl ProcessReconciler {
    pub fn new(pool: SqlitePool, registry: Arc<ProcessRegistry>, supervisor_id: String) -> Self {
        Self {
            pool,
            registry,
            supervisor_instance_id: supervisor_id,
        }
    }

    /// Reconcile: find DB executions marked Running that have no live registry entry.
    /// Uses TransitionService for atomic Running→Lost with event log.
    /// Idempotent: repeated calls produce no duplicate events.
    pub async fn reconcile(&self) -> Result<Vec<String>, CoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, lifecycle FROM execution_attempts WHERE lifecycle = 'running'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                e.to_string(),
                ErrorSource::System,
            )
        })?;

        let svc = TransitionService::new(self.pool.clone());
        let mut lost = Vec::new();

        for (exec_id, _lc) in &rows {
            if !self.registry.is_alive(exec_id).await {
                let idempotency_key =
                    format!("reconcile-lost-{exec_id}-{}", self.supervisor_instance_id);
                match svc
                    .transition_execution(
                        exec_id,
                        &ExecutionLifecycle::Lost,
                        Some(&format!("reconciled_by:{}", self.supervisor_instance_id)),
                        &idempotency_key,
                    )
                    .await
                {
                    Ok(()) => lost.push(exec_id.clone()),
                    Err(e) => {
                        // Already Lost or idempotent — not an error
                        tracing::debug!(execution_id = %exec_id, error = %e, "reconcile_skip");
                    }
                }
            }
        }
        Ok(lost)
    }
}
