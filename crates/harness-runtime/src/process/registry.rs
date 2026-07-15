//! ProcessRegistry — in-memory tracking of active subprocesses.

use std::collections::HashMap;
use std::sync::Arc;

use harness_core::{CoreError, ErrorCode, ErrorSource};
use tokio::sync::RwLock;

use super::types::ProcessState;

struct RegistryEntry {
    pid: u32,
    cancel: tokio_util::sync::CancellationToken,
    state: Arc<RwLock<ProcessState>>,
}

pub struct ProcessRegistry {
    entries: RwLock<HashMap<String, RegistryEntry>>,
}

impl Default for ProcessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(
        &self,
        execution_id: String,
        pid: u32,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let state = Arc::new(RwLock::new(ProcessState::Running));
        self.entries
            .write()
            .await
            .insert(execution_id, RegistryEntry { pid, cancel, state });
    }

    pub async fn register_with_state(
        &self,
        execution_id: String,
        pid: u32,
        cancel: tokio_util::sync::CancellationToken,
        state: Arc<RwLock<ProcessState>>,
    ) {
        self.entries
            .write()
            .await
            .insert(execution_id, RegistryEntry { pid, cancel, state });
    }

    pub async fn cancel(&self, execution_id: &str) -> Result<(), CoreError> {
        let guard = self.entries.read().await;
        if let Some(entry) = guard.get(execution_id) {
            entry.cancel.cancel();
            Ok(())
        } else {
            Err(CoreError::new(
                ErrorCode::ProcessCancelled,
                "execution not found in registry",
                ErrorSource::System,
            ))
        }
    }

    pub async fn get_state(&self, execution_id: &str) -> Option<ProcessState> {
        let guard = self.entries.read().await;
        if let Some(entry) = guard.get(execution_id) {
            Some(entry.state.read().await.clone())
        } else {
            None
        }
    }

    pub async fn get_pid(&self, execution_id: &str) -> Option<u32> {
        self.entries.read().await.get(execution_id).map(|e| e.pid)
    }

    pub async fn is_alive(&self, execution_id: &str) -> bool {
        self.entries.read().await.contains_key(execution_id)
    }

    pub async fn list_active(&self) -> Vec<String> {
        self.entries.read().await.keys().cloned().collect()
    }

    pub async fn remove(&self, execution_id: &str) {
        self.entries.write().await.remove(execution_id);
    }
}
