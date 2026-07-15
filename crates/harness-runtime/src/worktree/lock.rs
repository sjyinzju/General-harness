//! Repository-scoped async lock — worktree add/remove/prune are repository
//! administrative operations and must be serialized per repository.
//!
//! Lock identity = canonical common git directory. Different repositories
//! proceed in parallel. This is an in-process lock only; cross-supervisor
//! safety continues to rely on the persisted Operation claim/fencing.
//! (Async tokio mutex — no sync MutexGuard is ever held across await.)

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Default)]
pub struct RepositoryLocks {
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl RepositoryLocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the administrative lock for a repository identity (canonical
    /// common git dir). Returned guard is owned and may be held across await.
    pub async fn acquire(&self, repository_identity: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut map = self.locks.lock().await;
            map.entry(repository_identity.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }
}
