//! ManagedCargoRunDir — isolated Cargo target directory for a single
//! run, with ownership marker and automatic cleanup.
//!
//! Two strategies are supported:
//!
//! ## Strategy A: Shared stable cache (preferred)
//! All quick/delta runs reuse `target/cargo-shared`.  No per-run
//! replication; no automated cleanup needed.
//!
//! ## Strategy B: Managed isolated run directory (when workspace lock
//! isolation requires it)
//! `target/harness-cargo-runs/<run_id>/` with ownership marker.
//! Cleaned up at run end via DeletionGuard.
//!
//! # invariants
//! - Never deletes `target/debug` or `target/cargo-shared`.
//! - Isolated run directories must carry `.harness-owned.json`.
//! - No permanent per-run Cargo target retention.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::guard::DeletionGuard;
use super::types::{
    CleanupAction, CleanupEntry, CleanupResult, ManagedDirKind, MarkerState, OwnershipMarker,
    OWNERSHIP_MARKER_FILENAME,
};

/// Path to the shared stable Cargo cache directory, relative to the
/// repo's `target/` directory.
pub const SHARED_CARGO_CACHE: &str = "cargo-shared";

/// A managed isolated Cargo target directory for a single run.
pub struct ManagedCargoRunDir {
    path: PathBuf,
    run_id: String,
}

impl ManagedCargoRunDir {
    /// Create an isolated Cargo target directory under
    /// `target/harness-cargo-runs/<run_id>/`.
    pub fn create(
        managed_cargo_root: &Path,
        run_id: &str,
        code_head: &str,
    ) -> Result<Self, CoreError> {
        validate_run_id(run_id)?;

        std::fs::create_dir_all(managed_cargo_root).map_err(|e| {
            cargo_err(format!(
                "create cargo run root {}: {e}",
                managed_cargo_root.display()
            ))
        })?;

        let final_dir = managed_cargo_root.join(run_id);
        if final_dir.exists() {
            return Err(cargo_err(format!(
                "cargo run dir already exists: {}",
                final_dir.display()
            )));
        }

        let tmp_dir = managed_cargo_root.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&tmp_dir)
            .map_err(|e| cargo_err(format!("create staging dir {}: {e}", tmp_dir.display())))?;

        let marker = OwnershipMarker::new_active(
            ManagedDirKind::HarnessManagedCargoRun,
            run_id.to_string(),
            std::process::id(),
            code_head.to_string(),
        );
        super::temp_dir::write_marker_atomic(&tmp_dir, &marker)?;

        if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(cargo_err(format!(
                "rename {} -> {}: {e}",
                tmp_dir.display(),
                final_dir.display()
            )));
        }

        let canonical = final_dir
            .canonicalize()
            .map_err(|e| cargo_err(format!("canonicalize {}: {e}", final_dir.display())))?;
        let root_canonical = managed_cargo_root.canonicalize().map_err(|e| {
            cargo_err(format!(
                "canonicalize root {}: {e}",
                managed_cargo_root.display()
            ))
        })?;
        if !canonical.starts_with(&root_canonical) {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(cargo_err(format!(
                "cargo run dir escaped root: {}",
                canonical.display()
            )));
        }

        tracing::info!(
            run_id = %run_id,
            path = %canonical.display(),
            "managed cargo run dir created"
        );

        Ok(Self {
            path: canonical,
            run_id: run_id.to_string(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Finalize the marker state.
    pub fn finalize(&self, state: MarkerState) -> Result<(), CoreError> {
        let marker_path = self.path.join(OWNERSHIP_MARKER_FILENAME);
        let raw = std::fs::read_to_string(&marker_path)
            .map_err(|e| cargo_err(format!("read marker: {e}")))?;
        let mut marker: OwnershipMarker =
            serde_json::from_str(&raw).map_err(|e| cargo_err(format!("parse marker: {e}")))?;
        marker.state = state;
        marker.completed_at = Some(chrono::Utc::now());
        super::temp_dir::write_marker_atomic(&self.path, &marker)?;
        Ok(())
    }

    /// Clean up using the DeletionGuard.
    pub fn cleanup_with_guard(
        &self,
        guard: &DeletionGuard,
        managed_cargo_root: &Path,
    ) -> CleanupEntry {
        let entry = guard.guarded_delete(
            &self.path,
            managed_cargo_root,
            Some(ManagedDirKind::HarnessManagedCargoRun),
        );
        if entry.action == CleanupAction::Delete {
            tracing::info!(
                run_id = %self.run_id,
                "managed cargo run dir cleaned up"
            );
        }
        entry
    }
}

/// Resolve the shared stable Cargo cache path under `target/cargo-shared`.
pub fn shared_cargo_cache(repo_root: &Path) -> PathBuf {
    repo_root.join("target").join(SHARED_CARGO_CACHE)
}

/// Check whether the shared cargo cache exists and is usable.
pub fn ensure_shared_cache(repo_root: &Path) -> Result<PathBuf, CoreError> {
    let cache = shared_cargo_cache(repo_root);
    std::fs::create_dir_all(&cache).map_err(|e| {
        cargo_err(format!(
            "create shared cargo cache {}: {e}",
            cache.display()
        ))
    })?;
    Ok(cache)
}

/// Scan for stale owned Cargo run directories eligible for cleanup.
pub fn scan_stale_cargo_runs(
    managed_cargo_root: &Path,
    guard: &DeletionGuard,
    stale_grace: std::time::Duration,
    apply: bool,
) -> CleanupResult {
    let mut result = CleanupResult::default();

    let entries = match std::fs::read_dir(managed_cargo_root) {
        Ok(iter) => iter,
        Err(_) => return result,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        result.examined += 1;

        // Read marker.
        let marker_path = path.join(OWNERSHIP_MARKER_FILENAME);
        let marker: Option<OwnershipMarker> = std::fs::read_to_string(&marker_path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok());

        match &marker {
            Some(m) if m.is_active() => {
                // Check grace period for active markers.
                let age = chrono::Utc::now()
                    .signed_duration_since(m.created_at)
                    .to_std()
                    .unwrap_or(std::time::Duration::ZERO);
                if age < stale_grace {
                    result.preserved += 1;
                    result.entries.push(CleanupEntry {
                        path,
                        action: CleanupAction::Preserve,
                        reason: format!(
                            "within grace period ({:.0}s remaining)",
                            (stale_grace - age).as_secs()
                        ),
                    });
                    continue;
                }
            }
            Some(_) => {
                // Terminal state — eligible.
            }
            None => {
                // No marker — preserve.
                result.preserved += 1;
                result.entries.push(CleanupEntry {
                    path,
                    action: CleanupAction::Preserve,
                    reason: "no ownership marker".into(),
                });
                continue;
            }
        }

        let entry_result = if apply {
            guard.guarded_delete(
                &path,
                managed_cargo_root,
                Some(ManagedDirKind::HarnessManagedCargoRun),
            )
        } else {
            guard.dry_run(
                &path,
                managed_cargo_root,
                Some(ManagedDirKind::HarnessManagedCargoRun),
            )
        };

        match entry_result.action {
            CleanupAction::Delete => result.deleted += 1,
            CleanupAction::Preserve => result.preserved += 1,
        }
        result.entries.push(entry_result);
    }

    result
}

// ── Helpers ────────────────────────────────────────────────────────

fn validate_run_id(s: &str) -> Result<(), CoreError> {
    if s.is_empty() || s.len() > 128 || s == "." || s == ".." || s.contains('/') || s.contains('\\')
    {
        return Err(cargo_err(format!("invalid run_id: {s:?}")));
    }
    Ok(())
}

fn cargo_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_finalize_cargo_run_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-cargo-runs");

        let dir = ManagedCargoRunDir::create(&root, "run-cargo-1", "abc").unwrap();
        assert!(dir.path().exists());
        assert!(dir.path().join(OWNERSHIP_MARKER_FILENAME).exists());

        let raw = std::fs::read_to_string(dir.path().join(OWNERSHIP_MARKER_FILENAME)).unwrap();
        let marker: OwnershipMarker = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker.kind, ManagedDirKind::HarnessManagedCargoRun);

        dir.finalize(MarkerState::Completed).unwrap();
    }

    #[test]
    fn shared_cache_path() {
        let cache = shared_cargo_cache(Path::new("E:/repo"));
        assert_eq!(cache, PathBuf::from("E:/repo/target/cargo-shared"));
    }

    #[test]
    fn stale_scan_preserves_active_within_grace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-cargo-runs");

        let _dir = ManagedCargoRunDir::create(&root, "run-fresh", "abc").unwrap();
        let cfg = super::super::types::LivenessConfig::for_test(tmp.path());
        let guard = super::super::guard::DeletionGuard::new(cfg, vec![]);

        // Active marker, very short grace period... but still active with
        // current PID. The guard will reject it due to active PID.
        let result = scan_stale_cargo_runs(
            &root,
            &guard,
            std::time::Duration::from_secs(3600), // long grace
            false,                                // dry-run
        );
        assert_eq!(result.examined, 1);
        // Should be preserved because marker is active and PID is alive.
        assert_eq!(result.preserved, 1);
    }
}
