//! HarnessTempDir — managed runtime temporary directory with ownership
//! marker, atomic creation, and safe cleanup.
//!
//! Layout: `<managed_temp_root>/<run_id>/`
//!
//! # invariants
//! - Created atomically (temp name → populate marker → rename).
//! - `.harness-owned.json` written before the directory is visible.
//! - Run-id matches directory name; marker.kind == HarnessManagedTemp.
//! - Cleanup always passes through DeletionGuard.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::guard::DeletionGuard;
use super::types::{
    CleanupAction, CleanupEntry, ManagedDirKind, MarkerState, OwnershipMarker,
    OWNERSHIP_MARKER_FILENAME,
};

const TMP_PREFIX: &str = ".tmp-";

/// A created, harness-owned runtime temp directory.
#[derive(Debug)]
pub struct HarnessTempDir {
    path: PathBuf,
    run_id: String,
}

impl HarnessTempDir {
    /// Atomically create a managed temp directory for `run_id`.
    ///
    /// The directory is staged under a hidden temp name, the ownership
    /// marker is written, and then the directory is renamed into place.
    pub fn create(
        managed_temp_root: &Path,
        run_id: &str,
        code_head: &str,
    ) -> Result<Self, CoreError> {
        validate_run_id(run_id)?;

        std::fs::create_dir_all(managed_temp_root).map_err(|e| {
            temp_err(format!(
                "create managed temp root {}: {e}",
                managed_temp_root.display()
            ))
        })?;

        let final_dir = managed_temp_root.join(run_id);
        if final_dir.exists() {
            return Err(temp_err(format!(
                "temp directory already exists: {}",
                final_dir.display()
            )));
        }

        // Stage under a temp name.
        let tmp_dir = managed_temp_root.join(format!("{TMP_PREFIX}{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&tmp_dir)
            .map_err(|e| temp_err(format!("create staging dir {}: {e}", tmp_dir.display())))?;

        // Write the ownership marker.
        let marker = OwnershipMarker::new_active(
            ManagedDirKind::HarnessManagedTemp,
            run_id.to_string(),
            std::process::id(),
            code_head.to_string(),
        );
        write_marker_atomic(&tmp_dir, &marker)?;

        // Rename into place.
        if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(temp_err(format!(
                "rename {} -> {}: {e}",
                tmp_dir.display(),
                final_dir.display()
            )));
        }

        // Symlink-escape guard.
        let canonical = final_dir
            .canonicalize()
            .map_err(|e| temp_err(format!("canonicalize {}: {e}", final_dir.display())))?;
        let root_canonical = managed_temp_root.canonicalize().map_err(|e| {
            temp_err(format!(
                "canonicalize root {}: {e}",
                managed_temp_root.display()
            ))
        })?;
        if !canonical.starts_with(&root_canonical) {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(temp_err(format!(
                "temp directory escaped managed root via symlink: {} not under {}",
                canonical.display(),
                root_canonical.display()
            )));
        }

        tracing::info!(
            run_id = %run_id,
            path = %canonical.display(),
            "harness temp dir created"
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

    /// Finalize the marker to a terminal state.
    pub fn finalize(&self, state: MarkerState) -> Result<(), CoreError> {
        let marker_path = self.path.join(OWNERSHIP_MARKER_FILENAME);
        let raw = std::fs::read_to_string(&marker_path)
            .map_err(|e| temp_err(format!("read marker: {e}")))?;
        let mut marker: OwnershipMarker =
            serde_json::from_str(&raw).map_err(|e| temp_err(format!("parse marker: {e}")))?;
        marker.state = state;
        marker.completed_at = Some(chrono::Utc::now().to_rfc3339());
        write_marker_atomic(&self.path, &marker)?;
        Ok(())
    }

    /// Clean up this directory using the DeletionGuard.
    /// The guard must confirm ownership before deletion.
    pub fn cleanup_with_guard(
        &self,
        guard: &DeletionGuard,
        managed_temp_root: &Path,
    ) -> CleanupEntry {
        let entry = guard.guarded_delete(
            &self.path,
            managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        if entry.action == CleanupAction::Delete {
            tracing::info!(
                run_id = %self.run_id,
                path = %self.path.display(),
                "harness temp dir cleaned up"
            );
        } else {
            tracing::warn!(
                run_id = %self.run_id,
                path = %self.path.display(),
                reason = %entry.reason,
                "harness temp dir cleanup blocked"
            );
        }
        entry
    }

    /// Retry cleanup with bounded retries for transient file-lock issues.
    pub async fn cleanup_with_retry(
        &self,
        guard: &DeletionGuard,
        managed_temp_root: &Path,
    ) -> CleanupEntry {
        let delays = [
            std::time::Duration::from_millis(100),
            std::time::Duration::from_millis(250),
            std::time::Duration::from_millis(500),
        ];

        for &delay in &delays {
            let entry = self.cleanup_with_guard(guard, managed_temp_root);
            if entry.action == CleanupAction::Delete || !self.path.exists() {
                return entry;
            }
            tracing::debug!(
                run_id = %self.run_id,
                delay_ms = delay.as_millis(),
                "cleanup retry"
            );
            tokio::time::sleep(delay).await;
        }

        // Final attempt.
        self.cleanup_with_guard(guard, managed_temp_root)
    }
}

impl Drop for HarnessTempDir {
    fn drop(&mut self) {
        // Best-effort: if the directory still exists and we're the owner,
        // try to mark it abandoned so the startup janitor can find it.
        if self.path.exists() {
            let _ = self.finalize(MarkerState::Abandoned);
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────

pub(crate) fn write_marker_atomic(dir: &Path, marker: &OwnershipMarker) -> Result<(), CoreError> {
    let tmp = dir.join(format!("{}.tmp", OWNERSHIP_MARKER_FILENAME));
    let final_path = dir.join(OWNERSHIP_MARKER_FILENAME);
    let json = serde_json::to_string_pretty(marker)
        .map_err(|e| temp_err(format!("serialize marker: {e}")))?;
    std::fs::write(&tmp, &json)
        .map_err(|e| temp_err(format!("write marker tmp {}: {e}", tmp.display())))?;
    if final_path.exists() {
        std::fs::remove_file(&final_path)
            .map_err(|e| temp_err(format!("remove old marker: {e}")))?;
    }
    std::fs::rename(&tmp, &final_path).map_err(|e| temp_err(format!("rename marker: {e}")))?;
    Ok(())
}

fn validate_run_id(s: &str) -> Result<(), CoreError> {
    if s.is_empty() || s.len() > 128 {
        return Err(temp_err(format!(
            "invalid run_id (empty or too long): {s:?}"
        )));
    }
    if s == "." || s == ".." {
        return Err(temp_err(format!("run_id traversal rejected: {s:?}")));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(temp_err(format!(
            "run_id contains illegal characters: {s:?}"
        )));
    }
    Ok(())
}

fn temp_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_cleanup_temp_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-temp");
        std::fs::create_dir_all(&root).unwrap();

        let dir = HarnessTempDir::create(&root, "run-test-1", "abc123").unwrap();
        assert!(dir.path().exists());
        assert!(dir.path().join(OWNERSHIP_MARKER_FILENAME).exists());

        // Verify marker content.
        let raw = std::fs::read_to_string(dir.path().join(OWNERSHIP_MARKER_FILENAME)).unwrap();
        let marker: OwnershipMarker = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker.run_id, "run-test-1");
        assert_eq!(marker.kind, ManagedDirKind::HarnessManagedTemp);
        assert_eq!(marker.state, MarkerState::Active);
        assert_eq!(
            marker.schema_version,
            super::super::types::MARKER_SCHEMA_VERSION
        );

        // Finalize.
        dir.finalize(MarkerState::Completed).unwrap();
        let raw = std::fs::read_to_string(dir.path().join(OWNERSHIP_MARKER_FILENAME)).unwrap();
        let marker: OwnershipMarker = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker.state, MarkerState::Completed);
        assert!(marker.completed_at.is_some());
    }

    #[test]
    fn duplicate_creation_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-temp");
        HarnessTempDir::create(&root, "dup-run", "abc").unwrap();
        let err = HarnessTempDir::create(&root, "dup-run", "abc").unwrap_err();
        assert!(err.message.contains("already exists"));
    }

    #[test]
    fn rejects_bad_run_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-temp");
        for bad in ["", "..", ".", "a/b", "a\\b", "run id"] {
            assert!(
                HarnessTempDir::create(&root, bad, "head").is_err(),
                "run_id {bad:?} must be rejected"
            );
        }
    }
}
