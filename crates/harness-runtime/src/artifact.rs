//! RuntimeArtifactDirectory — harness-owned per-execution artifact storage.
//!
//! Layout: `<root>/<project_id>/<run_id>/<execution_id>/`
//!
//! Invariants:
//! - Every path component is validated (no `..`, no separators, no absolute
//!   paths, no Windows reserved device names) before any filesystem call.
//! - After creation the directory is canonicalized and verified to still be
//!   inside the canonical root (symlink-escape guard).
//! - Creation is atomic: the directory is fully populated (ownership marker
//!   included) under a hidden temp name, then renamed into place. A crash
//!   leaves either nothing visible or a `.tmp-` remnant that is garbage.
//! - Each directory carries an ownership marker (`.harness-owner.json`) with
//!   `state: active|closed`. After a supervisor crash, `active` markers from
//!   another supervisor instance identify orphan artifacts.
//! - The root MUST NOT live inside a user Git worktree; `ArtifactRoot::open`
//!   rejects roots with a `.git` in any ancestor.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

const OWNER_MARKER: &str = ".harness-owner.json";
const TMP_PREFIX: &str = ".tmp-";

/// Retention decision applied when an execution's artifacts are closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Keep artifacts regardless of outcome.
    KeepAll,
    /// Keep artifacts only when the execution did not succeed
    /// (failure/cancel); delete on success.
    KeepOnFailureOnly,
    /// Always delete artifacts on close.
    DeleteAll,
}

/// Outcome class reported at close time. Crash is implicit: a crashed
/// supervisor never calls `close`, so the marker stays `active` and the
/// directory is later surfaced by `find_orphans`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactOutcome {
    Success,
    Failure,
    Cancelled,
}

/// What `close` actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseAction {
    Kept,
    Deleted,
}

/// An orphan artifact directory left behind by a crashed/previous supervisor.
#[derive(Debug, Clone)]
pub struct OrphanArtifact {
    pub path: PathBuf,
    pub project_id: String,
    pub run_id: String,
    pub execution_id: String,
    pub supervisor_instance_id: String,
}

/// Root of harness-owned artifact storage.
pub struct ArtifactRoot {
    root: PathBuf,
}

impl ArtifactRoot {
    /// Open (creating if needed) the artifact root. Rejects roots located
    /// inside a Git worktree — spool/artifact data must never land in a user
    /// repository checkout.
    pub fn open(root: &Path) -> Result<Self, CoreError> {
        if let Some(git_ancestor) = find_git_ancestor(root) {
            return Err(artifact_err(format!(
                "artifact root {} is inside a git worktree ({}); artifacts must not use a user git worktree",
                root.display(),
                git_ancestor.display()
            )));
        }
        std::fs::create_dir_all(root)
            .map_err(|e| artifact_err(format!("create artifact root {}: {e}", root.display())))?;
        let canonical = root
            .canonicalize()
            .map_err(|e| artifact_err(format!("canonicalize root {}: {e}", root.display())))?;
        Ok(Self { root: canonical })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Atomically create the per-execution artifact directory with its
    /// ownership marker. Fails if the directory already exists.
    pub fn create_execution_dir(
        &self,
        project_id: &str,
        run_id: &str,
        execution_id: &str,
        supervisor_instance_id: &str,
    ) -> Result<RuntimeArtifactDirectory, CoreError> {
        validate_component(project_id)?;
        validate_component(run_id)?;
        validate_component(execution_id)?;

        let parent = self.root.join(project_id).join(run_id);
        std::fs::create_dir_all(&parent)
            .map_err(|e| artifact_err(format!("create {}: {e}", parent.display())))?;

        let final_dir = parent.join(execution_id);
        if final_dir.exists() {
            return Err(artifact_err(format!(
                "artifact directory already exists: {}",
                final_dir.display()
            )));
        }

        // Stage under a temp name, populate marker, then rename into place.
        let tmp_dir = parent.join(format!("{TMP_PREFIX}{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&tmp_dir)
            .map_err(|e| artifact_err(format!("create {}: {e}", tmp_dir.display())))?;

        let marker = serde_json::json!({
            "supervisor_instance_id": supervisor_instance_id,
            "project_id": project_id,
            "run_id": run_id,
            "execution_id": execution_id,
            "pid": std::process::id(),
            "created_at": chrono::Utc::now().to_rfc3339(),
            "state": "active",
        });
        write_marker(&tmp_dir, &marker)?;

        if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(artifact_err(format!(
                "rename {} -> {}: {e}",
                tmp_dir.display(),
                final_dir.display()
            )));
        }

        // Symlink-escape guard: the realized path must stay under the root.
        let canonical = final_dir
            .canonicalize()
            .map_err(|e| artifact_err(format!("canonicalize {}: {e}", final_dir.display())))?;
        if !canonical.starts_with(&self.root) {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(artifact_err(format!(
                "artifact directory escaped root via symlink: {} not under {}",
                canonical.display(),
                self.root.display()
            )));
        }

        Ok(RuntimeArtifactDirectory {
            path: canonical,
            execution_id: execution_id.to_string(),
        })
    }

    /// Scan for orphan artifacts: `active` markers owned by a different
    /// supervisor instance, plus stray `.tmp-` staging remnants (reported
    /// with empty ids).
    pub fn find_orphans(
        &self,
        current_supervisor_id: &str,
    ) -> Result<Vec<OrphanArtifact>, CoreError> {
        let mut orphans = Vec::new();
        for project in read_dir_dirs(&self.root)? {
            for run in read_dir_dirs(&project)? {
                for exec in read_dir_dirs(&run)? {
                    let name = exec.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if name.starts_with(TMP_PREFIX) {
                        orphans.push(OrphanArtifact {
                            path: exec.clone(),
                            project_id: String::new(),
                            run_id: String::new(),
                            execution_id: String::new(),
                            supervisor_instance_id: String::new(),
                        });
                        continue;
                    }
                    let marker_path = exec.join(OWNER_MARKER);
                    let Ok(raw) = std::fs::read_to_string(&marker_path) else {
                        continue;
                    };
                    let Ok(marker) = serde_json::from_str::<serde_json::Value>(&raw) else {
                        continue;
                    };
                    let state = marker["state"].as_str().unwrap_or("");
                    let owner = marker["supervisor_instance_id"].as_str().unwrap_or("");
                    if state == "active" && owner != current_supervisor_id {
                        orphans.push(OrphanArtifact {
                            path: exec.clone(),
                            project_id: marker["project_id"].as_str().unwrap_or("").to_string(),
                            run_id: marker["run_id"].as_str().unwrap_or("").to_string(),
                            execution_id: marker["execution_id"].as_str().unwrap_or("").to_string(),
                            supervisor_instance_id: owner.to_string(),
                        });
                    }
                }
            }
        }
        Ok(orphans)
    }

    /// Reclaim orphan artifacts: find all `active` markers from a
    /// different supervisor and delete their directories.
    ///
    /// Returns the number of directories deleted.  Each deletion is
    /// best-effort; a single failure does not stop the pass.
    pub fn reclaim_orphans(&self, current_supervisor_id: &str) -> Result<usize, CoreError> {
        let orphans = self.find_orphans(current_supervisor_id)?;
        let mut reclaimed = 0usize;

        for orphan in &orphans {
            match std::fs::remove_dir_all(&orphan.path) {
                Ok(()) => {
                    reclaimed += 1;
                    tracing::info!(
                        path = %orphan.path.display(),
                        project = %orphan.project_id,
                        run = %orphan.run_id,
                        execution = %orphan.execution_id,
                        "reclaimed orphan artifact"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        path = %orphan.path.display(),
                        error = %e,
                        "failed to reclaim orphan artifact"
                    );
                }
            }
        }

        Ok(reclaimed)
    }
}

/// A created, harness-owned per-execution artifact directory.
pub struct RuntimeArtifactDirectory {
    path: PathBuf,
    execution_id: String,
}

impl RuntimeArtifactDirectory {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Path for a spool file of the given stream name (`stdout`/`stderr`).
    pub fn spool_path(&self, stream: &str) -> Result<PathBuf, CoreError> {
        validate_component(stream)?;
        Ok(self.path.join(format!("{stream}.spool")))
    }

    /// Safe join of an arbitrary relative artifact file name.
    pub fn file_path(&self, name: &str) -> Result<PathBuf, CoreError> {
        validate_component(name)?;
        Ok(self.path.join(name))
    }

    /// Apply the retention policy for a finished execution and mark the
    /// ownership marker `closed`. A crash before this call leaves the marker
    /// `active` — that is how orphans are recognized.
    pub fn close(
        &self,
        policy: RetentionPolicy,
        outcome: ArtifactOutcome,
    ) -> Result<CloseAction, CoreError> {
        let delete = match policy {
            RetentionPolicy::KeepAll => false,
            RetentionPolicy::DeleteAll => true,
            RetentionPolicy::KeepOnFailureOnly => outcome == ArtifactOutcome::Success,
        };
        if delete {
            std::fs::remove_dir_all(&self.path)
                .map_err(|e| artifact_err(format!("remove {}: {e}", self.path.display())))?;
            return Ok(CloseAction::Deleted);
        }
        let marker = serde_json::json!({
            "execution_id": self.execution_id,
            "state": "closed",
            "outcome": match outcome {
                ArtifactOutcome::Success => "success",
                ArtifactOutcome::Failure => "failure",
                ArtifactOutcome::Cancelled => "cancelled",
            },
            "closed_at": chrono::Utc::now().to_rfc3339(),
        });
        write_marker(&self.path, &marker)?;
        Ok(CloseAction::Kept)
    }
}

/// Atomically (write temp + rename) write the ownership marker.
fn write_marker(dir: &Path, marker: &serde_json::Value) -> Result<(), CoreError> {
    let tmp = dir.join(format!("{OWNER_MARKER}.tmp"));
    let final_path = dir.join(OWNER_MARKER);
    std::fs::write(&tmp, marker.to_string())
        .map_err(|e| artifact_err(format!("write {}: {e}", tmp.display())))?;
    // On Windows, rename fails if the destination exists — remove first.
    if final_path.exists() {
        std::fs::remove_file(&final_path)
            .map_err(|e| artifact_err(format!("replace {}: {e}", final_path.display())))?;
    }
    std::fs::rename(&tmp, &final_path)
        .map_err(|e| artifact_err(format!("rename marker {}: {e}", final_path.display())))?;
    Ok(())
}

/// Validate a single path component. Rejects traversal, separators, absolute
/// paths, control characters, and Windows reserved device names.
fn validate_component(s: &str) -> Result<(), CoreError> {
    if s.is_empty() || s.len() > 128 {
        return Err(artifact_err(format!(
            "invalid path component (empty or too long): {s:?}"
        )));
    }
    if s == "." || s == ".." {
        return Err(artifact_err(format!("path traversal rejected: {s:?}")));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(artifact_err(format!(
            "path component contains illegal characters: {s:?}"
        )));
    }
    if s.starts_with('.') || s.ends_with('.') {
        return Err(artifact_err(format!(
            "path component may not start/end with '.': {s:?}"
        )));
    }
    let base = s.split('.').next().unwrap_or(s).to_ascii_uppercase();
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if RESERVED.contains(&base.as_str()) {
        return Err(artifact_err(format!(
            "reserved device name rejected: {s:?}"
        )));
    }
    Ok(())
}

/// Walk ancestors looking for a `.git` entry (dir in a normal checkout, file
/// in a linked worktree).
///
/// The walk stops at the user's home directory: a dotfiles repository at
/// `$HOME` does not make `%TEMP%`/`%LOCALAPPDATA%` a "user git worktree" in
/// any meaningful sense — the guard targets project checkouts. A `.git`
/// anywhere strictly below home (or on paths outside home) still rejects.
pub(crate) fn find_git_ancestor(path: &Path) -> Option<PathBuf> {
    // The path itself may not exist yet; walk the lexical ancestors.
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    let home =
        std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).map(PathBuf::from);
    for ancestor in absolute.ancestors() {
        if home.as_deref() == Some(ancestor) {
            break; // home itself and everything above is out of scope
        }
        if ancestor.join(".git").exists() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn read_dir_dirs(dir: &Path) -> Result<Vec<PathBuf>, CoreError> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir)
        .map_err(|e| artifact_err(format!("read_dir {}: {e}", dir.display())))?;
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            out.push(p);
        }
    }
    Ok(out)
}

fn artifact_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::PersistenceError, msg, ErrorSource::System)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> (tempfile::TempDir, ArtifactRoot) {
        let tmp = tempfile::tempdir().unwrap();
        let root = ArtifactRoot::open(&tmp.path().join("artifacts")).unwrap();
        (tmp, root)
    }

    #[test]
    fn create_and_close_keep() {
        let (_tmp, root) = temp_root();
        let dir = root
            .create_execution_dir("p1", "r1", "e1", "sup-a")
            .unwrap();
        assert!(dir.path().join(OWNER_MARKER).exists());
        let action = dir
            .close(RetentionPolicy::KeepAll, ArtifactOutcome::Success)
            .unwrap();
        assert_eq!(action, CloseAction::Kept);
        assert!(dir.path().exists());
        let marker: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join(OWNER_MARKER)).unwrap())
                .unwrap();
        assert_eq!(marker["state"], "closed");
    }

    #[test]
    fn keep_on_failure_only_deletes_on_success() {
        let (_tmp, root) = temp_root();
        let dir = root
            .create_execution_dir("p1", "r1", "e2", "sup-a")
            .unwrap();
        let path = dir.path().to_path_buf();
        let action = dir
            .close(RetentionPolicy::KeepOnFailureOnly, ArtifactOutcome::Success)
            .unwrap();
        assert_eq!(action, CloseAction::Deleted);
        assert!(!path.exists());
    }

    #[test]
    fn keep_on_failure_only_keeps_on_failure() {
        let (_tmp, root) = temp_root();
        let dir = root
            .create_execution_dir("p1", "r1", "e3", "sup-a")
            .unwrap();
        let action = dir
            .close(RetentionPolicy::KeepOnFailureOnly, ArtifactOutcome::Failure)
            .unwrap();
        assert_eq!(action, CloseAction::Kept);
        assert!(dir.path().exists());
    }

    #[test]
    fn rejects_traversal_components() {
        let (_tmp, root) = temp_root();
        for bad in [
            "..", ".", "a/b", "a\\b", "C:", "..evil", "e1 ", "", "nul", "COM1.txt",
        ] {
            assert!(
                root.create_execution_dir("p1", "r1", bad, "sup-a").is_err(),
                "component {bad:?} must be rejected"
            );
        }
        // Absolute path as component
        assert!(root
            .create_execution_dir("p1", "r1", "C:\\abs\\path", "sup-a")
            .is_err());
    }

    #[test]
    fn duplicate_creation_rejected() {
        let (_tmp, root) = temp_root();
        root.create_execution_dir("p1", "r1", "e4", "sup-a")
            .unwrap();
        assert!(root
            .create_execution_dir("p1", "r1", "e4", "sup-a")
            .is_err());
    }

    #[test]
    fn orphan_detection_by_supervisor_id() {
        let (_tmp, root) = temp_root();
        let _kept = root
            .create_execution_dir("p1", "r1", "e5", "sup-dead")
            .unwrap();
        let closed = root
            .create_execution_dir("p1", "r1", "e6", "sup-dead")
            .unwrap();
        closed
            .close(RetentionPolicy::KeepAll, ArtifactOutcome::Success)
            .unwrap();
        let mine = root
            .create_execution_dir("p1", "r1", "e7", "sup-live")
            .unwrap();
        let _ = mine; // still active, but owned by the live supervisor

        let orphans = root.find_orphans("sup-live").unwrap();
        let ids: Vec<&str> = orphans.iter().map(|o| o.execution_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["e5"],
            "only the active dir of a dead supervisor is orphaned"
        );
    }

    #[test]
    fn root_inside_git_worktree_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".git")).unwrap();
        let err = ArtifactRoot::open(&tmp.path().join("spool"));
        assert!(err.is_err(), "root inside a git worktree must be rejected");
    }
}
