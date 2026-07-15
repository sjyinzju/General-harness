//! Worktree ownership metadata — sidecar files OUTSIDE the task worktree.
//!
//! The sidecar lives NEXT TO the worktree directory (same harness-owned
//! parent), named `<dir>.harness.json`. It is never inside the worktree, so
//! it can never pollute the task's git diff. Writes are atomic
//! (tmp + rename). Removal keeps the sidecar as a tombstone
//! (`state: removed`) plus a `<dir>.harness.removed.json` diagnostics file.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::types::WorktreeRecord;

pub const SIDECAR_SUFFIX: &str = ".harness.json";
pub const DIAGNOSTICS_SUFFIX: &str = ".harness.removed.json";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorktreeMetadata {
    pub schema_version: u32,
    pub worktree_id: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    /// Canonical common git directory.
    pub repository_identity: String,
    /// Canonical worktree path.
    pub worktree_path: String,
    pub branch: String,
    pub base_commit: String,
    pub owner_supervisor_id: String,
    pub operation_id: String,
    pub created_at: String,
    /// active | removing | removed
    pub state: String,
}

impl WorktreeMetadata {
    pub fn from_record(record: &WorktreeRecord) -> Self {
        Self {
            schema_version: 1,
            worktree_id: record.worktree_id.clone(),
            project_id: record.project_id.clone(),
            task_id: record.task_id.clone(),
            execution_id: record.execution_id.clone(),
            repository_identity: record.repository_identity.clone(),
            worktree_path: record.worktree_path.clone(),
            branch: record.branch_name.clone(),
            base_commit: record.base_commit.clone(),
            owner_supervisor_id: record.owner_supervisor_id.clone(),
            operation_id: record.operation_id.clone(),
            created_at: record.created_at.clone(),
            state: "active".into(),
        }
    }

    /// Does this metadata prove ownership of the worktree described by the
    /// record? (identity triple: worktree_id + repository + canonical path)
    pub fn matches_record(&self, record: &WorktreeRecord) -> bool {
        self.worktree_id == record.worktree_id
            && self.repository_identity == record.repository_identity
            && self.worktree_path == record.worktree_path
            && self.branch == record.branch_name
    }
}

/// Sidecar path for a worktree directory: `<dir>.harness.json` (sibling).
pub fn sidecar_path(worktree_path: &Path) -> PathBuf {
    sibling_with_suffix(worktree_path, SIDECAR_SUFFIX)
}

/// Diagnostics tombstone path: `<dir>.harness.removed.json` (sibling).
pub fn diagnostics_path(worktree_path: &Path) -> PathBuf {
    sibling_with_suffix(worktree_path, DIAGNOSTICS_SUFFIX)
}

fn sibling_with_suffix(worktree_path: &Path, suffix: &str) -> PathBuf {
    let name = worktree_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("worktree");
    worktree_path.with_file_name(format!("{name}{suffix}"))
}

/// Atomically write the sidecar (tmp + rename).
pub fn write_sidecar(worktree_path: &Path, meta: &WorktreeMetadata) -> Result<(), CoreError> {
    let path = sidecar_path(worktree_path);
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_string_pretty(meta)
        .map_err(|e| meta_err(format!("serialize metadata: {e}")))?;
    std::fs::write(&tmp, body).map_err(|e| meta_err(format!("write {}: {e}", tmp.display())))?;
    if path.exists() {
        std::fs::remove_file(&path)
            .map_err(|e| meta_err(format!("replace {}: {e}", path.display())))?;
    }
    std::fs::rename(&tmp, &path)
        .map_err(|e| meta_err(format!("rename sidecar {}: {e}", path.display())))?;
    Ok(())
}

/// Read the sidecar; `Ok(None)` when absent, `Err` on unreadable/corrupt.
pub fn read_sidecar(worktree_path: &Path) -> Result<Option<WorktreeMetadata>, CoreError> {
    let path = sidecar_path(worktree_path);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)
        .map_err(|e| meta_err(format!("read {}: {e}", path.display())))?;
    let meta = serde_json::from_str(&raw)
        .map_err(|e| meta_err(format!("corrupt sidecar {}: {e}", path.display())))?;
    Ok(Some(meta))
}

/// Update sidecar state (atomic rewrite).
pub fn set_sidecar_state(worktree_path: &Path, state: &str) -> Result<(), CoreError> {
    let Some(mut meta) = read_sidecar(worktree_path)? else {
        return Err(meta_err(format!(
            "sidecar missing for {}",
            worktree_path.display()
        )));
    };
    meta.state = state.to_string();
    write_sidecar(worktree_path, &meta)
}

/// Write pre-removal diagnostics (HEAD + bounded dirty list) as a sibling
/// tombstone so removed worktrees keep a diff reference.
pub fn write_removal_diagnostics(
    worktree_path: &Path,
    worktree_id: &str,
    head: Option<&str>,
    dirty_entries: &[String],
) -> Result<(), CoreError> {
    let path = diagnostics_path(worktree_path);
    let body = serde_json::json!({
        "schema_version": 1,
        "worktree_id": worktree_id,
        "head": head,
        "dirty_entry_count": dirty_entries.len(),
        "dirty_entries": dirty_entries.iter().take(100).collect::<Vec<_>>(),
        "removed_at": chrono::Utc::now().to_rfc3339(),
    });
    std::fs::write(&path, body.to_string())
        .map_err(|e| meta_err(format!("write diagnostics {}: {e}", path.display())))?;
    Ok(())
}

fn meta_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
