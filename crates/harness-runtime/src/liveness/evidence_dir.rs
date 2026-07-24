//! HarnessEvidenceDir — managed evidence directory with ownership
//! marker, retention policy, and content validation.
//!
//! Layout: `<managed_evidence_root>/<code_head>/<run_id>/`
//!
//! # invariants
//! - Each evidence run directory carries a `.harness-owned.json` marker.
//! - Only small text artifacts are permitted (JSON, JSONL, text logs, small
//!   SQLite evidence, failure summaries).  Build artifacts (PDB, EXE, RLIB,
//!   RMeta, deps, incremental, build directories) are REJECTED.
//! - Retention policy caps the number of successful and failed runs kept.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::types::{
    CleanupAction, CleanupEntry, CleanupResult, EvidenceRetention, ManagedDirKind, MarkerState,
    OwnershipMarker, OWNERSHIP_MARKER_FILENAME,
};

// File extensions / directory names that are FORBIDDEN in evidence.
const FORBIDDEN_EXTENSIONS: [&str; 5] = ["pdb", "exe", "rlib", "rmeta", "dll"];
const FORBIDDEN_DIRS: [&str; 4] = ["deps", "incremental", "build", ".fingerprint"];

/// A created, harness-owned evidence directory.
pub struct HarnessEvidenceDir {
    path: PathBuf,
    run_id: String,
    code_head: String,
}

impl HarnessEvidenceDir {
    /// Create an evidence directory under `<root>/<code_head>/<run_id>/`.
    pub fn create(
        managed_evidence_root: &Path,
        code_head: &str,
        run_id: &str,
    ) -> Result<Self, CoreError> {
        validate_component(code_head)?;
        validate_component(run_id)?;

        let parent = managed_evidence_root.join(code_head);
        std::fs::create_dir_all(&parent).map_err(|e| {
            evidence_err(format!("create evidence parent {}: {e}", parent.display()))
        })?;

        let final_dir = parent.join(run_id);
        if final_dir.exists() {
            return Err(evidence_err(format!(
                "evidence directory already exists: {}",
                final_dir.display()
            )));
        }

        // Atomic creation.
        let tmp_dir = parent.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&tmp_dir)
            .map_err(|e| evidence_err(format!("create staging dir {}: {e}", tmp_dir.display())))?;

        let marker = OwnershipMarker::new_active(
            ManagedDirKind::HarnessManagedEvidence,
            run_id.to_string(),
            std::process::id(),
            code_head.to_string(),
        );
        crate::liveness::temp_dir::write_marker_atomic(&tmp_dir, &marker)?;

        if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(evidence_err(format!(
                "rename {} -> {}: {e}",
                tmp_dir.display(),
                final_dir.display()
            )));
        }

        // Containment check.
        let canonical = final_dir
            .canonicalize()
            .map_err(|e| evidence_err(format!("canonicalize {}: {e}", final_dir.display())))?;
        let root_canonical = managed_evidence_root.canonicalize().map_err(|e| {
            evidence_err(format!(
                "canonicalize root {}: {e}",
                managed_evidence_root.display()
            ))
        })?;
        if !canonical.starts_with(&root_canonical) {
            let _ = std::fs::remove_dir_all(&final_dir);
            return Err(evidence_err(format!(
                "evidence directory escaped root: {}",
                canonical.display()
            )));
        }

        Ok(Self {
            path: canonical,
            run_id: run_id.to_string(),
            code_head: code_head.to_string(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn code_head(&self) -> &str {
        &self.code_head
    }

    /// Finalize the marker.
    pub fn finalize(&self, state: MarkerState) -> Result<(), CoreError> {
        update_marker_state(&self.path, state)
    }

    /// Validate that the directory only contains permitted evidence file
    /// types.  Returns a list of forbidden paths found.
    pub fn validate_contents(&self) -> Vec<String> {
        let mut forbidden = Vec::new();
        check_dir_contents(&self.path, &mut forbidden);
        forbidden
    }
}

/// Apply evidence retention policy to a managed evidence root.
/// Returns a `CleanupResult` describing what was preserved and what
/// would be / was deleted.
pub fn apply_evidence_retention(
    evidence_root: &Path,
    retention: &EvidenceRetention,
    guard: &super::guard::DeletionGuard,
    apply: bool,
) -> CleanupResult {
    let mut result = CleanupResult::default();

    // Scan: <evidence_root>/<code_head>/<run_id>/
    let code_heads = match std::fs::read_dir(evidence_root) {
        Ok(iter) => iter,
        Err(_) => return result,
    };

    for ch_entry in code_heads.flatten() {
        let ch_path = ch_entry.path();
        if !ch_path.is_dir() {
            continue;
        }

        let runs = match std::fs::read_dir(&ch_path) {
            Ok(iter) => iter,
            Err(_) => continue,
        };

        let mut run_dirs: Vec<(PathBuf, Option<OwnershipMarker>)> = Vec::new();
        for run_entry in runs.flatten() {
            let run_path = run_entry.path();
            if !run_path.is_dir() {
                continue;
            }
            let marker = read_marker(&run_path);
            run_dirs.push((run_path, marker));
        }

        // Partition into successful and failed.
        let successful: Vec<_> = run_dirs
            .iter()
            .filter(|(_, m)| {
                m.as_ref()
                    .map(|m| m.state == MarkerState::Completed)
                    .unwrap_or(false)
            })
            .collect();
        let failed: Vec<_> = run_dirs
            .iter()
            .filter(|(_, m)| {
                m.as_ref()
                    .map(|m| matches!(m.state, MarkerState::Failed | MarkerState::Abandoned))
                    .unwrap_or(false)
            })
            .collect();
        let unmarked: Vec<_> = run_dirs.iter().filter(|(_, m)| m.is_none()).collect();

        // Keep the most recent N.
        let excess_successful = successful.len().saturating_sub(retention.max_successful);
        let excess_failed = failed.len().saturating_sub(retention.max_failed);

        // Process excess successful (oldest first, assuming lexical sort ~ chronological).
        let mut sorted_successful: Vec<_> = successful.iter().map(|(p, m)| (p, m)).collect();
        sorted_successful.sort_by_key(|(p, _)| p.to_string_lossy().to_string());
        for (path, _marker) in sorted_successful.iter().take(excess_successful) {
            result.examined += 1;
            let entry = if apply {
                guard.guarded_delete(
                    path,
                    evidence_root,
                    Some(ManagedDirKind::HarnessManagedEvidence),
                )
            } else {
                guard.dry_run(
                    path,
                    evidence_root,
                    Some(ManagedDirKind::HarnessManagedEvidence),
                )
            };
            if entry.action == CleanupAction::Delete {
                result.deleted += 1;
            } else {
                result.preserved += 1;
            }
            result.entries.push(entry);
        }

        // Process excess failed.
        let mut sorted_failed: Vec<_> = failed.iter().map(|(p, m)| (p, m)).collect();
        sorted_failed.sort_by_key(|(p, _)| p.to_string_lossy().to_string());
        for (path, _marker) in sorted_failed.iter().take(excess_failed) {
            result.examined += 1;
            let entry = if apply {
                guard.guarded_delete(
                    path,
                    evidence_root,
                    Some(ManagedDirKind::HarnessManagedEvidence),
                )
            } else {
                guard.dry_run(
                    path,
                    evidence_root,
                    Some(ManagedDirKind::HarnessManagedEvidence),
                )
            };
            if entry.action == CleanupAction::Delete {
                result.deleted += 1;
            } else {
                result.preserved += 1;
            }
            result.entries.push(entry);
        }

        // Unmarked entries are always preserved.
        for (path, _) in &unmarked {
            result.examined += 1;
            result.preserved += 1;
            result.entries.push(CleanupEntry {
                path: (*path).clone(),
                action: CleanupAction::Preserve,
                reason: "unmarked evidence — preserved for manual review".into(),
            });
        }
    }

    result
}

// ── Helpers ────────────────────────────────────────────────────────

fn read_marker(dir: &Path) -> Option<OwnershipMarker> {
    let raw = std::fs::read_to_string(dir.join(OWNERSHIP_MARKER_FILENAME)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn update_marker_state(dir: &Path, state: MarkerState) -> Result<(), CoreError> {
    let marker_path = dir.join(OWNERSHIP_MARKER_FILENAME);
    let raw = std::fs::read_to_string(&marker_path)
        .map_err(|e| evidence_err(format!("read marker: {e}")))?;
    let mut marker: OwnershipMarker =
        serde_json::from_str(&raw).map_err(|e| evidence_err(format!("parse marker: {e}")))?;
    marker.state = state;
    marker.completed_at = Some(chrono::Utc::now());
    crate::liveness::temp_dir::write_marker_atomic(dir, &marker)?;
    Ok(())
}

fn validate_component(s: &str) -> Result<(), CoreError> {
    if s.is_empty() || s.len() > 64 || s == "." || s == ".." || s.contains('/') || s.contains('\\')
    {
        return Err(evidence_err(format!("invalid component: {s:?}")));
    }
    Ok(())
}

fn check_dir_contents(dir: &Path, forbidden: &mut Vec<String>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if FORBIDDEN_DIRS.contains(&name) {
                        forbidden.push(format!("forbidden directory in evidence: {}", p.display()));
                    }
                }
                check_dir_contents(&p, forbidden);
            } else if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                if FORBIDDEN_EXTENSIONS.contains(&ext.to_lowercase().as_str()) {
                    forbidden.push(format!("forbidden file type in evidence: {}", p.display()));
                }
            }
        }
    }
}

fn evidence_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_finalize_evidence_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-evidence");

        let dir = HarnessEvidenceDir::create(&root, "abc123def", "run-001").unwrap();
        assert!(dir.path().exists());
        assert!(dir.path().join(OWNERSHIP_MARKER_FILENAME).exists());

        // Validate empty contents.
        let forbidden = dir.validate_contents();
        assert!(forbidden.is_empty());

        // Write a valid evidence file.
        std::fs::write(dir.path().join("results.json"), r#"{"status": "ok"}"#).unwrap();
        let forbidden = dir.validate_contents();
        assert!(forbidden.is_empty());

        // Finalize.
        dir.finalize(MarkerState::Completed).unwrap();
        let raw = std::fs::read_to_string(dir.path().join(OWNERSHIP_MARKER_FILENAME)).unwrap();
        let marker: OwnershipMarker = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker.state, MarkerState::Completed);
    }

    #[test]
    fn forbidden_files_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-evidence");

        let dir = HarnessEvidenceDir::create(&root, "abc", "run-f").unwrap();

        // Plant a forbidden file.
        std::fs::write(dir.path().join("test.pdb"), b"fake pdb").unwrap();
        std::fs::create_dir_all(dir.path().join("deps").join("sub")).unwrap();

        let forbidden = dir.validate_contents();
        assert!(!forbidden.is_empty(), "should detect forbidden content");
    }

    #[test]
    fn retention_keeps_most_recent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("harness-evidence");

        // Create 5 successful evidence dirs.
        for i in 0..5 {
            let dir = HarnessEvidenceDir::create(&root, "head1", &format!("run-{:03}", i)).unwrap();
            dir.finalize(MarkerState::Completed).unwrap();
        }

        let cfg = super::super::types::LivenessConfig::for_test(tmp.path());
        let guard = super::super::guard::DeletionGuard::new(cfg, vec![]);
        let retention = EvidenceRetention::default();

        // Dry-run only — we don't delete in a unit test without guard setup.
        let result = apply_evidence_retention(&root, &retention, &guard, false);
        assert_eq!(result.examined, 2); // 5 total, keep 3, excess = 2
        let would_delete: Vec<_> = result
            .entries
            .iter()
            .filter(|e| e.action == CleanupAction::Delete)
            .collect();
        assert_eq!(would_delete.len(), 2, "should propose deleting 2 oldest");
    }
}
