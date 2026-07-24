//! DeletionGuard — multi-layer safety checks executed before ANY automated
//! deletion of a harness-managed directory.
//!
//! Every check that fails is collected; no single `bool` is used to make a
//! safety decision.  The guard returns a `SafetyVerdict` that either allows
//! (All DeletionGuarded) or denies with a complete list of reasons.
//!
//! # invariants
//! - Never panics on filesystem errors — logs and denies.
//! - Never follows symlinks during the check; `symlink_metadata` is used.
//! - The guard is a pure function of (path, config, live state); it performs
//!   no side effects except reading filesystem metadata and the ownership
//!   marker.

use std::path::{Path, PathBuf};

use super::types::{
    dir_size, is_pid_alive, CleanupAction, CleanupEntry, CleanupResult, LivenessConfig,
    ManagedDirKind, OwnershipMarker, SafetyVerdict, OWNERSHIP_MARKER_FILENAME,
};

/// Result of a full deletion-guard evaluation, including the marker
/// when it was successfully parsed (needed by callers for state checks).
pub struct GuardEvaluation {
    pub verdict: SafetyVerdict,
    pub marker: Option<OwnershipMarker>,
    pub canonical_path: Option<PathBuf>,
}

/// The core deletion safety gate.  Constructed once per cleanup pass
/// with the current liveness config.
pub struct DeletionGuard {
    config: LivenessConfig,
    /// Active execution IDs known to the current process registry.
    active_execution_ids: Vec<String>,
}

impl DeletionGuard {
    pub fn new(config: LivenessConfig, active_execution_ids: Vec<String>) -> Self {
        Self {
            config,
            active_execution_ids,
        }
    }

    /// Evaluate whether `path` may be safely deleted as a managed directory.
    /// `managed_root` determines which kind of directory we expect (temp,
    /// evidence, or cargo-run) and also provides the containment boundary.
    pub fn evaluate(
        &self,
        path: &Path,
        managed_root: &Path,
        expected_kind: Option<ManagedDirKind>,
    ) -> GuardEvaluation {
        let mut reasons: Vec<String> = Vec::new();
        let mut marker: Option<OwnershipMarker> = None;

        // ── 1. Path must exist ──────────────────────────────────
        if !path.exists() {
            reasons.push(format!("path does not exist: {}", path.display()));
            return GuardEvaluation {
                verdict: SafetyVerdict::Denied { reasons },
                marker: None,
                canonical_path: None,
            };
        }

        // ── 2. Not a symlink / junction / reparse point ─────────
        match path.symlink_metadata() {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    reasons.push(format!("path is a symlink: {}", path.display()));
                }
            }
            Err(e) => {
                reasons.push(format!("cannot read metadata for {}: {e}", path.display()));
                return GuardEvaluation {
                    verdict: SafetyVerdict::Denied { reasons },
                    marker: None,
                    canonical_path: None,
                };
            }
        }

        // ── 3. Canonicalize (symlink-escape guard) ──────────────
        let canonical = match path.canonicalize() {
            Ok(c) => c,
            Err(e) => {
                reasons.push(format!("cannot canonicalize {}: {e}", path.display()));
                return GuardEvaluation {
                    verdict: SafetyVerdict::Denied { reasons },
                    marker: None,
                    canonical_path: None,
                };
            }
        };
        let canonical_path: Option<PathBuf> = Some(canonical.clone());

        // ── 4. Within the managed root ──────────────────────────
        let managed_canonical = match managed_root.canonicalize() {
            Ok(c) => c,
            Err(e) => {
                reasons.push(format!(
                    "cannot canonicalize managed root {}: {e}",
                    managed_root.display()
                ));
                return GuardEvaluation {
                    verdict: SafetyVerdict::Denied { reasons },
                    marker: None,
                    canonical_path: canonical_path.clone(),
                };
            }
        };

        if !canonical.starts_with(&managed_canonical) {
            reasons.push(format!(
                "path {} is not under managed root {}",
                canonical.display(),
                managed_canonical.display()
            ));
        }

        // ── 5. Not the managed root itself ─────────────────────
        if canonical == managed_canonical {
            reasons.push(format!(
                "refusing to delete the managed root itself: {}",
                canonical.display()
            ));
        }

        // ── 6-12. Protected path checks ─────────────────────────
        let prot = &self.config.protected;

        if prot.is_protected(&canonical) {
            reasons.push(format!(
                "path is protected (repo/target/user/system root): {}",
                canonical.display()
            ));
        }
        if prot.is_under_shared_cargo(&canonical) {
            reasons.push(format!(
                "refusing to delete shared cargo target: {}",
                canonical.display()
            ));
        }
        if prot.is_git_dir(&canonical) {
            reasons.push("refusing to delete a .git directory".into());
        }

        // ── 13. Ownership marker check ──────────────────────────
        let marker_path = canonical.join(OWNERSHIP_MARKER_FILENAME);
        match std::fs::read_to_string(&marker_path) {
            Ok(raw) => {
                // Strip UTF-8 BOM if present.
                let cleaned = raw.strip_prefix('\u{FEFF}').unwrap_or(&raw);
                match serde_json::from_str::<OwnershipMarker>(cleaned) {
                    Ok(m) => {
                        // Schema version check.
                        if m.schema_version != super::types::MARKER_SCHEMA_VERSION {
                            reasons.push(format!(
                                "marker schema version {} != expected {}",
                                m.schema_version,
                                super::types::MARKER_SCHEMA_VERSION
                            ));
                        }

                        // Kind matches expected (if specified).
                        if let Some(expected) = expected_kind {
                            if m.kind != expected {
                                reasons.push(format!(
                                    "marker kind {:?} does not match expected {:?}",
                                    m.kind, expected
                                ));
                            }
                        }

                        // Kind matches the managed root.
                        let root_kind = managed_root_to_kind(&managed_canonical);
                        if root_kind != m.kind {
                            reasons.push(format!(
                                "marker kind {:?} does not match managed root kind {:?}",
                                m.kind, root_kind
                            ));
                        }

                        // Run ID matches directory name.
                        if let Some(dir_name) = canonical.file_name().and_then(|n| n.to_str()) {
                            if dir_name != m.run_id {
                                reasons.push(format!(
                                    "directory name {dir_name} != marker run_id {}",
                                    m.run_id
                                ));
                            }
                        }

                        marker = Some(m);
                    }
                    Err(e) => {
                        reasons.push(format!(
                            "invalid ownership marker in {}: {e}",
                            marker_path.display()
                        ));
                    }
                }
            }
            Err(e) => {
                reasons.push(format!(
                    "missing or unreadable ownership marker in {}: {e}",
                    marker_path.display()
                ));
            }
        }

        // ── 14. Owner liveness check ────────────────────────────
        if let Some(ref m) = marker {
            if m.is_active() {
                // Check if the owning process is still alive.
                let pid_alive = is_pid_alive(m.owner_pid);

                // Compare process creation time to guard against PID reuse.
                let creation_matches =
                    check_process_creation_time(m.owner_pid, &m.owner_process_created_at);

                if pid_alive && creation_matches {
                    // The owner process is still running. Allow only if this
                    // is the current supervisor cleaning its own temp
                    // (caller must supply the current run_id).
                    // We cannot make that distinction here, so we deny and
                    // let the caller override with an explicit allowlist.
                    reasons.push(format!(
                        "owner PID {} is still alive with matching creation time; \
                         directory is active",
                        m.owner_pid
                    ));
                } else if pid_alive && !creation_matches {
                    // PID was reused — different process. Treat as stale.
                    // This is allowed: the original owner is gone.
                }
                // else: PID not alive → stale, allowed.
            }

            // ── 15. Active execution check ──────────────────────
            if self.active_execution_ids.contains(&m.run_id) && m.is_active() {
                reasons.push(format!(
                    "run_id {} has an active execution in the registry",
                    m.run_id
                ));
            }
        }

        // ── 16. No marker → deny (unless already denied) ────────
        if marker.is_none() {
            reasons.push(format!(
                "no valid ownership marker; refusing to delete unowned directory: {}",
                canonical.display()
            ));
        }

        let verdict = if reasons.is_empty() {
            SafetyVerdict::Allowed
        } else {
            SafetyVerdict::Denied { reasons }
        };

        GuardEvaluation {
            verdict,
            marker,
            canonical_path,
        }
    }

    /// Perform a guarded deletion of a managed directory.  Returns a
    /// `CleanupEntry` describing the outcome.  This is the ONLY public
    /// entry point for automated deletion.
    pub fn guarded_delete(
        &self,
        path: &Path,
        managed_root: &Path,
        expected_kind: Option<ManagedDirKind>,
    ) -> CleanupEntry {
        let eval = self.evaluate(path, managed_root, expected_kind);

        match eval.verdict {
            SafetyVerdict::Denied { reasons } => CleanupEntry {
                path: path.to_path_buf(),
                action: CleanupAction::Preserve,
                reason: reasons.join("; "),
            },
            SafetyVerdict::Allowed => {
                // ── TOCTOU revalidation before deletion ─────────
                // Re-read marker, re-canonicalize, re-check active state.
                if let Some(toctou_denial) =
                    self.toctou_revalidate(path, managed_root, expected_kind)
                {
                    return CleanupEntry {
                        path: path.to_path_buf(),
                        action: CleanupAction::Preserve,
                        reason: format!("TOCTOU revalidation failed: {toctou_denial}"),
                    };
                }

                // Measure before deleting.
                let size = dir_size(path);

                match std::fs::remove_dir_all(path) {
                    Ok(()) => CleanupEntry {
                        path: path.to_path_buf(),
                        action: CleanupAction::Delete,
                        reason: format!("deleted ({} bytes reclaimed)", size),
                    },
                    Err(e) => CleanupEntry {
                        path: path.to_path_buf(),
                        action: CleanupAction::Preserve,
                        reason: format!("delete failed: {e}"),
                    },
                }
            }
        }
    }

    /// Dry-run: evaluate and return what WOULD happen without deleting.
    pub fn dry_run(
        &self,
        path: &Path,
        managed_root: &Path,
        expected_kind: Option<ManagedDirKind>,
    ) -> CleanupEntry {
        let eval = self.evaluate(path, managed_root, expected_kind);

        match eval.verdict {
            SafetyVerdict::Denied { reasons } => CleanupEntry {
                path: path.to_path_buf(),
                action: CleanupAction::Preserve,
                reason: reasons.join("; "),
            },
            SafetyVerdict::Allowed => {
                let size = dir_size(path);
                CleanupEntry {
                    path: path.to_path_buf(),
                    action: CleanupAction::Delete,
                    reason: format!("would delete ({} bytes)", size),
                }
            }
        }
    }

    /// TOCTOU revalidation: re-read the marker, re-canonicalize the
    /// path, and re-check active state immediately before deletion.
    /// Returns `None` if the path is still safe to delete, or `Some(reason)`
    /// if conditions changed since the initial evaluation.
    fn toctou_revalidate(
        &self,
        path: &Path,
        managed_root: &Path,
        expected_kind: Option<ManagedDirKind>,
    ) -> Option<String> {
        // 1. Path must still exist.
        if !path.exists() {
            return Some("path disappeared since evaluation".into());
        }

        // 2. Re-canonicalize — guards against junction/symlink swap.
        let canonical = match path.canonicalize() {
            Ok(c) => c,
            Err(e) => return Some(format!("re-canonicalize failed: {e}")),
        };

        // 3. Still under managed root.
        let managed_canonical = match managed_root.canonicalize() {
            Ok(c) => c,
            Err(e) => return Some(format!("managed root re-canonicalize failed: {e}")),
        };
        if !canonical.starts_with(&managed_canonical) {
            return Some("path escaped managed root since evaluation".into());
        }

        // 4. Re-read the marker.
        let marker_path = canonical.join(OWNERSHIP_MARKER_FILENAME);
        let marker: OwnershipMarker = match std::fs::read_to_string(&marker_path) {
            Ok(raw) => match serde_json::from_str(&raw) {
                Ok(m) => m,
                Err(e) => return Some(format!("marker re-parse failed: {e}")),
            },
            Err(e) => return Some(format!("marker disappeared: {e}")),
        };

        // 5. Kind must still match.
        if let Some(expected) = expected_kind {
            if marker.kind != expected {
                return Some(format!(
                    "marker kind changed: {:?} != {:?}",
                    marker.kind, expected
                ));
            }
        }

        // 6. If the marker was switched to active by a new owner, refuse.
        if marker.is_active() {
            let pid_alive = is_pid_alive(marker.owner_pid);
            if pid_alive {
                return Some(format!(
                    "owner PID {} became active since evaluation",
                    marker.owner_pid
                ));
            }
        }

        // 7. Protected path re-check.
        if self.config.protected.is_protected(&canonical) {
            return Some("path became protected since evaluation".into());
        }

        // 8. Active execution re-check.
        if self.active_execution_ids.contains(&marker.run_id) && marker.is_active() {
            return Some(format!(
                "run_id {} became active since evaluation",
                marker.run_id
            ));
        }

        None
    }

    /// Lightweight safety check for paths that use a different marker
    /// system (e.g., the legacy `.harness-owner.json` artifact format).
    /// Performs canonicalization, containment, symlink, protected-path,
    /// shared-cargo, and .git checks but does NOT require the liveness
    /// ownership marker.
    ///
    /// Returns `None` if the path is safe to delete, or `Some(reason)`
    /// if it is blocked.
    pub fn validate_path_safety(&self, path: &Path, expected_root: &Path) -> Option<String> {
        // Path must exist.
        if !path.exists() {
            return Some(format!("path does not exist: {}", path.display()));
        }

        // Not a symlink/junction.
        if let Ok(meta) = path.symlink_metadata() {
            if meta.file_type().is_symlink() {
                return Some(format!("path is a symlink: {}", path.display()));
            }
        } else {
            return Some(format!("cannot read metadata: {}", path.display()));
        }

        // Canonicalize and check containment.
        let canonical = match path.canonicalize() {
            Ok(c) => c,
            Err(e) => return Some(format!("cannot canonicalize: {e}")),
        };
        let root_canonical = match expected_root.canonicalize() {
            Ok(c) => c,
            Err(e) => return Some(format!("cannot canonicalize root: {e}")),
        };
        if !canonical.starts_with(&root_canonical) {
            return Some(format!(
                "path {} is not under expected root {}",
                canonical.display(),
                root_canonical.display()
            ));
        }
        if canonical == root_canonical {
            return Some(format!(
                "refusing to delete root itself: {}",
                canonical.display()
            ));
        }

        // Protected path checks.
        if self.config.protected.is_protected(&canonical) {
            return Some(format!("path is protected: {}", canonical.display()));
        }
        if self.config.protected.is_under_shared_cargo(&canonical) {
            return Some(format!("refusing shared cargo: {}", canonical.display()));
        }
        if self.config.protected.is_git_dir(&canonical) {
            return Some("refusing .git directory".into());
        }

        None
    }

    /// Scan all immediate children of a managed root, evaluating each
    /// against the guard.  Returns a `CleanupResult`.
    pub fn scan_managed_root(
        &self,
        managed_root: &Path,
        expected_kind: ManagedDirKind,
        apply: bool,
    ) -> CleanupResult {
        let mut result = CleanupResult::default();

        let entries = match std::fs::read_dir(managed_root) {
            Ok(iter) => iter,
            Err(e) => {
                tracing::warn!(
                    root = %managed_root.display(),
                    error = %e,
                    "cannot read managed root"
                );
                return result;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            result.examined += 1;

            let entry_result = if apply {
                self.guarded_delete(&path, managed_root, Some(expected_kind))
            } else {
                self.dry_run(&path, managed_root, Some(expected_kind))
            };

            match entry_result.action {
                CleanupAction::Delete => {
                    result.deleted += 1;
                    // Parse the size from the reason string (best-effort).
                    result.reclaimed_bytes += parse_reclaimed_bytes(&entry_result.reason);
                }
                CleanupAction::Preserve => {
                    result.preserved += 1;
                }
            }
            result.entries.push(entry_result);
        }

        result
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn managed_root_to_kind(root: &Path) -> ManagedDirKind {
    let name = root.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if name.contains("temp") {
        ManagedDirKind::HarnessManagedTemp
    } else if name.contains("evidence") {
        ManagedDirKind::HarnessManagedEvidence
    } else if name.contains("cargo") {
        ManagedDirKind::HarnessManagedCargoRun
    } else {
        // Fallback: infer from path components.
        let path_str = root.to_string_lossy();
        if path_str.contains("harness-temp") {
            ManagedDirKind::HarnessManagedTemp
        } else if path_str.contains("harness-evidence") {
            ManagedDirKind::HarnessManagedEvidence
        } else if path_str.contains("harness-cargo") {
            ManagedDirKind::HarnessManagedCargoRun
        } else {
            ManagedDirKind::HarnessManagedTemp // safe default
        }
    }
}

/// Check whether a process with the given PID was created at the expected
/// time.  Used to guard against PID reuse.
///
/// Returns `true` when the PID is alive AND the process creation time
/// matches the expected RFC 3339 timestamp.  Returns `false` when:
/// - The PID is not alive (process exited).
/// - The PID is alive but creation time differs (PID was reused).
/// - The creation time cannot be read (fail-safe: treat as mismatch).
#[cfg(windows)]
#[allow(unsafe_code)]
fn check_process_creation_time(pid: u32, expected: &str) -> bool {
    use std::os::windows::io::RawHandle;

    // Parse the expected timestamp to seconds since Unix epoch.
    let expected_secs = match chrono::DateTime::parse_from_rfc3339(expected) {
        Ok(dt) => dt.timestamp(),
        Err(_) => {
            // Cannot parse expected — fail safe.
            tracing::warn!(
                pid = pid,
                expected = %expected,
                "cannot parse expected creation time; refusing creation-time match"
            );
            return false;
        }
    };

    unsafe {
        let handle = windows_sys::Win32::System::Threading::OpenProcess(
            windows_sys::Win32::System::Threading::PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        );
        if handle.is_null() {
            // Process not accessible or not alive.
            return false;
        }

        let mut creation: windows_sys::Win32::Foundation::FILETIME = std::mem::zeroed();
        let mut exit: windows_sys::Win32::Foundation::FILETIME = std::mem::zeroed();
        let mut kernel: windows_sys::Win32::Foundation::FILETIME = std::mem::zeroed();
        let mut user: windows_sys::Win32::Foundation::FILETIME = std::mem::zeroed();

        let ok = windows_sys::Win32::System::Threading::GetProcessTimes(
            handle as RawHandle,
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        );
        windows_sys::Win32::Foundation::CloseHandle(handle);

        if ok == 0 {
            // Cannot read process times — fail safe.
            tracing::warn!(
                pid = pid,
                "GetProcessTimes failed; refusing creation-time match"
            );
            return false;
        }

        // FILETIME: 100-nanosecond intervals since 1601-01-01.
        // Convert to seconds since Unix epoch (1970-01-01).
        let ft_u64 = (creation.dwHighDateTime as u64) << 32 | (creation.dwLowDateTime as u64);
        // 11644473600 = seconds between 1601 and 1970.
        let creation_secs = (ft_u64 / 10_000_000).saturating_sub(11_644_473_600);

        creation_secs == expected_secs as u64
    }
}

#[cfg(not(windows))]
fn check_process_creation_time(pid: u32, _expected: &str) -> bool {
    // On Unix we could read /proc/<pid>/stat starttime.
    // For now, conservative: check PID alive only.
    is_pid_alive(pid)
}

fn parse_reclaimed_bytes(reason: &str) -> u64 {
    // "deleted (12345 bytes reclaimed)" or "would delete (12345 bytes)"
    if let Some(start) = reason.find('(') {
        if let Some(end) = reason.find(" bytes") {
            let num_str = &reason[start + 1..end];
            return num_str.parse::<u64>().unwrap_or(0);
        }
    }
    0
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::liveness::types::MarkerState;

    fn test_config(temp_root: &Path) -> LivenessConfig {
        LivenessConfig::for_test(temp_root)
    }

    fn write_marker(dir: &Path, marker: &OwnershipMarker) {
        let path = dir.join(OWNERSHIP_MARKER_FILENAME);
        let tmp = dir.join(format!("{}.tmp", OWNERSHIP_MARKER_FILENAME));
        let json = serde_json::to_string_pretty(marker).unwrap();
        std::fs::write(&tmp, &json).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
    }

    fn make_owned_dir(
        root: &Path,
        run_id: &str,
        kind: ManagedDirKind,
        state: MarkerState,
    ) -> (PathBuf, OwnershipMarker) {
        let dir = root.join(run_id);
        std::fs::create_dir_all(&dir).unwrap();
        let marker = OwnershipMarker::new_active(
            kind,
            run_id.to_string(),
            std::process::id(),
            "test-head".into(),
        );
        let marker = marker.finalize(state);
        write_marker(&dir, &marker);
        (dir, marker)
    }

    // ── Safety boundary tests ────────────────────────────────────

    #[test]
    fn owned_child_inside_managed_root_allowed() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let (dir, _) = make_owned_dir(
            &cfg.managed_temp_root,
            "run-001",
            ManagedDirKind::HarnessManagedTemp,
            MarkerState::Completed,
        );

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert!(
            eval.verdict.is_allowed(),
            "owned completed child should be allowed; got: {:?}",
            eval.verdict
        );
    }

    #[test]
    fn missing_marker_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();
        let dir = cfg.managed_temp_root.join("run-no-marker");
        std::fs::create_dir_all(&dir).unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert!(!eval.verdict.is_allowed(), "missing marker must be denied");
        if let SafetyVerdict::Denied { reasons } = &eval.verdict {
            assert!(
                reasons
                    .iter()
                    .any(|r| r.contains("no valid ownership marker")),
                "should mention missing marker: {:?}",
                reasons
            );
        }
    }

    #[test]
    fn invalid_marker_json_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();
        let dir = cfg.managed_temp_root.join("run-bad-marker");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(OWNERSHIP_MARKER_FILENAME), "not valid json {{{").unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert!(
            !eval.verdict.is_allowed(),
            "invalid marker JSON must be denied"
        );
    }

    #[test]
    fn marker_kind_mismatch_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        // Write a temp-kind marker into the evidence root.
        let dir = cfg.managed_evidence_root.join("run-001");
        std::fs::create_dir_all(&dir).unwrap();
        let marker = OwnershipMarker::new_active(
            ManagedDirKind::HarnessManagedTemp, // wrong kind for this root
            "run-001".into(),
            std::process::id(),
            "head".into(),
        );
        let marker = marker.finalize(MarkerState::Completed);
        write_marker(&dir, &marker);

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &dir,
            &guard.config.managed_evidence_root,
            Some(ManagedDirKind::HarnessManagedEvidence),
        );
        assert!(!eval.verdict.is_allowed(), "kind mismatch must be denied");
        if let SafetyVerdict::Denied { reasons } = &eval.verdict {
            assert!(
                reasons.iter().any(|r| r.contains("kind")),
                "should mention kind mismatch: {:?}",
                reasons
            );
        }
    }

    #[test]
    fn repo_root_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &guard.config.protected.repo_root,
            &guard.config.managed_temp_root,
            None,
        );
        assert!(!eval.verdict.is_allowed(), "repo root must be denied");
        if let SafetyVerdict::Denied { reasons } = &eval.verdict {
            assert!(
                reasons.iter().any(|r| r.contains("protected")),
                "should mention protected path: {:?}",
                reasons
            );
        }
    }

    #[test]
    fn target_root_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &guard.config.protected.target_root,
            &guard.config.managed_temp_root,
            None,
        );
        assert!(!eval.verdict.is_allowed(), "target root must be denied");
    }

    #[test]
    fn managed_root_itself_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &guard.config.managed_temp_root,
            &guard.config.managed_temp_root,
            None,
        );
        assert!(
            !eval.verdict.is_allowed(),
            "managed root itself must be denied"
        );
    }

    #[test]
    fn shared_cargo_target_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.protected.shared_cargo_target).unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &guard.config.protected.shared_cargo_target,
            &guard.config.managed_temp_root,
            None,
        );
        assert!(
            !eval.verdict.is_allowed(),
            "shared cargo target must be denied"
        );
    }

    #[test]
    fn user_home_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &guard.config.protected.user_profile,
            &guard.config.managed_temp_root,
            None,
        );
        assert!(!eval.verdict.is_allowed(), "user home must be denied");
    }

    #[test]
    fn system_temp_root_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &guard.config.protected.system_temp_root,
            &guard.config.managed_temp_root,
            None,
        );
        assert!(
            !eval.verdict.is_allowed(),
            "system TEMP root must be denied"
        );
    }

    #[test]
    fn git_dir_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        let git_dir = cfg.protected.repo_root.join(".git");
        std::fs::create_dir_all(&git_dir).unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(&git_dir, &guard.config.managed_temp_root, None);
        assert!(!eval.verdict.is_allowed(), ".git must be denied");
    }

    #[test]
    fn path_not_under_managed_root_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        let outside_dir = tmp.path().join("outside");
        std::fs::create_dir_all(&outside_dir).unwrap();

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(&outside_dir, &guard.config.managed_temp_root, None);
        assert!(
            !eval.verdict.is_allowed(),
            "path outside managed root must be denied"
        );
    }

    #[test]
    fn run_id_mismatch_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let dir = cfg.managed_temp_root.join("run-001");
        std::fs::create_dir_all(&dir).unwrap();
        // Write a marker with a run_id different from the directory name.
        let marker = OwnershipMarker::new_active(
            ManagedDirKind::HarnessManagedTemp,
            "run-002".into(), // mismatched
            std::process::id(),
            "head".into(),
        );
        let marker = marker.finalize(MarkerState::Completed);
        write_marker(&dir, &marker);

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert!(!eval.verdict.is_allowed(), "run_id mismatch must be denied");
        if let SafetyVerdict::Denied { reasons } = &eval.verdict {
            assert!(
                reasons.iter().any(|r| r.contains("run_id")),
                "should mention run_id: {:?}",
                reasons
            );
        }
    }

    #[test]
    fn active_execution_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let (dir, _) = make_owned_dir(
            &cfg.managed_temp_root,
            "run-active",
            ManagedDirKind::HarnessManagedTemp,
            MarkerState::Active,
        );

        let guard = DeletionGuard::new(cfg, vec!["run-active".into()]);
        let eval = guard.evaluate(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert!(
            !eval.verdict.is_allowed(),
            "active execution must be denied"
        );
    }

    #[test]
    fn nonexistent_path_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();
        let nonexistent = cfg.managed_temp_root.join("does-not-exist");

        let guard = DeletionGuard::new(cfg, vec![]);
        let eval = guard.evaluate(&nonexistent, &guard.config.managed_temp_root, None);
        assert!(
            !eval.verdict.is_allowed(),
            "nonexistent path must be denied"
        );
    }

    #[test]
    fn dry_run_does_not_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let (dir, _) = make_owned_dir(
            &cfg.managed_temp_root,
            "run-dry",
            ManagedDirKind::HarnessManagedTemp,
            MarkerState::Completed,
        );
        assert!(dir.exists());

        let guard = DeletionGuard::new(cfg, vec![]);
        let entry = guard.dry_run(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert_eq!(
            entry.action,
            CleanupAction::Delete,
            "dry-run should say Delete"
        );
        assert!(dir.exists(), "dry-run must not actually delete");
        assert!(
            entry.reason.contains("would delete"),
            "dry-run reason must say 'would delete'"
        );
    }

    #[test]
    fn guarded_delete_actually_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let (dir, _) = make_owned_dir(
            &cfg.managed_temp_root,
            "run-del",
            ManagedDirKind::HarnessManagedTemp,
            MarkerState::Completed,
        );
        assert!(dir.exists());

        let guard = DeletionGuard::new(cfg, vec![]);
        let entry = guard.guarded_delete(
            &dir,
            &guard.config.managed_temp_root,
            Some(ManagedDirKind::HarnessManagedTemp),
        );
        assert_eq!(entry.action, CleanupAction::Delete);
        assert!(!dir.exists(), "guarded_delete must delete the directory");
    }

    #[test]
    fn scan_managed_root_collects_results() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        // Create two completed dirs + one unmarked dir.
        let (d1, _) = make_owned_dir(
            &cfg.managed_temp_root,
            "run-a",
            ManagedDirKind::HarnessManagedTemp,
            MarkerState::Completed,
        );
        let (d2, _) = make_owned_dir(
            &cfg.managed_temp_root,
            "run-b",
            ManagedDirKind::HarnessManagedTemp,
            MarkerState::Failed,
        );
        let d3 = cfg.managed_temp_root.join("run-c");
        std::fs::create_dir_all(&d3).unwrap();
        // d3 has NO marker.

        let guard = DeletionGuard::new(cfg, vec![]);
        let result = guard.scan_managed_root(
            &guard.config.managed_temp_root,
            ManagedDirKind::HarnessManagedTemp,
            false, // dry-run
        );

        assert_eq!(result.examined, 3);
        // Two should be allowed (dry-run "delete"), one preserved.
        let allowed: Vec<_> = result
            .entries
            .iter()
            .filter(|e| e.action == CleanupAction::Delete)
            .collect();
        let preserved: Vec<_> = result
            .entries
            .iter()
            .filter(|e| e.action == CleanupAction::Preserve)
            .collect();
        assert_eq!(allowed.len(), 2, "two marked dirs should be eligible");
        assert_eq!(preserved.len(), 1, "one unmarked dir should be preserved");

        // Verify nothing was actually deleted (dry-run).
        assert!(d1.exists());
        assert!(d2.exists());
        assert!(d3.exists());
    }

    #[test]
    fn safety_verdict_and_merges_reasons() {
        let v1 = SafetyVerdict::deny("reason one");
        let v2 = SafetyVerdict::deny("reason two");
        let merged = v1.and(v2);
        if let SafetyVerdict::Denied { reasons } = merged {
            assert_eq!(reasons.len(), 2);
            assert!(reasons.contains(&"reason one".to_string()));
            assert!(reasons.contains(&"reason two".to_string()));
        } else {
            panic!("merged should be Denied");
        }

        // Allowed + Denied = Denied.
        let a = SafetyVerdict::Allowed;
        let d = SafetyVerdict::deny("only this");
        let merged = a.and(d);
        if let SafetyVerdict::Denied { reasons } = merged {
            assert_eq!(reasons.len(), 1);
            assert_eq!(reasons[0], "only this");
        } else {
            panic!("should be Denied");
        }
    }

    #[test]
    fn liveness_config_validate_smoke() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(tmp.path());
        let errors = cfg.validate();
        // Managed roots are under tmp, which is fine.
        assert!(
            errors.is_empty(),
            "valid config should have no errors: {errors:?}"
        );
    }
}
