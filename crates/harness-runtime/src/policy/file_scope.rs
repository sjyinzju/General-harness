//! FileScopeValidator — workspace path safety and write-scope enforcement.
//!
//! Security model (I2B-3 closure):
//! - Absolute / drive / UNC / root-relative paths are rejected.
//! - Any `..` path component is rejected (component-based, not substring).
//! - Windows reserved device names are rejected (case-insensitive, trailing
//!   spaces/dots stripped, ADS `:` stripped before the stem check).
//! - Windows alternate data streams (`:` in a component) are rejected.
//! - Symlink / junction escape is detected by partial canonicalization of
//!   the nearest existing ancestor and explicitly denied.
//! - `.git` and harness metadata are protected case-insensitively.
//! - Path/scope matching is case-insensitive on Windows and Unicode-NFC
//!   normalized, so `.GIT`, `Secret/`, or NFD-encoded names cannot bypass a
//!   forbidden/allowed rule.
use std::path::{Component, Path, PathBuf};

use harness_core::contracts::task_envelope::FileScope;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedWorkspacePath(String);
impl NormalizedWorkspacePath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeRule {
    Allow(String),
    Deny(String),
    ReadOnly(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeDecision {
    Allowed,
    Denied(ScopeViolation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeViolation {
    OutsideWorkspace,
    OutsideWriteScope,
    DeniedPath,
    SymlinkEscape,
    GitMetadataProtected,
    HarnessMetadataProtected,
    AmbiguousPath(String),
    ReservedDeviceName,
    TraversalRejected,
    AbsolutePathRejected,
    AlternateDataStream,
}

pub struct FileScopeValidator {
    worktree_root: PathBuf,
    scope: FileScope,
}

impl FileScopeValidator {
    pub fn new(worktree_root: &Path, scope: FileScope) -> Result<Self, CoreError> {
        let root = worktree_root
            .canonicalize()
            .map_err(|e| pe(format!("canonicalize: {e}")))?;
        Ok(Self {
            worktree_root: root,
            scope,
        })
    }
    pub fn worktree_root(&self) -> &Path {
        &self.worktree_root
    }

    pub fn validate(&self, path: &str) -> Result<(ScopeDecision, PathBuf), CoreError> {
        // Normalize separators and strip trailing slashes.
        let n = path.replace('\\', "/").trim_end_matches('/').to_string();

        // Absolute / drive / UNC / root-relative rejection. Uses the
        // separator-normalized form so single-backslash (`\foo`) and UNC
        // (`\\srv\share`) paths are caught alongside `/etc` and `C:/`.
        let is_drive = cfg!(windows)
            && n.len() >= 2
            && n.as_bytes()[0].is_ascii_alphabetic()
            && n.as_bytes()[1] == b':'
            && (n.len() == 2 || n.as_bytes()[2] == b'/');
        if n.starts_with('/') || is_drive {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::AbsolutePathRejected),
                PathBuf::new(),
            ));
        }

        // Per-component checks: traversal, ADS, reserved device names.
        for c in Path::new(&n).components() {
            match c {
                Component::ParentDir => {
                    return Ok((
                        ScopeDecision::Denied(ScopeViolation::TraversalRejected),
                        PathBuf::new(),
                    ));
                }
                Component::Normal(p) => {
                    let s = p.to_string_lossy();
                    if cfg!(windows) && s.contains(':') {
                        return Ok((
                            ScopeDecision::Denied(ScopeViolation::AlternateDataStream),
                            PathBuf::new(),
                        ));
                    }
                    if is_reserved(&s) {
                        return Ok((
                            ScopeDecision::Denied(ScopeViolation::ReservedDeviceName),
                            PathBuf::new(),
                        ));
                    }
                }
                _ => {}
            }
        }

        let cand = self.worktree_root.join(&n);
        let (can, ur) = partial_canon(&cand)?;
        if !can.starts_with(&self.worktree_root) {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::SymlinkEscape),
                PathBuf::new(),
            ));
        }
        let full = if !ur.is_empty() { can.join(&ur) } else { can };
        let rel = full
            .strip_prefix(&self.worktree_root)
            .map(|r| r.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let rel_n = normalize(&rel);

        if is_git(&rel_n) {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::GitMetadataProtected),
                full,
            ));
        }
        if is_harness(&rel_n) {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::HarnessMetadataProtected),
                full,
            ));
        }
        for d in &self.scope.forbidden_paths {
            if gm(&rel_n, &normalize(d)) {
                return Ok((ScopeDecision::Denied(ScopeViolation::DeniedPath), full));
            }
        }
        if self.scope.allowed_paths.is_empty() {
            return Ok((ScopeDecision::Allowed, full));
        }
        for a in &self.scope.allowed_paths {
            if gm(&rel_n, &normalize(a)) {
                return Ok((ScopeDecision::Allowed, full));
            }
        }
        Ok((
            ScopeDecision::Denied(ScopeViolation::OutsideWriteScope),
            full,
        ))
    }
}

/// NFC-normalize, and on Windows also lowercase, so that case and Unicode
/// form cannot be used to dodge a rule.
fn normalize(s: &str) -> String {
    let nfc: String = s.nfc().collect();
    if cfg!(windows) {
        nfc.to_lowercase()
    } else {
        nfc
    }
}

fn gm(path: &str, glob: &str) -> bool {
    if glob == "**" || glob == "**/*" {
        return true;
    }
    if let Some(p) = glob.strip_suffix("**") {
        return path.starts_with(p.trim_end_matches('/'));
    }
    if let Some(s) = glob.strip_prefix('*') {
        return path.ends_with(s);
    }
    if let Some(p) = glob.strip_suffix('*') {
        return path.starts_with(p.trim_end_matches('/'));
    }
    path == glob
}
fn is_git(p: &str) -> bool {
    p == ".git" || p.starts_with(".git/") || p.contains("/.git/") || p.ends_with("/.git")
}
fn is_harness(p: &str) -> bool {
    p.contains(".harness")
}
fn is_reserved(n: &str) -> bool {
    // Windows ignores trailing spaces and dots in file names; strip them so
    // `CON ` / `CON.` are caught. Split on `.` and `:` to get the stem so
    // `CON.txt` and `CON:ads` are caught.
    let core = n.trim_end_matches([' ', '.']);
    let stem = core
        .split(['.', ':'])
        .next()
        .unwrap_or(core)
        .to_ascii_uppercase();
    [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ]
    .contains(&stem.as_str())
}
fn partial_canon(path: &Path) -> Result<(PathBuf, String), CoreError> {
    let mut e = path.to_path_buf();
    let mut s: Vec<String> = Vec::new();
    while !e.exists() {
        if let Some(n) = e.file_name().and_then(|n| n.to_str()) {
            s.push(n.to_string());
            e = e
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("/"));
        } else {
            break;
        }
    }
    let c = e
        .canonicalize()
        .map_err(|x| pe(format!("canonicalize {}: {x}", path.display())))?;
    s.reverse();
    Ok((c, s.join("/")))
}
fn pe(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn v(allowed: &[&str]) -> (tempfile::TempDir, FileScopeValidator) {
        let t = tempfile::tempdir().unwrap();
        let r = t.path().join("worktree");
        std::fs::create_dir_all(&r).unwrap();
        std::fs::create_dir_all(r.join("src")).unwrap();
        std::fs::write(r.join("README.md"), "# test").unwrap();
        let s = FileScope {
            allowed_paths: allowed.iter().map(|s| s.to_string()).collect(),
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        };
        (t, FileScopeValidator::new(&r, s).unwrap())
    }

    #[test]
    fn exact_file() {
        let (_t, v) = v(&["README.md"]);
        assert!(matches!(
            v.validate("README.md").unwrap().0,
            ScopeDecision::Allowed
        ));
    }
    #[test]
    fn dir_prefix() {
        let (_t, v) = v(&["src/**"]);
        assert!(matches!(
            v.validate("src/auth/callback.ts").unwrap().0,
            ScopeDecision::Allowed
        ));
    }
    #[test]
    fn outside() {
        let (_t, v) = v(&["src/**"]);
        assert!(matches!(
            v.validate("README.md").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::OutsideWriteScope)
        ));
    }
    #[test]
    fn denied_prio() {
        let t = tempfile::tempdir().unwrap();
        let r = t.path().join("w");
        std::fs::create_dir_all(&r).unwrap();
        std::fs::create_dir_all(r.join("secret")).unwrap();
        std::fs::write(r.join("secret").join("key.txt"), "k").unwrap();
        let v2 = FileScopeValidator::new(
            &r,
            FileScope {
                allowed_paths: vec!["**".into()],
                forbidden_paths: vec!["secret/**".into()],
                readable_paths: vec![],
                scope_expansion_allowed: false,
            },
        )
        .unwrap();
        assert!(matches!(
            v2.validate("secret/key.txt").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::DeniedPath)
        ));
    }
    #[test]
    fn prefix_confusion_is_outside_scope() {
        // "src/a" must NOT match the exact glob "src/ab". The only correct,
        // stable denial reason is OutsideWriteScope (no PrefixConfusion
        // variant exists — that dead branch was removed).
        let (_t, v) = v(&["src/ab"]);
        std::fs::write(v.worktree_root().join("src").join("a"), "").unwrap();
        let r = v.validate("src/a").unwrap().0;
        assert!(
            matches!(r, ScopeDecision::Denied(ScopeViolation::OutsideWriteScope)),
            "expected OutsideWriteScope, got {r:?}"
        );
    }
    #[test]
    fn absolute() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate("/etc/passwd").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::AbsolutePathRejected)
        ));
    }
    #[test]
    fn traversal() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate("../outside").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::TraversalRejected)
        ));
    }
    #[test]
    fn gitmeta() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate(".git/config").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::GitMetadataProtected)
        ));
    }
    #[test]
    fn gitmeta_case_insensitive() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate(".GIT/config").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::GitMetadataProtected)
        ));
    }
    #[test]
    fn harnessmeta() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate(".harness-owner.json").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::HarnessMetadataProtected)
        ));
    }
    #[test]
    fn reserved() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate("con.txt").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::ReservedDeviceName)
        ));
    }
    #[test]
    fn reserved_trailing_space() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate("CON ").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::ReservedDeviceName)
        ));
    }
    #[cfg(windows)]
    #[test]
    fn ads_rejected() {
        let (_t, v) = v(&["**"]);
        assert!(matches!(
            v.validate("README.md:hidden").unwrap().0,
            ScopeDecision::Denied(ScopeViolation::AlternateDataStream)
        ));
    }
    #[test]
    fn forbidden_case_insensitive() {
        // On Windows `Secret/` must be denied when `secret/**` is forbidden.
        let t = tempfile::tempdir().unwrap();
        let r = t.path().join("w");
        std::fs::create_dir_all(&r).unwrap();
        let v2 = FileScopeValidator::new(
            &r,
            FileScope {
                allowed_paths: vec!["**".into()],
                forbidden_paths: vec!["secret/**".into()],
                readable_paths: vec![],
                scope_expansion_allowed: false,
            },
        )
        .unwrap();
        let d = v2.validate("Secret/key.txt").unwrap().0;
        if cfg!(windows) {
            assert!(
                matches!(d, ScopeDecision::Denied(ScopeViolation::DeniedPath)),
                "expected DeniedPath on Windows, got {d:?}"
            );
        } else {
            assert!(matches!(d, ScopeDecision::Allowed), "{d:?}");
        }
    }
    #[test]
    fn unicode_normalization() {
        // NFD-encoded `é` (U+0065 U+0301) must match an NFC `é` (U+00E9)
        // forbidden rule.
        let nfc_e = "\u{00e9}"; // é
        let nfd_e = "e\u{0301}"; // e + combining acute
        let t = tempfile::tempdir().unwrap();
        let r = t.path().join("w");
        std::fs::create_dir_all(&r).unwrap();
        let forbidden = format!("caf{nfc_e}/**");
        let v2 = FileScopeValidator::new(
            &r,
            FileScope {
                allowed_paths: vec!["**".into()],
                forbidden_paths: vec![forbidden],
                readable_paths: vec![],
                scope_expansion_allowed: false,
            },
        )
        .unwrap();
        let path = format!("caf{nfd_e}/secret.txt");
        let d = v2.validate(&path).unwrap().0;
        assert!(
            matches!(d, ScopeDecision::Denied(ScopeViolation::DeniedPath)),
            "NFD path must match NFC forbidden rule: {d:?}"
        );
    }
    #[test]
    fn symlink_escape_denied() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("w");
        std::fs::create_dir_all(&root).unwrap();
        let outside = t.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let link = root.join("link");
        if make_symlink(&outside, &link).is_err() {
            eprintln!("symlink creation unsupported on this platform/session — skipping");
            return;
        }
        let v2 = FileScopeValidator::new(
            &root,
            FileScope {
                allowed_paths: vec!["**".into()],
                forbidden_paths: vec![],
                readable_paths: vec![],
                scope_expansion_allowed: false,
            },
        )
        .unwrap();
        let d = v2.validate("link/file").unwrap().0;
        assert!(
            matches!(d, ScopeDecision::Denied(ScopeViolation::SymlinkEscape)),
            "expected SymlinkEscape, got {d:?}"
        );
    }
    #[test]
    fn nonexistent_under_allowed_scope_allowed() {
        // A nonexistent path whose nearest existing ancestor is the
        // worktree root must still be evaluated against scope rules — and
        // allowed when under a `**` allow rule.
        let (_t, v) = v(&["**"]);
        let d = v.validate("nonexistent/dir/file.txt").unwrap().0;
        assert!(
            matches!(d, ScopeDecision::Allowed),
            "nonexistent path under ** must be allowed, got {d:?}"
        );
    }

    fn make_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_dir(target, link)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (target, link);
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "no symlinks",
            ))
        }
    }
}
