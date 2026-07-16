//! FileScopeValidator.
use std::path::{Component, Path, PathBuf};

use harness_core::contracts::task_envelope::FileScope;
use harness_core::{CoreError, ErrorCode, ErrorSource};

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
    PrefixConfusion(String),
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
        let n = path.replace('\\', "/").trim_end_matches('/').to_string();
        if path.starts_with('/')
            || (cfg!(windows) && path.len() >= 2 && &path[1..2] == ":")
            || path.starts_with("\\\\")
        {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::AbsolutePathRejected),
                PathBuf::new(),
            ));
        }
        if n.contains("/..") || n == ".." || n.starts_with("../") {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::TraversalRejected),
                PathBuf::new(),
            ));
        }
        for c in Path::new(&n).components() {
            if let Component::Normal(p) = c {
                if is_reserved(&p.to_string_lossy()) {
                    return Ok((
                        ScopeDecision::Denied(ScopeViolation::ReservedDeviceName),
                        PathBuf::new(),
                    ));
                }
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
        if is_git(&rel) {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::GitMetadataProtected),
                full,
            ));
        }
        if is_harness(&rel) {
            return Ok((
                ScopeDecision::Denied(ScopeViolation::HarnessMetadataProtected),
                full,
            ));
        }
        for d in &self.scope.forbidden_paths {
            if gm(&rel, d) {
                return Ok((ScopeDecision::Denied(ScopeViolation::DeniedPath), full));
            }
        }
        if self.scope.allowed_paths.is_empty() {
            return Ok((ScopeDecision::Allowed, full));
        }
        for a in &self.scope.allowed_paths {
            if gm(&rel, a) {
                if !a.contains('*') && rel.len() < a.len() && a.starts_with(&rel) {
                    return Ok((
                        ScopeDecision::Denied(ScopeViolation::PrefixConfusion(a.clone())),
                        full,
                    ));
                }
                return Ok((ScopeDecision::Allowed, full));
            }
        }
        Ok((
            ScopeDecision::Denied(ScopeViolation::OutsideWriteScope),
            full,
        ))
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
    let b = n.split('.').next().unwrap_or(n).to_ascii_uppercase();
    [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ]
    .contains(&b.as_str())
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
    fn prefix() {
        let (_t, v) = v(&["src/ab"]);
        std::fs::write(v.worktree_root().join("src").join("a"), "").unwrap();
        let r = v.validate("src/a").unwrap().0;
        assert!(
            matches!(
                r,
                ScopeDecision::Denied(ScopeViolation::PrefixConfusion { .. })
            ) || matches!(r, ScopeDecision::Denied(ScopeViolation::OutsideWriteScope)),
            "{r:?}"
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
}
