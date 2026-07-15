//! Path and branch naming policy for harness worktrees.
//!
//! Identifiers are strictly validated (never silently transformed): reject
//! `..`, absolute-path injection, separators, control characters, Windows
//! reserved device names, and over-long values. Branch names additionally
//! pass `git check-ref-format --branch` before use (manager enforces).

use std::path::{Component, Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

const MAX_COMPONENT_LEN: usize = 100;
const MAX_BRANCH_LEN: usize = 200;
pub const BRANCH_PREFIX: &str = "harness/";

/// Validate one identifier used in paths and branch names.
pub fn validate_identifier(s: &str) -> Result<(), CoreError> {
    if s.is_empty() || s.len() > MAX_COMPONENT_LEN {
        return Err(name_err(format!(
            "identifier empty or longer than {MAX_COMPONENT_LEN}: {s:?}"
        )));
    }
    if s == "." || s == ".." {
        return Err(name_err(format!("path traversal rejected: {s:?}")));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(name_err(format!(
            "identifier contains illegal characters (allowed: [A-Za-z0-9._-]): {s:?}"
        )));
    }
    if s.starts_with('.') || s.ends_with('.') || s.ends_with(' ') {
        return Err(name_err(format!(
            "identifier may not start/end with '.' or end with space: {s:?}"
        )));
    }
    let base = s.split('.').next().unwrap_or(s).to_ascii_uppercase();
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if RESERVED.contains(&base.as_str()) {
        return Err(name_err(format!("reserved device name rejected: {s:?}")));
    }
    Ok(())
}

/// Deterministic worktree id for a task execution.
pub fn worktree_id(task_id: &str, execution_id: &str) -> Result<String, CoreError> {
    validate_identifier(task_id)?;
    validate_identifier(execution_id)?;
    Ok(format!("wt-{task_id}-{execution_id}"))
}

/// Deterministic branch name: `harness/<task_id>/<execution_id>`.
/// Distinct task/execution pairs can never collide.
pub fn branch_name(task_id: &str, execution_id: &str) -> Result<String, CoreError> {
    validate_identifier(task_id)?;
    validate_identifier(execution_id)?;
    let name = format!("{BRANCH_PREFIX}{task_id}/{execution_id}");
    validate_branch_name(&name)?;
    Ok(name)
}

/// Static branch-name validation (git's own `check-ref-format --branch` is
/// run additionally by the manager before any ref is created).
pub fn validate_branch_name(name: &str) -> Result<(), CoreError> {
    if name.is_empty() || name.len() > MAX_BRANCH_LEN {
        return Err(name_err(format!("branch name empty or too long: {name:?}")));
    }
    let illegal = name.chars().any(|c| {
        c.is_ascii_control() || matches!(c, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
    });
    if illegal
        || name.starts_with('/')
        || name.ends_with('/')
        || name.ends_with('.')
        || name.ends_with(".lock")
        || name.contains("//")
        || name.contains("..")
        || name.contains("@{")
        || name == "@"
    {
        return Err(name_err(format!("invalid branch name: {name:?}")));
    }
    for segment in name.split('/') {
        if segment.is_empty() || segment.starts_with('.') {
            return Err(name_err(format!("invalid branch segment in: {name:?}")));
        }
    }
    Ok(())
}

/// Default worktree path layout:
/// `<worktree_root>/<project_id>/<task_id>-<execution_id>`.
pub fn default_worktree_path(
    worktree_root: &Path,
    project_id: &str,
    task_id: &str,
    execution_id: &str,
) -> Result<PathBuf, CoreError> {
    validate_identifier(project_id)?;
    validate_identifier(task_id)?;
    validate_identifier(execution_id)?;
    Ok(worktree_root
        .join(project_id)
        .join(format!("{task_id}-{execution_id}")))
}

/// A candidate worktree path must resolve strictly under the harness-owned
/// worktree root: no `..`, no absolute-path escape, no root aliasing.
/// (Post-creation the manager re-verifies with `canonicalize`.)
pub fn ensure_under_root(root: &Path, candidate: &Path) -> Result<(), CoreError> {
    let Ok(rel) = candidate.strip_prefix(root) else {
        return Err(name_err(format!(
            "worktree path {} escapes the harness worktree root {}",
            candidate.display(),
            root.display()
        )));
    };
    if rel.as_os_str().is_empty() {
        return Err(name_err("worktree path may not equal the root".into()));
    }
    for comp in rel.components() {
        match comp {
            Component::Normal(part) => {
                let Some(s) = part.to_str() else {
                    return Err(name_err("non-UTF-8 path component rejected".into()));
                };
                validate_identifier(s)?;
            }
            _ => {
                return Err(name_err(format!(
                    "illegal component in worktree path: {}",
                    candidate.display()
                )));
            }
        }
    }
    Ok(())
}

fn name_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

/// Canonicalize for interop with external tools: `std::fs::canonicalize` on
/// Windows yields extended-length paths (`\\?\C:\...`) which git (MSYS)
/// rejects ("Invalid argument"). Strip the prefix after canonicalizing.
pub fn canonicalize_for_git(p: &Path) -> std::io::Result<PathBuf> {
    Ok(strip_extended_prefix(&p.canonicalize()?))
}

/// Strip the Windows `\\?\` / `\\?\UNC\` extended-length prefix.
pub fn strip_extended_prefix(p: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        let s = p.as_os_str().to_string_lossy();
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            return PathBuf::from(rest);
        }
        p.to_path_buf()
    }
    #[cfg(not(windows))]
    {
        p.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_names_are_stable_and_distinct() {
        let a = branch_name("t1", "e1").unwrap();
        let b = branch_name("t1", "e2").unwrap();
        assert_eq!(a, "harness/t1/e1");
        assert_ne!(a, b);
    }

    #[test]
    fn branch_sanitization_rejects_injection() {
        for bad in [
            "../evil", "a b", "a..b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b", "a\\b", "a//b",
            ".hidden", "end.", "x.lock", "@", "a@{b",
        ] {
            assert!(
                validate_branch_name(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(validate_branch_name("harness/t-1/e_2.x").is_ok());
    }

    #[test]
    fn identifier_rejects_traversal_and_reserved() {
        for bad in [
            "..", "a/b", "a\\b", "C:", "nul", "COM3.txt", ".dot", "dot.", "",
        ] {
            assert!(
                validate_identifier(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(validate_identifier("t-123_ok.v2").is_ok());
    }

    #[test]
    fn path_escape_rejected() {
        let root = PathBuf::from("C:\\data\\worktrees");
        assert!(ensure_under_root(&root, &root.join("p1").join("t1-e1")).is_ok());
        assert!(ensure_under_root(&root, &root).is_err());
        assert!(ensure_under_root(&root, &root.join("..").join("x")).is_err());
        assert!(ensure_under_root(&root, &PathBuf::from("C:\\elsewhere\\x")).is_err());
        assert!(ensure_under_root(&root, &root.join("a").join("..").join("b")).is_err());
    }
}
