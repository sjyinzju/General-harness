/// Path validation utility.
pub fn is_path_within_scope(path: &str, worktree_root: &str, allowed_globs: &[String]) -> bool {
    let normalized = normalize_path(path);
    let root = normalize_path(worktree_root);

    // Must start with worktree root
    if !normalized.starts_with(&root) {
        return false;
    }

    // Must not access .git or .harness
    if normalized.contains("/.git/") || normalized.ends_with("/.git") {
        return false;
    }
    if normalized.contains("/.harness/") {
        return false;
    }

    // Must match at least one allowed glob (simple prefix/suffix match)
    let relative = normalized.strip_prefix(&root).unwrap_or(&normalized);
    let relative = relative.trim_start_matches('/');

    if allowed_globs.is_empty() {
        return true; // no restriction = allow all within worktree
    }

    allowed_globs.iter().any(|g| simple_glob_match(relative, g))
}

fn normalize_path(p: &str) -> String {
    p.replace('\\', "/").trim_end_matches('/').to_string()
}

fn simple_glob_match(path: &str, glob: &str) -> bool {
    if glob == "**" || glob == "**/*" {
        return true;
    }
    if let Some(prefix) = glob.strip_suffix("**") {
        return path.starts_with(prefix);
    }
    if let Some(prefix) = glob.strip_suffix('*') {
        return path.starts_with(prefix);
    }
    path.starts_with(glob) || path == glob
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_scope() {
        assert!(is_path_within_scope(
            "/worktree/src/auth/callback.ts",
            "/worktree",
            &["src/**".into()]
        ));
    }

    #[test]
    fn test_path_escape() {
        assert!(!is_path_within_scope(
            "/worktree/../outside",
            "/worktree",
            &["src/**".into()]
        ));
    }

    #[test]
    fn test_git_dir_blocked() {
        assert!(!is_path_within_scope(
            "/worktree/.git/config",
            "/worktree",
            &["**".into()]
        ));
    }
}
