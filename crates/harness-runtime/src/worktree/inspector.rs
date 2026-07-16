//! RepositoryInspector — read-only Git repository facts via stable
//! machine-readable output (`rev-parse`, `status --porcelain=v1 -z`,
//! `worktree list --porcelain`). Never mutates the repository and never
//! judges success from localized stderr text.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::git::GitRunner;

/// Facts about a located repository.
#[derive(Debug, Clone)]
pub struct RepositoryFacts {
    /// Canonical main-worktree root.
    pub repository_root: PathBuf,
    /// Canonical `.git` directory of the inspected worktree.
    pub git_dir: PathBuf,
    /// Canonical common git directory — the repository identity (shared by
    /// all linked worktrees).
    pub common_git_dir: PathBuf,
    pub head_commit: Option<String>,
    /// Current branch; `None` when HEAD is detached.
    pub current_branch: Option<String>,
    pub is_bare: bool,
    pub git_version: String,
    pub supports_worktrees: bool,
    /// Main worktree has uncommitted changes (`status --porcelain=v1 -z`).
    pub dirty: bool,
}

/// One entry from `git worktree list --porcelain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeListEntry {
    pub path: PathBuf,
    pub head: Option<String>,
    /// Short branch name (without `refs/heads/`); `None` when detached/bare.
    pub branch: Option<String>,
    pub bare: bool,
    pub detached: bool,
    pub locked: bool,
    pub prunable: bool,
}

pub struct RepositoryInspector {
    git: GitRunner,
}

impl RepositoryInspector {
    pub fn new(git: GitRunner) -> Self {
        Self { git }
    }

    pub fn git(&self) -> &GitRunner {
        &self.git
    }

    /// Locate the repository containing `path` and collect its facts.
    /// Fails with a structured error when `path` is not inside a git
    /// repository (judged by exit code, not stderr text).
    pub async fn locate_repository(&self, path: &Path) -> Result<RepositoryFacts, CoreError> {
        if !path.exists() {
            return Err(ws_err(format!("path does not exist: {}", path.display())));
        }

        let probe = self
            .git
            .run(path, &["rev-parse", "--is-inside-work-tree"])
            .await?;
        if !probe.success() {
            // Could still be a bare repository directory.
            let bare_probe = self
                .git
                .run(path, &["rev-parse", "--is-bare-repository"])
                .await?;
            if !(bare_probe.success() && bare_probe.stdout.trim() == "true") {
                return Err(ws_err(format!("not a git repository: {}", path.display())));
            }
        }

        let is_bare = self
            .git
            .run_ok(path, &["rev-parse", "--is-bare-repository"])
            .await?
            .trim()
            == "true";

        let git_dir_raw = self
            .git
            .run_ok(path, &["rev-parse", "--absolute-git-dir"])
            .await?;
        let common_raw = self
            .git
            .run_ok(path, &["rev-parse", "--git-common-dir"])
            .await?;
        let git_dir = canonicalize_str(path, &git_dir_raw)?;
        let common_git_dir = canonicalize_str(path, &common_raw)?;

        let repository_root = if is_bare {
            common_git_dir.clone()
        } else {
            let top = self
                .git
                .run_ok(path, &["rev-parse", "--show-toplevel"])
                .await?;
            canonicalize_str(path, &top)?
        };

        // HEAD may not resolve in an empty repository — that is not an error.
        let head = self
            .git
            .run(path, &["rev-parse", "--verify", "HEAD"])
            .await?;
        let head_commit = head.success().then(|| head.stdout.trim().to_string());

        let branch_out = self
            .git
            .run(path, &["rev-parse", "--abbrev-ref", "HEAD"])
            .await?;
        let current_branch = if branch_out.success() {
            let b = branch_out.stdout.trim().to_string();
            (b != "HEAD").then_some(b)
        } else {
            None
        };

        let git_version = self.git.run_ok(path, &["--version"]).await?;
        // Linked worktrees exist since git 2.5; `--porcelain` since 2.7.
        let supports_worktrees = parse_git_version(&git_version)
            .map(|(maj, min)| maj > 2 || (maj == 2 && min >= 7))
            .unwrap_or(false);

        let dirty = if is_bare {
            false
        } else {
            self.is_dirty(&repository_root).await?
        };

        Ok(RepositoryFacts {
            repository_root,
            git_dir,
            common_git_dir,
            head_commit,
            current_branch,
            is_bare,
            git_version,
            supports_worktrees,
            dirty,
        })
    }

    /// Uncommitted changes in the worktree at `path` (NUL-separated porcelain
    /// v1 — stable machine output, no locale dependence).
    pub async fn is_dirty(&self, path: &Path) -> Result<bool, CoreError> {
        let out = self
            .git
            .run_ok(path, &["status", "--porcelain=v1", "-z"])
            .await?;
        Ok(!out.trim_matches('\0').trim().is_empty())
    }

    /// Number of changed entries (for diagnostics).
    pub async fn dirty_entries(&self, path: &Path) -> Result<Vec<String>, CoreError> {
        let out = self
            .git
            .run_ok(path, &["status", "--porcelain=v1", "-z"])
            .await?;
        Ok(out
            .split('\0')
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .collect())
    }

    /// Resolve a commit-ish to a full commit OID. Fails when unresolvable.
    pub async fn resolve_commit(&self, repo: &Path, rev: &str) -> Result<String, CoreError> {
        let spec = format!("{rev}^{{commit}}");
        let out = self
            .git
            .run(repo, &["rev-parse", "--verify", "--quiet", &spec])
            .await?;
        if !out.success() {
            return Err(ws_err(format!("base commit not found: {rev}")));
        }
        Ok(out.stdout.trim().to_string())
    }

    /// Does a local branch with this name already exist?
    pub async fn branch_exists(&self, repo: &Path, branch: &str) -> Result<bool, CoreError> {
        let refname = format!("refs/heads/{branch}");
        let out = self
            .git
            .run(repo, &["rev-parse", "--verify", "--quiet", &refname])
            .await?;
        Ok(out.success())
    }

    /// Validate a branch name with git itself.
    pub async fn check_branch_name(&self, cwd: &Path, branch: &str) -> Result<bool, CoreError> {
        let out = self
            .git
            .run(cwd, &["check-ref-format", "--branch", branch])
            .await?;
        Ok(out.success())
    }

    /// Linked + main worktrees from `git worktree list --porcelain`.
    pub async fn list_worktrees(&self, repo: &Path) -> Result<Vec<WorktreeListEntry>, CoreError> {
        let out = self
            .git
            .run_ok(repo, &["worktree", "list", "--porcelain"])
            .await?;
        Ok(parse_worktree_porcelain(&out))
    }

    /// Does `path` belong to the repository identified by `common_git_dir`?
    pub async fn path_belongs_to_repository(
        &self,
        path: &Path,
        common_git_dir: &Path,
    ) -> Result<bool, CoreError> {
        if !path.exists() {
            return Ok(false);
        }
        let out = self
            .git
            .run(path, &["rev-parse", "--git-common-dir"])
            .await?;
        if !out.success() {
            return Ok(false);
        }
        let Ok(canonical) = canonicalize_str(path, out.stdout.trim()) else {
            return Ok(false);
        };
        Ok(canonical == common_git_dir)
    }
}

/// Parse `git worktree list --porcelain` output: blank-line separated blocks
/// of `worktree <path>` / `HEAD <oid>` / `branch <ref>` / flag lines.
pub fn parse_worktree_porcelain(text: &str) -> Vec<WorktreeListEntry> {
    let mut entries = Vec::new();
    let mut current: Option<WorktreeListEntry> = None;
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            if let Some(e) = current.take() {
                entries.push(e);
            }
            continue;
        }
        if let Some(path) = line.strip_prefix("worktree ") {
            if let Some(e) = current.take() {
                entries.push(e);
            }
            current = Some(WorktreeListEntry {
                path: PathBuf::from(path),
                head: None,
                branch: None,
                bare: false,
                detached: false,
                locked: false,
                prunable: false,
            });
            continue;
        }
        let Some(e) = current.as_mut() else { continue };
        if let Some(oid) = line.strip_prefix("HEAD ") {
            e.head = Some(oid.to_string());
        } else if let Some(refname) = line.strip_prefix("branch ") {
            e.branch = Some(
                refname
                    .strip_prefix("refs/heads/")
                    .unwrap_or(refname)
                    .to_string(),
            );
        } else if line == "bare" {
            e.bare = true;
        } else if line == "detached" {
            e.detached = true;
        } else if line == "locked" || line.starts_with("locked ") {
            e.locked = true;
        } else if line == "prunable" || line.starts_with("prunable ") {
            e.prunable = true;
        }
    }
    if let Some(e) = current.take() {
        entries.push(e);
    }
    entries
}

fn parse_git_version(s: &str) -> Option<(u32, u32)> {
    // "git version 2.45.1.windows.1"
    let rest = s.trim().strip_prefix("git version ")?;
    let mut parts = rest.split('.');
    let maj = parts.next()?.parse().ok()?;
    let min = parts.next()?.parse().ok()?;
    Some((maj, min))
}

fn canonicalize_str(base: &Path, raw: &str) -> Result<PathBuf, CoreError> {
    let p = PathBuf::from(raw.trim());
    let joined = if p.is_absolute() { p } else { base.join(p) };
    super::naming::canonicalize_for_git(&joined)
        .map_err(|e| ws_err(format!("canonicalize {}: {e}", joined.display())))
}

// ── WorktreeGitVerifier impl ────────────────────────────────────────

#[async_trait::async_trait]
impl super::git_verifier::WorktreeGitVerifier for RepositoryInspector {
    async fn verify_worktree_git(
        &self,
        worktree_path: &Path,
        expected_common_dir: &Path,
        expected_branch: &str,
    ) -> Result<super::git_verifier::GitVerificationResult, CoreError> {
        // Must resolve to a directory that exists.
        if !worktree_path.is_dir() {
            return Ok(super::git_verifier::GitVerificationResult {
                listed: false,
                common_dir_matches: false,
                branch_matches: false,
                head_readable: false,
                admin_intact: false,
                ambiguous: false,
            });
        }
        // Read common git dir from the worktree.
        let common_raw = self
            .git()
            .run(worktree_path, &["rev-parse", "--git-common-dir"])
            .await;
        let common_dir = common_raw.as_ref().ok().filter(|o| o.success()).map(|o| {
            let p = PathBuf::from(o.stdout.trim());
            if p.is_absolute() {
                p
            } else {
                worktree_path.join(p)
            }
        });
        let common_dir_matches = common_dir
            .as_ref()
            .and_then(|cd| cd.canonicalize().ok())
            .zip(expected_common_dir.canonicalize().ok())
            .map(|(a, b)| a == b)
            .unwrap_or(false);

        // Read HEAD.
        let head = self.git().run(worktree_path, &["rev-parse", "HEAD"]).await;
        let head_readable = head.as_ref().map(|o| o.success()).unwrap_or(false);

        // Read actual branch.
        let branch = self
            .git()
            .run(worktree_path, &["rev-parse", "--abbrev-ref", "HEAD"])
            .await;
        let actual_branch = branch
            .as_ref()
            .ok()
            .filter(|o| o.success())
            .map(|o| o.stdout.trim().to_string());
        let branch_matches = actual_branch.as_deref() == Some(expected_branch);

        // Check if listed in `git worktree list`.
        let root = expected_common_dir.parent().unwrap_or(expected_common_dir);
        let repo_root = if root.as_os_str().is_empty() {
            expected_common_dir
        } else {
            root
        };
        let listed = if repo_root.exists() {
            self.list_worktrees(repo_root)
                .await
                .map(|entries| {
                    entries.iter().any(|e| {
                        e.path.canonicalize().ok().as_deref()
                            == worktree_path.canonicalize().ok().as_deref()
                            || e.path == *worktree_path
                    })
                })
                .unwrap_or(false)
        } else {
            false
        };

        let admin_intact = common_dir.is_some() && head_readable && listed;

        Ok(super::git_verifier::GitVerificationResult {
            listed,
            common_dir_matches,
            branch_matches,
            head_readable,
            admin_intact,
            ambiguous: false,
        })
    }
}

fn ws_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn porcelain_parse_multi_entry() {
        let text = "worktree C:/repo\nHEAD 1111111111111111111111111111111111111111\nbranch refs/heads/main\n\nworktree C:/wt/a\nHEAD 2222222222222222222222222222222222222222\nbranch refs/heads/harness/t1\nlocked because\n\nworktree C:/wt/b\nHEAD 3333333333333333333333333333333333333333\ndetached\nprunable gitdir file points to non-existent location\n";
        let entries = parse_worktree_porcelain(text);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].branch.as_deref(), Some("main"));
        assert!(entries[1].locked);
        assert_eq!(entries[1].branch.as_deref(), Some("harness/t1"));
        assert!(entries[2].detached);
        assert!(entries[2].prunable);
        assert_eq!(entries[2].branch, None);
    }

    #[test]
    fn git_version_parse() {
        assert_eq!(
            parse_git_version("git version 2.45.1.windows.1"),
            Some((2, 45))
        );
        assert_eq!(parse_git_version("git version 2.7.0"), Some((2, 7)));
        assert_eq!(parse_git_version("nonsense"), None);
    }
}
