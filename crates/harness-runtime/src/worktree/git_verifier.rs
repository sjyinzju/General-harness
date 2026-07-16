//! WorktreeGitVerifier — trait for cross-checking worktree identity
//! against live `git worktree list --porcelain` output. Used by the
//! lease module during acquire to confirm the worktree is still
//! registered as a git worktree.

use std::path::Path;

use harness_core::{CoreError, ErrorCode, ErrorSource};

/// Result of a git-level worktree identity check.
#[derive(Debug, Clone)]
pub struct GitVerificationResult {
    /// The worktree's canonical path appears in `git worktree list`.
    pub listed: bool,
    /// Common git directory reported by git matches the record.
    pub common_dir_matches: bool,
    /// Actual branch matches the recorded branch.
    pub branch_matches: bool,
    /// HEAD is readable.
    pub head_readable: bool,
    /// Git administrative metadata is intact (the path resolves as a
    /// worktree of the expected repository).
    pub admin_intact: bool,
    /// The path is registered as belonging to multiple repositories
    /// (ambiguous — safety rejects).
    pub ambiguous: bool,
}

/// Cross-check a worktree path against live git state.
#[async_trait::async_trait]
pub trait WorktreeGitVerifier: Send + Sync {
    /// Verify that `worktree_path` is still a registered linked worktree
    /// of the repository identified by `expected_common_dir`, with
    /// `expected_branch`.
    async fn verify_worktree_git(
        &self,
        worktree_path: &Path,
        expected_common_dir: &Path,
        expected_branch: &str,
    ) -> Result<GitVerificationResult, CoreError>;
}

/// No-op verifier for tests / environments without git available.
pub struct NoOpGitVerifier;

#[async_trait::async_trait]
impl WorktreeGitVerifier for NoOpGitVerifier {
    async fn verify_worktree_git(
        &self,
        _worktree_path: &Path,
        _expected_common_dir: &Path,
        _expected_branch: &str,
    ) -> Result<GitVerificationResult, CoreError> {
        Ok(GitVerificationResult {
            listed: true,
            common_dir_matches: true,
            branch_matches: true,
            head_readable: true,
            admin_intact: true,
            ambiguous: false,
        })
    }
}

fn _we(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
