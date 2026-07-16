//! GitDiffScopeValidator — validate the REAL changed paths in a worktree
//! against a FileScopeValidator, using machine-readable git output.
//!
//! All git access goes through `GitRunner` (and therefore ProcessManager).
//! Success/failure is judged by exit code; no localized stderr is parsed.
//! Changed paths come from git itself, never from agent self-report.

use std::collections::HashSet;
use std::path::Path;

use harness_core::{CoreError, ErrorCode, ErrorSource};

use crate::worktree::GitRunner;

use super::file_scope::{FileScopeValidator, ScopeDecision, ScopeViolation};

/// Which diff areas to include in validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffIncludes {
    pub staged: bool,
    pub unstaged: bool,
    pub untracked: bool,
}

impl Default for DiffIncludes {
    fn default() -> Self {
        Self {
            staged: true,
            unstaged: true,
            untracked: true,
        }
    }
}

/// Where a change lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffArea {
    Staged,
    Unstaged,
    Untracked,
}

/// Kind of change reported by `git diff --name-status` / ls-files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed { from: String },
    Copied { from: String },
    TypeChange,
    Binary,
    Submodule,
    Untracked,
}

#[derive(Debug, Clone)]
pub struct ChangedPath {
    pub path: String,
    pub kind: ChangeKind,
    pub area: DiffArea,
    pub scope: ScopeDecision,
}

#[derive(Debug, Clone)]
pub struct RenameEvidence {
    pub from: String,
    pub to: String,
    pub from_scope: ScopeDecision,
    pub to_scope: ScopeDecision,
}

#[derive(Debug, Clone)]
pub struct UntrackedEvidence {
    pub path: String,
    pub scope: ScopeDecision,
}

/// Result of validating a worktree diff. Path summaries only — the full
/// diff content is never stored here (large diffs reference an artifact
/// spool path via the evidence record's `artifact_reference`).
#[derive(Debug, Clone)]
pub struct ScopeValidationReport {
    pub changed_paths: Vec<ChangedPath>,
    pub allowed_changes: usize,
    pub violations: Vec<(String, ScopeViolation)>,
    pub rename_evidence: Vec<RenameEvidence>,
    pub untracked_evidence: Vec<UntrackedEvidence>,
    pub binary_files: Vec<String>,
    pub submodule_files: Vec<String>,
    pub clean: bool,
}

pub struct GitDiffScopeValidator {
    git: GitRunner,
}

/// Mutable accumulators shared across staged/unstaged collection.
struct Accum {
    changed_paths: Vec<ChangedPath>,
    violations: Vec<(String, ScopeViolation)>,
    rename_evidence: Vec<RenameEvidence>,
    binary_files: Vec<String>,
    submodule_files: Vec<String>,
    allowed: usize,
}

impl Accum {
    fn new() -> Self {
        Self {
            changed_paths: Vec::new(),
            violations: Vec::new(),
            rename_evidence: Vec::new(),
            binary_files: Vec::new(),
            submodule_files: Vec::new(),
            allowed: 0,
        }
    }
}

impl GitDiffScopeValidator {
    pub fn new(git: GitRunner) -> Self {
        Self { git }
    }
    pub fn git(&self) -> &GitRunner {
        &self.git
    }

    /// Validate every changed path in `worktree` against `validator`.
    pub async fn validate(
        &self,
        worktree: &Path,
        validator: &FileScopeValidator,
        includes: DiffIncludes,
    ) -> Result<ScopeValidationReport, CoreError> {
        let submodules = self.submodule_paths(worktree).await?;
        let staged_binary = if includes.staged {
            self.binary_paths(worktree, true).await?
        } else {
            HashSet::new()
        };
        let unstaged_binary = if includes.unstaged {
            self.binary_paths(worktree, false).await?
        } else {
            HashSet::new()
        };

        let mut accum = Accum::new();
        let mut untracked_evidence = Vec::new();

        if includes.staged {
            self.collect_name_status(
                worktree,
                validator,
                true,
                &submodules,
                &staged_binary,
                &mut accum,
            )
            .await?;
        }
        if includes.unstaged {
            self.collect_name_status(
                worktree,
                validator,
                false,
                &submodules,
                &unstaged_binary,
                &mut accum,
            )
            .await?;
        }
        if includes.untracked {
            let out = self
                .git
                .run_ok(
                    worktree,
                    &["ls-files", "--others", "--exclude-standard", "-z"],
                )
                .await?;
            for path in out.split('\0') {
                if path.is_empty() {
                    continue;
                }
                let (scope, viol) = self.validate_path(validator, path);
                if let Some(v) = viol {
                    accum.violations.push((path.to_string(), v));
                } else if matches!(scope, ScopeDecision::Allowed) {
                    accum.allowed += 1;
                }
                untracked_evidence.push(UntrackedEvidence {
                    path: path.to_string(),
                    scope: scope.clone(),
                });
                accum.changed_paths.push(ChangedPath {
                    path: path.to_string(),
                    kind: ChangeKind::Untracked,
                    area: DiffArea::Untracked,
                    scope,
                });
            }
        }

        let clean = accum.violations.is_empty();
        Ok(ScopeValidationReport {
            changed_paths: accum.changed_paths,
            allowed_changes: accum.allowed,
            violations: accum.violations,
            rename_evidence: accum.rename_evidence,
            untracked_evidence,
            binary_files: accum.binary_files,
            submodule_files: accum.submodule_files,
            clean,
        })
    }

    /// Collect staged or unstaged changes from `git diff --name-status -z`.
    async fn collect_name_status(
        &self,
        worktree: &Path,
        validator: &FileScopeValidator,
        staged: bool,
        submodules: &HashSet<String>,
        binary: &HashSet<String>,
        accum: &mut Accum,
    ) -> Result<(), CoreError> {
        let mut args = vec!["diff", "--name-status", "-z"];
        if staged {
            args.push("--cached");
        }
        let out = self.git.run_ok(worktree, &args).await?;
        let toks: Vec<&str> = out.split('\0').collect();
        let area = if staged {
            DiffArea::Staged
        } else {
            DiffArea::Unstaged
        };
        let mut i = 0;
        while i < toks.len() {
            let status = toks[i];
            if status.is_empty() {
                i += 1;
                continue;
            }
            let c = status.chars().next().unwrap();
            match c {
                'R' | 'C' => {
                    let from = toks.get(i + 1).copied().unwrap_or("").to_string();
                    let to = toks.get(i + 2).copied().unwrap_or("").to_string();
                    i += 3;
                    let (from_scope, from_viol) = self.validate_path(validator, &from);
                    let (to_scope, to_viol) = self.validate_path(validator, &to);
                    if let Some(v) = from_viol {
                        accum.violations.push((from.clone(), v));
                    } else if matches!(from_scope, ScopeDecision::Allowed) {
                        accum.allowed += 1;
                    }
                    if let Some(v) = to_viol {
                        accum.violations.push((to.clone(), v));
                    } else if matches!(to_scope, ScopeDecision::Allowed) {
                        accum.allowed += 1;
                    }
                    let kind = if c == 'R' {
                        ChangeKind::Renamed { from: from.clone() }
                    } else {
                        ChangeKind::Copied { from: from.clone() }
                    };
                    accum.rename_evidence.push(RenameEvidence {
                        from: from.clone(),
                        to: to.clone(),
                        from_scope: from_scope.clone(),
                        to_scope: to_scope.clone(),
                    });
                    if binary.contains(&to) {
                        accum.binary_files.push(to.clone());
                    }
                    accum.changed_paths.push(ChangedPath {
                        path: to,
                        kind,
                        area: area.clone(),
                        scope: to_scope,
                    });
                }
                _ => {
                    let path = toks.get(i + 1).copied().unwrap_or("").to_string();
                    i += 2;
                    let (scope, viol) = self.validate_path(validator, &path);
                    if let Some(v) = viol {
                        accum.violations.push((path.clone(), v));
                    } else if matches!(scope, ScopeDecision::Allowed) {
                        accum.allowed += 1;
                    }
                    let kind = if submodules.contains(&path) {
                        accum.submodule_files.push(path.clone());
                        ChangeKind::Submodule
                    } else if binary.contains(&path) {
                        accum.binary_files.push(path.clone());
                        ChangeKind::Binary
                    } else {
                        match c {
                            'A' => ChangeKind::Added,
                            'M' => ChangeKind::Modified,
                            'D' => ChangeKind::Deleted,
                            'T' => ChangeKind::TypeChange,
                            _ => ChangeKind::Modified,
                        }
                    };
                    accum.changed_paths.push(ChangedPath {
                        path,
                        kind,
                        area: area.clone(),
                        scope,
                    });
                }
            }
        }
        Ok(())
    }

    /// Validate a single path; returns (decision, Some(violation) if denied).
    fn validate_path(
        &self,
        validator: &FileScopeValidator,
        path: &str,
    ) -> (ScopeDecision, Option<ScopeViolation>) {
        match validator.validate(path) {
            Ok((ScopeDecision::Allowed, _)) => (ScopeDecision::Allowed, None),
            Ok((ScopeDecision::Denied(v), _)) => (ScopeDecision::Denied(v.clone()), Some(v)),
            Err(_) => (
                ScopeDecision::Denied(ScopeViolation::AmbiguousPath(path.to_string())),
                Some(ScopeViolation::AmbiguousPath(path.to_string())),
            ),
        }
    }

    /// Paths that are submodules (gitlink mode 160000) via `git ls-files -s -z`.
    async fn submodule_paths(&self, worktree: &Path) -> Result<HashSet<String>, CoreError> {
        let out = self.git.run_ok(worktree, &["ls-files", "-s", "-z"]).await?;
        let mut set = HashSet::new();
        for tok in out.split('\0') {
            if tok.is_empty() {
                continue;
            }
            // Format: "<mode> <sha> <stage>\t<path>"
            let mode = tok.split_whitespace().next().unwrap_or("");
            if mode == "160000" {
                if let Some(path) = tok.split('\t').nth(1) {
                    set.insert(path.to_string());
                }
            }
        }
        Ok(set)
    }

    /// Paths whose diff is binary, via `git diff --numstat -z`. A `-` in the
    /// added column means git could not produce a textual diff (binary).
    async fn binary_paths(
        &self,
        worktree: &Path,
        staged: bool,
    ) -> Result<HashSet<String>, CoreError> {
        let mut args = vec!["diff", "--numstat", "-z"];
        if staged {
            args.push("--cached");
        }
        let out = self.git.run_ok(worktree, &args).await?;
        let toks: Vec<&str> = out.split('\0').collect();
        let mut set = HashSet::new();
        let mut i = 0;
        while i < toks.len() {
            let t = toks[i];
            if t.is_empty() {
                i += 1;
                continue;
            }
            // `<added>\t<deleted>\t<path>` ; for renames path is `src` and the
            // next token is `dst`.
            let parts: Vec<&str> = t.splitn(3, '\t').collect();
            if parts.len() == 3 {
                let (added, _deleted, path) = (parts[0], parts[1], parts[2]);
                if added == "-" {
                    set.insert(path.to_string());
                    if i + 1 < toks.len() && !is_numstat_record(toks[i + 1]) {
                        set.insert(toks[i + 1].to_string());
                        i += 1;
                    }
                }
            }
            i += 1;
        }
        Ok(set)
    }
}

fn is_numstat_record(t: &str) -> bool {
    let first = t.split('\t').next().unwrap_or("");
    first == "-" || first.chars().all(|c| c.is_ascii_digit())
}

#[allow(dead_code)]
fn diff_err(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
