//! Resource Overlap Engine — pure, deterministic conflict detection.
//!
//! The engine checks whether a set of requested [`ResourceClaimSpec`] values
//! conflicts with a set of existing active claims. It uses component-path
//! semantics so `src/a/` does NOT match `src/ab/`.

use super::normalize::NormalizedResourcePath;
use super::spec::ClaimGroupSpec;
use super::types::{
    AccessMode, ClaimConflict, ClaimDecision, ConflictReason, ResourceIdentity, ResourceKind,
};

/// A lightweight view of an existing active claim for conflict checking.
#[derive(Debug, Clone)]
pub struct ExistingClaim {
    pub identity: ResourceIdentity,
    pub mode: AccessMode,
    pub group_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
}

/// The resource overlap engine.
///
/// All methods are pure functions — no I/O, no state, no async.
pub struct ResourceOverlapEngine;

impl ResourceOverlapEngine {
    /// Check whether a [`ClaimGroupSpec`] conflicts with any existing claims.
    ///
    /// Returns [`ClaimDecision::Compatible`] if all claims can be acquired,
    /// or [`ClaimDecision::Conflict`] with structured conflict details.
    pub fn check_conflicts(
        spec: &ClaimGroupSpec,
        existing_claims: &[ExistingClaim],
    ) -> ClaimDecision {
        // 1. Normalize the spec.
        let normalized = match spec.normalize() {
            Ok(n) => n,
            Err(reason) => return ClaimDecision::InvalidSpec { reason },
        };

        // 2. Check each requested claim against all existing claims.
        let mut conflicts: Vec<ClaimConflict> = Vec::new();

        for (req_identity, req_mode) in &normalized.claims {
            for existing in existing_claims {
                if !identity_overlaps(req_identity, &existing.identity) {
                    continue; // Disjoint — no conflict.
                }

                // Overlap — check access mode compatibility.
                if !req_mode.is_compatible_with(existing.mode) {
                    conflicts.push(ClaimConflict {
                        requested_identity: req_identity.clone(),
                        requested_mode: *req_mode,
                        conflicting_identity: existing.identity.clone(),
                        conflicting_mode: existing.mode,
                        reason: Self::classify_conflict_reason(
                            req_identity,
                            *req_mode,
                            &existing.identity,
                            existing.mode,
                        ),
                        conflicting_task_id: existing.task_id.clone(),
                        conflicting_execution_id: existing.execution_id.clone(),
                        conflicting_group_id: existing.group_id.clone(),
                    });
                }
            }
        }

        if conflicts.is_empty() {
            ClaimDecision::Compatible
        } else {
            ClaimDecision::Conflict { conflicts }
        }
    }

    /// Check whether two individual identities overlap in resource scope.
    pub fn identities_overlap(a: &ResourceIdentity, b: &ResourceIdentity) -> bool {
        identity_overlaps(a, b)
    }

    /// Check whether two access modes are compatible.
    pub fn modes_compatible(a: AccessMode, b: AccessMode) -> bool {
        a.is_compatible_with(b)
    }

    /// Determine if two existing claim sets conflict with each other.
    /// Used by the reconciler to detect invariant violations.
    pub fn detect_conflicting_active(
        claims: &[ExistingClaim],
    ) -> Vec<(ExistingClaim, ExistingClaim, ConflictReason)> {
        let mut conflicts = Vec::new();
        for i in 0..claims.len() {
            for j in (i + 1)..claims.len() {
                let a = &claims[i];
                let b = &claims[j];
                if identity_overlaps(&a.identity, &b.identity) && !a.mode.is_compatible_with(b.mode)
                {
                    conflicts.push((
                        a.clone(),
                        b.clone(),
                        Self::classify_conflict_reason(&a.identity, a.mode, &b.identity, b.mode),
                    ));
                }
            }
        }
        conflicts
    }

    fn classify_conflict_reason(
        requested: &ResourceIdentity,
        req_mode: AccessMode,
        existing: &ResourceIdentity,
        ex_mode: AccessMode,
    ) -> ConflictReason {
        if req_mode != ex_mode || !req_mode.is_compatible_with(ex_mode) {
            // Incompatible modes — figure out the structural reason.
            match (requested, existing) {
                (ResourceIdentity::Logical { key: a }, ResourceIdentity::Logical { key: b })
                    if a == b =>
                {
                    ConflictReason::LogicalKeyCollision { key: a.to_string() }
                }
                (
                    ResourceIdentity::Path {
                        repository_identity: _repo_a,
                        kind: ResourceKind::RepositoryWide,
                        ..
                    },
                    ResourceIdentity::Path {
                        repository_identity: _repo_b,
                        normalized_path: path_b,
                        ..
                    },
                ) => ConflictReason::RepositoryWideCoversPath {
                    repository_identity: _repo_a.clone(),
                    requested_path: path_b.clone(),
                },
                (
                    ResourceIdentity::Path {
                        repository_identity: _repo_a,
                        normalized_path: path_a,
                        ..
                    },
                    ResourceIdentity::Path {
                        repository_identity: _repo_b,
                        kind: ResourceKind::RepositoryWide,
                        ..
                    },
                ) => ConflictReason::RepositoryWideCoversPath {
                    repository_identity: _repo_b.clone(),
                    requested_path: path_a.clone(),
                },
                _ => ConflictReason::AccessModeConflict {
                    requested: req_mode,
                    existing: ex_mode,
                },
            }
        } else {
            ConflictReason::AccessModeConflict {
                requested: req_mode,
                existing: ex_mode,
            }
        }
    }
}

// ── Overlap detection ──────────────────────────────────────────────────

/// Returns `true` when two resource identities overlap in scope.
fn identity_overlaps(a: &ResourceIdentity, b: &ResourceIdentity) -> bool {
    match (a, b) {
        (
            ResourceIdentity::Path {
                repository_identity: repo_a,
                kind: kind_a,
                normalized_path: path_a,
            },
            ResourceIdentity::Path {
                repository_identity: repo_b,
                kind: kind_b,
                normalized_path: path_b,
            },
        ) => {
            // Different repositories → never overlap.
            if repo_a != repo_b {
                return false;
            }

            // RepositoryWide covers anything in the same repo.
            if matches!(kind_a, ResourceKind::RepositoryWide)
                || matches!(kind_b, ResourceKind::RepositoryWide)
            {
                return true;
            }

            // Same normalized path → overlap.
            if path_a == path_b {
                return true;
            }

            // Component-prefix logic for directories.
            // Parse paths (they should always parse since they were normalized).
            let path_a_norm = NormalizedResourcePath::new(path_a);
            let path_b_norm = NormalizedResourcePath::new(path_b);

            match (path_a_norm, path_b_norm) {
                (Ok(pa), Ok(pb)) => {
                    let a_is_dir = matches!(kind_a, ResourceKind::DirectoryPrefix);
                    let b_is_dir = matches!(kind_b, ResourceKind::DirectoryPrefix);

                    if a_is_dir && pa.is_component_prefix_of(&pb) {
                        return true; // a/ contains b/...
                    }
                    if b_is_dir && pb.is_component_prefix_of(&pa) {
                        return true; // b/ contains a/...
                    }
                    false
                }
                _ => false, // shouldn't happen for validated paths
            }
        }
        (ResourceIdentity::Logical { key: key_a }, ResourceIdentity::Logical { key: key_b }) => {
            key_a == key_b
        }
        // Path vs Logical → never overlap (different domains).
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_claim::NormalizedResourcePath;
    use crate::resource_claim::{LogicalResourceKey, ResourceClaimSpec};

    // ── Helpers ────────────────────────────────────────────────────────

    fn ex_file(path: &str, mode: AccessMode) -> ExistingClaim {
        ExistingClaim {
            identity: ResourceIdentity::Path {
                repository_identity: "repo".into(),
                kind: ResourceKind::ExactFile,
                normalized_path: NormalizedResourcePath::new(path).unwrap().to_string(),
            },
            mode,
            group_id: format!("g-{path}"),
            task_id: format!("t-{path}"),
            execution_id: Some(format!("e-{path}")),
        }
    }

    fn ex_dir(path: &str, mode: AccessMode) -> ExistingClaim {
        ExistingClaim {
            identity: ResourceIdentity::Path {
                repository_identity: "repo".into(),
                kind: ResourceKind::DirectoryPrefix,
                normalized_path: NormalizedResourcePath::new(path).unwrap().to_string(),
            },
            mode,
            group_id: format!("g-dir-{path}"),
            task_id: format!("t-dir-{path}"),
            execution_id: Some(format!("e-dir-{path}")),
        }
    }

    fn ex_logical(key: &str, mode: AccessMode) -> ExistingClaim {
        ExistingClaim {
            identity: ResourceIdentity::Logical {
                key: LogicalResourceKey::new(key).unwrap(),
            },
            mode,
            group_id: format!("g-log-{key}"),
            task_id: format!("t-log-{key}"),
            execution_id: Some(format!("e-log-{key}")),
        }
    }

    fn ex_repo_wide(mode: AccessMode) -> ExistingClaim {
        ExistingClaim {
            identity: ResourceIdentity::Path {
                repository_identity: "repo".into(),
                kind: ResourceKind::RepositoryWide,
                normalized_path: String::new(),
            },
            mode,
            group_id: "g-repo".into(),
            task_id: "t-repo".into(),
            execution_id: Some("e-repo".into()),
        }
    }

    fn spec_exact(path: &str, mode: AccessMode) -> ClaimGroupSpec {
        ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::exact_file("repo", path, mode)],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        }
    }

    fn spec_dir(path: &str, mode: AccessMode) -> ClaimGroupSpec {
        ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::directory_prefix("repo", path, mode)],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[test]
    fn test_exact_same_overlap() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Write)];
        let spec = spec_exact("src/a.rs", AccessMode::Write);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_exact_different_no_overlap() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Write)];
        let spec = spec_exact("src/b.rs", AccessMode::Write);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_exact_inside_directory_overlap() {
        let existing = vec![ex_dir("src/auth", AccessMode::Write)];
        let spec = spec_exact("src/auth/login.rs", AccessMode::Read);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_directory_ancestor_descendant() {
        let existing = vec![ex_dir("src/auth", AccessMode::Write)];
        let spec = spec_dir("src/auth/sub", AccessMode::Read);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_component_prefix_confusion_no_overlap() {
        // `src/a` should NOT conflict with `src/ab` (component boundary).
        let existing = vec![ex_dir("src/a", AccessMode::Write)];
        let spec = spec_exact("src/ab", AccessMode::Read);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_repo_wide_overlap() {
        let existing = vec![ex_repo_wide(AccessMode::Write)];
        let spec = spec_exact("src/any/file.rs", AccessMode::Read);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_different_repositories_no_overlap() {
        let existing = vec![ExistingClaim {
            identity: ResourceIdentity::Path {
                repository_identity: "repo-a".into(),
                kind: ResourceKind::ExactFile,
                normalized_path: "src/a.rs".into(),
            },
            mode: AccessMode::Write,
            group_id: "g1".into(),
            task_id: "t1".into(),
            execution_id: None,
        }];
        let spec = spec_exact("src/a.rs", AccessMode::Write);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_logical_same_key_conflict() {
        let existing = vec![ex_logical("database-schema", AccessMode::Write)];
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::logical(
                "database-schema",
                AccessMode::Read,
            )],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_logical_different_key_no_conflict() {
        let existing = vec![ex_logical("database-schema", AccessMode::Write)];
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::logical("ci-pipeline", AccessMode::Read)],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_read_read_compatible() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Read)];
        let spec = spec_exact("src/a.rs", AccessMode::Read);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_read_write_conflict() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Read)];
        let spec = spec_exact("src/a.rs", AccessMode::Write);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_write_write_conflict() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Write)];
        let spec = spec_exact("src/a.rs", AccessMode::Write);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_logical_vs_path_no_conflict() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Write)];
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::logical(
                "database-schema",
                AccessMode::Write,
            )],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_windows_case_behavior() {
        // Case normalization is applied in NormalizedResourcePath.
        // Claims with different cases should normalize to the same path.
        let existing = vec![ExistingClaim {
            identity: ResourceIdentity::Path {
                repository_identity: "repo".into(),
                kind: ResourceKind::ExactFile,
                normalized_path: NormalizedResourcePath::new("src/auth.rs")
                    .unwrap()
                    .to_string(),
            },
            mode: AccessMode::Write,
            group_id: "g1".into(),
            task_id: "t1".into(),
            execution_id: None,
        }];
        // Spec uses a different case; normalization lowercases it.
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::exact_file(
                "repo",
                "Src/Auth.RS",
                AccessMode::Read,
            )],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_unicode_normalization_consistency() {
        // NFD vs NFC — should normalize to same path and conflict.
        // Use a simple NFD/NFC pair: 'é' = U+00E9 (NFC) = 'e' + U+0301 (NFD)
        let existing = vec![ExistingClaim {
            identity: ResourceIdentity::Path {
                repository_identity: "repo".into(),
                kind: ResourceKind::ExactFile,
                // NFC form
                normalized_path: NormalizedResourcePath::new("src/caf\u{00e9}.rs")
                    .unwrap()
                    .to_string(),
            },
            mode: AccessMode::Write,
            group_id: "g1".into(),
            task_id: "t1".into(),
            execution_id: None,
        }];
        // NFD form — should normalize to same NFC form "src/café.rs"
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::exact_file(
                "repo",
                "src/cafe\u{0301}.rs", // NFD: 'e' + combining acute accent
                AccessMode::Write,
            )],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(
            matches!(result, ClaimDecision::Conflict { .. }),
            "expected Conflict, got {result:?}"
        );
    }

    #[test]
    fn test_invalid_spec_rejected() {
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::exact_file(
                "repo",
                "../outside",
                AccessMode::Read,
            )],
            project_id: "p1".into(),
            task_id: "t-new".into(),
            execution_id: "e-new".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let result = ResourceOverlapEngine::check_conflicts(&spec, &[]);
        assert!(matches!(result, ClaimDecision::InvalidSpec { .. }));
    }

    #[test]
    fn test_multiple_readers_compatible() {
        let existing = vec![
            ex_file("src/a.rs", AccessMode::Read),
            ex_file("src/b.rs", AccessMode::Read),
        ];
        let spec = spec_exact("src/a.rs", AccessMode::Read);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert_eq!(result, ClaimDecision::Compatible);
    }

    #[test]
    fn test_mixed_readers_and_writer() {
        let existing = vec![ex_file("src/a.rs", AccessMode::Read)];
        let spec = spec_exact("src/a.rs", AccessMode::Write);
        let result = ResourceOverlapEngine::check_conflicts(&spec, &existing);
        assert!(matches!(result, ClaimDecision::Conflict { .. }));
    }

    #[test]
    fn test_detect_conflicting_active_invariant() {
        let claims = vec![
            ex_file("src/a.rs", AccessMode::Write),
            ex_file("src/a.rs", AccessMode::Read),
        ];
        let conflicts = ResourceOverlapEngine::detect_conflicting_active(&claims);
        assert_eq!(conflicts.len(), 1);
    }

    #[test]
    fn test_detect_no_false_positive_for_compatible() {
        let claims = vec![
            ex_file("src/a.rs", AccessMode::Read),
            ex_file("src/a.rs", AccessMode::Read),
            ex_file("src/b.rs", AccessMode::Write),
        ];
        let conflicts = ResourceOverlapEngine::detect_conflicting_active(&claims);
        assert_eq!(conflicts.len(), 0);
    }
}
