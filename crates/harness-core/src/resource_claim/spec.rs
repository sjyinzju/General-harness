//! Claim specifications — what a task requests, and what gets recorded.
//!
//! [`ResourceClaimSpec`] is the input: a single resource + access mode.
//! [`ClaimGroupSpec`] bundles multiple specs for atomic acquisition.
//! [`ResourceClaimRecord`] is the persisted outcome.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::normalize::NormalizedResourcePath;
use super::types::{
    AccessMode, ClaimLifecycle, LogicalResourceKey, ResourceIdentity, ResourceKind,
};

/// A single resource claim specification — what one task/execution wants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceClaimSpec {
    /// The kind of resource being claimed.
    pub kind: ResourceKind,
    /// The access mode requested.
    pub mode: AccessMode,
    /// For path resources: the repository-relative path.
    pub resource_path: Option<String>,
    /// For path resources: the canonical repository identity.
    pub repository_identity: Option<String>,
    /// For logical resources: the logical key.
    pub logical_key: Option<String>,
}

impl ResourceClaimSpec {
    /// Create an exact file claim.
    pub fn exact_file(repo: &str, path: &str, mode: AccessMode) -> Self {
        ResourceClaimSpec {
            kind: ResourceKind::ExactFile,
            mode,
            resource_path: Some(path.to_string()),
            repository_identity: Some(repo.to_string()),
            logical_key: None,
        }
    }

    /// Create a directory prefix claim.
    pub fn directory_prefix(repo: &str, path: &str, mode: AccessMode) -> Self {
        ResourceClaimSpec {
            kind: ResourceKind::DirectoryPrefix,
            mode,
            resource_path: Some(path.to_string()),
            repository_identity: Some(repo.to_string()),
            logical_key: None,
        }
    }

    /// Create a repository-wide claim.
    pub fn repository_wide(repo: &str, mode: AccessMode) -> Self {
        ResourceClaimSpec {
            kind: ResourceKind::RepositoryWide,
            mode,
            resource_path: None,
            repository_identity: Some(repo.to_string()),
            logical_key: None,
        }
    }

    /// Create a logical resource claim.
    pub fn logical(key: &str, mode: AccessMode) -> Self {
        ResourceClaimSpec {
            kind: ResourceKind::Logical,
            mode,
            resource_path: None,
            repository_identity: None,
            logical_key: Some(key.to_string()),
        }
    }

    /// Convert to a canonical [`ResourceIdentity`] after normalization.
    pub fn to_identity(&self) -> Result<ResourceIdentity, String> {
        match self.kind {
            ResourceKind::Logical => {
                let key = self
                    .logical_key
                    .as_deref()
                    .ok_or("logical claim missing logical_key")?;
                Ok(ResourceIdentity::Logical {
                    key: LogicalResourceKey::new(key)?,
                })
            }
            _ => {
                let repo = self
                    .repository_identity
                    .as_deref()
                    .ok_or("path claim missing repository_identity")?;
                // RepositoryWide has no specific path — empty string is valid.
                let raw_path = self.resource_path.as_deref().unwrap_or("");
                let normalized =
                    if matches!(self.kind, ResourceKind::RepositoryWide) && raw_path.is_empty() {
                        String::new()
                    } else {
                        NormalizedResourcePath::new(raw_path)?.to_string()
                    };
                Ok(ResourceIdentity::Path {
                    repository_identity: repo.to_string(),
                    kind: self.kind.clone(),
                    normalized_path: normalized,
                })
            }
        }
    }
}

/// A set of resource claims to acquire atomically.
///
/// All claims within a group must be compatible with all existing active claims.
/// If any conflict is found, none of the claims are acquired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimGroupSpec {
    /// Individual claim specifications.
    pub claims: Vec<ResourceClaimSpec>,
    /// The project that owns this group.
    pub project_id: String,
    /// The task that owns this group.
    pub task_id: String,
    /// The execution that owns this group.
    pub execution_id: String,
    /// The repository identity (for path resources).
    pub repository_identity: String,
    /// The worktree this group is scoped to.
    pub worktree_id: Option<String>,
    /// The workspace lease authorizing this group.
    pub lease_id: Option<String>,
}

impl ClaimGroupSpec {
    /// Compute the group identity after normalization.
    ///
    /// This normalizes all claims (dedup, upgrade Read→Write, sort) and
    /// produces a stable [`ClaimGroupIdentity`] with a deterministic request hash.
    pub fn normalize(&self) -> Result<ClaimGroupIdentity, String> {
        if self.claims.is_empty() {
            return Err("claim group must have at least one claim".into());
        }

        let mut normalized: Vec<(ResourceIdentity, AccessMode)> = Vec::new();

        for spec in &self.claims {
            let identity = spec.to_identity()?;

            // Check for duplicate resource identities within the group.
            if let Some(pos) = normalized.iter().position(|(id, _)| *id == identity) {
                // Same identity — upgrade to the stronger mode.
                let (_, existing_mode) = &normalized[pos];
                let upgraded = existing_mode.stronger(spec.mode);
                normalized[pos] = (identity, upgraded);
            } else {
                // Also check if one identity subsumes another (e.g., DirectoryPrefix
                // already covers an ExactFile inside it).
                let mut subsumed = false;
                let mut i = 0;
                while i < normalized.len() {
                    let (existing_id, existing_mode) = &normalized[i];
                    if identity_overlap(Some(&identity), Some(existing_id)) == OverlapResult::Subset
                    {
                        // The new claim is already covered by an existing wider claim.
                        // Upgrade the existing claim's mode if needed.
                        let upgraded = existing_mode.stronger(spec.mode);
                        normalized[i] = (existing_id.clone(), upgraded);
                        subsumed = true;
                        break;
                    } else if identity_overlap(Some(existing_id), Some(&identity))
                        == OverlapResult::Subset
                    {
                        // The existing claim is covered by the new wider claim.
                        // Remove the existing and let the new one take its place.
                        let _upgraded = spec.mode.stronger(*existing_mode);
                        normalized.remove(i);
                        // Continue checking remaining against the new wider claim.
                        continue; // don't increment i since we removed
                    }
                    i += 1;
                }
                if !subsumed {
                    normalized.push((identity, spec.mode));
                }
            }
        }

        // Stable sort by identity.
        normalized.sort_by_key(|(a, _)| identity_sort_key(a));

        // Compute the request hash.
        let request_hash = compute_request_hash(&normalized);

        Ok(ClaimGroupIdentity {
            claims: normalized,
            request_hash,
            project_id: self.project_id.clone(),
            task_id: self.task_id.clone(),
            execution_id: self.execution_id.clone(),
            repository_identity: self.repository_identity.clone(),
            worktree_id: self.worktree_id.clone(),
            lease_id: self.lease_id.clone(),
        })
    }
}

/// The normalized, stable identity of a claim group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimGroupIdentity {
    /// Normalized (identity, mode) pairs — deduplicated, sorted.
    pub claims: Vec<(ResourceIdentity, AccessMode)>,
    /// Deterministic hash of all claims.
    pub request_hash: String,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub repository_identity: String,
    pub worktree_id: Option<String>,
    pub lease_id: Option<String>,
}

/// A persisted resource claim record within a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceClaimRecord {
    pub claim_id: String,
    pub group_id: String,
    pub kind: ResourceKind,
    pub normalized_resource: String,
    pub access_mode: AccessMode,
    pub lifecycle: ClaimLifecycle,
}

// ── Internal helpers ──────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
enum OverlapResult {
    Same,
    Subset,
    Superset,
    Overlap,
    Disjoint,
}

/// Determine the overlap relationship between two resource identities.
///
/// Returns `None` when the identities are in different scopes (different repos,
/// or one path and one logical).
fn identity_overlap(a: Option<&ResourceIdentity>, b: Option<&ResourceIdentity>) -> OverlapResult {
    let a = match a {
        Some(id) => id,
        None => return OverlapResult::Disjoint,
    };
    let b = match b {
        Some(id) => id,
        None => return OverlapResult::Disjoint,
    };

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
            // Different repos → disjoint.
            if repo_a != repo_b {
                return OverlapResult::Disjoint;
            }

            // RepositoryWide covers everything in the same repo.
            let a_is_repo_wide = matches!(kind_a, ResourceKind::RepositoryWide);
            let b_is_repo_wide = matches!(kind_b, ResourceKind::RepositoryWide);

            if a_is_repo_wide && b_is_repo_wide {
                return OverlapResult::Same;
            }
            if a_is_repo_wide {
                return OverlapResult::Superset;
            }
            if b_is_repo_wide {
                return OverlapResult::Subset;
            }

            // Same path → overlap.
            if path_a == path_b {
                return match (kind_a, kind_b) {
                    (ResourceKind::ExactFile, ResourceKind::ExactFile) => OverlapResult::Same,
                    (ResourceKind::DirectoryPrefix, ResourceKind::DirectoryPrefix) => {
                        OverlapResult::Same
                    }
                    (ResourceKind::DirectoryPrefix, ResourceKind::ExactFile) => {
                        OverlapResult::Superset
                    }
                    (ResourceKind::ExactFile, ResourceKind::DirectoryPrefix) => {
                        OverlapResult::Subset
                    }
                    _ => OverlapResult::Overlap,
                };
            }

            // Component-prefix check: does directory A contain file/dir B?
            let path_a_norm = NormalizedResourcePath::new(path_a)
                .unwrap_or_else(|_| NormalizedResourcePath::new("__invalid__").unwrap());
            let path_b_norm = NormalizedResourcePath::new(path_b)
                .unwrap_or_else(|_| NormalizedResourcePath::new("__invalid__").unwrap());

            let a_is_dir = matches!(kind_a, ResourceKind::DirectoryPrefix);
            let b_is_dir = matches!(kind_b, ResourceKind::DirectoryPrefix);

            if a_is_dir && path_a_norm.is_component_prefix_of(&path_b_norm) {
                return OverlapResult::Superset;
            }
            if b_is_dir && path_b_norm.is_component_prefix_of(&path_a_norm) {
                return OverlapResult::Subset;
            }

            OverlapResult::Disjoint
        }
        (ResourceIdentity::Logical { key: key_a }, ResourceIdentity::Logical { key: key_b }) => {
            if key_a == key_b {
                OverlapResult::Same
            } else {
                OverlapResult::Disjoint
            }
        }
        // Path vs Logical → disjoint (different domains).
        _ => OverlapResult::Disjoint,
    }
}

/// Sort key for stable ordering of resource identities.
fn identity_sort_key(id: &ResourceIdentity) -> String {
    match id {
        ResourceIdentity::Path {
            repository_identity,
            kind,
            normalized_path,
        } => {
            let kind_str = match kind {
                ResourceKind::RepositoryWide => "0",
                ResourceKind::DirectoryPrefix => "1",
                ResourceKind::ExactFile => "2",
                ResourceKind::Logical => "3",
            };
            format!("path:{repository_identity}:{kind_str}:{normalized_path}")
        }
        ResourceIdentity::Logical { key } => {
            format!("logical:{key}")
        }
    }
}

/// Compute a deterministic SHA-256 hash of the normalized claim set.
fn compute_request_hash(claims: &[(ResourceIdentity, AccessMode)]) -> String {
    let mut hasher = Sha256::new();
    for (identity, mode) in claims {
        hasher.update(identity_sort_key(identity).as_bytes());
        hasher.update(b"|");
        let mode_str = match mode {
            AccessMode::Read => "r",
            AccessMode::Write => "w",
        };
        hasher.update(mode_str.as_bytes());
        hasher.update(b"\n");
    }
    let result = hasher.finalize();
    hex::encode(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_file_spec() {
        let spec = ResourceClaimSpec::exact_file("repo/a", "src/main.rs", AccessMode::Write);
        let id = spec.to_identity().unwrap();
        assert_eq!(
            id,
            ResourceIdentity::Path {
                repository_identity: "repo/a".into(),
                kind: ResourceKind::ExactFile,
                normalized_path: "src/main.rs".into(),
            }
        );
    }

    #[test]
    fn test_directory_prefix_spec() {
        let spec = ResourceClaimSpec::directory_prefix("repo/a", "src/auth/", AccessMode::Read);
        let id = spec.to_identity().unwrap();
        assert_eq!(
            id,
            ResourceIdentity::Path {
                repository_identity: "repo/a".into(),
                kind: ResourceKind::DirectoryPrefix,
                normalized_path: "src/auth".into(),
            }
        );
    }

    #[test]
    fn test_logical_spec() {
        let spec = ResourceClaimSpec::logical("database-schema", AccessMode::Write);
        let id = spec.to_identity().unwrap();
        assert_eq!(
            id,
            ResourceIdentity::Logical {
                key: LogicalResourceKey::new("database-schema").unwrap(),
            }
        );
    }

    #[test]
    fn test_repository_wide_spec() {
        let spec = ResourceClaimSpec::repository_wide("repo/a", AccessMode::Read);
        let id = spec.to_identity().unwrap();
        assert_eq!(
            id,
            ResourceIdentity::Path {
                repository_identity: "repo/a".into(),
                kind: ResourceKind::RepositoryWide,
                normalized_path: "".into(),
            }
        );
    }

    #[test]
    fn test_empty_group_rejected() {
        let spec = ClaimGroupSpec {
            claims: vec![],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        assert!(spec.normalize().is_err());
    }

    #[test]
    fn test_duplicate_normalization() {
        let spec = ClaimGroupSpec {
            claims: vec![
                ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Read),
                ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Read),
            ],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let id = spec.normalize().unwrap();
        // Only one claim after dedup.
        assert_eq!(id.claims.len(), 1);
        assert_eq!(id.claims[0].1, AccessMode::Read);
    }

    #[test]
    fn test_read_upgraded_to_write() {
        let spec = ClaimGroupSpec {
            claims: vec![
                ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Read),
                ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Write),
            ],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let id = spec.normalize().unwrap();
        assert_eq!(id.claims.len(), 1);
        assert_eq!(id.claims[0].1, AccessMode::Write);
    }

    #[test]
    fn test_directory_subsumes_exact() {
        let spec = ClaimGroupSpec {
            claims: vec![
                ResourceClaimSpec::directory_prefix("repo", "src/auth", AccessMode::Read),
                ResourceClaimSpec::exact_file("repo", "src/auth/login.rs", AccessMode::Write),
            ],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let id = spec.normalize().unwrap();
        // Directory subsumes the exact file => 1 claim with Write mode.
        assert_eq!(id.claims.len(), 1);
        let (rid, mode) = &id.claims[0];
        assert_eq!(*mode, AccessMode::Write);
        match rid {
            ResourceIdentity::Path { kind, .. } => {
                assert_eq!(*kind, ResourceKind::DirectoryPrefix);
            }
            _ => panic!("expected path identity"),
        }
    }

    #[test]
    fn test_stable_ordering() {
        let spec = ClaimGroupSpec {
            claims: vec![
                ResourceClaimSpec::exact_file("repo", "src/z.rs", AccessMode::Read),
                ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Read),
                ResourceClaimSpec::logical("ci-pipeline", AccessMode::Read),
            ],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let id1 = spec.normalize().unwrap();
        // Rebuild with reversed order.
        let spec2 = ClaimGroupSpec {
            claims: vec![
                ResourceClaimSpec::logical("ci-pipeline", AccessMode::Read),
                ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Read),
                ResourceClaimSpec::exact_file("repo", "src/z.rs", AccessMode::Read),
            ],
            ..spec
        };
        let id2 = spec2.normalize().unwrap();
        assert_eq!(id1.claims, id2.claims);
        assert_eq!(id1.request_hash, id2.request_hash);
    }

    #[test]
    fn test_stable_request_hash() {
        let spec = ClaimGroupSpec {
            claims: vec![
                ResourceClaimSpec::exact_file("repo", "src/main.rs", AccessMode::Write),
                ResourceClaimSpec::logical("database-schema", AccessMode::Read),
            ],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: None,
            lease_id: None,
        };
        let id1 = spec.normalize().unwrap();
        let id2 = spec.normalize().unwrap();
        assert!(!id1.request_hash.is_empty());
        assert_eq!(id1.request_hash, id2.request_hash);
    }

    #[test]
    fn test_invalid_resource_path_rejected() {
        let spec = ResourceClaimSpec::exact_file("repo", "../outside", AccessMode::Read);
        assert!(spec.to_identity().is_err());
    }

    #[test]
    fn test_claim_group_record_roundtrip() {
        let spec = ClaimGroupSpec {
            claims: vec![ResourceClaimSpec::exact_file(
                "repo",
                "src/a.rs",
                AccessMode::Write,
            )],
            project_id: "p1".into(),
            task_id: "t1".into(),
            execution_id: "e1".into(),
            repository_identity: "repo".into(),
            worktree_id: Some("wt-1".into()),
            lease_id: Some("lease-1".into()),
        };
        let id = spec.normalize().unwrap();
        assert_eq!(id.project_id, "p1");
        assert_eq!(id.task_id, "t1");
        assert_eq!(id.worktree_id, Some("wt-1".into()));
        assert_eq!(id.lease_id, Some("lease-1".into()));
    }
}
