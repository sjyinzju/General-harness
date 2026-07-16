//! Core resource claim types — serializable, comparable, hashable.

use serde::{Deserialize, Serialize};

/// The kind of resource a claim targets.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// A single exact file by repository-relative path.
    ExactFile,
    /// A directory and all its descendants by repository-relative path.
    DirectoryPrefix,
    /// The entire repository — conflicts with any path resource in the same repo.
    RepositoryWide,
    /// A named logical resource (e.g. "database-schema", "ci-pipeline").
    Logical,
}

/// Access mode for a resource claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// Shared read — compatible with other readers, conflicts with writers.
    Read,
    /// Exclusive write — conflicts with both readers and writers.
    Write,
}

impl AccessMode {
    /// The strongest mode wins when collapsing duplicates within a claim group.
    pub fn stronger(self, other: AccessMode) -> AccessMode {
        match (self, other) {
            (AccessMode::Write, _) | (_, AccessMode::Write) => AccessMode::Write,
            _ => AccessMode::Read,
        }
    }

    /// Returns `true` when the two modes are compatible under the conflict matrix.
    pub fn is_compatible_with(self, other: AccessMode) -> bool {
        matches!((self, other), (AccessMode::Read, AccessMode::Read))
    }
}

/// Canonical identity of a resource that the overlap engine compares.
///
/// Two claims conflict when their [`ResourceIdentity`] values overlap AND
/// their [`AccessMode`] combination is incompatible.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceIdentity {
    /// A repository-relative normalized path with a repository identity scope.
    Path {
        repository_identity: String,
        kind: ResourceKind,
        normalized_path: String,
    },
    /// A logical resource key — conflicts only with the exact same key.
    Logical {
        key: LogicalResourceKey,
    },
}

impl ResourceIdentity {
    /// Build a path-scoped identity.
    pub fn path(
        repository_identity: &str,
        kind: ResourceKind,
        normalized_path: &str,
    ) -> Self {
        ResourceIdentity::Path {
            repository_identity: repository_identity.to_string(),
            kind,
            normalized_path: normalized_path.to_string(),
        }
    }

    /// Build a logical identity.
    pub fn logical(key: LogicalResourceKey) -> Self {
        ResourceIdentity::Logical { key }
    }

    /// The repository identity for path resources; `None` for logical.
    pub fn repository_identity(&self) -> Option<&str> {
        match self {
            ResourceIdentity::Path {
                repository_identity, ..
            } => Some(repository_identity.as_str()),
            ResourceIdentity::Logical { .. } => None,
        }
    }
}

/// A normalized logical resource key.
///
/// Logical keys are compared exactly — different keys never conflict.
/// Valid keys are non-empty, trimmed, lowercase strings matching `[a-z0-9_-]+`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogicalResourceKey(String);

impl LogicalResourceKey {
    /// Create a new logical resource key after validation and normalization.
    pub fn new(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim().to_lowercase();
        if trimmed.is_empty() {
            return Err("logical resource key must not be empty".into());
        }
        if trimmed.len() > 128 {
            return Err("logical resource key must not exceed 128 characters".into());
        }
        if !trimmed
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(format!(
                "logical resource key '{}' contains invalid characters (allowed: a-z, 0-9, -, _)",
                trimmed
            ));
        }
        Ok(LogicalResourceKey(trimmed))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LogicalResourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The lifecycle of a claim group (and its constituent claim rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimLifecycle {
    /// The claim group has been acquired and is active.
    Active,
    /// The claim group was explicitly released.
    Released,
    /// The claim group expired (TTL elapsed or lease expired).
    Expired,
}

impl ClaimLifecycle {
    pub fn is_terminal(&self) -> bool {
        matches!(self, ClaimLifecycle::Released | ClaimLifecycle::Expired)
    }
}

/// Why two claims conflict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictReason {
    /// Same resource with incompatible access modes.
    AccessModeConflict {
        requested: AccessMode,
        existing: AccessMode,
    },
    /// Path resources overlap in the filesystem.
    PathOverlap {
        requested_path: String,
        existing_path: String,
    },
    /// Logical resource keys are identical.
    LogicalKeyCollision {
        key: String,
    },
    /// A RepositoryWide claim covers the requested path resource.
    RepositoryWideCoversPath {
        repository_identity: String,
        requested_path: String,
    },
}

/// A single identified conflict between a requested and existing claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimConflict {
    /// The resource identity that was requested.
    pub requested_identity: ResourceIdentity,
    /// The access mode that was requested.
    pub requested_mode: AccessMode,
    /// The resource identity of the conflicting existing claim.
    pub conflicting_identity: ResourceIdentity,
    /// The access mode of the conflicting existing claim.
    pub conflicting_mode: AccessMode,
    /// Why the claims conflict.
    pub reason: ConflictReason,
    /// The task that owns the conflicting claim.
    pub conflicting_task_id: String,
    /// The execution that owns the conflicting claim (if known).
    pub conflicting_execution_id: Option<String>,
    /// The group that owns the conflicting claim.
    pub conflicting_group_id: String,
}

/// Outcome of a conflict check or acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimDecision {
    /// All claims compatible — can acquire.
    Compatible,
    /// One or more claims conflict.
    Conflict {
        conflicts: Vec<ClaimConflict>,
    },
    /// The claim group spec contains internal inconsistencies.
    InvalidSpec {
        reason: String,
    },
}
