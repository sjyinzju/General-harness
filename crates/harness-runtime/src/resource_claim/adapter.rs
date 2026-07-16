//! TaskEnvelope / FileScope → ResourceClaimSpec adapter.
//!
//! Converts a frozen Gate C [`TaskEnvelope`] into conservative
//! [`ResourceClaimSpec`] values. This is a pure, deterministic mapping:
//!
//! - exact write scope → ExactFile Write
//! - directory write scope → DirectoryPrefix Write
//! - exact read scope → ExactFile Read
//! - directory read scope → DirectoryPrefix Read
//! - glob that can extract a stable static directory prefix → DirectoryPrefix
//! - otherwise-unsafe write glob → RepositoryWide Write (or RequiresExplicitClaim)
//! - denied scope → no Claim
//! - logical resource → only from explicit task metadata or explicit ClaimSpec
//!
//! This adapter does NOT modify the frozen `TaskEnvelope` contract.

use harness_core::contracts::task_envelope::TaskEnvelope;
use harness_core::resource_claim::{AccessMode, ClaimGroupSpec, ResourceClaimSpec};

/// Result of deriving claims from a [`TaskEnvelope`].
#[derive(Debug, Clone)]
pub enum DeriveClaimsOutcome {
    /// Successfully derived a claim group spec.
    Claims(ClaimGroupSpec),
    /// The scope is too broad to safely derive claims; explicit
    /// [`ResourceClaimSpec`] is required.
    RequiresExplicitClaim {
        reason: String,
    },
}

/// Derive conservative [`ClaimGroupSpec`] from a [`TaskEnvelope`].
pub fn derive_claims_from_envelope(
    envelope: &TaskEnvelope,
    repository_identity: &str,
) -> DeriveClaimsOutcome {
    let mut claims: Vec<ResourceClaimSpec> = Vec::new();

    // Process allowed_paths (write scope).
    for path in &envelope.scope.allowed_paths {
        match classify_path(path) {
            PathClass::ExactFile => {
                claims.push(ResourceClaimSpec::exact_file(
                    repository_identity,
                    path,
                    AccessMode::Write,
                ));
            }
            PathClass::DirectoryPrefix => {
                claims.push(ResourceClaimSpec::directory_prefix(
                    repository_identity,
                    path,
                    AccessMode::Write,
                ));
            }
            PathClass::GlobWithPrefix(prefix) => {
                claims.push(ResourceClaimSpec::directory_prefix(
                    repository_identity,
                    &prefix,
                    AccessMode::Write,
                ));
            }
            PathClass::GlobAmbiguous => {
                // Cannot safely narrow — the entire repo is at risk.
                return DeriveClaimsOutcome::RequiresExplicitClaim {
                    reason: format!(
                        "write scope path '{}' is too broad to derive a safe claim",
                        path
                    ),
                };
            }
        }
    }

    // Process readable_paths (read scope).
    for path in &envelope.scope.readable_paths {
        match classify_path(path) {
            PathClass::ExactFile => {
                claims.push(ResourceClaimSpec::exact_file(
                    repository_identity,
                    path,
                    AccessMode::Read,
                ));
            }
            PathClass::DirectoryPrefix => {
                claims.push(ResourceClaimSpec::directory_prefix(
                    repository_identity,
                    path,
                    AccessMode::Read,
                ));
            }
            PathClass::GlobWithPrefix(prefix) => {
                claims.push(ResourceClaimSpec::directory_prefix(
                    repository_identity,
                    &prefix,
                    AccessMode::Read,
                ));
            }
            PathClass::GlobAmbiguous => {
                // Ambiguous read glob — skip (read-only is safe).
                continue;
            }
        }
    }

    // Process explicit resource_claims from the envelope.
    for rc in &envelope.resource_claims {
        if let Some(claim) = convert_explicit_claim(rc, repository_identity) {
            claims.push(claim);
        }
    }

    // If claims are empty after processing, return RequiresExplicitClaim.
    // An empty claim group would be rejected anyway.
    if claims.is_empty() {
        return DeriveClaimsOutcome::RequiresExplicitClaim {
            reason: "no claims could be derived from the envelope".into(),
        };
    }

    let spec = ClaimGroupSpec {
        claims,
        project_id: envelope.project_id.clone(),
        task_id: envelope.task_id.clone(),
        execution_id: String::new(), // filled in by the caller
        repository_identity: repository_identity.to_string(),
        worktree_id: None, // filled in by the caller
        lease_id: None,    // filled in by the caller
    };

    DeriveClaimsOutcome::Claims(spec)
}

// ── Path classification ──────────────────────────────────────────────

enum PathClass {
    /// Looks like a single file: "src/auth/callback.rs"
    ExactFile,
    /// Looks like a directory: "src/auth/" or "src/auth"
    DirectoryPrefix,
    /// Glob with a stable static prefix: "src/**" → prefix "src"
    GlobWithPrefix(String),
    /// Glob without a safe static prefix: "**/*.rs", "src/**/*.test.ts"
    GlobAmbiguous,
}

fn classify_path(path: &str) -> PathClass {
    let trimmed = path.trim_end_matches('/');

    // Glob patterns.
    if path.contains('*') {
        return classify_glob(path);
    }

    // Heuristic: if it ends with a file-like extension, treat as exact file.
    // Otherwise treat as directory prefix.
    if let Some(last) = trimmed.rsplit('/').next() {
        if last.contains('.') && !last.starts_with('.') {
            return PathClass::ExactFile;
        }
    }

    PathClass::DirectoryPrefix
}

fn classify_glob(path: &str) -> PathClass {
    // Find the first `*` and take the prefix before it.
    let star_pos = path.find('*').unwrap_or(path.len());

    if star_pos == 0 {
        // Glob at the start: "**/*.rs" or "*.rs" — ambiguous.
        return PathClass::GlobAmbiguous;
    }

    let prefix = &path[..star_pos];
    // The prefix must end at a directory boundary.
    let prefix = prefix.trim_end_matches('/');

    if prefix.is_empty() {
        return PathClass::GlobAmbiguous;
    }

    // If the glob is simple "dir/**" → directory prefix.
    // If it's "dir/**/*.ext" → still ambiguous (extension filter, not structural).
    let after_star = &path[star_pos..];
    if after_star == "*" || after_star == "**" || after_star == "**/" {
        // Simple recursive glob — safe to treat as directory prefix.
        return PathClass::GlobWithPrefix(prefix.to_string());
    }

    // More complex glob like "dir/**/*.rs" — ambiguous because the extension
    // filter means not ALL files in the directory are captured.
    // But for conservative claim purposes, we can still claim the directory
    // as a DirectoryPrefix since the task might write anywhere underneath.
    PathClass::GlobAmbiguous
}

// ── Explicit claim conversion ────────────────────────────────────────

fn convert_explicit_claim(
    rc: &harness_core::contracts::task_envelope::ResourceClaim,
    repository_identity: &str,
) -> Option<ResourceClaimSpec> {
    let mode = match rc.access_mode.as_str() {
        "write" => AccessMode::Write,
        "read" => AccessMode::Read,
        _ => return None,
    };

    match rc.resource_type.as_str() {
        "file" | "exact_file" => {
            let path = rc.resource_path.as_deref()?;
            Some(ResourceClaimSpec::exact_file(
                repository_identity,
                path,
                mode,
            ))
        }
        "directory" | "directory_prefix" => {
            let path = rc.resource_path.as_deref()?;
            Some(ResourceClaimSpec::directory_prefix(
                repository_identity,
                path,
                mode,
            ))
        }
        "repo" | "repository_wide" => {
            Some(ResourceClaimSpec::repository_wide(repository_identity, mode))
        }
        "logical" => {
            let key = rc.resource_name.as_deref()?;
            Some(ResourceClaimSpec::logical(key, mode))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};

    fn envelope(allowed: &[&str], readable: &[&str]) -> TaskEnvelope {
        TaskEnvelope {
            task_id: "t1".into(),
            project_id: "p1".into(),
            task_goal: "test".into(),
            scope: FileScope {
                allowed_paths: allowed.iter().map(|s| s.to_string()).collect(),
                forbidden_paths: vec![],
                readable_paths: readable.iter().map(|s| s.to_string()).collect(),
                scope_expansion_allowed: false,
            },
            resource_claims: vec![],
            dependencies: vec![],
            acceptance_checks: vec![],
            allowed_tools: vec![],
            output_schema: String::new(),
            budget: TaskBudget {
                max_turns: 10,
                max_time_ms: 60000,
                max_cost_cents: None,
            },
            goal_contract_version: 1,
            plan_version: 1,
        }
    }

    #[test]
    fn test_exact_write_scope_derives_exact_write() {
        let env = envelope(&["src/auth/callback.rs"], &[]);
        let result = derive_claims_from_envelope(&env, "repo");
        match result {
            DeriveClaimsOutcome::Claims(spec) => {
                assert_eq!(spec.claims.len(), 1);
                let c = &spec.claims[0];
                assert_eq!(c.mode, AccessMode::Write);
                assert_eq!(c.resource_path.as_deref(), Some("src/auth/callback.rs"));
            }
            _ => panic!("expected Claims"),
        }
    }

    #[test]
    fn test_directory_write_scope_derives_directory_write() {
        let env = envelope(&["src/auth/"], &[]);
        let result = derive_claims_from_envelope(&env, "repo");
        match result {
            DeriveClaimsOutcome::Claims(spec) => {
                assert_eq!(spec.claims.len(), 1);
                let c = &spec.claims[0];
                assert_eq!(c.mode, AccessMode::Write);
            }
            _ => panic!("expected Claims"),
        }
    }

    #[test]
    fn test_glob_with_prefix_derives_directory() {
        let env = envelope(&["src/**"], &[]);
        let result = derive_claims_from_envelope(&env, "repo");
        match result {
            DeriveClaimsOutcome::Claims(spec) => {
                assert_eq!(spec.claims.len(), 1);
            }
            _ => panic!("expected Claims"),
        }
    }

    #[test]
    fn test_ambiguous_glob_requires_explicit() {
        let env = envelope(&["**/*.rs"], &[]);
        let result = derive_claims_from_envelope(&env, "repo");
        assert!(matches!(
            result,
            DeriveClaimsOutcome::RequiresExplicitClaim { .. }
        ));
    }

    #[test]
    fn test_read_scope_derives_read_claim() {
        let env = envelope(&[], &["src/shared/"]);
        let result = derive_claims_from_envelope(&env, "repo");
        match result {
            DeriveClaimsOutcome::Claims(spec) => {
                let read_claim = spec
                    .claims
                    .iter()
                    .find(|c| c.mode == AccessMode::Read);
                assert!(read_claim.is_some());
            }
            _ => panic!("expected Claims"),
        }
    }

    #[test]
    fn test_explicit_logical_claim_converted() {
        let mut env = envelope(&["src/a.rs"], &[]);
        env.resource_claims.push(
            harness_core::contracts::task_envelope::ResourceClaim {
                resource_type: "logical".into(),
                resource_path: None,
                resource_name: Some("database-schema".into()),
                access_mode: "write".into(),
            },
        );
        let result = derive_claims_from_envelope(&env, "repo");
        match result {
            DeriveClaimsOutcome::Claims(spec) => {
                // Should have both the file claim and the logical claim.
                assert!(spec.claims.len() >= 2);
                let log_claim = spec
                    .claims
                    .iter()
                    .find(|c| c.logical_key.as_deref() == Some("database-schema"));
                assert!(log_claim.is_some());
            }
            _ => panic!("expected Claims"),
        }
    }

    #[test]
    fn test_empty_scope_requires_explicit() {
        let env = envelope(&[], &[]);
        let result = derive_claims_from_envelope(&env, "repo");
        assert!(matches!(
            result,
            DeriveClaimsOutcome::RequiresExplicitClaim { .. }
        ));
    }
}
