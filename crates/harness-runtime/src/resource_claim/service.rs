//! ResourceClaimService — production service with lease/fencing validation.
//!
//! Wraps [`ResourceClaimRepo`] and enforces:
//! - Lease is Active (via injected validator).
//! - Fencing token equals the current worktree epoch.
//! - Claim `expires_at` never exceeds the lease `expires_at`.
//! - Old/stale fencing tokens cannot acquire, renew, replace, or release.
//!
//! The service computes the bounded `expires_at` timestamp and passes it
//! to the repo. The repo does not compute its own TTL — all expiry
//! decisions are made here, bounded by the lease validator.
//!
//! `lease_token` is validated by the service but never passed to the repo
//! (only the `fencing_token` and `lease_id` go to the database).

use std::sync::Arc;

use harness_core::resource_claim::{ClaimDecision, ClaimGroupSpec};
use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::repo::{AcquireOutcome, ClaimGroupRecord, ClaimGuard, ResourceClaimRepo};
use crate::lease::clock::Clock;

/// Trait for lease validation that the claim service depends on.
#[async_trait::async_trait]
pub trait ResourceClaimLeaseValidator: Send + Sync {
    /// Validate that the lease is active and the caller holds the correct
    /// token and fencing token.
    async fn validate_lease(
        &self,
        lease_id: &str,
        lease_token: &str,
        fencing_token: i64,
    ) -> Result<(), CoreError>;

    /// Get the lease's current expiry, or None if not found.
    async fn get_lease_expires_at(&self, lease_id: &str) -> Result<Option<String>, CoreError>;
}

pub struct ResourceClaimService {
    repo: ResourceClaimRepo,
    validator: Box<dyn ResourceClaimLeaseValidator + Send + Sync>,
    clock: Arc<dyn Clock + Send + Sync>,
    default_claim_duration_secs: u32,
}

impl ResourceClaimService {
    pub fn new(
        repo: ResourceClaimRepo,
        validator: Box<dyn ResourceClaimLeaseValidator + Send + Sync>,
        clock: Arc<dyn Clock + Send + Sync>,
    ) -> Self {
        Self {
            repo,
            validator,
            clock,
            default_claim_duration_secs: 300, // 5 minutes
        }
    }

    /// Check conflicts (read-only, no validation needed).
    pub async fn check_conflicts(&self, spec: &ClaimGroupSpec) -> Result<ClaimDecision, CoreError> {
        self.repo.check_conflicts(spec).await
    }

    /// Acquire a claim group with lease/fencing validation.
    ///
    /// The claim's `expires_at` is bounded by the minimum of the requested
    /// duration and the remaining lease lifetime. The service computes the
    /// final `expires_at` timestamp and passes it to the repo.
    pub async fn acquire_group(
        &self,
        spec: &ClaimGroupSpec,
        guard: &ClaimGuard,
        idempotency_key: &str,
    ) -> Result<AcquireOutcome, CoreError> {
        // 1. Validate lease + fencing.
        self.validate_guard(guard).await?;

        // 2. Compute claim expiry bounded by lease expiry.
        let expires_at = self
            .compute_claim_expires_at(guard, self.default_claim_duration_secs)
            .await?;

        // 3. Delegate to repo with explicit bounded expires_at.
        self.repo
            .acquire_group(spec, guard, idempotency_key, &expires_at)
            .await
    }

    /// Get a claim group by ID.
    pub async fn get_group(&self, group_id: &str) -> Result<ClaimGroupRecord, CoreError> {
        self.repo.get_group(group_id).await
    }

    /// List active claim groups for a task.
    pub async fn list_active_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<ClaimGroupRecord>, CoreError> {
        self.repo.list_active_for_task(task_id).await
    }

    /// List active claim groups for an execution.
    pub async fn list_active_for_execution(
        &self,
        execution_id: &str,
    ) -> Result<Vec<ClaimGroupRecord>, CoreError> {
        self.repo.list_active_for_execution(execution_id).await
    }

    /// List active claim groups for a repository.
    pub async fn list_active_for_repository(
        &self,
        repository_identity: &str,
    ) -> Result<Vec<ClaimGroupRecord>, CoreError> {
        self.repo
            .list_active_for_repository(repository_identity)
            .await
    }

    /// Renew a claim group with lease/fencing validation.
    ///
    /// The new expiry is bounded by the lease expiry. The service computes
    /// the final `expires_at` and passes it to the repo.
    pub async fn renew_group(
        &self,
        group_id: &str,
        guard: &ClaimGuard,
        duration_secs: u32,
    ) -> Result<(), CoreError> {
        // 1. Validate lease + fencing.
        self.validate_guard(guard).await?;

        // 2. Compute claim expiry bounded by lease expiry.
        let expires_at = self.compute_claim_expires_at(guard, duration_secs).await?;

        // 3. Delegate to repo.
        self.repo.renew_group(group_id, guard, &expires_at).await
    }

    /// Release a claim group with lease/fencing validation.
    pub async fn release_group(
        &self,
        group_id: &str,
        guard: &ClaimGuard,
        reason: &str,
    ) -> Result<(), CoreError> {
        // 1. Validate lease + fencing.
        self.validate_guard(guard).await?;

        // 2. Delegate to repo.
        self.repo.release_group(group_id, guard, reason).await
    }

    /// Expire due claim groups.
    pub async fn expire_due_groups(&self) -> Result<Vec<String>, CoreError> {
        let now = self.clock.now_sql();
        self.repo.expire_due_groups(&now).await
    }

    /// Replace a claim group atomically with lease/fencing validation.
    ///
    /// The new claim's expiry is bounded by the lease expiry.
    pub async fn replace_group(
        &self,
        old_group_id: &str,
        new_spec: &ClaimGroupSpec,
        guard: &ClaimGuard,
        idempotency_key: &str,
    ) -> Result<AcquireOutcome, CoreError> {
        // 1. Validate lease + fencing.
        self.validate_guard(guard).await?;

        // 2. Compute claim expiry bounded by lease expiry.
        let expires_at = self
            .compute_claim_expires_at(guard, self.default_claim_duration_secs)
            .await?;

        // 3. Delegate to repo.
        self.repo
            .replace_group(old_group_id, new_spec, guard, idempotency_key, &expires_at)
            .await
    }

    // ── Internal helpers ─────────────────────────────────────────────

    async fn validate_guard(&self, guard: &ClaimGuard) -> Result<(), CoreError> {
        self.validator
            .validate_lease(&guard.lease_id, &guard.lease_token, guard.fencing_token)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::WorkspaceLeaseExpired,
                    format!(
                        "claim lease validation failed for lease {}: {}",
                        guard.lease_id, e.message
                    ),
                    ErrorSource::System,
                )
            })
    }

    /// Compute the claim's `expires_at` timestamp, bounded by the lease expiry.
    ///
    /// Returns a SQL datetime string representing the earlier of:
    /// - `now + requested_secs`
    /// - the lease's `expires_at`
    async fn compute_claim_expires_at(
        &self,
        guard: &ClaimGuard,
        requested_secs: u32,
    ) -> Result<String, CoreError> {
        let lease_expires = self
            .validator
            .get_lease_expires_at(&guard.lease_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::WorkspaceLeaseExpired,
                    format!("lease not found: {}", guard.lease_id),
                    ErrorSource::System,
                )
            })?;

        let lease_dt = parse_sql_dt(&lease_expires).ok_or_else(|| {
            CoreError::new(
                ErrorCode::PersistenceError,
                format!("cannot parse lease expiry: {lease_expires}"),
                ErrorSource::System,
            )
        })?;

        let now = self.clock.now();
        let remaining_secs = lease_dt.signed_duration_since(now).num_seconds().max(0) as u32;

        if remaining_secs == 0 {
            return Err(CoreError::new(
                ErrorCode::WorkspaceLeaseExpired,
                "lease already expired",
                ErrorSource::System,
            ));
        }

        let bounded_secs = requested_secs.min(remaining_secs);
        let expires_dt = now + chrono::Duration::seconds(bounded_secs as i64);
        Ok(expires_dt.format("%Y-%m-%d %H:%M:%S").to_string())
    }
}

fn parse_sql_dt(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().into())
}
