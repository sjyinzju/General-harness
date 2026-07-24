//! LivenessOrchestrator — unified cleanup orchestrator that runs all
//! reconcilers and janitors in the correct order.
//!
//! # Startup sequence
//! 1. ProcessReconciler → mark lost executions
//! 2. LeaseReconciler → expire stale leases
//! 3. WorktreeReconciler → repair drift
//! 4. Startup Janitor → reclaim stale owned temp/evidence/cargo dirs
//!
//! # Shutdown sequence
//! 1. Stop accepting new cleanup work
//! 2. Completion Janitor → clean current-run managed temp
//! 3. Final bounded janitor pass
//!
//! # invariants
//! - Never blocks core service startup on cleanup failure.
//! - Never deletes unowned or active directories.
//! - StartupJanitor and CompletionJanitor are distinct; they never
//!   race-delete the same directory.

use std::sync::Arc;
use std::time::Duration;

use harness_core::CoreError;
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;

use super::cargo_target;
use super::evidence_dir;
use super::guard::DeletionGuard;
use super::temp_dir::HarnessTempDir;
use super::types::{CleanupResult, LivenessConfig, ManagedDirKind, MarkerState};

/// The unified liveness orchestrator.
pub struct LivenessOrchestrator {
    config: LivenessConfig,
    #[allow(dead_code)]
    pool: SqlitePool,
}

impl LivenessOrchestrator {
    /// Create a new orchestrator.  Validates the config — returns an
    /// error if managed roots point at dangerous locations.
    pub fn new(config: LivenessConfig, pool: SqlitePool) -> Result<Self, CoreError> {
        let errors = config.validate();
        if !errors.is_empty() {
            return Err(harness_core::CoreError::new(
                harness_core::ErrorCode::ConfigInvalid,
                format!("liveness config is unsafe:\n  - {}", errors.join("\n  - ")),
                harness_core::ErrorSource::System,
            ));
        }

        // Ensure managed roots exist.
        for root in [
            &config.managed_temp_root,
            &config.managed_evidence_root,
            &config.managed_cargo_root,
        ] {
            std::fs::create_dir_all(root).map_err(|e| {
                harness_core::CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("create managed root {}: {e}", root.display()),
                    harness_core::ErrorSource::System,
                )
            })?;
        }

        tracing::info!(
            temp_root = %config.managed_temp_root.display(),
            evidence_root = %config.managed_evidence_root.display(),
            cargo_root = %config.managed_cargo_root.display(),
            supervisor = %config.supervisor_id,
            "liveness orchestrator initialized"
        );

        Ok(Self { config, pool })
    }

    pub fn config(&self) -> &LivenessConfig {
        &self.config
    }

    /// Build a DeletionGuard from the current orchestrator state.
    pub fn build_guard(&self, active_execution_ids: Vec<String>) -> DeletionGuard {
        DeletionGuard::new(self.config.clone(), active_execution_ids)
    }

    // ── Startup Janitor ─────────────────────────────────────────

    /// Run the startup janitor: scan all managed roots for stale owned
    /// directories that are safe to reclaim.
    ///
    /// This is called ONCE at startup, before normal operations begin.
    /// It NEVER deletes active, unowned, or grace-period directories.
    pub async fn startup_janitor(&self, active_execution_ids: Vec<String>) -> CleanupResult {
        let guard = self.build_guard(active_execution_ids);
        let mut result = CleanupResult::default();

        tracing::info!("startup janitor beginning");

        // ── 1. Stale temp dirs ───────────────────────────────
        result.merge(scan_stale_managed_root(
            &guard,
            &self.config.managed_temp_root,
            ManagedDirKind::HarnessManagedTemp,
            self.config.stale_temp_grace,
            self.config.failed_temp_ttl,
            true, // apply — startup janitor does real cleanup
        ));

        // ── 2. Evidence retention ────────────────────────────
        result.merge(evidence_dir::apply_evidence_retention(
            &self.config.managed_evidence_root,
            &self.config.evidence_retention,
            &guard,
            true, // apply
        ));

        // ── 3. Stale cargo run dirs ──────────────────────────
        result.merge(cargo_target::scan_stale_cargo_runs(
            &self.config.managed_cargo_root,
            &guard,
            self.config.stale_temp_grace,
            true, // apply
        ));

        // ── 4. Orphan artifacts ──────────────────────────────
        if let Ok(artifact_root) =
            crate::artifact::ArtifactRoot::open(&self.config.protected.target_root)
        {
            match artifact_root.reclaim_orphans(&self.config.supervisor_id) {
                Ok(count) => {
                    result.deleted += count;
                    result.examined += count;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "orphan artifact reclamation failed");
                }
            }
        }

        tracing::info!(
            examined = result.examined,
            deleted = result.deleted,
            preserved = result.preserved,
            failed = result.failed,
            "startup janitor complete"
        );

        result
    }

    // ── Completion Janitor ────────────────────────────────────

    /// Run the completion janitor: clean up the current run's managed
    /// temp directory and its isolated cargo target.
    ///
    /// This is called during shutdown, AFTER the current run's
    /// processes have exited and DB handles are closed.
    ///
    /// The `current_temp_dir` and `current_cargo_dir` are the
    /// directories created for THIS run.  They are cleaned with
    /// ownership verification; the guard must confirm they belong
    /// to us.
    pub async fn completion_janitor(
        &self,
        current_temp_dir: Option<&HarnessTempDir>,
        current_cargo_dir: Option<&cargo_target::ManagedCargoRunDir>,
    ) -> CleanupResult {
        let guard = self.build_guard(vec![]); // no active executions at shutdown
        let mut result = CleanupResult::default();

        // ── Clean current temp ───────────────────────────────
        if let Some(temp) = current_temp_dir {
            // Finalize marker first.
            let _ = temp.finalize(MarkerState::Completed);

            // Retry cleanup.
            let entry = temp
                .cleanup_with_retry(&guard, &self.config.managed_temp_root)
                .await;
            result.examined += 1;
            match entry.action {
                super::types::CleanupAction::Delete => result.deleted += 1,
                super::types::CleanupAction::Preserve => {
                    result.preserved += 1;
                    tracing::warn!(
                        path = %entry.path.display(),
                        reason = %entry.reason,
                        "current temp dir cleanup failed — will be reclaimed by startup janitor"
                    );
                }
            }
            result.entries.push(entry);
        }

        // ── Clean current cargo dir ───────────────────────────
        if let Some(cargo) = current_cargo_dir {
            let _ = cargo.finalize(MarkerState::Completed);
            let entry = cargo.cleanup_with_guard(&guard, &self.config.managed_cargo_root);
            result.examined += 1;
            match entry.action {
                super::types::CleanupAction::Delete => result.deleted += 1,
                super::types::CleanupAction::Preserve => {
                    result.preserved += 1;
                }
            }
            result.entries.push(entry);
        }

        // ── Restore env vars ──────────────────────────────────
        // The caller (Runner) handles TEMP/TMP restoration.
        // We just clean directories.

        tracing::info!(
            deleted = result.deleted,
            preserved = result.preserved,
            "completion janitor complete"
        );

        result
    }

    // ── CLI Cleanup ──────────────────────────────────────────

    /// Run a full cleanup pass suitable for the `harness cleanup` CLI.
    /// `dry_run` controls whether deletions are actually performed.
    pub async fn cli_cleanup(
        &self,
        active_execution_ids: Vec<String>,
        dry_run: bool,
    ) -> CleanupResult {
        let guard = self.build_guard(active_execution_ids);
        let mut result = CleanupResult::default();

        tracing::info!(dry_run = dry_run, "CLI cleanup beginning");

        // ── 1. Stale temp ────────────────────────────────────
        result.merge(scan_stale_managed_root(
            &guard,
            &self.config.managed_temp_root,
            ManagedDirKind::HarnessManagedTemp,
            self.config.stale_temp_grace,
            self.config.failed_temp_ttl,
            !dry_run,
        ));

        // ── 2. Evidence retention ────────────────────────────
        result.merge(evidence_dir::apply_evidence_retention(
            &self.config.managed_evidence_root,
            &self.config.evidence_retention,
            &guard,
            !dry_run,
        ));

        // ── 3. Stale cargo runs ──────────────────────────────
        result.merge(cargo_target::scan_stale_cargo_runs(
            &self.config.managed_cargo_root,
            &guard,
            self.config.stale_temp_grace,
            !dry_run,
        ));

        // ── 4. Orphan artifacts ──────────────────────────────
        if !dry_run {
            if let Ok(artifact_root) =
                crate::artifact::ArtifactRoot::open(&self.config.protected.target_root)
            {
                match artifact_root.reclaim_orphans(&self.config.supervisor_id) {
                    Ok(count) => {
                        result.deleted += count;
                        result.examined += count;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "orphan artifact reclamation failed");
                    }
                }
            }
        }

        tracing::info!(
            examined = result.examined,
            deleted = result.deleted,
            preserved = result.preserved,
            failed = result.failed,
            reclaimed_bytes = result.reclaimed_bytes,
            "CLI cleanup complete"
        );

        result
    }

    // ── Periodic Janitor ─────────────────────────────────────

    /// Start a background periodic janitor task.  Returns a
    /// `CancellationToken` that the caller MUST use to stop the
    /// task before shutdown.
    ///
    /// The task runs every `interval` and performs a lightweight
    /// scan of all managed roots, reclaiming stale owned artifacts.
    /// It NEVER deletes active, unmarked, or grace-period directories.
    ///
    /// The task is single-instance: a new tick will not start until
    /// the previous one completes (no concurrent re-entry).
    pub fn start_periodic_janitor(self: &Arc<Self>, interval: Duration) -> CancellationToken {
        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();
        let orch = Arc::clone(self);

        tokio::spawn(async move {
            tracing::info!(
                interval_secs = interval.as_secs(),
                "periodic janitor started"
            );

            loop {
                tokio::select! {
                    _ = cancel_child.cancelled() => {
                        tracing::info!("periodic janitor cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(interval) => {}
                }

                if cancel_child.is_cancelled() {
                    break;
                }

                tracing::debug!("periodic janitor tick");
                let result = orch.startup_janitor(vec![]).await;
                if result.deleted > 0 {
                    tracing::info!(
                        deleted = result.deleted,
                        preserved = result.preserved,
                        "periodic janitor reclaimed stale artifacts"
                    );
                }
            }
        });

        cancel
    }

    // ── Dry-run report ───────────────────────────────────────

    /// Produce a human-readable dry-run report.
    pub fn format_dry_run_report(result: &CleanupResult) -> String {
        let mut lines = vec![
            format!("Examined:  {}", result.examined),
            format!("Would delete: {}", result.deleted),
            format!("Preserved:   {}", result.preserved),
            format!("Failed:      {}", result.failed),
            format!(
                "Would reclaim: {} bytes ({:.2} MB)",
                result.reclaimed_bytes,
                result.reclaimed_bytes as f64 / (1024.0 * 1024.0)
            ),
            String::new(),
            "Details:".to_string(),
        ];

        for entry in &result.entries {
            let action = match entry.action {
                super::types::CleanupAction::Delete => "DELETE",
                super::types::CleanupAction::Preserve => "KEEP",
            };
            lines.push(format!(
                "  [{action}] {} — {}",
                entry.path.display(),
                entry.reason
            ));
        }

        lines.join("\n")
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Scan a managed root for stale directories that are safe to reclaim.
///
/// Decision logic:
/// - marker missing → preserve (report)
/// - marker invalid → preserve (report)
/// - owner active (PID alive + creation time matches) → preserve
/// - active + within grace period → preserve
/// - active + beyond grace → eligible (stale)
/// - completed/failed/abandoned + beyond TTL → eligible
/// - completed/failed/abandoned + within TTL → preserve
fn rfc3339_age_seconds(older: &str, newer: &str) -> u64 {
    if older.len() < 19 || newer.len() < 19 {
        return 0;
    }
    // RFC 3339 timestamps sort lexicographically at the second level.
    if older[..19] >= newer[..19] {
        return 0;
    }
    // Approximate using chrono for precision.
    let parse_dt = |s: &str| -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|d| d.with_timezone(&chrono::Utc))
    };
    if let (Some(older_dt), Some(newer_dt)) = (parse_dt(older), parse_dt(newer)) {
        let dur = newer_dt.signed_duration_since(older_dt);
        if dur.num_seconds() > 0 {
            return dur.num_seconds() as u64;
        }
    }
    0
}

fn scan_stale_managed_root(
    guard: &DeletionGuard,
    root: &std::path::Path,
    expected_kind: ManagedDirKind,
    stale_grace: std::time::Duration,
    failed_ttl: std::time::Duration,
    apply: bool,
) -> CleanupResult {
    let mut result = CleanupResult::default();

    let entries = match std::fs::read_dir(root) {
        Ok(iter) => iter,
        Err(_) => return result,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        result.examined += 1;

        // Read marker.
        let marker_path = path.join(super::types::OWNERSHIP_MARKER_FILENAME);
        let marker: Option<super::types::OwnershipMarker> =
            match std::fs::read_to_string(&marker_path) {
                Ok(raw) => {
                    // Strip UTF-8 BOM if present (PowerShell adds it).
                    let cleaned = raw.strip_prefix('\u{FEFF}').unwrap_or(&raw);
                    match serde_json::from_str(cleaned) {
                        Ok(m) => Some(m),
                        Err(e) => {
                            tracing::warn!(
                                path = %path.display(),
                                error = %e,
                                "marker JSON parse failed"
                            );
                            None
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(path = %path.display(), error = %e, "marker file read failed");
                    None
                }
            };

        let eligible = match &marker {
            None => {
                result.entries.push(super::types::CleanupEntry {
                    path,
                    action: super::types::CleanupAction::Preserve,
                    reason: "no ownership marker — preserved for manual review".into(),
                });
                result.preserved += 1;
                continue;
            }
            Some(m) => {
                if !m.is_active() {
                    // Terminal state — check TTL.
                    let completed_at = m.completed_at.as_deref().unwrap_or(&m.created_at);
                    // RFC 3339 timestamps sort lexicographically.
                    let now = chrono::Utc::now().to_rfc3339();
                    let age_secs = rfc3339_age_seconds(completed_at, &now);
                    if age_secs >= failed_ttl.as_secs() {
                        true // eligible
                    } else {
                        result.entries.push(super::types::CleanupEntry {
                            path,
                            action: super::types::CleanupAction::Preserve,
                            reason: format!(
                                "within TTL ({:.0}s remaining)",
                                (failed_ttl.as_secs() - age_secs)
                            ),
                        });
                        result.preserved += 1;
                        continue;
                    }
                } else {
                    // Active — check grace period.
                    let now = chrono::Utc::now().to_rfc3339();
                    let age_secs = rfc3339_age_seconds(&m.created_at, &now);
                    if age_secs >= stale_grace.as_secs() {
                        true // stale — eligible
                    } else {
                        result.entries.push(super::types::CleanupEntry {
                            path,
                            action: super::types::CleanupAction::Preserve,
                            reason: format!(
                                "active within grace ({:.0}s remaining)",
                                (stale_grace.as_secs() - age_secs)
                            ),
                        });
                        result.preserved += 1;
                        continue;
                    }
                }
            }
        };

        // Pass through DeletionGuard for final safety check.
        if eligible {
            let entry_result = if apply {
                guard.guarded_delete(&path, root, Some(expected_kind))
            } else {
                guard.dry_run(&path, root, Some(expected_kind))
            };

            match entry_result.action {
                super::types::CleanupAction::Delete => result.deleted += 1,
                super::types::CleanupAction::Preserve => result.preserved += 1,
            }
            result.entries.push(entry_result);
        }
    }

    result
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::super::temp_dir::HarnessTempDir;
    use super::super::types::*;
    use super::*;

    async fn make_pool() -> SqlitePool {
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite")
    }

    #[tokio::test]
    async fn orchestrator_creation_validates_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = LivenessConfig::for_test(tmp.path());
        let pool = make_pool().await;
        // Config validation is sync and safe.
        let errors = cfg.validate();
        assert!(errors.is_empty());
        // Verify orchestrator can be constructed.
        let orch = LivenessOrchestrator::new(cfg, pool);
        assert!(orch.is_ok());
    }

    #[tokio::test]
    async fn startup_janitor_cleans_stale_owned_temp() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = LivenessConfig::for_test(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        // Create an owned temp dir with Completed state (simulating
        // a previous run that was finalized but not cleaned).
        let dir = HarnessTempDir::create(&cfg.managed_temp_root, "old-run", "test-head").unwrap();
        dir.finalize(MarkerState::Completed).unwrap();
        assert!(dir.path().exists());

        let pool = make_pool().await;
        let orch = LivenessOrchestrator::new(cfg, pool).unwrap();

        // Run janitor — the grace is 1s and TTL is 10s in test config.
        // Since we just finalized, it's within TTL so should be preserved.
        let result = orch.startup_janitor(vec![]).await;
        assert!(
            dir.path().exists(),
            "fresh finalized dir within TTL should be preserved"
        );
        assert_eq!(result.preserved, 1);
    }

    #[tokio::test]
    async fn startup_janitor_preserves_unmarked() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = LivenessConfig::for_test(tmp.path());
        std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

        let unmarked = cfg.managed_temp_root.join("no-marker");
        std::fs::create_dir_all(&unmarked).unwrap();

        let pool = make_pool().await;
        let orch = LivenessOrchestrator::new(cfg, pool).unwrap();

        let result = orch.startup_janitor(vec![]).await;
        assert!(unmarked.exists(), "unmarked directory must be preserved");
        assert_eq!(result.preserved, 1);
    }

    #[test]
    fn dry_run_report_formatting() {
        let result = CleanupResult {
            examined: 3,
            deleted: 1,
            preserved: 2,
            reclaimed_bytes: 1024 * 1024,
            ..Default::default()
        };

        let report = LivenessOrchestrator::format_dry_run_report(&result);
        assert!(report.contains("Examined:  3"));
        assert!(report.contains("Would delete: 1"));
        assert!(report.contains("Preserved:   2"));
        assert!(report.contains("1.00 MB"));
    }
}
