//! RunContext — unified managed-temp provider for a single harness run.
//!
//! Every harness CLI invocation, production-graph build, or supervisor
//! start MUST create a RunContext.  It:
//! - Creates managed temp/evidence/cargo directories with ownership markers.
//! - Provides a unified `TempPathProvider` so production code never calls
//!   `std::env::temp_dir()` directly.
//! - Redirects `TEMP`/`TMP` for the current process and all children.
//! - Ensures cleanup on drop (best-effort).
//!
//! # invariants
//! - One RunContext per harness run.
//! - `run_id` is unique per invocation.
//! - All managed directories carry `.harness-owned.json`.
//! - `TEMP`/`TMP` are restored on `shutdown()`.

use std::path::{Path, PathBuf};

use harness_core::{CoreError, ErrorCode, ErrorSource};

use super::cargo_target::ManagedCargoRunDir;
use super::evidence_dir::HarnessEvidenceDir;
use super::guard::DeletionGuard;
use super::temp_dir::HarnessTempDir;
use super::types::{CleanupAction, CleanupResult, LivenessConfig, MarkerState};

/// Trait for providing managed temp paths.  All production code that
/// needs a temporary directory MUST go through this provider rather
/// than calling `std::env::temp_dir()` directly.
pub trait TempPathProvider: Send + Sync {
    /// Root of the managed temp directory for the current run.
    fn managed_temp_root(&self) -> &Path;

    /// Root for managed evidence output.
    fn managed_evidence_root(&self) -> &Path;

    /// Path for a named temporary file or subdirectory within the
    /// managed temp root.
    fn temp_path(&self, name: &str) -> PathBuf {
        self.managed_temp_root().join(name)
    }

    /// Current run identifier.
    fn run_id(&self) -> &str;
}

/// The production RunContext.  Created once at the start of a run
/// and shared via Arc.
pub struct RunContext {
    run_id: String,
    code_head: String,
    config: LivenessConfig,
    managed_temp: Option<HarnessTempDir>,
    managed_evidence: Option<HarnessEvidenceDir>,
    managed_cargo: Option<ManagedCargoRunDir>,
    /// Saved original TEMP/TMP environment values.
    original_temp: Option<String>,
    original_tmp: Option<String>,
}

impl RunContext {
    /// Create a new RunContext, allocating managed temp + evidence
    /// directories.  Redirects process `TEMP`/`TMP` if `redirect_env`
    /// is true (default for production).
    pub fn create(
        repo_root: &Path,
        code_head: &str,
        redirect_env: bool,
    ) -> Result<Self, CoreError> {
        let run_id = format!("run-{}", uuid::Uuid::new_v4());
        let supervisor_id = format!("sup-{}", uuid::Uuid::new_v4());
        let config = LivenessConfig::for_repo(repo_root, supervisor_id);

        // Validate config before any filesystem work.
        let errors = config.validate();
        if !errors.is_empty() {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                format!("unsafe liveness config:\n  - {}", errors.join("\n  - ")),
                ErrorSource::System,
            ));
        }

        // Save original env before redirecting.
        let original_temp = std::env::var("TEMP").ok();
        let original_tmp = std::env::var("TMP").ok();

        // Create managed temp directory.
        let managed_temp = HarnessTempDir::create(&config.managed_temp_root, &run_id, code_head)?;

        // Create managed evidence directory.
        let managed_evidence =
            HarnessEvidenceDir::create(&config.managed_evidence_root, code_head, &run_id)?;

        // Redirect env for this process + children.
        if redirect_env {
            let temp_path = managed_temp.path().to_string_lossy().to_string();
            std::env::set_var("TEMP", &temp_path);
            std::env::set_var("TMP", &temp_path);
            tracing::info!(
                run_id = %run_id,
                path = %temp_path,
                "TEMP/TMP redirected to managed temp"
            );
        }

        tracing::info!(
            run_id = %run_id,
            code_head = %code_head,
            "RunContext created"
        );

        Ok(Self {
            run_id,
            code_head: code_head.to_string(),
            config,
            managed_temp: Some(managed_temp),
            managed_evidence: Some(managed_evidence),
            managed_cargo: None,
            original_temp,
            original_tmp,
        })
    }

    /// Create an isolated Cargo target directory for this run.
    /// Only call this when workspace lock isolation requires it
    /// (Strategy B); otherwise use the shared cache.
    pub fn create_cargo_run_dir(&mut self) -> Result<(), CoreError> {
        let dir = ManagedCargoRunDir::create(
            &self.config.managed_cargo_root,
            &self.run_id,
            &self.code_head,
        )?;
        self.managed_cargo = Some(dir);
        Ok(())
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn code_head(&self) -> &str {
        &self.code_head
    }

    pub fn config(&self) -> &LivenessConfig {
        &self.config
    }

    pub fn managed_temp(&self) -> Option<&HarnessTempDir> {
        self.managed_temp.as_ref()
    }

    pub fn managed_evidence(&self) -> Option<&HarnessEvidenceDir> {
        self.managed_evidence.as_ref()
    }

    pub fn managed_cargo(&self) -> Option<&ManagedCargoRunDir> {
        self.managed_cargo.as_ref()
    }

    /// Build a DeletionGuard from the current context.
    pub fn build_guard(&self, active_execution_ids: Vec<String>) -> DeletionGuard {
        DeletionGuard::new(self.config.clone(), active_execution_ids)
    }

    /// Shutdown: restore environment, finalize markers, and attempt
    /// cleanup of the current run's managed temp directory.
    ///
    /// This should be called in a `finally`-style block at the end
    /// of every run.  On failure, directories are left for the
    /// Startup Janitor.
    pub async fn shutdown(mut self, run_succeeded: bool) -> CleanupResult {
        let mut result = CleanupResult::default();

        // ── Restore environment ──────────────────────────────────
        if let Some(ref temp) = self.original_temp {
            std::env::set_var("TEMP", temp);
        }
        if let Some(ref tmp) = self.original_tmp {
            std::env::set_var("TMP", tmp);
        }
        tracing::info!("TEMP/TMP restored to original values");

        // ── Finalize markers ────────────────────────────────────
        let final_state = if run_succeeded {
            MarkerState::Completed
        } else {
            MarkerState::Failed
        };

        if let Some(ref temp) = self.managed_temp {
            let _ = temp.finalize(final_state);
        }
        if let Some(ref ev) = self.managed_evidence {
            let _ = ev.finalize(final_state);
        }
        if let Some(ref cargo) = self.managed_cargo {
            let _ = cargo.finalize(final_state);
        }

        // ── Wait for child processes ────────────────────────────
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // ── Cleanup managed temp (bounded retry) ────────────────
        let guard = self.build_guard(vec![]);

        if let Some(temp) = self.managed_temp.take() {
            let entry = temp
                .cleanup_with_retry(&guard, &self.config.managed_temp_root)
                .await;
            if entry.action == CleanupAction::Delete {
                result.deleted += 1;
            } else {
                result.preserved += 1;
                tracing::warn!(
                    reason = %entry.reason,
                    "managed temp cleanup failed — left for startup janitor"
                );
            }
            result.entries.push(entry);
        }

        // ── Cleanup cargo dir ───────────────────────────────────
        if let Some(cargo) = self.managed_cargo.take() {
            let entry = cargo.cleanup_with_guard(&guard, &self.config.managed_cargo_root);
            if entry.action == CleanupAction::Delete {
                result.deleted += 1;
            } else {
                result.preserved += 1;
            }
            result.entries.push(entry);
        }

        result.examined = result.deleted + result.preserved;
        tracing::info!(
            deleted = result.deleted,
            preserved = result.preserved,
            "RunContext shutdown complete"
        );

        result
    }
}

impl TempPathProvider for RunContext {
    fn managed_temp_root(&self) -> &Path {
        self.managed_temp
            .as_ref()
            .map(|t| t.path())
            .unwrap_or_else(|| self.config.managed_temp_root.as_path())
    }

    fn managed_evidence_root(&self) -> &Path {
        self.config.managed_evidence_root.as_path()
    }

    fn run_id(&self) -> &str {
        &self.run_id
    }
}

impl Drop for RunContext {
    fn drop(&mut self) {
        // Restore env if we still hold it.
        if let Some(ref temp) = self.original_temp {
            std::env::set_var("TEMP", temp);
        }
        if let Some(ref tmp) = self.original_tmp {
            std::env::set_var("TMP", tmp);
        }
        // Markers are finalized as abandoned so startup janitor can find them.
        if let Some(ref temp) = self.managed_temp {
            let _ = temp.finalize(MarkerState::Abandoned);
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_context_creates_managed_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = RunContext::create(tmp.path(), "test-head", false).unwrap();

        assert!(ctx.managed_temp().is_some());
        assert!(ctx.managed_evidence().is_some());
        assert!(ctx.run_id().starts_with("run-"));

        let temp = ctx.managed_temp().unwrap();
        assert!(temp.path().exists());
        assert!(temp
            .path()
            .join(super::super::types::OWNERSHIP_MARKER_FILENAME)
            .exists());
    }

    #[test]
    fn temp_path_provider_trait() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = RunContext::create(tmp.path(), "head", false).unwrap();

        let provider: &dyn TempPathProvider = &ctx;
        let spool = provider.temp_path("stdout.spool");
        assert!(spool.starts_with(provider.managed_temp_root()));
    }

    #[tokio::test]
    async fn shutdown_cleans_temp() {
        let tmp = tempfile::tempdir().unwrap();
        let ctx = RunContext::create(tmp.path(), "head", false).unwrap();
        let temp_path = ctx.managed_temp().unwrap().path().to_path_buf();
        assert!(temp_path.exists());

        let result = ctx.shutdown(true).await;
        assert!(!temp_path.exists(), "shutdown must clean managed temp");
        assert_eq!(result.deleted, 1);
    }

    #[test]
    fn drop_restores_env() {
        let orig_temp = std::env::var("TEMP").ok();
        let tmp = tempfile::tempdir().unwrap();

        {
            let ctx = RunContext::create(tmp.path(), "head", true).unwrap();
            // TEMP should point to managed dir.
            let current = std::env::var("TEMP").unwrap();
            assert!(current.contains("harness-temp"));
            drop(ctx);
        }

        // After drop, TEMP should be restored.
        let restored = std::env::var("TEMP").ok();
        assert_eq!(restored, orig_temp);
    }
}
