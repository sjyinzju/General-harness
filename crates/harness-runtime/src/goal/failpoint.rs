//! Crash failpoints for goal loop testing.
//!
//! Failpoints are disabled by default in production and enabled only during
//! explicit crash/takeover E2E tests. Each failpoint blocks at a specific
//! point in the goal loop, allowing the test harness to force-terminate the
//! Supervisor process and verify recovery.
//!
//! # Safety
//!
//! - All failpoints are gated behind `cfg(debug_assertions)` or an explicit
//!   environment variable `HARNESS_FAILPOINT_ENABLE=1`.
//! - In production (release, no env var), failpoints are no-ops.
//! - Failpoints never corrupt state — they only block/pause execution.

use std::sync::atomic::{AtomicBool, Ordering};

/// Global failpoint enable flag. Only set during E2E tests.
static FAILPOINT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable failpoints. Call once before starting E2E tests.
pub fn enable_failpoints() {
    FAILPOINT_ENABLED.store(true, Ordering::Release);
}

/// Disable failpoints. Call after E2E tests complete.
pub fn disable_failpoints() {
    FAILPOINT_ENABLED.store(false, Ordering::Release);
}

/// Check if failpoints are enabled.
pub fn failpoints_enabled() -> bool {
    if cfg!(debug_assertions) {
        return FAILPOINT_ENABLED.load(Ordering::Acquire)
            || std::env::var("HARNESS_FAILPOINT_ENABLE").is_ok();
    }
    std::env::var("HARNESS_FAILPOINT_ENABLE").is_ok()
}

/// A named failpoint that can be triggered at specific points in execution.
#[derive(Debug, Clone)]
pub struct Failpoint {
    pub name: &'static str,
    pub description: &'static str,
}

impl Failpoint {
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        Self { name, description }
    }

    /// Hit this failpoint. If failpoints are enabled, this will:
    /// 1. Write a `.hit` marker file (deterministic signal for the test harness)
    /// 2. Block until the failpoint is released by the test harness
    ///
    /// The hit marker is written BEFORE blocking, so the runner can observe
    /// the failpoint was reached without log scanning or sleep-based heuristics.
    ///
    /// When failpoints are disabled (production), this is a no-op.
    pub async fn hit(&self) {
        if !failpoints_enabled() {
            return;
        }

        // Ensure failpoint directory exists
        let fp_dir = std::path::Path::new("target/harness-failpoints");
        let _ = std::fs::create_dir_all(fp_dir);

        // 1. Write hit marker BEFORE blocking (deterministic signal for runner)
        let hit_file = fp_dir.join(format!("{}.hit", self.name));
        if let Err(e) = std::fs::write(&hit_file, chrono::Utc::now().to_rfc3339()) {
            tracing::warn!(
                failpoint = self.name,
                error = %e,
                "failed to write failpoint hit marker"
            );
        }

        tracing::info!(
            failpoint = self.name,
            description = self.description,
            hit_file = %hit_file.display(),
            "failpoint hit — hit marker written, waiting for release signal"
        );

        // 2. Poll for release signal file
        let release_file = format!("target/harness-failpoints/{}.release", self.name);

        loop {
            if std::path::Path::new(&release_file).exists() {
                tracing::info!(failpoint = self.name, "failpoint released");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

// ── Well-known failpoints ───────────────────────────────────────────

/// After a task has been integrated but before the GoalObservation is persisted.
/// Use this to test Supervisor crash + observation recovery.
pub const AFTER_TASK_INTEGRATED: Failpoint = Failpoint::new(
    "after_task_integrated",
    "Task integration complete, before GoalObservation persisted",
);

/// Before the GoalObservation is persisted to the database.
/// Use this to test Supervisor crash + observation import from IntegrationResult.
pub const BEFORE_GOAL_OBSERVATION_PERSISTED: Failpoint = Failpoint::new(
    "before_goal_observation_persisted",
    "About to persist GoalObservation, before INSERT",
);

/// After Planner invocation, before PlanRevision activation.
pub const AFTER_PLANNER_INVOCATION: Failpoint = Failpoint::new(
    "after_planner_invocation",
    "Planner returned, before PlanRevision created",
);

/// After Evaluator invocation, before assessment persisted.
pub const AFTER_EVALUATOR_INVOCATION: Failpoint = Failpoint::new(
    "after_evaluator_invocation",
    "Evaluator returned, before assessment persisted",
);

/// Check if a specific failpoint was hit (reads the .hit marker file).
/// Returns Some(timestamp) if the failpoint was hit, None otherwise.
/// Used by test harnesses for deterministic observation without log scanning.
pub fn check_failpoint_hit(name: &str) -> Option<String> {
    let hit_file = format!("target/harness-failpoints/{}.hit", name);
    std::fs::read_to_string(&hit_file)
        .ok()
        .map(|s| s.trim().to_string())
}

/// Clean up failpoint markers for a specific failpoint.
pub fn cleanup_failpoint(name: &str) {
    let fp_dir = std::path::Path::new("target/harness-failpoints");
    let hit_file = fp_dir.join(format!("{}.hit", name));
    let release_file = fp_dir.join(format!("{}.release", name));
    let _ = std::fs::remove_file(&hit_file);
    let _ = std::fs::remove_file(&release_file);
}

/// Clean up all failpoint markers.
pub fn cleanup_all_failpoints() {
    let fp_dir = std::path::Path::new("target/harness-failpoints");
    if fp_dir.exists() {
        let _ = std::fs::remove_dir_all(fp_dir);
    }
}
