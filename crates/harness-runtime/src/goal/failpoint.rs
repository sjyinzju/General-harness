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

    /// Hit this failpoint. If failpoints are enabled, this will block
    /// until the failpoint is released by the test harness. Otherwise,
    /// it's a no-op.
    ///
    /// The test harness sets a file-based signal (e.g., a marker file)
    /// that this function checks before proceeding.
    pub async fn hit(&self) {
        if !failpoints_enabled() {
            return;
        }

        tracing::info!(
            failpoint = self.name,
            description = self.description,
            "failpoint hit — waiting for release signal"
        );

        // Poll for release signal file. The test harness creates a file
        // named `<failpoint_name>.release` in the failpoint directory.
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
