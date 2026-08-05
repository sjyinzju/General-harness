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
//
// F1–F10 fault injection matrix plus F0/core-takeover.
// Each failpoint is placed AFTER the durable transaction commits and BEFORE
// the next side-effect (dispatch, enqueue, spawn, invoke).
//
// Naming convention: `AFTER_<DURABLE_EVENT>_BEFORE_<NEXT_SIDE_EFFECT>`

/// F1: Goal INSERT committed, before Planner is invoked.
/// The Goal row is durably persisted; Planning work has not been claimed.
pub const F1_AFTER_GOAL_PERSISTED_BEFORE_PLANNING: Failpoint = Failpoint::new(
    "f1_after_goal_persisted_before_planning",
    "Goal committed to DB, Planner not yet invoked",
);

/// F2: PlanRevision INSERT committed, before PlannedTask materialization/dispatch.
pub const F2_AFTER_PLAN_REVISION_COMMITTED_BEFORE_TASK_DISPATCH: Failpoint = Failpoint::new(
    "f2_after_plan_revision_committed_before_task_dispatch",
    "PlanRevision committed, PlannedTasks not yet materialized",
);

/// F3: TaskEngineeringLoop row committed, before Executor spawn.
pub const F3_AFTER_TASK_LOOP_COMMITTED_BEFORE_EXECUTOR_SPAWN: Failpoint = Failpoint::new(
    "f3_after_task_loop_committed_before_executor_spawn",
    "Task loop committed, Executor not yet spawned",
);

/// F4: Executor result committed (CompletedTask), before Verification committed.
pub const F4_AFTER_EXECUTOR_RESULT_COMMITTED_BEFORE_VERIFICATION: Failpoint = Failpoint::new(
    "f4_after_executor_result_committed_before_verification",
    "Executor result committed, Verification not yet persisted",
);

/// F5: Verification PASS committed, before Candidate persisted.
pub const F5_AFTER_VERIFICATION_PASS_COMMITTED_BEFORE_CANDIDATE: Failpoint = Failpoint::new(
    "f5_after_verification_pass_committed_before_candidate",
    "Verification PASS committed, Candidate not yet created",
);

/// F6: Review Approved committed, before Controlled Commit.
pub const F6_AFTER_REVIEW_APPROVED_COMMITTED_BEFORE_CONTROLLED_COMMIT: Failpoint = Failpoint::new(
    "f6_after_review_approved_committed_before_controlled_commit",
    "Review Approved committed, Controlled Commit not yet executed",
);

/// F7: Controlled Commit created, before Integration enqueue.
pub const F7_AFTER_CONTROLLED_COMMIT_CREATED_BEFORE_INTEGRATION_ENQUEUE: Failpoint = Failpoint::new(
    "f7_after_controlled_commit_created_before_integration_enqueue",
    "Controlled Commit created, Integration not yet enqueued",
);

/// F8: IntegrationResult committed, before GoalObservation.
pub const F8_AFTER_INTEGRATION_RESULT_COMMITTED_BEFORE_GOAL_OBSERVATION: Failpoint = Failpoint::new(
    "f8_after_integration_result_committed_before_goal_observation",
    "IntegrationResult committed, GoalObservation not yet persisted",
);

/// F9: GoalObservation committed, before Evaluator invocation.
pub const F9_AFTER_GOAL_OBSERVATION_COMMITTED_BEFORE_EVALUATOR: Failpoint = Failpoint::new(
    "f9_after_goal_observation_committed_before_evaluator",
    "GoalObservation committed, Evaluator not yet invoked",
);

/// F10: Assessment committed, before CompletionPolicy transition.
pub const F10_AFTER_ASSESSMENT_COMMITTED_BEFORE_COMPLETION_POLICY: Failpoint = Failpoint::new(
    "f10_after_assessment_committed_before_completion_policy",
    "Assessment committed, CompletionPolicy not yet applied",
);

/// F0 / Core-takeover: Supervisor lease acquired, before any work dispatch.
pub const F0_CORE_TAKEOVER: Failpoint = Failpoint::new(
    "f0_core_takeover",
    "Supervisor lease acquired, before any goal dispatch",
);

// ── Legacy aliases (kept for backward compatibility) ───────────────

/// After a task has been integrated but before the GoalObservation is persisted.
pub const AFTER_TASK_INTEGRATED: Failpoint =
    F8_AFTER_INTEGRATION_RESULT_COMMITTED_BEFORE_GOAL_OBSERVATION;

/// Before the GoalObservation is persisted to the database.
pub const BEFORE_GOAL_OBSERVATION_PERSISTED: Failpoint =
    F8_AFTER_INTEGRATION_RESULT_COMMITTED_BEFORE_GOAL_OBSERVATION;

/// After Planner invocation, before PlanRevision activation.
pub const AFTER_PLANNER_INVOCATION: Failpoint = F1_AFTER_GOAL_PERSISTED_BEFORE_PLANNING;

/// After Evaluator invocation, before assessment persisted.
pub const AFTER_EVALUATOR_INVOCATION: Failpoint =
    F10_AFTER_ASSESSMENT_COMMITTED_BEFORE_COMPLETION_POLICY;

/// All F1-F10 failpoints in order for matrix iteration.
pub const FAULT_MATRIX: &[Failpoint] = &[
    F1_AFTER_GOAL_PERSISTED_BEFORE_PLANNING,
    F2_AFTER_PLAN_REVISION_COMMITTED_BEFORE_TASK_DISPATCH,
    F3_AFTER_TASK_LOOP_COMMITTED_BEFORE_EXECUTOR_SPAWN,
    F4_AFTER_EXECUTOR_RESULT_COMMITTED_BEFORE_VERIFICATION,
    F5_AFTER_VERIFICATION_PASS_COMMITTED_BEFORE_CANDIDATE,
    F6_AFTER_REVIEW_APPROVED_COMMITTED_BEFORE_CONTROLLED_COMMIT,
    F7_AFTER_CONTROLLED_COMMIT_CREATED_BEFORE_INTEGRATION_ENQUEUE,
    F8_AFTER_INTEGRATION_RESULT_COMMITTED_BEFORE_GOAL_OBSERVATION,
    F9_AFTER_GOAL_OBSERVATION_COMMITTED_BEFORE_EVALUATOR,
    F10_AFTER_ASSESSMENT_COMMITTED_BEFORE_COMPLETION_POLICY,
];

/// Check if a specific failpoint was hit (reads the .hit marker file).
/// Returns Some(timestamp) if the failpoint was hit, None otherwise.
/// Used by test harnesses for deterministic observation without log scanning.
pub fn check_failpoint_hit(name: &str) -> Option<String> {
    let hit_file = format!("target/harness-failpoints/{}.hit", name);
    std::fs::read_to_string(&hit_file)
        .ok()
        .map(|s| s.trim().to_string())
}

/// Release a specific failpoint (write the .release marker file).
/// Called by the test harness to unblock a Supervisor blocked at a failpoint.
pub fn release_failpoint(name: &str) {
    let fp_dir = std::path::Path::new("target/harness-failpoints");
    let _ = std::fs::create_dir_all(fp_dir);
    let release_file = fp_dir.join(format!("{}.release", name));
    if let Err(e) = std::fs::write(&release_file, chrono::Utc::now().to_rfc3339()) {
        tracing::warn!(
            failpoint = name,
            error = %e,
            "failed to write failpoint release marker"
        );
    } else {
        tracing::info!(failpoint = name, "failpoint release marker written");
    }
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

/// Check if a failpoint hit marker exists (non-blocking).
pub fn is_failpoint_hit(name: &str) -> bool {
    let hit_file = format!("target/harness-failpoints/{}.hit", name);
    std::path::Path::new(&hit_file).exists()
}
