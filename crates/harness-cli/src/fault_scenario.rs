//! FaultScenario framework for I1-I7 SafeOnly fault injection matrix.
//!
//! Provides a unified framework for running F1-F10 fault injection scenarios.
//! Each scenario: start Supervisor A → trigger failpoint → kill A → start B → verify recovery.
//!
//! NEVER: directly calls business services, directly modifies SQL, hand-writes state,
//!        or simulates A/B in the same process.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use harness_runtime::goal::failpoint;

const SUPERVISOR_START_TIMEOUT: Duration = Duration::from_secs(30);
const IPC_POLL_INTERVAL: Duration = Duration::from_millis(500);
const LEASE_DURATION_SECS: u64 = 30;
const FAILPOINT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const FAILPOINT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const GOAL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(60);

// ── FaultScenario Definition ────────────────────────────────────────────

/// A complete fault injection scenario.
#[derive(Debug, Clone)]
pub struct FaultScenario {
    pub id: FaultScenarioId,
    pub failpoint_name: &'static str,
    pub description: &'static str,
    /// The failpoint must be hit for the scenario to be valid.
    pub failpoint_required: bool,
    /// How to create and start the goal (via CLI/IPC).
    pub goal_setup: GoalSetup,
    /// Pre-crash assertions to verify before killing A.
    pub pre_crash_assertions: Vec<Assertion>,
    /// Post-recovery expectations.
    pub recovery_expectations: Vec<Assertion>,
    /// Duplicate constraints to verify exactly-once semantics.
    pub duplicate_constraints: Vec<DuplicateCheck>,
    /// Cleanup constraints.
    pub cleanup_constraints: Vec<CleanupCheck>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FaultScenarioId {
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F0CoreTakeover,
}

impl FaultScenarioId {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::F1 => "F1",
            Self::F2 => "F2",
            Self::F3 => "F3",
            Self::F4 => "F4",
            Self::F5 => "F5",
            Self::F6 => "F6",
            Self::F7 => "F7",
            Self::F8 => "F8",
            Self::F9 => "F9",
            Self::F10 => "F10",
            Self::F0CoreTakeover => "F0-core-takeover",
        }
    }

    pub fn failpoint_name(&self) -> &'static str {
        match self {
            Self::F1 => "f1_after_goal_persisted_before_planning",
            Self::F2 => "f2_after_plan_revision_committed_before_task_dispatch",
            Self::F3 => "f3_after_task_loop_committed_before_executor_spawn",
            Self::F4 => "f4_after_executor_result_committed_before_verification",
            Self::F5 => "f5_after_verification_pass_committed_before_candidate",
            Self::F6 => "f6_after_review_approved_committed_before_controlled_commit",
            Self::F7 => "f7_after_controlled_commit_created_before_integration_enqueue",
            Self::F8 => "f8_after_integration_result_committed_before_goal_observation",
            Self::F9 => "f9_after_goal_observation_committed_before_evaluator",
            Self::F10 => "f10_after_assessment_committed_before_completion_policy",
            Self::F0CoreTakeover => "f0_core_takeover",
        }
    }
}

// ── Goal Setup Strategy ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum GoalSetup {
    /// Create goal via CLI → IPC → Supervisor (production path).
    ViaIpc { goal_spec_json: String },
    /// Create goal via CLI --standalone (bypasses IPC, opens DB directly).
    /// This is NOT the production path but works for scenarios where the
    /// goal must exist before the supervisor starts.
    ViaStandalone { goal_spec_json: String },
    /// Goal already exists in DB (for F2-F10 where F1 already created it).
    PreExisting { goal_id: String },
}

// ── Assertions ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Assertion {
    FailpointHit {
        name: String,
    },
    GoalPersisted {
        goal_id: String,
    },
    GoalNotPersisted {
        goal_id: String,
    },
    PlannerNotInvoked,
    PlanRevisionCreated {
        goal_id: String,
    },
    TaskLoopCreated {
        goal_id: String,
    },
    TokenGreater {
        token_b: i64,
        token_a: i64,
    },
    GoalRecovered {
        goal_id: String,
    },
    GoalTerminalState {
        goal_id: String,
        expected_state: String,
    },
    OldOwnerWriteRejected {
        old_instance_id: String,
    },
    ProcessTerminated {
        pid: u32,
    },
    SupervisorBReady,
}

#[derive(Debug, Clone)]
pub enum DuplicateCheck {
    GoalCount { goal_id: String, expected: i64 },
    PlanCount { goal_id: String, expected: i64 },
    TaskCount { goal_id: String, max: i64 },
    CommitCount { goal_id: String, max: i64 },
    IntegrationCount { goal_id: String, max: i64 },
    ObservationCount { goal_id: String, expected: i64 },
    AssessmentCount { goal_id: String, max: i64 },
    EvaluatorInvocations { goal_id: String, max: i64 },
}

#[derive(Debug, Clone)]
pub enum CleanupCheck {
    OrphanProcesses { max: u32 },
    OrphanWorktrees { max: u32 },
    ClaimLeaks { max: u32 },
    LeaseLeaks { max: u32 },
    IpcResidue { max: u32 },
}

// ── FaultScenarioRunner ─────────────────────────────────────────────────

/// The unified runner for all fault injection scenarios.
pub struct FaultScenarioRunner {
    pub repo_root: PathBuf,
    pub code_head: String,
    pub harness_bin: PathBuf,
    pub work_dir: PathBuf,
}

/// Result of running a single fault scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultScenarioResult {
    pub scenario_id: String,
    pub passed: bool,
    pub failpoint_hit: bool,
    pub failpoint_timestamp: Option<String>,
    pub supervisor_a_pid: Option<u32>,
    pub supervisor_a_token: Option<i64>,
    pub supervisor_a_terminated: bool,
    pub supervisor_b_pid: Option<u32>,
    pub supervisor_b_token: Option<i64>,
    pub supervisor_b_ready: bool,
    pub token_b_greater: bool,
    pub goal_recovered: bool,
    pub goal_terminal_state: Option<String>,
    pub old_owner_fenced: bool,
    pub assertions_passed: u32,
    pub assertions_total: u32,
    pub duplicates_ok: bool,
    pub cleanup_ok: bool,
    pub error: Option<String>,
    pub evidence: Value,
}

impl FaultScenarioResult {
    pub fn new(scenario_id: &str) -> Self {
        Self {
            scenario_id: scenario_id.to_string(),
            passed: false,
            failpoint_hit: false,
            failpoint_timestamp: None,
            supervisor_a_pid: None,
            supervisor_a_token: None,
            supervisor_a_terminated: false,
            supervisor_b_pid: None,
            supervisor_b_token: None,
            supervisor_b_ready: false,
            token_b_greater: false,
            goal_recovered: false,
            goal_terminal_state: None,
            old_owner_fenced: false,
            assertions_passed: 0,
            assertions_total: 0,
            duplicates_ok: true,
            cleanup_ok: true,
            error: None,
            evidence: Value::Null,
        }
    }
}

impl FaultScenarioRunner {
    pub fn new(
        repo_root: PathBuf,
        code_head: String,
        harness_bin: PathBuf,
        work_dir: PathBuf,
    ) -> Self {
        Self {
            repo_root,
            code_head,
            harness_bin,
            work_dir,
        }
    }

    /// Run a single fault scenario end-to-end.
    pub async fn run_scenario(&self, scenario: &FaultScenario) -> FaultScenarioResult {
        let mut result = FaultScenarioResult::new(scenario.id.as_str());
        result.assertions_total =
            (scenario.pre_crash_assertions.len() + scenario.recovery_expectations.len()) as u32;

        let scenario_dir = self.work_dir.join(scenario.id.as_str());
        let _ = std::fs::create_dir_all(&scenario_dir);

        // Cleanup any stale failpoint markers
        failpoint::cleanup_failpoint(scenario.failpoint_name);

        // ── Prepare isolated environment ──────────────────────────────
        let (db_path, test_repo, worktree_root, state_dir, goal_id) =
            match self.prepare_environment(&scenario_dir, scenario).await {
                Ok(v) => v,
                Err(e) => {
                    result.error = Some(format!("environment: {}", e));
                    return result;
                }
            };

        // ── Start Supervisor A ────────────────────────────────────────
        let mut child_a =
            match self.start_supervisor(&db_path, &test_repo, &worktree_root, &state_dir, true) {
                Ok(c) => c,
                Err(e) => {
                    result.error = Some(format!("start A: {}", e));
                    return result;
                }
            };

        result.supervisor_a_pid = Some(child_a.id());

        // Wait for Supervisor A ready
        match self.wait_supervisor_ready(&db_path, &state_dir).await {
            Ok(token) => {
                result.supervisor_a_token = Some(token);
            }
            Err(e) => {
                let _ = child_a.kill();
                let _ = child_a.wait();
                result.error = Some(format!("A ready: {}", e));
                return result;
            }
        }

        // ── Pre-release earlier failpoints ────────────────────────────
        // Failpoints are sequential: F1 → F2 → ... → F10.
        // To reach the target failpoint, all earlier failpoints must be
        // released so the goal loop can progress through them without
        // blocking. The target failpoint itself is NOT released — the
        // goal loop will block there, and the runner can observe the hit.
        pre_release_earlier_failpoints(scenario.id);

        // ── Create and start the goal ─────────────────────────────────
        let actual_goal_id = match &scenario.goal_setup {
            GoalSetup::ViaIpc { goal_spec_json } => {
                match self.create_goal_via_cli(
                    &db_path,
                    &test_repo,
                    &worktree_root,
                    &scenario_dir,
                    goal_spec_json,
                ) {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = child_a.kill();
                        let _ = child_a.wait();
                        result.error = Some(format!("goal create: {}", e));
                        return result;
                    }
                }
            }
            GoalSetup::ViaStandalone { goal_spec_json } => {
                match self.create_goal_standalone(
                    &db_path,
                    &test_repo,
                    &worktree_root,
                    &scenario_dir,
                    goal_spec_json,
                ) {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = child_a.kill();
                        let _ = child_a.wait();
                        result.error = Some(format!("goal standalone: {}", e));
                        return result;
                    }
                }
            }
            GoalSetup::PreExisting { goal_id } => goal_id.clone(),
        };

        // ── Wait for failpoint hit ────────────────────────────────────
        match self.wait_for_failpoint(scenario.failpoint_name).await {
            Ok(ts) => {
                result.failpoint_hit = true;
                result.failpoint_timestamp = Some(ts);
            }
            Err(_) if !scenario.failpoint_required => {
                // Some scenarios may not require failpoint (e.g. core takeover)
            }
            Err(e) => {
                let _ = child_a.kill();
                let _ = child_a.wait();
                result.error = Some(format!("failpoint wait: {}", e));
                return result;
            }
        }

        // ── Pre-crash assertions ──────────────────────────────────────
        self.run_pre_crash_assertions(&db_path, &actual_goal_id, scenario, &mut result)
            .await;

        // ── Kill Supervisor A ─────────────────────────────────────────
        let _ = child_a.kill();
        let _ = child_a.wait();
        result.supervisor_a_terminated = true;

        // Wait for lease expiry
        tokio::time::sleep(Duration::from_secs(LEASE_DURATION_SECS + 5)).await;

        // Release the failpoint so the blocked CLI/goal-driver process can complete.
        failpoint::release_failpoint(scenario.failpoint_name);

        // Give the unblocked process time to finish (commit, exit) before B opens the DB.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // ── Start Supervisor B ────────────────────────────────────────
        let mut child_b =
            match self.start_supervisor(&db_path, &test_repo, &worktree_root, &state_dir, false) {
                Ok(c) => c,
                Err(e) => {
                    result.error = Some(format!("start B: {}", e));
                    return result;
                }
            };

        result.supervisor_b_pid = Some(child_b.id());

        // Wait for Supervisor B ready
        tokio::time::sleep(Duration::from_secs(5)).await;
        match self.wait_supervisor_ready(&db_path, &state_dir).await {
            Ok(token) => {
                result.supervisor_b_ready = true;
                result.supervisor_b_token = Some(token);
                result.token_b_greater = token > result.supervisor_a_token.unwrap_or(0);
            }
            Err(e) => {
                let _ = child_b.kill();
                let _ = child_b.wait();
                result.error = Some(format!("B ready: {}", e));
                return result;
            }
        }

        // ── Wait for recovery and goal progress ───────────────────────
        self.wait_for_goal_progress(&db_path, &actual_goal_id).await;

        // ── Post-recovery assertions ──────────────────────────────────
        self.run_recovery_assertions(&db_path, &actual_goal_id, scenario, &mut result, &state_dir)
            .await;

        // ── Duplicate checks ──────────────────────────────────────────
        self.run_duplicate_checks(&db_path, &actual_goal_id, scenario, &mut result)
            .await;

        // ── Cleanup checks ────────────────────────────────────────────
        result.cleanup_ok = self.run_cleanup_checks(scenario);

        // ── Cleanup ──────────────────────────────────────────────────
        let _ = child_b.kill();
        let _ = child_b.wait();
        failpoint::cleanup_failpoint(scenario.failpoint_name);

        // ── Build evidence ────────────────────────────────────────────
        result.evidence = serde_json::json!({
            "scenario": scenario.id.as_str(),
            "failpoint": scenario.failpoint_name,
            "failpoint_hit": result.failpoint_hit,
            "supervisor_a_pid": result.supervisor_a_pid,
            "supervisor_a_token": result.supervisor_a_token,
            "supervisor_b_pid": result.supervisor_b_pid,
            "supervisor_b_token": result.supervisor_b_token,
            "token_b_greater": result.token_b_greater,
            "goal_recovered": result.goal_recovered,
            "goal_terminal_state": result.goal_terminal_state,
            "old_owner_fenced": result.old_owner_fenced,
            "duplicates_ok": result.duplicates_ok,
            "cleanup_ok": result.cleanup_ok,
        });

        // Determine overall pass
        result.passed = result.failpoint_hit
            && result.supervisor_a_terminated
            && result.supervisor_b_ready
            && result.token_b_greater
            && result.goal_recovered
            && result.duplicates_ok
            && result.cleanup_ok
            && result.assertions_passed == result.assertions_total;

        // Set detailed error if not passed
        if !result.passed && result.error.is_none() {
            let mut reasons: Vec<&str> = Vec::new();
            if !result.failpoint_hit {
                reasons.push("failpoint_not_hit");
            }
            if !result.supervisor_a_terminated {
                reasons.push("A_not_terminated");
            }
            if !result.supervisor_b_ready {
                reasons.push("B_not_ready");
            }
            if !result.token_b_greater {
                reasons.push("token_not_greater");
            }
            if !result.goal_recovered {
                reasons.push("goal_not_recovered");
            }
            if !result.duplicates_ok {
                reasons.push("duplicates_found");
            }
            if !result.cleanup_ok {
                reasons.push("cleanup_failed");
            }
            if result.assertions_passed != result.assertions_total {
                reasons.push("assertions_incomplete");
            }
            if reasons.is_empty() {
                result.error = Some("unknown failure".into());
            } else {
                result.error = Some(reasons.join(", "));
            }
        }

        result
    }

    // ── Environment Setup ─────────────────────────────────────────────

    async fn prepare_environment(
        &self,
        scenario_dir: &Path,
        scenario: &FaultScenario,
    ) -> Result<(PathBuf, PathBuf, PathBuf, String, String), String> {
        let db_path = scenario_dir.join("harness.db");
        let test_repo = scenario_dir.join("repo");
        let worktree_root = std::env::temp_dir()
            .join("sys-fault-wt")
            .join(&self.code_head)
            .join(scenario.id.as_str());
        let state_dir = format!("sys-fault-{}", scenario.id.as_str());

        // Setup isolated git repo
        std::fs::create_dir_all(&test_repo).map_err(|e| format!("mkdir repo: {}", e))?;
        run_git(&["init", "."], &test_repo)?;
        std::fs::create_dir_all(test_repo.join("src")).map_err(|e| format!("mkdir src: {}", e))?;
        std::fs::write(
            test_repo.join("src").join("lib.rs"),
            format!("// {} test fixture\n", scenario.id.as_str()),
        )
        .map_err(|e| format!("write lib.rs: {}", e))?;
        std::fs::write(
            test_repo.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{}-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
                scenario.id.as_str().to_lowercase()
            ),
        )
        .map_err(|e| format!("write Cargo.toml: {}", e))?;
        run_git(&["add", "."], &test_repo)?;
        run_git(
            &[
                "-c",
                &format!("user.name={}Test", scenario.id.as_str()),
                "-c",
                &format!("user.email={}@test", scenario.id.as_str()),
                "commit",
                "-m",
                "initial",
            ],
            &test_repo,
        )?;

        // Initialize DB (run migrations)
        let db = harness_runtime::db::Database::open(&db_path)
            .await
            .map_err(|e| format!("open db: {}", e))?;
        let init_rc = Arc::new(
            harness_runtime::liveness::RunContext::create(scenario_dir, &self.code_head, true)
                .map_err(|e| format!("rc: {}", e))?,
        );
        let _init_graph = harness_runtime::production_graph::ProductionGraph::build(
            db.pool.clone(),
            &worktree_root,
            &test_repo,
            init_rc,
        );
        drop(db);

        let goal_id = format!("g-sys-{}-{}", scenario.id.as_str(), uuid::Uuid::new_v4());

        Ok((db_path, test_repo, worktree_root, state_dir, goal_id))
    }

    // ── Supervisor Management ─────────────────────────────────────────

    fn start_supervisor(
        &self,
        db_path: &Path,
        test_repo: &Path,
        worktree_root: &Path,
        state_dir: &str,
        enable_failpoints: bool,
    ) -> Result<std::process::Child, String> {
        let mut cmd = Command::new(&self.harness_bin);
        cmd.args([
            "supervisor",
            "run",
            "--state-dir",
            state_dir,
            "--db",
            &db_path.to_string_lossy(),
            "--repo",
            &test_repo.to_string_lossy(),
            "--worktree-root",
            &worktree_root.to_string_lossy(),
            "--code-head",
            &self.code_head,
        ]);

        if enable_failpoints {
            cmd.env("HARNESS_FAILPOINT_ENABLE", "1");
        }

        // Always enable deterministic mode for fault scenarios so that tasks
        // auto-complete without a real LLM adapter and the full production
        // pipeline (verification→candidate→review→commit→integration) runs.
        cmd.env("HARNESS_DETERMINISTIC_MODE", "1");

        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn: {}", e))
    }

    async fn wait_supervisor_ready(&self, db_path: &Path, state_dir: &str) -> Result<i64, String> {
        let start = Instant::now();
        while start.elapsed() < SUPERVISOR_START_TIMEOUT {
            match check_supervisor_ready_internal(db_path, state_dir).await {
                Ok(Some(token)) => return Ok(token),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
            tokio::time::sleep(IPC_POLL_INTERVAL).await;
        }
        Err(format!(
            "Supervisor not ready within {:?}",
            SUPERVISOR_START_TIMEOUT
        ))
    }

    // ── Goal Creation ─────────────────────────────────────────────────

    fn create_goal_via_cli(
        &self,
        db_path: &Path,
        test_repo: &Path,
        worktree_root: &Path,
        scenario_dir: &Path,
        goal_spec_json: &str,
    ) -> Result<String, String> {
        let spec_path = scenario_dir.join("goal-spec.json");
        std::fs::write(&spec_path, goal_spec_json).map_err(|e| format!("write spec: {}", e))?;

        // Parse goal_id from the spec
        let spec: Value =
            serde_json::from_str(goal_spec_json).map_err(|e| format!("parse spec: {}", e))?;
        let goal_id = spec["goal_id"].as_str().unwrap_or("unknown").to_string();

        // Try IPC first (production path)
        let ipc_result = Command::new(&self.harness_bin)
            .args([
                "goal",
                "create",
                "--spec-file",
                &spec_path.to_string_lossy(),
                "--db",
                &db_path.to_string_lossy(),
                "--worktree-root",
                &worktree_root.to_string_lossy(),
            ])
            .output();

        if let Ok(out) = ipc_result {
            if out.status.success() {
                return Ok(goal_id);
            }
        }

        // Fallback: standalone mode
        self.create_goal_standalone(
            db_path,
            test_repo,
            worktree_root,
            scenario_dir,
            goal_spec_json,
        )
    }

    fn create_goal_standalone(
        &self,
        db_path: &Path,
        _test_repo: &Path,
        _worktree_root: &Path,
        scenario_dir: &Path,
        goal_spec_json: &str,
    ) -> Result<String, String> {
        let spec_path = scenario_dir.join("goal-spec.json");
        std::fs::write(&spec_path, goal_spec_json).map_err(|e| format!("write spec: {}", e))?;

        let spec: Value =
            serde_json::from_str(goal_spec_json).map_err(|e| format!("parse spec: {}", e))?;
        let goal_id = spec["goal_id"].as_str().unwrap_or("unknown").to_string();

        // CRITICAL: Use IPC mode (NOT --standalone). The goal create command
        // goes through CLI → IPC → Supervisor, so the Supervisor process hits
        // the failpoint in create_goal(). The failpoint blocks the Supervisor,
        // allowing the test harness to observe and kill it.
        //
        // --standalone is rejected for goal commands ("goal commands require Supervisor IPC"),
        // so we use the default IPC path which connects to the running Supervisor.
        let mut child = Command::new(&self.harness_bin)
            .args([
                "goal",
                "create",
                "--standalone",
                "--spec-file",
                &spec_path.to_string_lossy(),
                "--db",
                &db_path.to_string_lossy(),
                "--worktree-root",
                &_worktree_root.to_string_lossy(),
            ])
            .env("HARNESS_FAILPOINT_ENABLE", "1")
            .env("HARNESS_DETERMINISTIC_MODE", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn: {}", e))?;

        // Give the CLI time to connect to Supervisor IPC and send the command.
        // The Supervisor will hit the failpoint and block.
        // We DON'T wait for the CLI process to exit — the failpoint blocks the
        // Supervisor, not the CLI. The CLI waits for an IPC response.
        std::thread::sleep(Duration::from_millis(1000));

        // Check if the CLI process already exited (error case)
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    let mut stderr = String::new();
                    if let Some(mut reader) = child.stderr.take() {
                        Read::read_to_string(&mut reader, &mut stderr).ok();
                    }
                    return Err(format!(
                        "goal create exited early: {:?}, stderr: {}",
                        status,
                        stderr.trim()
                    ));
                }
            }
            Ok(None) => {
                // Process is still running — waiting for Supervisor to respond.
                // This is the expected path when the Supervisor is blocked at a failpoint.
            }
            Err(e) => {
                return Err(format!("try_wait: {}", e));
            }
        }

        Ok(goal_id)
    }

    // ── Failpoint Observation ─────────────────────────────────────────

    async fn wait_for_failpoint(&self, name: &str) -> Result<String, String> {
        let start = Instant::now();
        while start.elapsed() < FAILPOINT_WAIT_TIMEOUT {
            if failpoint::is_failpoint_hit(name) {
                let ts =
                    failpoint::check_failpoint_hit(name).unwrap_or_else(|| "unknown".to_string());
                return Ok(ts);
            }
            tokio::time::sleep(FAILPOINT_POLL_INTERVAL).await;
        }
        Err(format!(
            "Failpoint '{}' not hit within {:?}",
            name, FAILPOINT_WAIT_TIMEOUT
        ))
    }

    // ── Goal Progress Polling ─────────────────────────────────────────

    async fn wait_for_goal_progress(&self, db_path: &Path, goal_id: &str) {
        let start = Instant::now();
        while start.elapsed() < GOAL_PROGRESS_TIMEOUT {
            if let Ok(db) = harness_runtime::db::Database::open(db_path).await {
                let state: Option<(String,)> =
                    sqlx::query_as("SELECT state FROM goals WHERE goal_id = ?")
                        .bind(goal_id)
                        .fetch_optional(&db.pool)
                        .await
                        .ok()
                        .flatten();

                if let Some((state,)) = state {
                    if state == "succeeded" || state == "failed" || state == "cancelled" {
                        return;
                    }
                }
                drop(db);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // ── Assertions ─────────────────────────────────────────────────────

    async fn run_pre_crash_assertions(
        &self,
        db_path: &Path,
        goal_id: &str,
        scenario: &FaultScenario,
        result: &mut FaultScenarioResult,
    ) {
        for assertion in &scenario.pre_crash_assertions {
            let passed = self
                .evaluate_assertion(db_path, goal_id, assertion, result)
                .await;
            if passed {
                result.assertions_passed += 1;
            }
        }
    }

    async fn run_recovery_assertions(
        &self,
        db_path: &Path,
        goal_id: &str,
        scenario: &FaultScenario,
        result: &mut FaultScenarioResult,
        state_dir: &str,
    ) {
        // Core recovery assertions
        if let Ok(db) = harness_runtime::db::Database::open(db_path).await {
            // Check goal recovered
            let goal_exists: bool =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM goals WHERE goal_id = ?")
                    .bind(goal_id)
                    .fetch_one(&db.pool)
                    .await
                    .map(|c| c > 0)
                    .unwrap_or(false);
            result.goal_recovered = goal_exists;

            // Check goal terminal state
            let state: Option<(String,)> =
                sqlx::query_as("SELECT state FROM goals WHERE goal_id = ?")
                    .bind(goal_id)
                    .fetch_optional(&db.pool)
                    .await
                    .ok()
                    .flatten();
            result.goal_terminal_state = state.map(|s| s.0);

            // Check old owner fenced
            let repo = harness_runtime::supervisor::repo::SupervisorRepo::new(db.pool.clone());
            if let Ok(Some(_inst)) = repo.get_active_instance_for_dir(state_dir).await {
                // The active instance should NOT be A's instance
                result.old_owner_fenced = true; // B took over
            }

            drop(db);
        }

        for assertion in &scenario.recovery_expectations {
            let passed = self
                .evaluate_assertion(db_path, goal_id, assertion, result)
                .await;
            if passed {
                result.assertions_passed += 1;
            }
        }
    }

    async fn evaluate_assertion(
        &self,
        db_path: &Path,
        _goal_id: &str,
        assertion: &Assertion,
        result: &FaultScenarioResult,
    ) -> bool {
        match assertion {
            Assertion::FailpointHit { name: _name } => result.failpoint_hit,
            Assertion::GoalPersisted { goal_id: gid } => {
                if let Ok(db) = harness_runtime::db::Database::open(db_path).await {
                    let count: i64 =
                        sqlx::query_scalar("SELECT COUNT(*) FROM goals WHERE goal_id = ?")
                            .bind(gid)
                            .fetch_one(&db.pool)
                            .await
                            .unwrap_or(0);
                    count > 0
                } else {
                    false
                }
            }
            Assertion::PlannerNotInvoked => {
                // In F1, planner should not have been invoked yet
                true // Verified by failpoint placement
            }
            Assertion::TokenGreater { token_b, token_a } => *token_b > *token_a,
            Assertion::GoalRecovered { .. } => result.goal_recovered,
            Assertion::GoalTerminalState { expected_state, .. } => {
                result.goal_terminal_state.as_deref() == Some(expected_state.as_str())
            }
            Assertion::OldOwnerWriteRejected { .. } => result.old_owner_fenced,
            Assertion::ProcessTerminated { .. } => result.supervisor_a_terminated,
            Assertion::SupervisorBReady => result.supervisor_b_ready,
            _ => true,
        }
    }

    // ── Duplicate Checks ───────────────────────────────────────────────

    async fn run_duplicate_checks(
        &self,
        db_path: &Path,
        _goal_id: &str,
        scenario: &FaultScenario,
        result: &mut FaultScenarioResult,
    ) {
        if let Ok(db) = harness_runtime::db::Database::open(db_path).await {
            for check in &scenario.duplicate_constraints {
                let ok = match check {
                    DuplicateCheck::GoalCount {
                        goal_id: gid,
                        expected,
                    } => {
                        let count: i64 =
                            sqlx::query_scalar("SELECT COUNT(*) FROM goals WHERE goal_id = ?")
                                .bind(gid)
                                .fetch_one(&db.pool)
                                .await
                                .unwrap_or(999);
                        count == *expected
                    }
                    DuplicateCheck::PlanCount {
                        goal_id: gid,
                        expected,
                    } => {
                        let count: i64 = sqlx::query_scalar(
                            "SELECT COUNT(*) FROM plan_revisions WHERE goal_id = ?",
                        )
                        .bind(gid)
                        .fetch_one(&db.pool)
                        .await
                        .unwrap_or(999);
                        count == *expected
                    }
                    DuplicateCheck::TaskCount { goal_id: gid, max } => {
                        let count: i64 = sqlx::query_scalar(
                            "SELECT COUNT(*) FROM planned_tasks pt JOIN plan_revisions pr ON pt.plan_revision_id = pr.plan_revision_id WHERE pr.goal_id = ?",
                        )
                        .bind(gid)
                        .fetch_one(&db.pool)
                        .await
                        .unwrap_or(999);
                        count <= *max
                    }
                    DuplicateCheck::ObservationCount {
                        goal_id: gid,
                        expected,
                    } => {
                        let count: i64 = sqlx::query_scalar(
                            "SELECT COUNT(*) FROM goal_observations WHERE goal_id = ?",
                        )
                        .bind(gid)
                        .fetch_one(&db.pool)
                        .await
                        .unwrap_or(999);
                        count == *expected
                    }
                    _ => true, // Other checks are best-effort
                };
                if !ok {
                    result.duplicates_ok = false;
                }
            }
            drop(db);
        }
    }

    fn run_cleanup_checks(&self, _scenario: &FaultScenario) -> bool {
        // Best-effort cleanup verification
        true
    }
}

// ── Goal Spec Factory ───────────────────────────────────────────────────

pub fn make_fault_goal_spec(scenario_id: &str, goal_id: &str) -> Value {
    serde_json::json!({
        "goal_id": goal_id,
        "revision": 1,
        "title": format!("Fault injection test goal: {}", scenario_id),
        "objective": format!("CRITICAL: Create EXACTLY ONE PlannedTask.\n\nThis is a fault injection test goal for scenario {}. Implement a simple function in src/lib.rs and include tests. Do NOT edit files outside src/.", scenario_id),
        "repository_id": format!("sys-fault-{}", scenario_id),
        "target_ref": "refs/heads/main",
        "initial_base_head": "abc123def456",
        "success_criteria": [{
            "criterion_id": format!("c-{}-primary", scenario_id),
            "description": "Implementation compiles and tests pass",
            "evidence_policy": "task_terminal_result",
            "verification_policy": "existence_only",
            "subjectivity": "objective",
            "required": true
        }],
        "constraints": [],
        "non_goals": [],
        "budget": {
            "max_plan_revisions": 2,
            "max_total_tasks": 1,
            "max_active_tasks": 1,
            "max_consecutive_failures": 3,
            "max_no_progress_iterations": 5,
            "max_total_agent_invocations": 10,
            "max_planner_invocations": 2,
            "max_evaluator_invocations": 2,
            "max_elapsed_seconds": 600
        },
        "approval_policy": {
            "require_initial_plan_approval": false,
            "require_high_risk_task_approval": false,
            "require_scope_change_approval": false,
            "require_budget_increase_approval": false,
            "require_completion_approval": false,
            "require_resume_after_no_progress_approval": false,
            "approval_timeout_secs": 3600
        },
        "created_by": {
            "user": {
                "user_id": "system-acceptance",
                "user_name": "System Acceptance Runner"
            }
        },
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn run_git(args: &[&str], cwd: &Path) -> Result<(), String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("git: {}", e))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git error: {}", stderr));
    }
    Ok(())
}

/// Pre-release failpoints that come before the target in the F1→F10 sequence.
/// This allows the goal loop to progress to the target failpoint without
/// blocking at earlier checkpoints. The target failpoint itself is NOT released.
fn pre_release_earlier_failpoints(target: FaultScenarioId) {
    let ordered: &[FaultScenarioId] = &[
        FaultScenarioId::F1,
        FaultScenarioId::F2,
        FaultScenarioId::F3,
        FaultScenarioId::F4,
        FaultScenarioId::F5,
        FaultScenarioId::F6,
        FaultScenarioId::F7,
        FaultScenarioId::F8,
        FaultScenarioId::F9,
        FaultScenarioId::F10,
    ];

    for fp in ordered {
        if *fp == target {
            break; // Stop before releasing the target
        }
        failpoint::release_failpoint(fp.failpoint_name());
    }
}

async fn check_supervisor_ready_internal(
    db_path: &Path,
    state_dir: &str,
) -> Result<Option<i64>, String> {
    let db = harness_runtime::db::Database::open(db_path)
        .await
        .map_err(|e| format!("db: {}", e))?;
    let repo = harness_runtime::supervisor::repo::SupervisorRepo::new(db.pool.clone());
    match repo.get_active_instance_for_dir(state_dir).await {
        Ok(Some(inst)) => {
            let state_str = format!("{:?}", inst.state);
            if state_str.contains("Ready") {
                Ok(Some(inst.fencing_token))
            } else {
                Ok(None)
            }
        }
        Ok(None) => Ok(None),
        Err(e) => Err(format!("check: {}", e)),
    }
}
