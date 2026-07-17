//! VerificationExecutionService — deterministic verification command execution.
//! All processes go through ProcessManager. No direct Command, AgentAdapter, retry, or Worktree deletion.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::verification::{
    FailureClassification, VerificationStepKind, VerificationStepResult, VerificationStepStatus,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use super::content_validator::VerificationContentValidator;
use crate::process::manager::ProcessManager;
use crate::process::types::{
    CapturePolicy, ProcessSpec, ProcessState, ProcessTermination, StdinMode,
};

// ── Process result ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ProcessResult {
    pub exit_code: i32,
    pub duration_ms: u64,
    pub stdout_preview: Option<String>,
    pub stderr_preview: Option<String>,
    pub timed_out: bool,
    pub terminated: bool,
}

// ── Step execution request ────────────────────────────────────────────

pub struct StepExecutionRequest {
    pub verification_run_id: String,
    pub step_id: String,
    pub plan_id: String,
    pub execution_id: String,
    pub task_id: String,
    pub worktree_id: String,
    pub worktree_path: PathBuf,
    pub expected_fencing: i64,
    pub verification_owner_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub working_directory: PathBuf,
    pub timeout: Duration,
    pub allowed_env_var_names: Vec<String>,
    pub env_overrides: HashMap<String, String>,
    pub step_kind: VerificationStepKind,
    pub required: bool,
    pub sequence_index: u32,
}

// ── Step execution outcome ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum StepExecutionOutcome {
    Completed {
        step_result: VerificationStepResult,
        exit_code: i32,
        duration_ms: u64,
    },
    PolicyBlocked {
        reason: String,
    },
    WorkspaceDenied {
        reason: String,
    },
    OwnershipLost {
        reason: String,
    },
    InfrastructureError {
        reason: String,
    },
    Duplicate {
        existing_op_id: String,
    },
    IdempotencyConflict {
        existing_hash: String,
        new_hash: String,
    },
}

// ── Process executor trait (testable) ─────────────────────────────────

/// Runs a command and returns the result. Test fakes track start counts.
#[async_trait::async_trait]
pub trait ProcessExecutor: Send + Sync {
    async fn execute(
        &self,
        executable: &Path,
        args: &[String],
        cwd: &Path,
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessResult;
}

// ── ProcessManager adapter (production only path) ─────────────────────

/// Routes ALL verification process execution through ProcessManager.
/// Never calls std::process::Command or tokio::process::Command directly.
pub struct ProcessManagerAdapter {
    manager: Arc<ProcessManager>,
}

impl ProcessManagerAdapter {
    pub fn new(manager: Arc<ProcessManager>) -> Self {
        Self { manager }
    }
}

#[async_trait::async_trait]
impl ProcessExecutor for ProcessManagerAdapter {
    async fn execute(
        &self,
        executable: &Path,
        args: &[String],
        cwd: &Path,
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessResult {
        let spec = ProcessSpec {
            executable: executable.to_path_buf(),
            args: args.to_vec(),
            working_directory: cwd.to_path_buf(),
            env_overrides: env.clone(),
            env_removals: vec![],
            stdin_mode: StdinMode::Closed,
            timeout,
            graceful_shutdown_timeout: Duration::from_secs(2),
            stdout_capture: CapturePolicy::Spool {
                max_memory_bytes: 4096,
            },
            stderr_capture: CapturePolicy::Spool {
                max_memory_bytes: 4096,
            },
            output_byte_limit: 64 * 1024,
            spool_dir: Some(
                std::env::temp_dir().join(format!("harness-vrfy-{}", uuid::Uuid::new_v4())),
            ),
            known_secrets: vec![],
            allowed_env_var_names: env.keys().cloned().collect(),
            execution_id: format!("vrfy-{}", uuid::Uuid::new_v4()),
            runtime_profile_id: "verification".into(),
        };

        let start = std::time::Instant::now();
        let handle = match self.manager.spawn(&spec).await {
            Ok(h) => h,
            Err(_) => {
                return ProcessResult {
                    exit_code: -1,
                    duration_ms: 0,
                    stdout_preview: None,
                    stderr_preview: None,
                    timed_out: false,
                    terminated: false,
                }
            }
        };

        // Poll until terminal.
        loop {
            let state = handle.state.read().await.clone();
            match state {
                ProcessState::Completed { outcome } => {
                    let duration_ms = start.elapsed().as_millis() as u64;
                    let timed_out = outcome.termination == ProcessTermination::Timeout;
                    return ProcessResult {
                        exit_code: outcome.exit_code.unwrap_or(-1),
                        duration_ms,
                        stdout_preview: outcome.stdout_preview,
                        stderr_preview: outcome.stderr_preview,
                        timed_out,
                        terminated: true,
                    };
                }
                ProcessState::Starting | ProcessState::Running => {
                    if start.elapsed() > timeout + Duration::from_secs(5) {
                        return ProcessResult {
                            exit_code: -1,
                            duration_ms: timeout.as_millis() as u64,
                            stdout_preview: None,
                            stderr_preview: None,
                            timed_out: true,
                            terminated: false,
                        };
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

// ── Fake executor for tests (tracks start count) ──────────────────────

pub struct FakeProcessExecutor {
    pub start_count: Arc<AtomicUsize>,
    pub exit_code: std::sync::Mutex<i32>,
    pub fail_spawn: std::sync::atomic::AtomicBool,
    pub stdout_text: std::sync::Mutex<String>,
    pub stderr_text: std::sync::Mutex<String>,
    pub hang_forever: std::sync::atomic::AtomicBool,
}

impl FakeProcessExecutor {
    pub fn new(start_count: Arc<AtomicUsize>) -> Self {
        Self {
            start_count,
            exit_code: std::sync::Mutex::new(0),
            fail_spawn: std::sync::atomic::AtomicBool::new(false),
            stdout_text: std::sync::Mutex::new(String::new()),
            stderr_text: std::sync::Mutex::new(String::new()),
            hang_forever: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl ProcessExecutor for FakeProcessExecutor {
    async fn execute(
        &self,
        _executable: &Path,
        _args: &[String],
        _cwd: &Path,
        _env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessResult {
        self.start_count.fetch_add(1, Ordering::SeqCst);

        if self.fail_spawn.load(Ordering::SeqCst) {
            return ProcessResult {
                exit_code: -1,
                duration_ms: 0,
                stdout_preview: None,
                stderr_preview: None,
                timed_out: false,
                terminated: false,
            };
        }

        if self.hang_forever.load(Ordering::SeqCst) {
            tokio::time::sleep(timeout + Duration::from_millis(100)).await;
            return ProcessResult {
                exit_code: -1,
                duration_ms: timeout.as_millis() as u64,
                stdout_preview: None,
                stderr_preview: None,
                timed_out: true,
                terminated: false,
            };
        }

        let ec = *self.exit_code.lock().unwrap();
        let stdout = self.stdout_text.lock().unwrap().clone();
        let stderr = self.stderr_text.lock().unwrap().clone();
        ProcessResult {
            exit_code: ec,
            duration_ms: 10,
            terminated: true,
            stdout_preview: if stdout.is_empty() {
                None
            } else {
                Some(stdout)
            },
            stderr_preview: if stderr.is_empty() {
                None
            } else {
                Some(stderr)
            },
            timed_out: false,
        }
    }
}

// ── Service ───────────────────────────────────────────────────────────

pub struct VerificationExecutionService {
    pool: SqlitePool,
    executor: Arc<dyn ProcessExecutor>,
}

impl VerificationExecutionService {
    pub fn new(pool: SqlitePool, executor: Arc<dyn ProcessExecutor>) -> Self {
        Self { pool, executor }
    }

    /// Execute a single verification step. Returns structured outcome.
    pub async fn execute_step(&self, req: &StepExecutionRequest) -> StepExecutionOutcome {
        // ── 0. Idempotency check ──────────────────────────────────
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT op_id, request_hash, status FROM verification_step_operations WHERE idempotency_key = ?",
        )
        .bind(&req.idempotency_key).fetch_optional(&self.pool).await.unwrap_or(None);

        if let Some((existing_op_id, existing_hash, _existing_status)) = existing {
            if existing_hash == req.request_hash {
                return StepExecutionOutcome::Duplicate { existing_op_id };
            }
            return StepExecutionOutcome::IdempotencyConflict {
                existing_hash,
                new_hash: req.request_hash.clone(),
            };
        }

        // ── 1. Ownership consistency ──────────────────────────────
        if let Some(outcome) = self.check_ownership(req).await {
            return outcome;
        }

        // ── 2. Workspace validation ───────────────────────────────
        if let Some(outcome) = self.validate_workspace(&req.working_directory, &req.worktree_path) {
            return outcome;
        }

        // ── 3. Policy check ───────────────────────────────────────
        if let Some(outcome) = evaluate_policy(&req.executable, &req.args) {
            return outcome;
        }

        // ── 4. Record step operation ──────────────────────────────
        let op_id = format!("step-op-{}", Uuid::new_v4());
        if let Err(e) = self.insert_step_op(&op_id, req).await {
            return StepExecutionOutcome::InfrastructureError {
                reason: format!("insert step op: {e}"),
            };
        }

        // ── 5. Transition to Running ──────────────────────────────
        let _ = sqlx::query(
            "UPDATE verification_step_operations SET status='running', process_start_count=process_start_count+1, started_at=datetime('now') WHERE op_id=?",
        ).bind(&op_id).execute(&self.pool).await;

        // ── 6. Execute ────────────────────────────────────────────
        let result = self
            .executor
            .execute(
                &req.executable,
                &req.args,
                &req.working_directory,
                &req.env_overrides,
                req.timeout,
            )
            .await;

        let duration_ms = result.duration_ms;
        let exit_code = result.exit_code;

        // ── 7. Classify ───────────────────────────────────────────
        let (status, _classification) =
            classify(exit_code, &req.step_kind, result.timed_out, req.timeout);

        // ── 8. Validate output (no secrets) ───────────────────────
        if let Some(ref stdout) = result.stdout_preview {
            if VerificationContentValidator::validate_text(stdout).is_err() {
                let _ = self
                    .mark_step_terminal(&op_id, "validation_failed", "secret in stdout")
                    .await;
                return StepExecutionOutcome::InfrastructureError {
                    reason: "output contained secrets".into(),
                };
            }
        }

        // ── 9. Persist step result ────────────────────────────────
        let result_id = format!("sr-{}", Uuid::new_v4());
        let step_result = VerificationStepResult {
            result_id: result_id.clone(),
            run_id: req.verification_run_id.clone(),
            step_id: req.step_id.clone(),
            plan_id: req.plan_id.clone(),
            status: status.clone(),
            detail_json: Some(
                serde_json::json!({"exit_code": exit_code, "duration_ms": duration_ms}).to_string(),
            ),
            started_at: None,
            completed_at: None,
            duration_ms: Some(duration_ms),
            error_message: if exit_code != 0 {
                Some(format!("exit {exit_code}"))
            } else {
                None
            },
        };

        let status_str = status_to_str(&status);
        let outcome_json = step_result.detail_json.clone().unwrap_or_default();
        let _ = sqlx::query(
            "UPDATE verification_step_operations SET status=?, process_exit_code=?, duration_ms=?, outcome_json=?, completed_at=datetime('now') WHERE op_id=?",
        )
        .bind(status_str).bind(exit_code).bind(duration_ms as i64).bind(&outcome_json).bind(&op_id)
        .execute(&self.pool).await;

        StepExecutionOutcome::Completed {
            step_result,
            exit_code,
            duration_ms,
        }
    }

    async fn check_ownership(&self, req: &StepExecutionRequest) -> Option<StepExecutionOutcome> {
        let lc_row: Option<(String,)> =
            sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id = ?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();

        match lc_row {
            Some((lc,)) if lc == "running" => {}
            Some((lc,)) => {
                return Some(StepExecutionOutcome::OwnershipLost {
                    reason: format!("run lifecycle is '{lc}'"),
                })
            }
            None => {
                return Some(StepExecutionOutcome::OwnershipLost {
                    reason: "run not found".into(),
                })
            }
        }

        let owner_row: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT owner_kind, owner_id, fencing_token FROM resource_handoffs WHERE execution_id = ?",
        ).bind(&req.execution_id).fetch_optional(&self.pool).await.ok().flatten();

        match owner_row {
            Some((kind, oid, fence)) => {
                if kind != "verification" || oid != req.verification_owner_id {
                    return Some(StepExecutionOutcome::OwnershipLost {
                        reason: format!("handoff owner is {kind}/{oid}"),
                    });
                }
                if fence != req.expected_fencing {
                    return Some(StepExecutionOutcome::OwnershipLost {
                        reason: format!("fencing mismatch: {fence} vs {}", req.expected_fencing),
                    });
                }
            }
            None => {
                return Some(StepExecutionOutcome::OwnershipLost {
                    reason: "handoff not found".into(),
                })
            }
        }
        None
    }

    fn validate_workspace(&self, cwd: &Path, wt_root: &Path) -> Option<StepExecutionOutcome> {
        if cwd.to_string_lossy().contains("..") {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: ".. traversal".into(),
            });
        }
        let cwd_canon = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
        let wt_canon = std::fs::canonicalize(wt_root).unwrap_or_else(|_| wt_root.to_path_buf());
        if !cwd_canon.starts_with(&wt_canon) {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: "cwd outside worktree".into(),
            });
        }
        if !cwd_canon.exists() {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: "cwd missing".into(),
            });
        }
        None
    }

    async fn insert_step_op(
        &self,
        op_id: &str,
        req: &StepExecutionRequest,
    ) -> Result<(), CoreError> {
        let cfg_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            req.executable.hash(&mut h);
            req.args.hash(&mut h);
            format!("{:016x}", h.finish())
        };
        sqlx::query(
            "INSERT INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES (?,?,?,?,?,?,?,?,'pending',?,?)",
        )
        .bind(op_id).bind(&req.verification_run_id).bind(&req.step_id).bind(&req.plan_id)
        .bind(&req.execution_id).bind(&cfg_hash).bind(&req.worktree_id).bind(req.expected_fencing)
        .bind(&req.idempotency_key).bind(&req.request_hash)
        .execute(&self.pool).await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert step op: {e}"), ErrorSource::System))?;
        Ok(())
    }

    async fn mark_step_terminal(
        &self,
        op_id: &str,
        status: &str,
        reason: &str,
    ) -> Result<(), CoreError> {
        sqlx::query("UPDATE verification_step_operations SET status=?, outcome_json=?, completed_at=datetime('now') WHERE op_id=?")
            .bind(status).bind(reason).bind(op_id).execute(&self.pool).await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("mark step: {e}"), ErrorSource::System))?;
        Ok(())
    }
}

// ── Policy evaluation ─────────────────────────────────────────────────

fn evaluate_policy(exe: &Path, args: &[String]) -> Option<StepExecutionOutcome> {
    let lower = exe.to_string_lossy().to_lowercase();
    if lower.contains("cmd.exe")
        || lower.contains("powershell")
        || lower.contains("bash")
        || lower.contains("sh")
    {
        return Some(StepExecutionOutcome::PolicyBlocked {
            reason: "shell denied by default".into(),
        });
    }
    for a in args {
        let al = a.to_lowercase();
        if al.contains("&&")
            || al.contains("||")
            || al.contains(";")
            || al.contains("`")
            || al.contains("$(")
        {
            return Some(StepExecutionOutcome::PolicyBlocked {
                reason: format!("metachar in arg: {a}"),
            });
        }
    }
    None
}

// ── Classification ────────────────────────────────────────────────────

fn classify(
    exit_code: i32,
    kind: &VerificationStepKind,
    timed_out: bool,
    timeout: Duration,
) -> (VerificationStepStatus, Option<FailureClassification>) {
    if timed_out {
        return (
            VerificationStepStatus::Failed,
            Some(FailureClassification::TimeoutExpired {
                duration_ms: timeout.as_millis() as u64,
            }),
        );
    }
    if exit_code == 0 {
        return (VerificationStepStatus::Passed, None);
    }
    let fc = match kind {
        VerificationStepKind::AcceptanceCheck | VerificationStepKind::CustomCheck => {
            FailureClassification::AcceptanceTestFailure {
                failed_checks: vec![format!("exit={exit_code}")],
            }
        }
        _ => FailureClassification::InfrastructureError {
            reason: format!("exit={exit_code}"),
        },
    };
    (VerificationStepStatus::Failed, Some(fc))
}

fn status_to_str(s: &VerificationStepStatus) -> &'static str {
    match s {
        VerificationStepStatus::Passed => "completed",
        VerificationStepStatus::Failed => "failed",
        VerificationStepStatus::Blocked => "blocked",
        VerificationStepStatus::Error => "error",
        VerificationStepStatus::Skipped => "skipped",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use std::sync::atomic::AtomicUsize;
    use tempfile::TempDir;

    struct TestCtx {
        svc: VerificationExecutionService,
        db: Database,
        executor: Arc<FakeProcessExecutor>,
        start_count: Arc<AtomicUsize>,
        worktree_dir: TempDir,
        _td: TempDir, // keep DB tempdir alive
        db_dir: PathBuf,
    }

    async fn setup() -> TestCtx {
        let td = tempfile::tempdir().unwrap();
        let db_path = td.path().join("exec.db");
        let db_dir = td.path().to_path_buf();
        let db = Database::open(&db_path).await.unwrap();
        let pool = db.pool.clone();

        // Seed prerequisite data.
        sqlx::query(
            "INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')").execute(&pool).await.unwrap();
        // Verification run — Running (ownership already taken).
        sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-1','t1','e1','p1','hash-aaa',1,'[]')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, idempotency_key, request_hash) VALUES ('run-1','plan-1','hash-aaa',1,'e1','t1','p1','running','ikey-run','hash-run')").execute(&pool).await.unwrap();
        // Handoff — VerificationOwned.
        sqlx::query("INSERT INTO resource_handoffs (handoff_id, project_id, task_id, execution_id, worktree_id, lease_id, fencing_token, owner_kind, owner_id, status) VALUES ('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(&pool).await.unwrap();

        // Real worktree directory.
        let wt_dir = tempfile::tempdir().unwrap();

        let start_count = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(FakeProcessExecutor::new(start_count.clone()));
        let svc = VerificationExecutionService::new(pool, executor.clone());

        TestCtx {
            svc,
            db,
            executor,
            start_count,
            worktree_dir: wt_dir,
            _td: td,
            db_dir,
        }
    }

    fn make_req(ctx: &TestCtx, ikey: &str, hash: &str) -> StepExecutionRequest {
        StepExecutionRequest {
            verification_run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            execution_id: "e1".into(),
            task_id: "t1".into(),
            worktree_id: "wt1".into(),
            worktree_path: ctx.worktree_dir.path().to_path_buf(),
            expected_fencing: 5,
            verification_owner_id: "verify-run-1".into(),
            idempotency_key: ikey.into(),
            request_hash: hash.into(),
            executable: PathBuf::from("test-exe"),
            args: vec!["--check".into()],
            working_directory: ctx.worktree_dir.path().to_path_buf(),
            timeout: Duration::from_secs(5),
            allowed_env_var_names: vec![],
            env_overrides: HashMap::new(),
            step_kind: VerificationStepKind::AcceptanceCheck,
            required: true,
            sequence_index: 0,
        }
    }

    // ── Normal execution ────────────────────────────────────────────
    #[tokio::test]
    async fn test_normal_execution_success() {
        let ctx = setup().await;
        ctx.executor.exit_code.lock().unwrap().clone_from(&0);
        let req = make_req(&ctx, "ikey-norm", "hash-norm");

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(result, StepExecutionOutcome::Completed { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_nonzero_exit_failed() {
        let ctx = setup().await;
        *ctx.executor.exit_code.lock().unwrap() = 1;
        let req = make_req(&ctx, "ikey-fail", "hash-fail");

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(
            result,
            StepExecutionOutcome::Completed { exit_code: 1, .. }
        ));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 1);
    }

    // ── Ownership violations → zero process start ───────────────────
    #[tokio::test]
    async fn test_not_running_rejected() {
        let ctx = setup().await;
        // Change run lifecycle to something other than running.
        sqlx::query("UPDATE verification_runs SET lifecycle='created' WHERE run_id='run-1'")
            .execute(&ctx.db.pool)
            .await
            .unwrap();
        let req = make_req(&ctx, "ikey-own1", "hash-own1");

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(result, StepExecutionOutcome::OwnershipLost { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_wrong_owner_rejected() {
        let ctx = setup().await;
        let mut req = make_req(&ctx, "ikey-own2", "hash-own2");
        req.verification_owner_id = "wrong-owner".into();

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(result, StepExecutionOutcome::OwnershipLost { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_stale_fencing_rejected() {
        let ctx = setup().await;
        let mut req = make_req(&ctx, "ikey-own3", "hash-own3");
        req.expected_fencing = 99;

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(result, StepExecutionOutcome::OwnershipLost { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    // ── Workspace violations → zero process start ───────────────────
    #[tokio::test]
    async fn test_cwd_outside_worktree_rejected() {
        let ctx = setup().await;
        let mut req = make_req(&ctx, "ikey-ws1", "hash-ws1");
        req.working_directory = std::env::temp_dir(); // outside worktree

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(
            result,
            StepExecutionOutcome::WorkspaceDenied { .. }
        ));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_dot_dot_traversal_rejected() {
        let ctx = setup().await;
        let mut req = make_req(&ctx, "ikey-ws2", "hash-ws2");
        req.working_directory = ctx.worktree_dir.path().join("..");

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(
            result,
            StepExecutionOutcome::WorkspaceDenied { .. }
        ));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    // ── Policy violations → zero process start ──────────────────────
    #[tokio::test]
    async fn test_shell_denied() {
        let ctx = setup().await;
        let mut req = make_req(&ctx, "ikey-pol1", "hash-pol1");
        req.executable = PathBuf::from("bash");

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(result, StepExecutionOutcome::PolicyBlocked { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_metachar_in_args_rejected() {
        let ctx = setup().await;
        let mut req = make_req(&ctx, "ikey-pol2", "hash-pol2");
        req.args = vec!["safe".into(), "evil&&rm".into()];

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(result, StepExecutionOutcome::PolicyBlocked { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 0);
    }

    // ── Idempotency ─────────────────────────────────────────────────
    #[tokio::test]
    async fn test_same_key_same_hash_duplicate() {
        let ctx = setup().await;
        let req = make_req(&ctx, "ikey-dup", "hash-dup");

        let r1 = ctx.svc.execute_step(&req).await;
        assert!(matches!(r1, StepExecutionOutcome::Completed { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 1);

        // Same request — must return Duplicate, NOT start another process.
        let r2 = ctx.svc.execute_step(&req).await;
        assert!(matches!(r2, StepExecutionOutcome::Duplicate { .. }));
        assert_eq!(
            ctx.start_count.load(Ordering::SeqCst),
            1,
            "response-lost must NOT restart process"
        );
    }

    #[tokio::test]
    async fn test_same_key_different_hash_conflict() {
        let ctx = setup().await;
        let req1 = make_req(&ctx, "ikey-conflict", "hash-aaa");
        ctx.svc.execute_step(&req1).await;

        let req2 = make_req(&ctx, "ikey-conflict", "hash-bbb");
        let result = ctx.svc.execute_step(&req2).await;
        assert!(matches!(
            result,
            StepExecutionOutcome::IdempotencyConflict { .. }
        ));
    }

    // ── File-backed two-pool concurrency: exactly one process ───────
    #[tokio::test]
    async fn test_two_pool_one_process_winner() {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;

        let ctx = setup().await;
        // Second independent pool to the same file.
        let db_path = ctx.db_dir.join("exec.db");
        let opts2 = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
            .unwrap()
            .create_if_missing(false)
            .foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(30));
        let pool2 = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts2)
            .await
            .unwrap();

        let start_count2 = Arc::new(AtomicUsize::new(0));
        let exec2 = Arc::new(FakeProcessExecutor::new(start_count2.clone()));
        let svc2 = VerificationExecutionService::new(pool2, exec2);

        let req = make_req(&ctx, "ikey-conc", "hash-conc");

        let (r1, r2) = tokio::join!(ctx.svc.execute_step(&req), svc2.execute_step(&req));

        let has_completed = matches!(r1, StepExecutionOutcome::Completed { .. })
            || matches!(r2, StepExecutionOutcome::Completed { .. });
        assert!(has_completed, "one must complete");

        let total_starts =
            ctx.start_count.load(Ordering::SeqCst) + start_count2.load(Ordering::SeqCst);
        assert_eq!(
            total_starts, 1,
            "only one process must start; two starts = split-brain"
        );
    }

    // ── Timeout ─────────────────────────────────────────────────────
    #[tokio::test]
    async fn test_timeout_classified() {
        let ctx = setup().await;
        ctx.executor.hang_forever.store(true, Ordering::SeqCst);
        let mut req = make_req(&ctx, "ikey-timeout", "hash-timeout");
        req.timeout = Duration::from_millis(100);

        let result = ctx.svc.execute_step(&req).await;
        // hang_forever causes the process to time out
        assert!(matches!(result, StepExecutionOutcome::Completed { .. }));
        assert_eq!(ctx.start_count.load(Ordering::SeqCst), 1);
    }

    // ── No retry/agent/provider switch/worktree deletion ────────────
    #[tokio::test]
    async fn test_no_side_effects() {
        let ctx = setup().await;
        let req = make_req(&ctx, "ikey-side", "hash-side");
        ctx.svc.execute_step(&req).await;

        // No new execution created.
        let execs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&ctx.db.pool)
            .await
            .unwrap();
        assert_eq!(execs.0, 1);

        // No task lifecycle change.
        let task_lc: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'")
            .fetch_one(&ctx.db.pool)
            .await
            .unwrap();
        assert_eq!(task_lc.0, "submitted");

        // Lease and Claim remain — not checked here but no release code exists.
    }

    // ── No secret in output ─────────────────────────────────────────
    #[tokio::test]
    async fn test_secret_in_output_rejected() {
        let ctx = setup().await;
        *ctx.executor.stdout_text.lock().unwrap() = "Bearer sk-abc-secret-token".into();
        let req = make_req(&ctx, "ikey-sec", "hash-sec");

        let result = ctx.svc.execute_step(&req).await;
        assert!(matches!(
            result,
            StepExecutionOutcome::InfrastructureError { .. }
        ));
    }
}
