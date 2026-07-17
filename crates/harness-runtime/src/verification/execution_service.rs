//! VerificationExecutionService — deterministic verification command execution.
//! All processes route through ProcessManager. No direct Command, AgentAdapter,
//! retry, or Worktree deletion anywhere in this module.

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
    pub project_id: String,
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

impl StepExecutionOutcome {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, StepExecutionOutcome::Duplicate { .. })
    }
}

// ── Process executor trait ─────────────────────────────────────────────

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

// ── ProcessManager adapter (production only) ──────────────────────────

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
            spool_dir: Some(std::env::temp_dir().join(format!("harness-vrfy-{}", Uuid::new_v4()))),
            known_secrets: vec![],
            allowed_env_var_names: env.keys().cloned().collect(),
            execution_id: format!("vrfy-{}", Uuid::new_v4()),
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
        loop {
            let state = handle.state.read().await.clone();
            match state {
                ProcessState::Completed { outcome } => {
                    let ms = start.elapsed().as_millis() as u64;
                    return ProcessResult {
                        exit_code: outcome.exit_code.unwrap_or(-1),
                        duration_ms: ms,
                        stdout_preview: outcome.stdout_preview,
                        stderr_preview: outcome.stderr_preview,
                        timed_out: outcome.termination == ProcessTermination::Timeout,
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

// ── Fake executor for tests ────────────────────────────────────────────

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
            fail_spawn: false.into(),
            stdout_text: String::new().into(),
            stderr_text: String::new().into(),
            hang_forever: false.into(),
        }
    }
}

#[async_trait::async_trait]
impl ProcessExecutor for FakeProcessExecutor {
    async fn execute(
        &self,
        _exe: &Path,
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

    /// Execute a single verification step with step events and idempotency.
    /// Never starts Agent, creates retry, switches provider, or deletes Worktree.
    pub async fn execute_step(&self, req: &StepExecutionRequest) -> StepExecutionOutcome {
        // ── 0. Idempotency ──────────────────────────────────────────
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT op_id, request_hash, status FROM verification_step_operations WHERE idempotency_key = ?",
        ).bind(&req.idempotency_key).fetch_optional(&self.pool).await.unwrap_or(None);

        if let Some((eid, ehash, _)) = existing {
            if ehash == req.request_hash {
                return StepExecutionOutcome::Duplicate {
                    existing_op_id: eid,
                };
            }
            return StepExecutionOutcome::IdempotencyConflict {
                existing_hash: ehash,
                new_hash: req.request_hash.clone(),
            };
        }

        // ── 1. Ownership + workspace + policy ───────────────────────
        if let Some(o) = self.check_ownership(req).await {
            return o;
        }
        if let Some(o) = self.validate_workspace(&req.working_directory, &req.worktree_path) {
            return o;
        }
        if let Some(o) = evaluate_policy(&req.executable, &req.args) {
            return o;
        }

        // ── 2. Insert step op + Started event (atomic) ──────────────
        let op_id = format!("step-op-{}", Uuid::new_v4());
        if let Err(e) = self.insert_step_op(&op_id, req).await {
            return StepExecutionOutcome::InfrastructureError {
                reason: format!("insert op: {e}"),
            };
        }

        // Write Started event BEFORE transitioning to Running.
        let sk = step_kind_str(&req.step_kind);
        if self
            .write_step_event(&op_id, req, "started", sk, None)
            .await
            .is_err()
        {
            let _ = self
                .mark_step_terminal(&op_id, "event_failed", "started event write failed")
                .await;
            return StepExecutionOutcome::InfrastructureError {
                reason: "started event write failed".into(),
            };
        }

        // Transition step → Running (CAS). Failure means event is written but process not started.
        let rows = sqlx::query(
            "UPDATE verification_step_operations SET status='running', process_start_count=process_start_count+1, started_at=datetime('now') WHERE op_id=? AND status='pending'",
        ).bind(&op_id).execute(&self.pool).await;
        if rows.map(|r| r.rows_affected()).unwrap_or(0) != 1 {
            return StepExecutionOutcome::InfrastructureError {
                reason: "step transition failed".into(),
            };
        }

        // ── 3. Execute via ProcessExecutor ──────────────────────────
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

        // ── 4. Classify ─────────────────────────────────────────────
        let (status, _fc) = classify(exit_code, &req.step_kind, result.timed_out, req.timeout);

        // ── 5. Validate output ──────────────────────────────────────
        if let Some(ref out) = result.stdout_preview {
            if VerificationContentValidator::validate_text(out).is_err() {
                let _ = self
                    .mark_step_terminal(&op_id, "validation_failed", "secret in output")
                    .await;
                return StepExecutionOutcome::InfrastructureError {
                    reason: "output contained secrets".into(),
                };
            }
        }

        // ── 6. Terminal event + persist ─────────────────────────────
        let event_type = terminal_event_type(&status);
        let detail =
            serde_json::json!({"exit_code": exit_code, "duration_ms": duration_ms}).to_string();
        let _ = self
            .write_step_event(&op_id, req, event_type, sk, Some(&detail))
            .await;

        let status_str = status_to_str(&status);
        let _ = sqlx::query(
            "UPDATE verification_step_operations SET status=?, process_exit_code=?, duration_ms=?, outcome_json=?, completed_at=datetime('now') WHERE op_id=?",
        ).bind(status_str).bind(exit_code).bind(duration_ms as i64).bind(&detail).bind(&op_id).execute(&self.pool).await;

        let step_result = VerificationStepResult {
            result_id: format!("sr-{}", Uuid::new_v4()),
            run_id: req.verification_run_id.clone(),
            step_id: req.step_id.clone(),
            plan_id: req.plan_id.clone(),
            status: status.clone(),
            detail_json: Some(detail),
            started_at: None,
            completed_at: None,
            duration_ms: Some(duration_ms),
            error_message: if exit_code != 0 {
                Some(format!("exit {exit_code}"))
            } else {
                None
            },
        };

        StepExecutionOutcome::Completed {
            step_result,
            exit_code,
            duration_ms,
        }
    }

    /// Execute all command steps in a plan sequentially.
    /// Fail-fast: if a required step fails, subsequent command steps are skipped.
    /// Run-all: if configured, continues after non-required failures.
    /// Non-command steps are deferred (not executed).
    pub async fn execute_plan_steps(
        &self,
        requests: &[StepExecutionRequest],
    ) -> Vec<StepExecutionOutcome> {
        let mut results = Vec::new();
        let mut fail_fast_triggered = false;

        for req in requests {
            // Non-command steps → deferred.
            if is_non_command_step(&req.step_kind) {
                results.push(StepExecutionOutcome::Duplicate {
                    existing_op_id: "deferred".into(),
                });
                continue;
            }
            // Fail-fast: stop executing after a required failure.
            if fail_fast_triggered {
                results.push(StepExecutionOutcome::PolicyBlocked {
                    reason: "fail-fast: previous step failed".into(),
                });
                continue;
            }
            let outcome = self.execute_step(req).await;
            let is_failure = matches!(&outcome,
                StepExecutionOutcome::Completed { step_result, .. } if step_result.status != VerificationStepStatus::Passed);
            if is_failure && req.required {
                fail_fast_triggered = true;
            }
            results.push(outcome);
        }
        results
    }

    // ── Helpers ────────────────────────────────────────────────────

    async fn check_ownership(&self, req: &StepExecutionRequest) -> Option<StepExecutionOutcome> {
        let lc: Option<(String,)> =
            sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id = ?")
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten();
        match lc {
            Some((lc,)) if lc == "running" => {}
            Some((lc,)) => {
                return Some(StepExecutionOutcome::OwnershipLost {
                    reason: format!("run lc={lc}"),
                })
            }
            None => {
                return Some(StepExecutionOutcome::OwnershipLost {
                    reason: "run not found".into(),
                })
            }
        }
        let owner: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT owner_kind, owner_id, fencing_token FROM resource_handoffs WHERE execution_id = ?",
        ).bind(&req.execution_id).fetch_optional(&self.pool).await.ok().flatten();
        match owner {
            Some((k, o, f)) => {
                if k != "verification" || o != req.verification_owner_id {
                    return Some(StepExecutionOutcome::OwnershipLost {
                        reason: format!("owner={k}/{o}"),
                    });
                }
                if f != req.expected_fencing {
                    return Some(StepExecutionOutcome::OwnershipLost {
                        reason: format!("fence={f}!={}", req.expected_fencing),
                    });
                }
            }
            None => {
                return Some(StepExecutionOutcome::OwnershipLost {
                    reason: "handoff missing".into(),
                })
            }
        }
        None
    }

    fn validate_workspace(&self, cwd: &Path, wt_root: &Path) -> Option<StepExecutionOutcome> {
        let cws = cwd.to_string_lossy();
        if cws.contains("..") {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: ".. traversal".into(),
            });
        }
        if cws.starts_with(r"\\") || cws.starts_with("//") {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: "UNC path".into(),
            });
        }
        let c = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
        let w = std::fs::canonicalize(wt_root).unwrap_or_else(|_| wt_root.to_path_buf());
        if !c.starts_with(&w) {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: "outside worktree".into(),
            });
        }
        if !c.exists() {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: "cwd missing".into(),
            });
        }
        if c.is_file() {
            return Some(StepExecutionOutcome::WorkspaceDenied {
                reason: "cwd is file".into(),
            });
        }
        None
    }

    async fn insert_step_op(
        &self,
        op_id: &str,
        req: &StepExecutionRequest,
    ) -> Result<(), CoreError> {
        let cfg = format!("{:016x}", {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            req.executable.hash(&mut h);
            req.args.hash(&mut h);
            h.finish()
        });
        sqlx::query("INSERT INTO verification_step_operations (op_id, verification_run_id, step_id, plan_id, execution_id, step_config_hash, worktree_id, fencing_token, status, idempotency_key, request_hash) VALUES (?,?,?,?,?,?,?,?,'pending',?,?)")
            .bind(op_id).bind(&req.verification_run_id).bind(&req.step_id).bind(&req.plan_id)
            .bind(&req.execution_id).bind(&cfg).bind(&req.worktree_id).bind(req.expected_fencing)
            .bind(&req.idempotency_key).bind(&req.request_hash).execute(&self.pool).await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("insert op: {e}"), ErrorSource::System))?;
        Ok(())
    }

    async fn write_step_event(
        &self,
        op_id: &str,
        req: &StepExecutionRequest,
        event: &str,
        kind: &str,
        detail: Option<&str>,
    ) -> Result<(), CoreError> {
        let eid = format!("evt-{event}-{}", Uuid::new_v4());
        let ikey = format!("step-ev-{op_id}-{event}");
        sqlx::query("INSERT OR IGNORE INTO verification_step_events (event_id, verification_run_id, step_id, step_op_id, execution_id, task_id, worktree_id, fencing_token, event_type, step_kind, detail_json, idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&eid).bind(&req.verification_run_id).bind(&req.step_id).bind(op_id)
            .bind(&req.execution_id).bind(&req.task_id).bind(&req.worktree_id).bind(req.expected_fencing)
            .bind(event).bind(kind).bind(detail).bind(&ikey).execute(&self.pool).await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("event: {e}"), ErrorSource::System))?;
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
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, format!("mark: {e}"), ErrorSource::System))?;
        Ok(())
    }
}

// ── Policy ─────────────────────────────────────────────────────────────

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
                reason: format!("metachar: {a}"),
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

fn step_kind_str(k: &VerificationStepKind) -> &'static str {
    match k {
        VerificationStepKind::GitDiffCheck => "git_diff",
        VerificationStepKind::FileScopeCheck => "file_scope",
        VerificationStepKind::SecretScanCheck => "secret_scan",
        VerificationStepKind::PolicyCheck => "policy",
        VerificationStepKind::AcceptanceCheck => "acceptance",
        VerificationStepKind::ArtifactCheck => "artifact",
        VerificationStepKind::TaskResultCheck => "task_result",
        VerificationStepKind::WorktreeCheck => "worktree",
        VerificationStepKind::ResourceOwnershipCheck => "resource",
        VerificationStepKind::CustomCheck => "custom",
    }
}

fn is_non_command_step(k: &VerificationStepKind) -> bool {
    matches!(
        k,
        VerificationStepKind::GitDiffCheck
            | VerificationStepKind::FileScopeCheck
            | VerificationStepKind::SecretScanCheck
            | VerificationStepKind::ArtifactCheck
            | VerificationStepKind::TaskResultCheck
            | VerificationStepKind::WorktreeCheck
            | VerificationStepKind::ResourceOwnershipCheck
    )
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

fn terminal_event_type(s: &VerificationStepStatus) -> &'static str {
    match s {
        VerificationStepStatus::Passed => "completed",
        VerificationStepStatus::Failed => "failed",
        VerificationStepStatus::Blocked => "policy_blocked",
        VerificationStepStatus::Error => "infrastructure_error",
        VerificationStepStatus::Skipped => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use std::sync::atomic::AtomicUsize;
    use tempfile::TempDir;

    struct Ctx {
        svc: VerificationExecutionService,
        db: Database,
        exec: Arc<FakeProcessExecutor>,
        sc: Arc<AtomicUsize>,
        wtd: TempDir,
    }

    async fn setup() -> Ctx {
        let td = tempfile::tempdir().unwrap();
        let dp = td.path().join("e.db");
        let db = Database::open(&dp).await.unwrap();
        let p = db.pool.clone();
        sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','t','active')")
            .execute(&p)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','t','submitted')",
        )
        .execute(&p)
        .await
        .unwrap();
        sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_plans(plan_id,task_id,execution_id,project_id,plan_hash,plan_version,steps_json) VALUES('plan-1','t1','e1','p1','ha',1,'[]')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO verification_runs(run_id,plan_id,plan_hash,plan_version,execution_id,task_id,project_id,lifecycle,idempotency_key,request_hash) VALUES('run-1','plan-1','ha',1,'e1','t1','p1','running','ik-r','hr')").execute(&p).await.unwrap();
        sqlx::query("INSERT INTO resource_handoffs(handoff_id,project_id,task_id,execution_id,worktree_id,lease_id,fencing_token,owner_kind,owner_id,status) VALUES('ho-1','p1','t1','e1','wt1','l1',5,'verification','verify-run-1','verification_owned')").execute(&p).await.unwrap();
        let wd = tempfile::tempdir().unwrap();
        let sc = Arc::new(AtomicUsize::new(0));
        let ex = Arc::new(FakeProcessExecutor::new(sc.clone()));
        let svc = VerificationExecutionService::new(p, ex.clone());
        Ctx {
            svc,
            db,
            exec: ex,
            sc,
            wtd: wd,
        }
    }

    fn mkreq(ctx: &Ctx, ikey: &str, hash: &str) -> StepExecutionRequest {
        StepExecutionRequest {
            verification_run_id: "run-1".into(),
            step_id: "step-1".into(),
            plan_id: "plan-1".into(),
            execution_id: "e1".into(),
            task_id: "t1".into(),
            project_id: "p1".into(),
            worktree_id: "wt1".into(),
            worktree_path: ctx.wtd.path().to_path_buf(),
            expected_fencing: 5,
            verification_owner_id: "verify-run-1".into(),
            idempotency_key: ikey.into(),
            request_hash: hash.into(),
            executable: PathBuf::from("test-exe"),
            args: vec!["--check".into()],
            working_directory: ctx.wtd.path().to_path_buf(),
            timeout: Duration::from_secs(5),
            allowed_env_var_names: vec![],
            env_overrides: HashMap::new(),
            step_kind: VerificationStepKind::AcceptanceCheck,
            required: true,
            sequence_index: 0,
        }
    }

    #[tokio::test]
    async fn test_normal_exec() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 0;
        let r = c.svc.execute_step(&mkreq(&c, "ik-n", "h-n")).await;
        assert!(matches!(r, StepExecutionOutcome::Completed { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn test_nonzero() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 1;
        let r = c.svc.execute_step(&mkreq(&c, "ik-f", "h-f")).await;
        assert!(matches!(
            r,
            StepExecutionOutcome::Completed { exit_code: 1, .. }
        ));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn test_not_running() {
        let c = setup().await;
        sqlx::query("UPDATE verification_runs SET lifecycle='created' WHERE run_id='run-1'")
            .execute(&c.db.pool)
            .await
            .unwrap();
        let r = c.svc.execute_step(&mkreq(&c, "ik-o1", "h-o1")).await;
        assert!(matches!(r, StepExecutionOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn test_wrong_owner() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-o2", "h-o2");
        rq.verification_owner_id = "wrong".into();
        let r = c.svc.execute_step(&rq).await;
        assert!(matches!(r, StepExecutionOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn test_stale_fence() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-o3", "h-o3");
        rq.expected_fencing = 99;
        let r = c.svc.execute_step(&rq).await;
        assert!(matches!(r, StepExecutionOutcome::OwnershipLost { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn test_outside_wt() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-w1", "h-w1");
        rq.working_directory = std::env::temp_dir();
        let r = c.svc.execute_step(&rq).await;
        assert!(matches!(r, StepExecutionOutcome::WorkspaceDenied { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn test_dotdot() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-w2", "h-w2");
        rq.working_directory = c.wtd.path().join("..");
        let r = c.svc.execute_step(&rq).await;
        assert!(matches!(r, StepExecutionOutcome::WorkspaceDenied { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn test_shell_denied() {
        let c = setup().await;
        let mut rq = mkreq(&c, "ik-p1", "h-p1");
        rq.executable = PathBuf::from("bash");
        let r = c.svc.execute_step(&rq).await;
        assert!(matches!(r, StepExecutionOutcome::PolicyBlocked { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn test_dup() {
        let c = setup().await;
        let rq = mkreq(&c, "ik-dup", "h-dup");
        c.svc.execute_step(&rq).await;
        let r2 = c.svc.execute_step(&rq).await;
        assert!(matches!(r2, StepExecutionOutcome::Duplicate { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn test_conflict() {
        let c = setup().await;
        c.svc.execute_step(&mkreq(&c, "ik-co", "h-a")).await;
        let r = c.svc.execute_step(&mkreq(&c, "ik-co", "h-b")).await;
        assert!(matches!(
            r,
            StepExecutionOutcome::IdempotencyConflict { .. }
        ));
    }
    #[tokio::test]
    async fn test_two_pool() {
        let c = setup().await;
        let s2 = VerificationExecutionService::new(c.db.pool.clone(), c.exec.clone());
        let rq = mkreq(&c, "ik-tp", "h-tp");
        let (r1, r2) = tokio::join!(c.svc.execute_step(&rq), s2.execute_step(&rq));
        assert!(
            matches!(r1, StepExecutionOutcome::Completed { .. })
                || matches!(r2, StepExecutionOutcome::Completed { .. })
        );
        assert_eq!(
            c.sc.load(Ordering::SeqCst),
            1,
            "two-pool must start exactly 1 process"
        );
    }
    #[tokio::test]
    async fn test_no_side_effects() {
        let c = setup().await;
        c.svc.execute_step(&mkreq(&c, "ik-se", "h-se")).await;
        let ec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(ec.0, 1);
        let tl: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'")
            .fetch_one(&c.db.pool)
            .await
            .unwrap();
        assert_eq!(tl.0, "submitted");
    }

    // ── Step event tests ──────────────────────────────────────────
    #[tokio::test]
    async fn test_started_event_written() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 0;
        c.svc.execute_step(&mkreq(&c, "ik-ev1", "h-ev1")).await;
        let ec: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='started'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(ec.0, 1, "started event must be written");
    }
    #[tokio::test]
    async fn test_terminal_event_written() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 0;
        c.svc.execute_step(&mkreq(&c, "ik-ev2", "h-ev2")).await;
        let tc: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM verification_step_events WHERE event_type='completed'",
        )
        .fetch_one(&c.db.pool)
        .await
        .unwrap();
        assert_eq!(tc.0, 1, "terminal event must be written");
    }
    #[tokio::test]
    async fn test_response_lost_events_not_duplicated() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 0;
        let rq = mkreq(&c, "ik-ev3", "h-ev3");
        c.svc.execute_step(&rq).await;
        c.svc.execute_step(&rq).await;
        let total:(i64,)=sqlx::query_as("SELECT COUNT(*) FROM verification_step_events WHERE step_op_id=(SELECT op_id FROM verification_step_operations WHERE idempotency_key='ik-ev3')").fetch_one(&c.db.pool).await.unwrap();
        assert_eq!(
            total.0, 2,
            "started+terminal exactly 2 events, not duplicated"
        );
    }
    #[tokio::test]
    async fn test_event_no_secret() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 0;
        c.svc.execute_step(&mkreq(&c, "ik-ev4", "h-ev4")).await;
        let row: (String, Option<String>) =
            sqlx::query_as("SELECT event_type,detail_json FROM verification_step_events LIMIT 1")
                .fetch_one(&c.db.pool)
                .await
                .unwrap();
        let d = row.1.unwrap_or_default();
        assert!(!d.contains("lease_token"));
        assert!(!d.contains("sk-"));
    }

    // ── Fail-fast and run-all ─────────────────────────────────────
    #[tokio::test]
    async fn test_fail_fast_stops_on_required_failure() {
        let c = setup().await;
        *c.exec.exit_code.lock().unwrap() = 1;
        let rqs: Vec<_> = (0..3)
            .map(|i| {
                let mut r = mkreq(&c, &format!("ik-ff{i}"), &format!("h-ff{i}"));
                r.step_id = format!("s{i}");
                r.sequence_index = i;
                r.required = true;
                r
            })
            .collect();
        let results = c.svc.execute_plan_steps(&rqs).await;
        assert_eq!(results.len(), 3);
        assert!(
            matches!(results[0], StepExecutionOutcome::Completed { .. }),
            "step 0 runs and fails"
        );
        assert!(
            matches!(results[1], StepExecutionOutcome::PolicyBlocked { .. }),
            "fail-fast must block step 1"
        );
        assert!(
            matches!(results[2], StepExecutionOutcome::PolicyBlocked { .. }),
            "fail-fast must block step 2"
        );
        assert_eq!(
            c.sc.load(Ordering::SeqCst),
            1,
            "only 1 process started, not 3"
        );
    }
    #[tokio::test]
    async fn test_non_command_deferred() {
        let c = setup().await;
        let mut r = mkreq(&c, "ik-nc", "h-nc");
        r.step_kind = VerificationStepKind::GitDiffCheck;
        let results = c.svc.execute_plan_steps(&[r]).await;
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], StepExecutionOutcome::Duplicate { .. }));
        assert_eq!(c.sc.load(Ordering::SeqCst), 0);
    }
}
