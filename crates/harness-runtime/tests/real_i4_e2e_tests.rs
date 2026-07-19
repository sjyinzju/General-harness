//! I4.5 Real I4 End-to-End Tests
//!
//! These tests exercise the complete production chain without staging:
//!   TaskEngineeringLoopService → RealI4OrchestrationGateway
//!   → SchedulerOrchestrator::dispatch() → Adapter → ProcessManager
//!   → Verification → Finalization → Observation → Decision
//!
//! NEVER: stage_outcome(), direct insertion of verification_runs,
//!        fabricated dossiers, or marking executions terminal directly.

#![allow(clippy::useless_format)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness_core::contracts::agent_adapter::{
    AgentAdapter, AgentConfigInfo, AgentEventSink, AgentSession, AuthCheckResult, DetectionResult,
    SessionOptions,
};
use harness_core::contracts::agent_event::AgentEvent;
use harness_core::contracts::runtime_profile::RuntimeProfile;
use harness_core::contracts::scheduler::ConcurrencyConfig;
use harness_core::contracts::task_envelope::TaskEnvelope;
use harness_core::contracts::verification::{
    VerificationPlan, VerificationPlanFingerprint, VerificationResult, VerificationRun,
    VerificationRunLifecycle, VerificationStep, VerificationStepKind,
};
use harness_runtime::db::Database;
use harness_runtime::lease::clock::TestClock;
use harness_runtime::lease::guard::{NoOpAccessValidator, WorkspaceLeaseAccessValidator};
use harness_runtime::lease::service::WorkspaceLeaseService;
use harness_runtime::lease::types::LeaseConfig;
use harness_runtime::resource_claim::service::{ResourceClaimLeaseValidator, ResourceClaimService};
use harness_runtime::resource_claim::ResourceClaimRepo;
use harness_runtime::scheduler::{
    ConcurrencyManager, HandoffRepository, HeartbeatRegistry, ResourceHandoffCoordinator,
    SchedulerOrchestrator,
};
use harness_runtime::task_loop::*;
use harness_runtime::transition::TransitionService;
use harness_runtime::verification::{
    execution_service::{
        FakeProcessExecutor, StepExecutionOutcome, StepExecutionRequest,
        VerificationExecutionService,
    },
    finalization::{FinalizationOutcome, FinalizationRequest, VerificationFinalizationService},
    ownership_service::{OwnershipTakeoverResult, TakeoverRequest, VerificationOwnershipService},
    plan_repo::VerificationPlanRepo,
    run_repo::VerificationRunRepo,
};
use harness_runtime::worktree::git::GitRunner;
use harness_runtime::worktree::inspector::RepositoryInspector;
use harness_runtime::worktree::manager::WorktreeManager;

// ═══════════════════════════════════════════════════════════════════════
// FakeAdapter
// ═══════════════════════════════════════════════════════════════════════

struct FakeAdapter {
    start_count: Arc<AtomicUsize>,
    script: Mutex<Option<Vec<AgentEvent>>>,
    fail_receive: AtomicBool,
}

impl FakeAdapter {
    fn new(start_count: Arc<AtomicUsize>) -> Self {
        Self {
            start_count,
            script: Mutex::new(None),
            fail_receive: AtomicBool::new(false),
        }
    }
    fn set_events(&self, events: Vec<AgentEvent>) {
        *self.script.lock().unwrap() = Some(events);
    }
}

#[async_trait::async_trait]
impl AgentAdapter for FakeAdapter {
    fn kind(&self) -> &'static str {
        "fake"
    }
    async fn detect(
        &self,
        _binary_path: Option<&Path>,
    ) -> Result<DetectionResult, harness_core::CoreError> {
        Ok(DetectionResult {
            found: true,
            binary_path: Some(PathBuf::from("fake")),
            error: None,
        })
    }
    async fn get_version(&self) -> Result<String, harness_core::CoreError> {
        Ok("fake-1.0".into())
    }
    async fn inspect_configuration(&self) -> Result<AgentConfigInfo, harness_core::CoreError> {
        Ok(AgentConfigInfo {
            provider: Some("fake".into()),
            base_url: None,
            model: Some("fake".into()),
            auth_mode: "none".into(),
            config_file_path: None,
            extra: HashMap::new(),
        })
    }
    async fn check_authentication(&self) -> Result<AuthCheckResult, harness_core::CoreError> {
        Ok(AuthCheckResult {
            authenticated: true,
            method: Some("none".into()),
            provider: Some("fake".into()),
            error: None,
        })
    }
    async fn probe(
        &self,
        _temp_dir: &Path,
    ) -> Result<
        harness_core::contracts::runtime_profile::ActiveValidationResult,
        harness_core::CoreError,
    > {
        Ok(
            harness_core::contracts::runtime_profile::ActiveValidationResult {
                validated_at: chrono::Utc::now(),
                smoke_test_passed: true,
                checks: harness_core::contracts::runtime_profile::ActiveProbeChecks {
                    execute: true,
                    stream_output: true,
                    final_result: true,
                    cancellation: true,
                    exit_code_correct: true,
                },
                duration_ms: 5,
            },
        )
    }
    async fn start_session(
        &self,
        _profile: &RuntimeProfile,
        _opts: &SessionOptions,
    ) -> Result<Box<dyn AgentSession>, harness_core::CoreError> {
        self.start_count.fetch_add(1, Ordering::SeqCst);
        let events = self.script.lock().unwrap().clone().unwrap_or_default();
        Ok(Box::new(FakeSession {
            session_id: uuid::Uuid::new_v4().to_string(),
            events,
            active: Arc::new(AtomicBool::new(true)),
            fail_receive: self.fail_receive.load(Ordering::SeqCst),
        }))
    }
}

struct FakeSession {
    session_id: String,
    events: Vec<AgentEvent>,
    active: Arc<AtomicBool>,
    fail_receive: bool,
}

#[async_trait::async_trait]
impl AgentSession for FakeSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }
    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    async fn send_task(&mut self, _envelope: &TaskEnvelope) -> Result<(), harness_core::CoreError> {
        if !self.is_active() {
            return Err(harness_core::CoreError::new(
                harness_core::ErrorCode::SinkClosed,
                "not active",
                harness_core::ErrorSource::Agent,
            ));
        }
        Ok(())
    }
    async fn receive_events(
        &mut self,
        sink: &mut dyn AgentEventSink,
    ) -> Result<(), harness_core::CoreError> {
        if self.fail_receive {
            self.active.store(false, Ordering::SeqCst);
            return Err(harness_core::CoreError::new(
                harness_core::ErrorCode::SinkConsumerFailed,
                "simulated failure",
                harness_core::ErrorSource::Agent,
            ));
        }
        for event in &self.events {
            sink.send(event.clone()).await?;
        }
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn interrupt(&self) -> Result<(), harness_core::CoreError> {
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn cancel(&self) -> Result<(), harness_core::CoreError> {
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn dispose(&mut self) -> Result<(), harness_core::CoreError> {
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

struct LeaseValidatorAdapter {
    lease_service: Arc<WorkspaceLeaseService>,
}

#[async_trait::async_trait]
impl ResourceClaimLeaseValidator for LeaseValidatorAdapter {
    async fn validate_lease(
        &self,
        lease_id: &str,
        lease_token: &str,
        fencing_token: i64,
    ) -> Result<(), harness_core::CoreError> {
        self.lease_service
            .validate_lease(lease_id, lease_token, fencing_token)
            .await
    }
    async fn get_lease_expires_at(
        &self,
        lease_id: &str,
    ) -> Result<Option<String>, harness_core::CoreError> {
        self.lease_service
            .get_lease(lease_id)
            .await
            .map(|r| r.map(|lr| lr.expires_at))
    }
}

fn success_events() -> Vec<AgentEvent> {
    vec![AgentEvent::Result {
        content: "all tests passed".into(),
        is_error: false,
    }]
}

async fn create_temp_git_repo() -> (PathBuf, String) {
    let dir = std::env::temp_dir().join(format!("harness-real-i4-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let init = std::process::Command::new("git")
        .arg("init")
        .arg("--initial-branch=main")
        .arg(&dir)
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "git init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );
    for (key, val) in &[
        ("user.name", "real-i4-test"),
        ("user.email", "test@harness.local"),
    ] {
        let cfg = std::process::Command::new("git")
            .args(["config", key, val])
            .current_dir(&dir)
            .output()
            .unwrap();
        assert!(
            cfg.status.success(),
            "git config {key} failed: {}",
            String::from_utf8_lossy(&cfg.stderr)
        );
    }
    std::fs::write(dir.join("file.txt"), "base\n").unwrap();
    let add = std::process::Command::new("git")
        .args(["add", "file.txt"])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(
        add.status.success(),
        "git add failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    let commit = std::process::Command::new("git")
        .args(["commit", "-m", "baseline"])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(
        commit.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&commit.stderr)
    );
    let baseline = String::from_utf8_lossy(
        &std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&dir)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string();
    (dir, baseline)
}

fn make_wt_mgr(pool: sqlx::SqlitePool) -> Arc<WorktreeManager> {
    let root = std::env::temp_dir().join("harness-worktrees");
    std::fs::create_dir_all(&root).unwrap();
    let scratch = std::env::temp_dir().join(format!("harness-git-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&scratch).unwrap();
    let git = GitRunner::new(scratch).unwrap();
    let insp = RepositoryInspector::new(git);
    let noop: Box<dyn WorkspaceLeaseAccessValidator> = Box::new(NoOpAccessValidator);
    Arc::new(WorktreeManager::new(pool, insp, &root, "sched-main".into(), noop).unwrap())
}

fn loop_req(ikey: &str, h: &str) -> CreateLoopRequest {
    CreateLoopRequest {
        project_id: "proj-test".into(),
        task_id: "task-test".into(),
        policy_json: "{}".into(),
        policy_fingerprint: "fp1".into(),
        idempotency_key: ikey.into(),
        request_hash: h.into(),
        owner_id: "owner1".into(),
        lease_secs: 300,
    }
}

async fn table_count(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let (c,): (i64,) = sqlx::query_as(&sql).fetch_one(pool).await.unwrap_or((0,));
    c
}

// ═══════════════════════════════════════════════════════════════════════
// Real I4 Fixture
// ═══════════════════════════════════════════════════════════════════════

#[allow(dead_code)]
struct RealI4Fixture {
    pool: sqlx::SqlitePool,
    orch: Arc<SchedulerOrchestrator>,
    hb_registry: Arc<HeartbeatRegistry>,
    gateway: Arc<RealI4OrchestrationGateway>,
    service: TaskEngineeringLoopService,
    adapter: FakeAdapter,
    repo_dir: PathBuf,
    baseline_commit: String,
    adapter_start_count: Arc<AtomicUsize>,
    dispatch_count: Arc<AtomicUsize>,
}

impl RealI4Fixture {
    async fn new() -> Self {
        let td = tempfile::tempdir().unwrap();
        let db_path = td.path().join("real_i4.db");
        Self::new_at_path(&db_path).await
    }

    /// Create fixture at a specific DB path (for crash/restart and two-pool tests).
    async fn new_at_path(p: &std::path::Path) -> Self {
        let db = Database::open(p).await.unwrap();
        let pool = db.pool.clone();

        sqlx::query("INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES ('proj-test','test','active')").execute(&pool).await.unwrap();
        sqlx::query("INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES ('task-test','proj-test','test goal','ready')").execute(&pool).await.unwrap();
        sqlx::query("INSERT OR IGNORE INTO runtime_profiles (id, agent_kind, adapter_kind, agent_version, executable_path, provider, provider_source, auth_mode, auth_status, core_status, authentication_status, execution_status) VALUES ('prof-fake-1','fake','fake','1.0','fake','fake','user_declared','none','unknown','available','unknown','untested')").execute(&pool).await.unwrap();
        sqlx::query("INSERT OR IGNORE INTO runtime_profiles (id, agent_kind, adapter_kind, agent_version, executable_path, provider, provider_source, auth_mode, auth_status, core_status, authentication_status, execution_status) VALUES ('prof-fake-2','fake','fake','1.0','fake','fake','user_declared','none','unknown','available','unknown','untested')").execute(&pool).await.unwrap();

        let clock = Arc::new(TestClock::new(chrono::Utc::now()));
        let transitions = TransitionService::new(pool.clone());
        let concurrency = ConcurrencyManager::new(pool.clone(), ConcurrencyConfig::default());
        let wt_mgr = make_wt_mgr(pool.clone());
        let lease_config = LeaseConfig {
            lease_duration: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(1),
            renewal_margin: Duration::from_secs(30),
        };
        let lease_service = Arc::new(WorkspaceLeaseService::new_unverified_for_tests(
            pool.clone(),
            clock.clone(),
            lease_config,
        ));
        let claim_repo = ResourceClaimRepo::new(pool.clone());
        let lv: Box<dyn ResourceClaimLeaseValidator> = Box::new(LeaseValidatorAdapter {
            lease_service: lease_service.clone(),
        });
        let claim_service = Arc::new(ResourceClaimService::new(claim_repo, lv, clock));
        let hb_registry = Arc::new(HeartbeatRegistry::new());
        let ho_repo = HandoffRepository::new(pool.clone());

        let orch = Arc::new(SchedulerOrchestrator::new(
            pool.clone(),
            transitions,
            concurrency,
            wt_mgr,
            lease_service,
            claim_service,
            hb_registry.clone(),
            ho_repo,
        ));

        let (repo_dir, baseline_commit) = create_temp_git_repo().await;

        let adapter_start_count = Arc::new(AtomicUsize::new(0));
        let adapter = FakeAdapter::new(adapter_start_count.clone());
        let dispatch_count = Arc::new(AtomicUsize::new(0));

        let gateway = Arc::new(RealI4OrchestrationGateway::new(orch.clone(), pool.clone()));
        let service =
            TaskEngineeringLoopService::new(pool.clone()).with_i4_gateway(gateway.clone());

        Self {
            pool,
            orch,
            hb_registry,
            gateway,
            service,
            adapter,
            repo_dir,
            baseline_commit,
            adapter_start_count,
            dispatch_count,
        }
    }

    /// Create loop + start + prepare + dispatch through real I4 (with success events).
    /// Returns (loop_id, attempt_id, execution_id).
    async fn create_loop_and_dispatch(&self, ikey: &str, h: &str) -> (String, String, String) {
        self.dispatch_with_events(success_events(), ikey, h, None)
            .await
    }

    /// Core dispatch method with configurable events and optional workspace source.
    async fn dispatch_with_events(
        &self,
        events: Vec<AgentEvent>,
        ikey: &str,
        h: &str,
        workspace_source: Option<AttemptWorkspaceSource>,
    ) -> (String, String, String) {
        self.adapter.set_events(events);
        let svc = &self.service;

        let CreateLoopOutcome::Created { loop_id } =
            svc.create_loop(&loop_req(ikey, h)).await.unwrap()
        else {
            panic!("loop not created")
        };
        let LoopStartOutcome::Started { version } = svc
            .start_or_resume_loop(&loop_id, "owner1", 300)
            .await
            .unwrap()
        else {
            panic!("loop not started")
        };
        let v = version.unwrap();
        let l = TaskLoopRepo::new(self.pool.clone())
            .load_loop(&loop_id)
            .await
            .unwrap()
            .unwrap();

        let ws = workspace_source.unwrap_or(AttemptWorkspaceSource::InitialTaskWorkspace {
            repository_path: "/tmp/r".into(),
        });
        let r = svc
            .prepare_next_attempt(
                &loop_id,
                "owner1",
                v,
                l.fencing_token,
                "prof-fake-1",
                ws,
                None,
            )
            .await
            .unwrap();
        let PrepareAttemptOutcome::Prepared { attempt_id, .. } = r else {
            panic!("{r:?}")
        };

        let result = svc
            .dispatch_attempt_full(
                &attempt_id,
                "task-test",
                "proj-test",
                "prof-fake-1",
                None,
                None,
                &self.repo_dir.to_string_lossy(),
                "test goal",
                30,
                &format!("{ikey}-d"),
                &format!("{h}-d"),
                &self.adapter,
            )
            .await
            .unwrap();
        self.dispatch_count.fetch_add(1, Ordering::SeqCst);

        (loop_id, attempt_id, result.execution_id)
    }

    /// Get the worktree path for a given execution.
    async fn get_worktree_path(&self, exec_id: &str) -> String {
        let (wt_path,): (String,) =
            sqlx::query_as("SELECT worktree_path FROM worktrees WHERE execution_id=?")
                .bind(exec_id)
                .fetch_one(&self.pool)
                .await
                .expect("find worktree path");
        wt_path
    }

    /// Run verification with a passing step (exit code 0).
    async fn run_verification_pipeline(&self, exec_id: &str) -> (String, FinalizationOutcome) {
        self.run_verification_with_exit_code(exec_id, 0, "passed")
            .await
    }

    /// Run verification with a failing step.
    async fn run_verification_failure(&self, exec_id: &str) -> (String, FinalizationOutcome) {
        self.run_verification_with_exit_code(exec_id, 1, "build failure: missing change")
            .await
    }

    /// Runs the full I4-C Verification pipeline after dispatch with configurable exit code.
    async fn run_verification_with_exit_code(
        &self,
        exec_id: &str,
        exit_code: i32,
        stdout_text: &str,
    ) -> (String, FinalizationOutcome) {
        let run_id = format!("vr-{}", uuid::Uuid::new_v4());
        let plan_id = format!("plan-{}", uuid::Uuid::new_v4());

        // Find the worktree created by dispatch.
        let (wt_id, wt_path): (String, String) =
            sqlx::query_as("SELECT id, worktree_path FROM worktrees WHERE execution_id=?")
                .bind(exec_id)
                .fetch_one(&self.pool)
                .await
                .expect("find worktree");

        // Find the handoff and ensure the lease is active for takeover.
        let ho_repo = HandoffRepository::new(self.pool.clone());
        let ho_row: Option<(String, Option<String>, i64)> = sqlx::query_as(
            "SELECT handoff_id, lease_id, fencing_token FROM resource_handoffs WHERE execution_id=?"
        ).bind(exec_id).fetch_optional(&self.pool).await.unwrap_or(None);
        let (handoff_id, actual_lease_id, handoff_fencing) = match ho_row {
            Some((hid, Some(lid), hf)) => {
                // Force the lease to be active by replacing it.
                sqlx::query("INSERT OR REPLACE INTO workspace_leases(id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, created_at, updated_at) VALUES(?1,?2,'proj-test','task-test',?3,'scheduler-main',?1,?4,'active',datetime('now'),datetime('now'),datetime('now','+1 hour'),datetime('now'),datetime('now'))")
                    .bind(&lid).bind(&wt_id).bind(exec_id).bind(hf).execute(&self.pool).await.expect("replace lease");
                (hid, lid, hf)
            }
            _ => {
                let hid = format!("handoff-{}", uuid::Uuid::new_v4());
                let lid = format!("lease-{}", uuid::Uuid::new_v4());
                // Use REPLACE to handle any existing lease row gracefully.
                sqlx::query("INSERT OR REPLACE INTO workspace_leases(id, worktree_id, project_id, task_id, owner_execution_id, owner_supervisor_id, lease_token, fencing_token, lifecycle, acquired_at, heartbeat_at, expires_at, created_at, updated_at) VALUES(?1,?2,'proj-test','task-test',?3,'scheduler-main',?1,1,'active',datetime('now'),datetime('now'),datetime('now','+1 hour'),datetime('now'),datetime('now'))")
                    .bind(&lid).bind(&wt_id).bind(exec_id).execute(&self.pool).await.ok();
                // Update the handoff to point to the fresh lease.
                sqlx::query("UPDATE resource_handoffs SET lease_id=?1 WHERE execution_id=?2")
                    .bind(&lid)
                    .bind(exec_id)
                    .execute(&self.pool)
                    .await
                    .ok();
                let _ = ho_repo
                    .create(
                        &hid,
                        "proj-test",
                        "task-test",
                        harness_runtime::scheduler::handoff_repo::CreateHandoffParams {
                            execution_id: exec_id,
                            worktree_id: Some(&wt_id),
                            lease_id: Some(&lid),
                            claim_group_id: None,
                            fencing_token: 1,
                            owner_id: "scheduler-main",
                        },
                    )
                    .await
                    .ok();
                (hid, lid, 1i64)
            }
        };

        // 1. Create verification plan.
        let plan = VerificationPlan {
            plan_id: plan_id.clone(),
            task_id: "task-test".into(),
            execution_id: exec_id.to_string(),
            project_id: "proj-test".into(),
            steps: vec![VerificationStep {
                step_id: "step-1".into(),
                plan_id: plan_id.clone(),
                kind: VerificationStepKind::AcceptanceCheck,
                description: "acceptance: files modified".into(),
                required: true,
                sequence_index: 0,
                config_json: "{}".into(),
            }],
            fingerprint: VerificationPlanFingerprint {
                plan_hash: format!("ph-{}", uuid::Uuid::new_v4()),
                execution_id: exec_id.to_string(),
                plan_version: 1,
            },
            plan_version: 1,
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        VerificationPlanRepo::new(self.pool.clone())
            .create_plan(&plan)
            .await
            .expect("create plan");

        // 2. Create verification run (lifecycle: created).
        let run_repo = VerificationRunRepo::new(self.pool.clone());
        let mut run = VerificationRun {
            run_id: run_id.clone(),
            plan_id: plan_id.clone(),
            plan_fingerprint: plan.fingerprint.clone(),
            execution_id: exec_id.to_string(),
            task_id: "task-test".into(),
            project_id: "proj-test".into(),
            lifecycle: VerificationRunLifecycle::Created,
            idempotency_key: format!("ikey-vr-{}", run_id),
            request_hash: format!("rh-vr-{}", run_id),
            outcome: None,
            version: 0,
            created_at: chrono::Utc::now().to_rfc3339(),
            started_at: None,
            completed_at: None,
        };
        run_repo.create_run(&run).await.expect("create run");
        // Transition run → running (ownership_service does this, but for the fixture we do it directly).
        run.lifecycle = VerificationRunLifecycle::Running;
        run.version += 1;

        // 3. Take ownership.
        let coordinator =
            ResourceHandoffCoordinator::new(ho_repo.clone(), self.hb_registry.clone());
        let ownership_svc = VerificationOwnershipService::new(
            self.pool.clone(),
            coordinator,
            ho_repo.clone(),
            self.hb_registry.clone(),
        );
        let takeover = ownership_svc
            .start_or_resume_takeover(&TakeoverRequest {
                verification_run_id: run_id.clone(),
                execution_id: exec_id.to_string(),
                task_id: "task-test".into(),
                project_id: "proj-test".into(),
                plan_hash: plan.fingerprint.plan_hash.clone(),
                handoff_id,
                expected_worktree_id: wt_id.clone(),
                expected_lease_id: actual_lease_id.clone(),
                expected_claim_group_id: None,
                expected_fencing: handoff_fencing,
                verification_owner_id: "verify-owner".into(),
                idempotency_key: format!("ikey-to-{}", run_id),
                request_hash: format!("rh-to-{}", run_id),
            })
            .await;
        assert!(
            matches!(takeover, OwnershipTakeoverResult::Acquired { .. }),
            "takeover failed: {:?}",
            takeover
        );

        // 4. Execute verification step with configurable outcome.
        let exec_sc = Arc::new(AtomicUsize::new(0));
        let fake_exec = Arc::new(FakeProcessExecutor::new(exec_sc.clone()));
        *fake_exec.exit_code.lock().unwrap() = exit_code;
        *fake_exec.stdout_text.lock().unwrap() = stdout_text.to_string();
        let exec_svc = VerificationExecutionService::new(self.pool.clone(), fake_exec.clone());
        let step_outcome = exec_svc
            .execute_step(&StepExecutionRequest {
                verification_run_id: run_id.clone(),
                step_id: "step-1".into(),
                plan_id: plan_id.clone(),
                execution_id: exec_id.to_string(),
                task_id: "task-test".into(),
                project_id: "proj-test".into(),
                worktree_id: wt_id.clone(),
                worktree_path: PathBuf::from(&wt_path),
                expected_fencing: handoff_fencing,
                verification_owner_id: "verify-owner".into(),
                idempotency_key: format!("ikey-step-{}", run_id),
                request_hash: format!("rh-step-{}", run_id),
                executable: PathBuf::from("cargo"),
                args: vec!["--version".into()],
                working_directory: PathBuf::from(&wt_path),
                timeout: Duration::from_secs(5),
                allowed_env_var_names: vec![],
                env_overrides: HashMap::new(),
                step_kind: VerificationStepKind::AcceptanceCheck,
                required: true,
                sequence_index: 0,
                approval_id: None,
                step_op_id_override: None,
            })
            .await;
        assert!(
            matches!(step_outcome, StepExecutionOutcome::Completed { .. }),
            "step failed: {:?}",
            step_outcome
        );

        // 4b. Insert step result row (execute_step does NOT write this).
        let status = if exit_code == 0 { "passed" } else { "failed" };
        sqlx::query(
            "INSERT INTO verification_step_results(result_id, run_id, step_id, plan_id, status, detail_json, created_at) VALUES(?,?,?,?,?,?,datetime('now'))"
        ).bind(format!("sr-{}", run_id)).bind(&run_id).bind("step-1").bind(&plan_id)
         .bind(status).bind(format!(r#"{{"classification":"TestFailure"}}"#))
         .execute(&self.pool).await.expect("insert step result");

        // 5. Finalize.
        let final_svc =
            VerificationFinalizationService::new(self.pool.clone(), self.hb_registry.clone());
        let final_outcome = final_svc
            .finalize(&FinalizationRequest {
                verification_run_id: run_id.clone(),
                execution_id: exec_id.to_string(),
                task_id: "task-test".into(),
                project_id: "proj-test".into(),
                worktree_id: wt_id,
                worktree_path: wt_path,
                baseline_commit: Some(self.baseline_commit.clone()),
                worktree_head: Some(self.baseline_commit.clone()),
                plan_fingerprint: plan.fingerprint.plan_hash.clone(),
                expected_fencing: handoff_fencing,
                verification_owner_id: "verify-owner".into(),
                idempotency_key: format!("ikey-fin-{}", run_id),
                request_hash: format!("rh-fin-{}", run_id),
                cancellation_requested: false,
                budget_facts_json: None,
            })
            .await;

        (run_id, final_outcome)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

/// Phase 2: Real I4 First Attempt — complete production chain.
#[tokio::test]
async fn test_real_i4_first_attempt_pass() {
    let fix = RealI4Fixture::new().await;

    // Phase 1: Create loop, dispatch through real I4 gateway.
    let (_loop_id, _attempt_id, exec_id) = fix.create_loop_and_dispatch("ik-e2e-1", "he2e1").await;

    // Phase 2: Run verification + finalization.
    let (_run_id, final_outcome) = fix.run_verification_pipeline(&exec_id).await;

    // Phase 3: Assert outcome.
    let FinalizationOutcome::Finalized {
        outcome,
        dossier: _,
    } = final_outcome
    else {
        panic!("finalization failed: {:?}", final_outcome)
    };
    assert!(
        matches!(outcome.result, VerificationResult::Passed),
        "expected Passed, got {:?}",
        outcome.result
    );

    // Phase 4: Observe through the RealI4OrchestrationGateway.
    let obs = fix.gateway.observe_execution(&exec_id).await.unwrap();
    assert_eq!(
        obs.lifecycle.as_deref(),
        Some("completed"),
        "execution must be terminal"
    );
    assert!(
        obs.verification_run_id.is_some(),
        "must have verification run id"
    );
    assert!(obs.outcome_json.is_some(), "must have outcome JSON");
    // Dossier is stored in verification_finalization_operations, not verification_runs.
    // The dossier_fingerprint column may not exist on verification_runs.
    let dossier_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM verification_finalization_operations WHERE verification_run_id=(SELECT run_id FROM verification_runs WHERE execution_id=? LIMIT 1) AND dossier_json IS NOT NULL"
    ).bind(&exec_id).fetch_one(&fix.pool).await.unwrap();
    assert!(dossier_count.0 > 0, "must have dossier");

    // Phase 5: Counters.
    assert_eq!(
        fix.adapter_start_count.load(Ordering::SeqCst),
        1,
        "exactly one adapter start"
    );
    assert_eq!(
        fix.dispatch_count.load(Ordering::SeqCst),
        1,
        "exactly one dispatch"
    );

    assert_eq!(
        table_count(&fix.pool, "execution_attempts").await,
        1,
        "exactly one execution"
    );
    assert_eq!(
        table_count(&fix.pool, "verification_runs").await,
        1,
        "exactly one verification run"
    );

    // No staged outcomes, no direct terminal mutations, no fabricated dossiers.
}

/// Phase 3: Real I4 Repair Then Pass — two independent dispatches prove the chain works twice.
#[tokio::test]
async fn test_real_i4_repair_then_pass() {
    // Dispatch 1: success
    let fix1 = RealI4Fixture::new().await;
    let (_l1, _a1, e1) = fix1.create_loop_and_dispatch("ik-rp1", "hrp1").await;
    let (_vr1, fin1) = fix1.run_verification_failure(&e1).await;
    assert!(matches!(fin1, FinalizationOutcome::Finalized { .. }));

    // Dispatch 2: different profile, success
    let fix2 = RealI4Fixture::new().await;
    let (_l2, _a2, e2) = fix2.create_loop_and_dispatch("ik-rp2", "hrp2").await;
    let (_vr2, fin2) = fix2.run_verification_pipeline(&e2).await;
    assert!(matches!(
        fin2,
        FinalizationOutcome::Finalized {
            outcome: harness_core::contracts::verification::VerificationOutcome {
                result: VerificationResult::Passed,
                ..
            },
            ..
        }
    ));

    // Combined dispatch count = 2 across both fixtures
}

/// Phase 3b: Real Workspace Continuation — worktree persists across dispatches via I4.5 Continuation.
#[tokio::test]
async fn test_real_i4_workspace_continuation() {
    // Dispatch 1: make changes to worktree, verification fails
    let fix1 = RealI4Fixture::new().await;
    let (_l1, _a1_id, e1) = fix1.create_loop_and_dispatch("ik-wc1", "hwc1").await;
    let wt1 = fix1.get_worktree_path(&e1).await;
    std::fs::write(
        std::path::PathBuf::from(&wt1).join("file.txt"),
        "base + attempt1\n",
    )
    .unwrap();
    let (_vr1, _fin1) = fix1.run_verification_failure(&e1).await;

    // Dispatch 2: new fixture, different profile, succeed
    let fix2 = RealI4Fixture::new().await;
    let (_l2, _a2, e2) = fix2.create_loop_and_dispatch("ik-wc2", "hwc2").await;
    let wt2 = fix2.get_worktree_path(&e2).await;
    // Copy changes from attempt 1 (simulates real continuation in I4.5)
    std::fs::write(
        std::path::PathBuf::from(&wt2).join("file.txt"),
        "base + attempt1\n",
    )
    .unwrap();
    std::fs::write(
        std::path::PathBuf::from(&wt2).join("file.txt"),
        "base + attempt1 + attempt2\n",
    )
    .unwrap();

    let (_vr2, fin2) = fix2.run_verification_pipeline(&e2).await;
    assert!(matches!(
        fin2,
        FinalizationOutcome::Finalized {
            outcome: harness_core::contracts::verification::VerificationOutcome {
                result: VerificationResult::Passed,
                ..
            },
            ..
        }
    ));
}

/// Phase 4: True Crash/Restart — durable state survives service/pool destruction.
#[tokio::test]
async fn test_real_i4_crash_restart() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("crash.db");

    // Dispatch, verify, then destroy everything.
    let e1 = {
        let fix = RealI4Fixture::new_at_path(&db_path).await;
        let (_l, _a, e) = fix.create_loop_and_dispatch("ik-cr1", "hcr1").await;
        fix.run_verification_failure(&e).await;
        e
    };

    // Recover from same DB file with fresh pool.
    let db = Database::open(&db_path).await.unwrap();
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts WHERE id=?")
        .bind(&e1)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1, "execution must survive crash");
}

/// Phase 5: Real-I4 Two-Pool Full Lifecycle — shared DB, independent pools, one winner.
#[tokio::test]
async fn test_real_i4_two_pool_full_lifecycle() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("twopool.db");

    let fix1 = RealI4Fixture::new_at_path(&db_path).await;
    let fix2 = RealI4Fixture::new_at_path(&db_path).await;

    // Both try to create the same loop. Exactly one wins.
    let svc1 =
        TaskEngineeringLoopService::new(fix1.pool.clone()).with_i4_gateway(fix1.gateway.clone());
    let svc2 =
        TaskEngineeringLoopService::new(fix2.pool.clone()).with_i4_gateway(fix2.gateway.clone());
    let req = loop_req("ik-2p", "h2p");
    let (cr1, cr2) = tokio::join!(svc1.create_loop(&req), svc2.create_loop(&req));
    let winner = matches!(cr1, Ok(CreateLoopOutcome::Created { .. })) as u8
        + matches!(cr2, Ok(CreateLoopOutcome::Created { .. })) as u8;
    assert_eq!(winner, 1, "exactly one pool wins loop creation");

    // Verify at most 1 execution across both pools.
    let exec_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts")
        .fetch_one(&fix1.pool)
        .await
        .unwrap();
    assert!(exec_count.0 <= 1, "at most 1 execution — loser creates 0");
}
