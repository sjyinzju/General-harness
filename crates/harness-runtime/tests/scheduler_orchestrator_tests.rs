//! SchedulerOrchestrator Golden Path integration tests.
//!
//! Uses file-backed SQLite, real git repository, real SchedulerOrchestrator
//! with all real services, and inline FakeAdapter with observable behavior.
//! Never calls real Claude, Codex, or any paid API.

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
use harness_core::contracts::scheduler::{ConcurrencyConfig, DispatchStatus};
use harness_core::contracts::task_envelope::TaskEnvelope;
use harness_runtime::db::Database;
use harness_runtime::lease::clock::TestClock;
use harness_runtime::lease::guard::{NoOpAccessValidator, WorkspaceLeaseAccessValidator};
use harness_runtime::lease::service::WorkspaceLeaseService;
use harness_runtime::lease::types::LeaseConfig;
use harness_runtime::resource_claim::service::{ResourceClaimLeaseValidator, ResourceClaimService};
use harness_runtime::resource_claim::ResourceClaimRepo;
use harness_runtime::scheduler::{
    heartbeat_registry::TakeoverResult, ConcurrencyManager, DispatchRequest, HandoffRepository,
    HeartbeatRegistry, ResourceHandoffCoordinator, SchedulerOrchestrator,
};
use harness_runtime::transition::TransitionService;
use harness_runtime::worktree::git::GitRunner;
use harness_runtime::worktree::inspector::RepositoryInspector;
use harness_runtime::worktree::manager::WorktreeManager;

// ── Inline FakeAdapter with start-count tracking ─────────────────────

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

    fn set_fail_receive(&self, fail: bool) {
        self.fail_receive.store(fail, Ordering::SeqCst);
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
                "simulated receive_events failure",
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

// ── Helpers ──────────────────────────────────────────────────────────

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
    let sid = uuid::Uuid::new_v4().to_string();
    vec![
        AgentEvent::SessionStarted {
            session_id: sid.clone(),
            profile_id: "prof-fake-1".into(),
        },
        AgentEvent::Result {
            content: "done".into(),
            is_error: false,
        },
        AgentEvent::ProcessExited {
            exit_code: 0,
            signal: None,
        },
        AgentEvent::SessionEnded {
            session_id: sid,
            synthetic: true,
            termination_reason: harness_core::contracts::agent_event::TerminationReason::Completed,
            result_received: true,
            process_exit_received: true,
        },
    ]
}

async fn create_temp_git_repo() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("harness-sched-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let status = std::process::Command::new("git")
        .arg("init")
        .arg("--initial-branch=main")
        .arg(&dir)
        .output()
        .unwrap();
    assert!(status.status.success(), "git init failed: {}", String::from_utf8_lossy(&status.stderr));
    // Configure git identity so commits succeed on systems without global config.
    for (key, val) in &[("user.name", "scheduler-test"), ("user.email", "test@harness.local")] {
        let cfg = std::process::Command::new("git")
            .args(["config", key, val])
            .current_dir(&dir)
            .output()
            .unwrap();
        assert!(cfg.status.success(), "git config {key} failed");
    }
    std::fs::write(dir.join("README.md"), "# test\n").unwrap();
    let add = std::process::Command::new("git")
        .args(["add", "README.md"])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(add.status.success(), "git add failed: {}", String::from_utf8_lossy(&add.stderr));
    let commit = std::process::Command::new("git")
        .args(["commit", "-m", "init"])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(commit.status.success(), "git commit failed: {}", String::from_utf8_lossy(&commit.stderr));
    dir
}

async fn make_wt_mgr(pool: sqlx::SqlitePool) -> Arc<WorktreeManager> {
    // Must use the same root that dispatch.rs hardcodes: temp_dir/harness-worktrees
    let root = std::env::temp_dir().join("harness-worktrees");
    std::fs::create_dir_all(&root).unwrap();
    let scratch = std::env::temp_dir().join(format!("harness-git-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&scratch).unwrap();
    let git = GitRunner::new(scratch).unwrap();
    let insp = RepositoryInspector::new(git);
    let noop: Box<dyn WorkspaceLeaseAccessValidator> = Box::new(NoOpAccessValidator);
    Arc::new(WorktreeManager::new(pool, insp, &root, "sched-main".into(), noop).unwrap())
}

async fn setup_orchestrator(
    db_path: &Path,
) -> (
    SchedulerOrchestrator,
    Arc<HeartbeatRegistry>,
    Arc<AtomicUsize>,
    FakeAdapter,
    sqlx::SqlitePool,
) {
    let db = Database::open(db_path).await.unwrap();
    let pool = db.pool.clone();

    sqlx::query("INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES ('proj-test','test','active')").execute(&pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES ('task-test','proj-test','test','ready')").execute(&pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO runtime_profiles (id, agent_kind, adapter_kind, agent_version, executable_path, provider, provider_source, auth_mode, auth_status, core_status, authentication_status, execution_status) VALUES ('prof-fake-1','fake','fake','1.0','fake','fake','user_declared','none','unknown','available','unknown','untested')").execute(&pool).await.unwrap();

    let clock = Arc::new(TestClock::new(chrono::Utc::now()));
    let transitions = TransitionService::new(pool.clone());
    let concurrency = ConcurrencyManager::new(pool.clone(), ConcurrencyConfig::default());
    let wt_mgr = make_wt_mgr(pool.clone()).await;

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

    let hb_reg = Arc::new(HeartbeatRegistry::new());
    let ho_repo = HandoffRepository::new(pool.clone());

    let orch = SchedulerOrchestrator::new(
        pool.clone(),
        transitions,
        concurrency,
        wt_mgr,
        lease_service,
        claim_service,
        hb_reg.clone(),
        ho_repo,
    );

    let start_count = Arc::new(AtomicUsize::new(0));
    let adapter = FakeAdapter::new(start_count.clone());

    (orch, hb_reg, start_count, adapter, pool)
}

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn golden_path_success_retains_resources() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let repo = create_temp_git_repo().await;
    let (orch, hb_reg, start_count, adapter, pool) = setup_orchestrator(&db_path).await;
    adapter.set_events(success_events());

    let outcome = orch
        .dispatch(&DispatchRequest {
            task_id: "task-test",
            project_id: "proj-test",
            profile_id: "prof-fake-1",
            repo_path: &repo,
            adapter: &adapter,
            task_goal: "test goal",
            timeout: Duration::from_secs(30),
            env: HashMap::new(),
        })
        .await
        .unwrap();

    assert_eq!(
        outcome.status,
        DispatchStatus::AgentCompleted,
        "dispatch failed: {:?}",
        outcome.terminal_outcome
    );
    let exec_id = outcome.execution_id.as_ref().unwrap();
    // Strict: agent started exactly once.
    assert_eq!(start_count.load(Ordering::SeqCst), 1);

    // ── Reservation released ────────────────────────────────────
    let ar: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM scheduler_reservations WHERE status='active' AND task_id='task-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ar.0, 0);

    // ── Task → Submitted ────────────────────────────────────────
    let tl: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='task-test'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(tl.0, "submitted");

    // ── Execution → Completed ───────────────────────────────────
    let el: (String,) = sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id=?")
        .bind(exec_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(el.0, "completed");

    // ── Active Lease retained ───────────────────────────────────
    let lc: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM workspace_leases WHERE lifecycle='active' AND task_id='task-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lc.0, 1);

    // ── Lease expiry extends beyond now ─────────────────────────
    let lease_exp: (String,) = sqlx::query_as(
        "SELECT expires_at FROM workspace_leases WHERE lifecycle='active' AND task_id='task-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let expiry_dt = chrono::NaiveDateTime::parse_from_str(&lease_exp.0, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|d| d.and_utc().into())
        .unwrap();
    assert!(
        expiry_dt > chrono::Utc::now(),
        "lease expiry must be in the future"
    );

    // ── Worktree DB record retained ─────────────────────────────
    let wt_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE execution_id=?")
        .bind(exec_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(wt_count.0 >= 1, "worktree DB record must exist");

    // ── Filesystem worktree path exists ─────────────────────────
    let wt_path: (Option<String>,) =
        sqlx::query_as("SELECT worktree_path FROM worktrees WHERE execution_id=? LIMIT 1")
            .bind(exec_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    if let Some(ref path_str) = wt_path.0 {
        assert!(
            std::path::Path::new(path_str).exists(),
            "filesystem worktree path must exist: {path_str}"
        );
    }

    // ── Heartbeat healthy in runtime registry ───────────────────
    let hb = hb_reg.inspect(exec_id).await.unwrap();
    assert_eq!(hb.status, "healthy");

    // ── Handoff persisted ───────────────────────────────────────
    let ho: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM resource_handoffs WHERE execution_id=?")
        .bind(exec_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ho.0, 1);

    // ── Handoff inspectable via coordinator (DB/registry consistent) ──
    let ho_repo = HandoffRepository::new(pool.clone());
    let coordinator = ResourceHandoffCoordinator::new(ho_repo, hb_reg.clone());
    let ci = coordinator.inspect_consistent(exec_id).await.unwrap();
    assert!(ci.consistent, "DB and registry must agree after success");
    assert_eq!(ci.db_owner_kind, "scheduler");
    assert_eq!(ci.db_owner_id, "scheduler-main");
    assert_eq!(ci.registry_owner_kind.as_deref(), Some("scheduler"));
    assert_eq!(ci.registry_owner_id.as_deref(), Some("scheduler-main"));

    // ── Exactly one execution for this task ─────────────────────
    let ec: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM execution_attempts WHERE task_id='task-test'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(ec.0, 1);

    let _ = std::fs::remove_dir_all(&repo);
}

#[tokio::test]
async fn response_lost_no_duplicate_agent() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let repo = create_temp_git_repo().await;
    let (orch, _hb, start_count, adapter, _pool) = setup_orchestrator(&db_path).await;
    adapter.set_events(success_events());

    let req = DispatchRequest {
        task_id: "task-test",
        project_id: "proj-test",
        profile_id: "prof-fake-1",
        repo_path: &repo,
        adapter: &adapter,
        task_goal: "test goal",
        timeout: Duration::from_secs(30),
        env: HashMap::new(),
    };

    let o1 = orch.dispatch(&req).await.unwrap();
    assert_eq!(
        o1.status,
        DispatchStatus::AgentCompleted,
        "dispatch failed: {:?}",
        o1.terminal_outcome
    );
    assert_eq!(start_count.load(Ordering::SeqCst), 1);

    let o2 = orch.dispatch(&req).await.unwrap();
    assert_eq!(
        start_count.load(Ordering::SeqCst),
        1,
        "response-lost must not restart agent"
    );
    assert_eq!(o2.execution_id, o1.execution_id);

    let _ = std::fs::remove_dir_all(&repo);
}

#[tokio::test]
async fn adapter_failure_releases_resources() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let repo = create_temp_git_repo().await;
    let (orch, _hb, _start_count, adapter, pool) = setup_orchestrator(&db_path).await;
    adapter.set_fail_receive(true);

    let outcome = orch
        .dispatch(&DispatchRequest {
            task_id: "task-test",
            project_id: "proj-test",
            profile_id: "prof-fake-1",
            repo_path: &repo,
            adapter: &adapter,
            task_goal: "test goal",
            timeout: Duration::from_secs(30),
            env: HashMap::new(),
        })
        .await
        .unwrap();
    assert!(matches!(outcome.status, DispatchStatus::Failed));

    let ar: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM scheduler_reservations WHERE status='active' AND task_id='task-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ar.0, 0);
    let lc: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM workspace_leases WHERE lifecycle='active' AND task_id='task-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lc.0, 0);
    let ho: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM resource_handoffs WHERE task_id='task-test'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(ho.0, 0);

    let _ = std::fs::remove_dir_all(&repo);
}

#[tokio::test]
async fn heartbeat_continues_after_dispatch() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let repo = create_temp_git_repo().await;
    let (orch, hb_reg, _sc, adapter, pool) = setup_orchestrator(&db_path).await;
    adapter.set_events(success_events());

    let outcome = orch
        .dispatch(&DispatchRequest {
            task_id: "task-test",
            project_id: "proj-test",
            profile_id: "prof-fake-1",
            repo_path: &repo,
            adapter: &adapter,
            task_goal: "test goal",
            timeout: Duration::from_secs(30),
            env: HashMap::new(),
        })
        .await
        .unwrap();
    let exec_id = outcome.execution_id.as_ref().unwrap();

    let hb = hb_reg.inspect(exec_id).await.unwrap();
    assert_eq!(hb.status, "healthy");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let hb2 = hb_reg.inspect(exec_id).await.unwrap();
    assert!(hb2.status.contains("healthy") || hb2.status.contains("degraded"));

    let exp: (String,) = sqlx::query_as(
        "SELECT expires_at FROM workspace_leases WHERE lifecycle='active' AND task_id='task-test'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let dt = chrono::NaiveDateTime::parse_from_str(&exp.0, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|d| d.and_utc().into())
        .unwrap();
    assert!(dt > chrono::Utc::now());

    hb_reg
        .cancel(exec_id, "scheduler-main", hb2.fencing_token)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&repo);
}

#[tokio::test]
async fn takeover_after_success() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let repo = create_temp_git_repo().await;
    let (orch, hb_reg, _sc, adapter, _pool) = setup_orchestrator(&db_path).await;
    adapter.set_events(success_events());

    let outcome = orch
        .dispatch(&DispatchRequest {
            task_id: "task-test",
            project_id: "proj-test",
            profile_id: "prof-fake-1",
            repo_path: &repo,
            adapter: &adapter,
            task_goal: "test goal",
            timeout: Duration::from_secs(30),
            env: HashMap::new(),
        })
        .await
        .unwrap();
    let exec_id = outcome.execution_id.as_ref().unwrap();

    let before = hb_reg.inspect(exec_id).await.unwrap();
    assert_eq!(before.owner_kind, "scheduler");
    assert_eq!(
        hb_reg
            .takeover(exec_id, "verify-run-42", before.fencing_token)
            .await,
        TakeoverResult::Acquired
    );

    let after = hb_reg.inspect(exec_id).await.unwrap();
    assert_eq!(after.owner_kind, "verification");
    assert_eq!(after.owner_id, "verify-run-42");

    hb_reg
        .cancel(exec_id, "verify-run-42", after.fencing_token)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&repo);
}

#[tokio::test]
async fn concurrent_dispatch_two_pools_one_winner() {
    let td = tempfile::tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let repo = create_temp_git_repo().await;

    let (orch1, _hb1, count1, adapter1, _pool1) = setup_orchestrator(&db_path).await;
    adapter1.set_events(success_events());

    // Second pool to same file with busy_timeout=30s
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;
    let opts2 = SqliteConnectOptions::from_str(&db_path.to_string_lossy())
        .unwrap()
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(30));
    let pool2 = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts2)
        .await
        .unwrap();

    let clock2 = Arc::new(TestClock::new(chrono::Utc::now()));
    let wt2 = make_wt_mgr(pool2.clone()).await;
    let ls2 = Arc::new(WorkspaceLeaseService::new_unverified_for_tests(
        pool2.clone(),
        clock2.clone(),
        LeaseConfig {
            lease_duration: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(1),
            renewal_margin: Duration::from_secs(30),
        },
    ));
    let cr2 = ResourceClaimRepo::new(pool2.clone());
    let lv2: Box<dyn ResourceClaimLeaseValidator> = Box::new(LeaseValidatorAdapter {
        lease_service: ls2.clone(),
    });
    let cs2 = Arc::new(ResourceClaimService::new(cr2, lv2, clock2));
    let hbr2 = Arc::new(HeartbeatRegistry::new());
    let hr2 = HandoffRepository::new(pool2.clone());
    let trans2 = TransitionService::new(pool2.clone());
    let conc2 = ConcurrencyManager::new(pool2.clone(), ConcurrencyConfig::default());
    let orch2 = SchedulerOrchestrator::new(pool2, trans2, conc2, wt2, ls2, cs2, hbr2, hr2);
    let count2 = Arc::new(AtomicUsize::new(0));
    let adapter2 = FakeAdapter::new(count2.clone());
    adapter2.set_events(success_events());

    // Same task_goal so both requests produce the same idempotency hash → one
    // is a true duplicate and must NOT start a second agent.
    let req = DispatchRequest {
        task_id: "task-test",
        project_id: "proj-test",
        profile_id: "prof-fake-1",
        repo_path: &repo,
        adapter: &adapter1,
        task_goal: "concurrent winner test",
        timeout: Duration::from_secs(30),
        env: HashMap::new(),
    };
    let req2 = DispatchRequest {
        task_id: "task-test",
        project_id: "proj-test",
        profile_id: "prof-fake-1",
        repo_path: &repo,
        adapter: &adapter2,
        task_goal: "concurrent winner test",
        timeout: Duration::from_secs(30),
        env: HashMap::new(),
    };

    let (r1, r2) = tokio::join!(orch1.dispatch(&req), orch2.dispatch(&req2));
    let o1 = r1.unwrap();
    let o2 = r2.unwrap();

    // Exactly one must be AgentCompleted.
    let one_completed =
        o1.status == DispatchStatus::AgentCompleted || o2.status == DispatchStatus::AgentCompleted;
    assert!(
        one_completed,
        "at least one should complete: {:?} / {:?} (outcomes: {:?} / {:?})",
        o1.status, o2.status, o1.terminal_outcome, o2.terminal_outcome
    );

    // Strict winner: FakeAdapter must be started exactly once.
    let total = count1.load(Ordering::SeqCst) + count2.load(Ordering::SeqCst);
    assert_eq!(
        total, 1,
        "only one adapter must start; two starts = split-brain"
    );

    // Only one non-terminal execution for this task.
    let exec_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM execution_attempts WHERE task_id='task-test' AND lifecycle NOT IN ('completed','failed','lost','cancelled')",
    )
    .fetch_one(&_pool1)
    .await
    .unwrap();
    assert_eq!(
        exec_count.0, 0,
        "no active executions after both complete/fail"
    );

    // No residual active reservation.
    let res_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM scheduler_reservations WHERE status='active' AND task_id='task-test'",
    )
    .fetch_one(&_pool1)
    .await
    .unwrap();
    assert_eq!(res_count.0, 0, "no active reservations after dispatch");

    // At most one active lease (winner retained it on success).
    let lease_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM workspace_leases WHERE lifecycle='active' AND task_id='task-test'",
    )
    .fetch_one(&_pool1)
    .await
    .unwrap();
    assert!(lease_count.0 <= 1, "at most one active lease");

    // The loser must not have started an agent — its status must not be AgentCompleted.
    let both_completed =
        o1.status == DispatchStatus::AgentCompleted && o2.status == DispatchStatus::AgentCompleted;
    assert!(
        !both_completed,
        "both cannot be AgentCompleted — duplicate dispatch must be rejected"
    );

    let _ = std::fs::remove_dir_all(&repo);
}
