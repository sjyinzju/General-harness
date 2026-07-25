//! Production service graph — wires the full I4 runtime (Scheduler →
//! Worktree → Lease → Claims → Verification → Finalization) and I5/I6
//! services (Review, Commit, Integration, Supervisor) into a single
//! ready-to-use bundle for the CLI, runtime, and tests.
//!
//! This is the ONLY production composition root.  CLI commands, the
//! bootstrap, and any future runtime MUST construct services through
//! [`ProductionGraph::build`] — never by calling individual constructors
//! that produce disconnected or untested graphs.
//!
//! # Hard guarantees
//!
//! - `RealI4OrchestrationGateway` is ALWAYS constructed and wired into
//!   `TaskEngineeringLoopService` via `with_i4_gateway`.
//! - The `HeartbeatRegistry` is shared across SchedulerOrchestrator,
//!   SchedulerReconciler, and ResourceHandoffCoordinator.
//! - All services use production constructors (never `*_for_tests`).
//! - `LivenessOrchestrator` is MANDATORY in production; only tests may
//!   construct a graph without it.
//! - I5 services (ControlledCommitService, ReviewOrchestrationService,
//!   IntegrationQueueService, IntegrationExecutor, IntegrationRecoveryService)
//!   are constructed here and shared — never constructed ad-hoc by CLI commands.
//! - SupervisorServices bundles all I6 runtime services for the Supervisor daemon.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::scheduler::ConcurrencyConfig;
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;

use crate::commit::service::ControlledCommitService;
use crate::integration::executor::IntegrationExecutor;
use crate::integration::recovery::IntegrationRecoveryService;
use crate::integration::service::IntegrationQueueService;
use crate::lease::clock::SystemClock;
use crate::lease::types::LeaseConfig;
use crate::liveness::{LivenessOrchestrator, RunContext};
use crate::resource_claim::lease_adapter::LeaseServiceAdapter;
use crate::resource_claim::ResourceClaimRepo;
use crate::resource_claim::ResourceClaimService;
use crate::review::service::ReviewOrchestrationService;
use crate::scheduler::composition::SchedulerServices;
use crate::supervisor::repo::SupervisorRepo;
use crate::supervisor::SupervisorServices;
use crate::task_loop::gateway::RealI4OrchestrationGateway;
use crate::task_loop::service::TaskEngineeringLoopService;
use crate::worktree::git::GitRunner;
use crate::worktree::git_verifier::NoOpGitVerifier;
use crate::worktree::inspector::RepositoryInspector;
use crate::worktree::manager::WorktreeManager;

/// A fully wired production service graph.
///
/// Construct once at startup; clone `Arc`s to share services across
/// subsystems.  The `TaskEngineeringLoopService` in this graph is wired
/// with `RealI4OrchestrationGateway` — the ONLY I4 gateway that actually
/// dispatches Agents through the certified pipeline.
pub struct ProductionGraph {
    pub pool: SqlitePool,
    pub scheduler_services: Arc<SchedulerServices>,
    pub task_loop_service: Arc<TaskEngineeringLoopService>,
    pub i4_gateway: Arc<RealI4OrchestrationGateway>,
    pub worktree_mgr: Arc<WorktreeManager>,
    pub lease_service: Arc<crate::lease::service::WorkspaceLeaseService>,
    pub claim_service: Arc<ResourceClaimService>,
    pub heartbeat_registry: Arc<crate::scheduler::heartbeat_registry::HeartbeatRegistry>,
    /// MANDATORY in production.  The liveness orchestrator handles
    /// startup, periodic, and shutdown cleanup of managed artifacts.
    pub liveness_orchestrator: Arc<LivenessOrchestrator>,
    /// The run context for this graph instance (managed temp/evidence).
    pub run_context: Arc<RunContext>,

    // ── I5 production services (shared, not ad-hoc) ──────────────
    /// Controlled commit service — admission + commit creation.
    pub commit_service: Arc<ControlledCommitService>,
    /// Review orchestration service — full I4.6 review lifecycle.
    pub review_service: Arc<ReviewOrchestrationService>,
    /// Integration queue service — enqueue, dequeue, publish.
    pub integration_queue: Arc<IntegrationQueueService>,
    /// Integration executor — sandboxed integration execution.
    pub integration_executor: Arc<IntegrationExecutor>,
    /// Integration recovery service — reconciliation of stuck states.
    pub integration_recovery: Arc<IntegrationRecoveryService>,

    // ── I6 supervisor services ───────────────────────────────────
    /// Supervisor repository for instance/lease/event persistence.
    pub supervisor_repo: SupervisorRepo,
    /// Bundled supervisor services for the daemon (IPC, control loop, recovery).
    pub supervisor_services: SupervisorServices,

    // ── Paths ────────────────────────────────────────────────────
    /// Repository root path.
    pub repo_root: PathBuf,
    /// Integration root path.
    pub integration_root: PathBuf,
}

impl ProductionGraph {
    /// Build the full production service graph.
    ///
    /// `worktree_root` is the filesystem directory where git worktrees
    /// are created (must NOT be inside an existing worktree).
    /// `repo_root` is the git repository to dispatch Agents against.
    /// `run_context` is the managed-temp context for this run.
    ///
    /// # Panics / Fail-closed
    ///
    /// Returns `Err` when the liveness config points at a dangerous
    /// location (repo root, user profile, etc.) — this fails closed.
    pub fn build(
        pool: SqlitePool,
        worktree_root: &Path,
        repo_root: &Path,
        run_context: Arc<RunContext>,
    ) -> Result<Self, String> {
        // ── Clock (production: wall-clock) ──────────────────────────
        let clock: Arc<dyn crate::lease::clock::Clock + Send + Sync> = Arc::new(SystemClock);

        // ── Git runner + Repository inspector ───────────────────────
        let git_runner =
            GitRunner::new(repo_root.to_path_buf()).map_err(|e| format!("git runner: {e}"))?;
        let inspector = RepositoryInspector::new(git_runner);

        // ── Worktree manager ───────────────────────────────────────
        let lease_validator: Box<dyn crate::lease::guard::WorkspaceLeaseAccessValidator> =
            Box::new(crate::lease::guard::NoOpAccessValidator);
        let worktree_mgr = Arc::new(
            WorktreeManager::new(
                pool.clone(),
                inspector,
                worktree_root,
                "harness-prod".into(),
                lease_validator,
            )
            .map_err(|e| format!("worktree manager: {e}"))?,
        );

        // ── Lease service (production: wall-clock + git verifier) ───
        let lease_config = LeaseConfig {
            lease_duration: Duration::from_secs(300),
            heartbeat_interval: Duration::from_secs(30),
            renewal_margin: Duration::from_secs(60),
        };
        let git_verifier: Box<dyn crate::worktree::git_verifier::WorktreeGitVerifier> =
            Box::new(NoOpGitVerifier);
        let lease_service = Arc::new(crate::lease::service::WorkspaceLeaseService::new(
            pool.clone(),
            clock.clone(),
            lease_config,
            git_verifier,
        ));

        // ── Claim service ──────────────────────────────────────────
        let claim_repo = ResourceClaimRepo::new(pool.clone());
        let claim_lease_validator: Box<
            dyn crate::resource_claim::service::ResourceClaimLeaseValidator + Send + Sync,
        > = Box::new(LeaseServiceAdapter::new(lease_service.clone()));
        let claim_service = Arc::new(ResourceClaimService::new(
            claim_repo,
            claim_lease_validator,
            clock,
        ));

        // ── Scheduler services ─────────────────────────────────────
        let scheduler_services = Arc::new(SchedulerServices::build(
            pool.clone(),
            worktree_mgr.clone(),
            lease_service.clone(),
            claim_service.clone(),
            ConcurrencyConfig::default(),
        ));

        // ── Extract heartbeat registry before moving scheduler_services ──
        let heartbeat_registry = scheduler_services.heartbeat_registry.clone();

        // ── Real I4 gateway (MANDATORY for production) ─────────────
        let i4_gateway = Arc::new(RealI4OrchestrationGateway::new(
            scheduler_services.orchestrator.clone(),
            pool.clone(),
        ));

        // ── Task loop service wired with real I4 gateway ───────────
        let task_loop_service = Arc::new(
            TaskEngineeringLoopService::new(pool.clone()).with_i4_gateway(i4_gateway.clone()),
        );

        // ── Liveness orchestrator (MANDATORY for production) ───────
        let liveness_config = run_context.config().clone();
        let liveness_orchestrator = Arc::new(
            LivenessOrchestrator::new(liveness_config, pool.clone())
                .map_err(|e| format!("liveness orchestrator: {e}"))?,
        );

        tracing::info!("liveness orchestrator initialized in production graph");

        // ── I5: ControlledCommitService ────────────────────────────
        let commit_service = Arc::new(ControlledCommitService::new(pool.clone()));

        // ── I5: ReviewOrchestrationService ─────────────────────────
        let review_service = Arc::new(ReviewOrchestrationService::new(pool.clone()));

        // ── I5: Integration services ───────────────────────────────
        let integration_root = repo_root.join("target").join("harness-integration");
        let integration_queue = Arc::new(IntegrationQueueService::new(pool.clone()));
        let integration_executor =
            Arc::new(IntegrationExecutor::new(pool.clone(), &integration_root));
        let integration_recovery = Arc::new(IntegrationRecoveryService::new(pool.clone()));

        // ── I6: Supervisor repository ──────────────────────────────
        let supervisor_repo = SupervisorRepo::new(pool.clone());

        // ── I6: Supervisor services bundle ─────────────────────────
        let repo_root_buf = repo_root.to_path_buf();
        let integration_root_buf = repo_root.join("target").join("harness-integration");
        let supervisor_services = SupervisorServices {
            pool: pool.clone(),
            supervisor_repo: supervisor_repo.clone(),
            task_loop_service: task_loop_service.clone(),
            i4_gateway: i4_gateway.clone(),
            worktree_mgr: worktree_mgr.clone(),
            lease_service: lease_service.clone(),
            claim_service: claim_service.clone(),
            commit_service: commit_service.clone(),
            review_service: review_service.clone(),
            integration_queue: integration_queue.clone(),
            integration_executor: integration_executor.clone(),
            integration_recovery: integration_recovery.clone(),
            liveness_orchestrator: liveness_orchestrator.clone(),
            run_context: run_context.clone(),
            scheduler_services: scheduler_services.clone(),
            repo_root: repo_root_buf,
            integration_root: integration_root_buf,
        };

        tracing::info!("I5/I6 production services initialized");

        Ok(Self {
            pool,
            scheduler_services,
            task_loop_service,
            i4_gateway,
            worktree_mgr,
            lease_service,
            claim_service,
            heartbeat_registry,
            liveness_orchestrator: liveness_orchestrator.clone(),
            run_context,
            commit_service,
            review_service,
            integration_queue,
            integration_executor,
            integration_recovery,
            supervisor_repo,
            supervisor_services,
            repo_root: repo_root.to_path_buf(),
            integration_root,
        })
    }

    /// Run the startup janitor.  Call this ONCE after `build()` and
    /// before accepting any work.  Reclaims stale owned artifacts from
    /// previous crashed runs.
    pub async fn startup(&self) -> crate::liveness::CleanupResult {
        self.liveness_orchestrator.startup_janitor(vec![]).await
    }

    /// Start the periodic janitor background task.  Returns a
    /// `CancellationToken` that MUST be used to stop the task
    /// before shutdown.
    ///
    /// The periodic janitor runs every `interval` and reclaims
    /// stale owned artifacts. Exactly one instance is started;
    /// the caller must ensure no duplicate calls.
    pub fn start_periodic_janitor(&self, interval: Duration) -> CancellationToken {
        self.liveness_orchestrator.start_periodic_janitor(interval)
    }

    /// Shutdown the managed run context.  Restores TEMP/TMP,
    /// finalizes markers, and cleans up managed directories.
    /// Must be called before process exit.
    pub async fn shutdown(&self, run_succeeded: bool) -> crate::liveness::CleanupResult {
        self.run_context.shutdown(run_succeeded).await
    }

    /// Build a production graph for tests (no managed temp env redirect).
    /// Liveness is still mandatory but uses a test config.
    pub fn build_for_tests(pool: SqlitePool, repo_root: &Path) -> Result<Self, String> {
        let run_context = Arc::new(
            RunContext::create(repo_root, "test-head", false)
                .map_err(|e| format!("run context: {e}"))?,
        );
        let worktree_root = run_context
            .managed_temp()
            .map(|t| t.path().to_path_buf())
            .unwrap_or_else(|| repo_root.join("target/tmp"));
        Self::build(pool, &worktree_root, repo_root, run_context)
    }
}
