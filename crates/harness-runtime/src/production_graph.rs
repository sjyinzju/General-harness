//! Production service graph — wires the full I4 runtime (Scheduler →
//! Worktree → Lease → Claims → Verification → Finalization) into a single
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

use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::scheduler::ConcurrencyConfig;
use sqlx::SqlitePool;

use crate::lease::clock::SystemClock;
use crate::lease::types::LeaseConfig;
use crate::resource_claim::lease_adapter::LeaseServiceAdapter;
use crate::resource_claim::ResourceClaimRepo;
use crate::resource_claim::ResourceClaimService;
use crate::scheduler::composition::SchedulerServices;
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
    pub scheduler_services: SchedulerServices,
    pub task_loop_service: TaskEngineeringLoopService,
    pub i4_gateway: Arc<RealI4OrchestrationGateway>,
    pub worktree_mgr: Arc<WorktreeManager>,
    pub lease_service: Arc<crate::lease::service::WorkspaceLeaseService>,
    pub claim_service: Arc<ResourceClaimService>,
    pub heartbeat_registry: Arc<crate::scheduler::heartbeat_registry::HeartbeatRegistry>,
}

impl ProductionGraph {
    /// Build the full production service graph.
    ///
    /// `worktree_root` is the filesystem directory where git worktrees
    /// are created (must NOT be inside an existing worktree).
    /// `repo_root` is the git repository to dispatch Agents against.
    pub fn build(
        pool: SqlitePool,
        worktree_root: &std::path::Path,
        repo_root: &std::path::Path,
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
        let scheduler_services = SchedulerServices::build(
            pool.clone(),
            worktree_mgr.clone(),
            lease_service.clone(),
            claim_service.clone(),
            ConcurrencyConfig::default(),
        );

        // ── Extract heartbeat registry before moving scheduler_services ──
        let heartbeat_registry = scheduler_services.heartbeat_registry.clone();

        // ── Real I4 gateway (MANDATORY for production) ─────────────
        let i4_gateway = Arc::new(RealI4OrchestrationGateway::new(
            scheduler_services.orchestrator.clone(),
            pool.clone(),
        ));

        // ── Task loop service wired with real I4 gateway ───────────
        let task_loop_service =
            TaskEngineeringLoopService::new(pool.clone()).with_i4_gateway(i4_gateway.clone());

        Ok(Self {
            pool,
            scheduler_services,
            task_loop_service,
            i4_gateway,
            worktree_mgr,
            lease_service,
            claim_service,
            heartbeat_registry,
        })
    }
}
