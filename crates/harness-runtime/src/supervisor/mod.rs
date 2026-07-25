//! Supervisor — long-running harness daemon with exclusive ownership,
//! lease-based fencing, durable control loop, IPC server, and crash recovery.
//!
//! # Architecture
//!
//! ```text
//! CLI / Client
//!     ↓ Local IPC
//! Supervisor
//!     ↓ Durable Command Dispatcher
//! Deterministic Control Loop
//!     ↓
//! Task / Process / Workspace / Review / Commit / Integration
//!     ↓
//! SQLite + Git + Managed Artifacts
//! ```
//!
//! # Hard guarantees
//!
//! - Single active Supervisor per `state_directory_id`.
//! - Lease + fencing token, not PID file only.
//! - Old fencing token writes are rejected at the database level.
//! - All state transitions are durable (state update + event in same transaction).
//! - Terminal states cannot be overwritten.

pub mod command_handler;
pub mod control_loop;
pub mod heartbeat;
pub mod lifecycle;
pub mod ownership;
pub mod recovery;
pub mod repo;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use harness_core::contracts::ipc::IpcConfig;
use harness_core::contracts::supervisor::{
    SupervisorConfig, SupervisorEvent, SupervisorInstance, SupervisorInstanceId, SupervisorState,
    SupervisorStatus,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing;

use crate::ipc::IpcServer;
use crate::supervisor::control_loop::{ControlLoop, ControlLoopConfig};

use self::command_handler::SupervisorCommandHandler;
use self::heartbeat::HeartbeatHandle;
use self::lifecycle::LifecycleFsm;
use self::ownership::OwnershipManager;
use self::repo::SupervisorRepo;

use crate::commit::service::ControlledCommitService;
use crate::goal::evaluator::ProductionGoalEvaluator;
use crate::goal::planner::ProductionGoalPlanner;
use crate::goal::service::GoalLoopService;
use crate::integration::executor::IntegrationExecutor;
use crate::integration::recovery::IntegrationRecoveryService;
use crate::integration::service::IntegrationQueueService;
use crate::liveness::{LivenessOrchestrator, RunContext};
use crate::prompt::PromptRegistry;
use crate::resource_claim::ResourceClaimService;
use crate::review::service::ReviewOrchestrationService;
use crate::scheduler::composition::SchedulerServices;
use crate::task_loop::gateway::RealI4OrchestrationGateway;
use crate::task_loop::service::TaskEngineeringLoopService;
use crate::worktree::manager::WorktreeManager;

/// Bundled production services for the Supervisor daemon.
///
/// This is the single composition point for all services the Supervisor
/// needs: IPC command handling, control loop execution, and crash recovery.
/// Constructed by [`ProductionGraph::build`] and passed to [`Supervisor::run`].
#[derive(Clone)]
pub struct SupervisorServices {
    pub pool: SqlitePool,
    pub supervisor_repo: SupervisorRepo,
    pub task_loop_service: Arc<TaskEngineeringLoopService>,
    pub i4_gateway: Arc<RealI4OrchestrationGateway>,
    pub worktree_mgr: Arc<WorktreeManager>,
    pub lease_service: Arc<crate::lease::service::WorkspaceLeaseService>,
    pub claim_service: Arc<ResourceClaimService>,
    pub commit_service: Arc<ControlledCommitService>,
    pub review_service: Arc<ReviewOrchestrationService>,
    pub integration_queue: Arc<IntegrationQueueService>,
    pub integration_executor: Arc<IntegrationExecutor>,
    pub integration_recovery: Arc<IntegrationRecoveryService>,
    pub liveness_orchestrator: Arc<LivenessOrchestrator>,
    pub run_context: Arc<RunContext>,
    /// Scheduler services (wrapped for Clone).
    pub scheduler_services: Arc<SchedulerServices>,
    /// Goal loop service for I7 outer-loop orchestration.
    pub goal_loop_service: Arc<GoalLoopService>,
    /// Goal Planner (production, optional — wired when profiles available).
    pub goal_planner: Option<Arc<ProductionGoalPlanner>>,
    /// Goal Evaluator (production, optional — wired when profiles available).
    pub goal_evaluator: Option<Arc<ProductionGoalEvaluator>>,
    /// Prompt registry for versioned goal prompts.
    pub prompt_registry: Arc<PromptRegistry>,
    /// Repository root path for integration and commit operations.
    pub repo_root: std::path::PathBuf,
    /// Integration root path for sandboxed execution.
    pub integration_root: std::path::PathBuf,
}

/// The Supervisor service — the single production owner of the harness runtime.
///
/// Construct via [`Supervisor::new`] and then call [`Supervisor::run`] for
/// foreground operation, or use the lifecycle methods for programmatic control.
pub struct Supervisor {
    config: SupervisorConfig,
    pool: SqlitePool,
    repo: SupervisorRepo,

    /// Bundled production services for IPC, control loop, and recovery.
    services: SupervisorServices,

    /// Current instance identity.
    instance: Arc<RwLock<Option<SupervisorInstance>>>,

    /// Lifecycle state machine.
    fsm: LifecycleFsm,

    /// Ownership manager (lease + fencing).
    ownership: OwnershipManager,

    /// Active heartbeat handle (None if not running).
    heartbeat: Arc<RwLock<Option<HeartbeatHandle>>>,
}

impl Supervisor {
    /// Create a new Supervisor. Does NOT start the control loop or
    /// acquire ownership — call [`run`] or use the step-by-step lifecycle.
    pub fn new(config: SupervisorConfig, pool: SqlitePool, services: SupervisorServices) -> Self {
        let repo = services.supervisor_repo.clone();
        let fsm = LifecycleFsm::new();
        let ownership = OwnershipManager::new(pool.clone(), config.clone());

        Self {
            config,
            pool,
            repo,
            services,
            instance: Arc::new(RwLock::new(None)),
            fsm,
            ownership,
            heartbeat: Arc::new(RwLock::new(None)),
        }
    }

    /// Run the Supervisor foreground (startup → ownership → recovery → ready).
    /// Blocks until shutdown is requested or an unrecoverable error occurs.
    ///
    /// After reaching Ready state, starts the IPC server (Named Pipe), control loop,
    /// and heartbeat. The IPC server must bind successfully before the supervisor
    /// is considered fully ready for external commands.
    pub async fn run(&self, state_directory_id: &str) -> Result<(), CoreError> {
        // 1. Create instance record
        let instance = self.create_instance(state_directory_id).await?;
        *self.instance.write().await = Some(instance.clone());

        // 2. Created → Starting
        self.transition_to(SupervisorState::Starting, &instance)
            .await?;

        // Refresh instance state (transition_to updates self.instance, not local var)
        let instance = {
            let guard = self.instance.read().await;
            guard.as_ref().unwrap().clone()
        };

        // 3. Starting → AcquiringOwnership
        self.transition_to(SupervisorState::AcquiringOwnership, &instance)
            .await?;

        let instance = {
            let guard = self.instance.read().await;
            guard.as_ref().unwrap().clone()
        };

        // 4. Acquire ownership (CAS on supervisor_leases)
        let result = self.ownership.acquire(&instance).await;

        match result {
            Ok(()) => self.run_ready_and_serve(&instance).await,
            Err(e) => {
                // Check if this is a "rejected" vs "stale owner" case
                let msg = e.to_string();
                if msg.contains("stale") {
                    // Takeover path
                    self.transition_to(SupervisorState::TakingOver, &instance)
                        .await?;

                    let takeover_result = self.ownership.takeover_and_acquire(&instance).await?;

                    let mut updated_instance = instance.clone();
                    updated_instance.fencing_token = takeover_result.new_fencing_token;
                    self.repo
                        .update_fencing_token(
                            &instance.instance_id,
                            takeover_result.new_fencing_token,
                        )
                        .await?;

                    *self.instance.write().await = Some(updated_instance.clone());

                    self.run_ready_and_serve(&updated_instance).await
                } else {
                    // Genuine rejection — healthy owner exists
                    self.emit_event(
                        &instance,
                        SupervisorEvent::SupervisorOwnershipRejected {
                            instance_id: instance.instance_id.clone(),
                            reason: msg,
                            active_instance_id: None,
                            occurred_at: Utc::now(),
                        },
                    )
                    .await?;

                    self.transition_to(SupervisorState::Failed, &instance)
                        .await?;

                    Err(e)
                }
            }
        }
    }

    /// Post-ownership: recover → ready → start IPC + control loop → serve until shutdown.
    async fn run_ready_and_serve(&self, instance: &SupervisorInstance) -> Result<(), CoreError> {
        // Recover
        self.transition_to(SupervisorState::Recovering, instance)
            .await?;
        self.run_startup_recovery(instance).await?;

        // Ready
        self.transition_to(SupervisorState::Ready, instance).await?;

        // Start heartbeat
        self.start_heartbeat(instance).await?;

        // ── Wire production IPC server ──────────────────────────────
        let cancel = CancellationToken::new();
        let ipc_endpoint = self.config.ipc_endpoint.clone();

        // Create command handler (shared with IPC server)
        let handler = Arc::new(SupervisorCommandHandler::new(
            self.pool.clone(),
            self.services.clone(),
            Some(instance.instance_id.clone()),
            instance.fencing_token,
        ));

        // Build IPC config from supervisor config
        let ipc_config = IpcConfig {
            max_frame_bytes: self.config.max_ipc_frame_bytes,
            max_connections: self.config.max_ipc_connections,
            max_inflight_requests: self.config.max_inflight_requests,
            ..Default::default()
        };

        let ipc_server = Arc::new(IpcServer::new(
            ipc_config,
            handler.clone(),
            self.pool.clone(),
        ));

        // Create ControlLoop
        let control_loop = ControlLoop::new(
            ControlLoopConfig {
                max_concurrency: self.config.max_operation_concurrency,
                ..Default::default()
            },
            self.pool.clone(),
            self.repo.clone(),
            instance.instance_id.clone(),
            instance.fencing_token,
        );

        // ── Spawn IPC accept loop ───────────────────────────────────
        let ipc_cancel = cancel.child_token();
        let ipc_server_task = {
            let ipc_server = ipc_server.clone();
            let endpoint = ipc_endpoint.clone();
            tokio::spawn(async move {
                tokio::select! {
                    result = ipc_server.serve(&endpoint) => {
                        if let Err(e) = result {
                            tracing::error!(error = %e, endpoint = %endpoint, "IPC server exited with error");
                        }
                    }
                    _ = ipc_cancel.cancelled() => {
                        tracing::info!("IPC server cancellation received");
                        ipc_server.shutdown().await;
                    }
                }
            })
        };

        // ── Spawn ControlLoop ───────────────────────────────────────
        let cl_cancel = cancel.child_token();
        let cl_handle = tokio::spawn(async move {
            control_loop.run(cl_cancel).await;
        });

        tracing::info!(
            instance_id = %instance.instance_id,
            fencing_token = instance.fencing_token,
            ipc_endpoint = %ipc_endpoint,
            "supervisor ready — IPC server bound, control loop started"
        );

        // ── Wait for shutdown signal ────────────────────────────────
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown signal (Ctrl+C) received, draining...");
            }
            _ = cancel.cancelled() => {
                tracing::info!("internal shutdown requested, draining...");
            }
        }

        // ── Graceful shutdown sequence ──────────────────────────────
        tracing::info!("beginning graceful shutdown...");

        // 1. Stop accepting new IPC connections
        ipc_server.shutdown().await;

        // 2. Cancel background tasks
        cancel.cancel();

        // 3. Wait for tasks to drain (with bounded timeout)
        let _ = tokio::time::timeout(
            Duration::from_secs(self.config.shutdown_grace_period_secs),
            async {
                let _ = tokio::join!(ipc_server_task, cl_handle);
            },
        )
        .await;

        // 4. Stop heartbeat
        self.stop_heartbeat().await;

        // 5. Transition through Draining → Stopping → Stopped
        let instance_refreshed = {
            let guard = self.instance.read().await;
            guard.as_ref().unwrap().clone()
        };

        self.transition_to(SupervisorState::Draining, &instance_refreshed)
            .await?;

        let instance_refreshed = {
            let guard = self.instance.read().await;
            guard.as_ref().unwrap().clone()
        };

        self.transition_to(SupervisorState::Stopping, &instance_refreshed)
            .await?;

        // Release lease
        self.ownership.release_lease(&instance_refreshed).await?;

        let instance_refreshed = {
            let guard = self.instance.read().await;
            guard.as_ref().unwrap().clone()
        };

        self.transition_to(SupervisorState::Stopped, &instance_refreshed)
            .await?;

        tracing::info!(
            instance_id = %instance.instance_id,
            "supervisor stopped cleanly"
        );

        Ok(())
    }

    /// Run startup reconciliation using the real RecoveryOrchestrator.
    async fn run_startup_recovery(&self, instance: &SupervisorInstance) -> Result<(), CoreError> {
        let orchestrator =
            recovery::RecoveryOrchestrator::new(self.pool.clone(), self.repo.clone())
                .with_services(
                    self.services.integration_recovery.clone(),
                    self.services.liveness_orchestrator.clone(),
                );

        let summary = orchestrator
            .reconcile(&instance.instance_id, instance.fencing_token)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::Internal,
                    format!("startup recovery failed: {e}"),
                    ErrorSource::Harness,
                )
            })?;

        if summary.has_errors() {
            tracing::warn!(
                recovery_id = %summary.recovery_id,
                errors = ?summary.errors,
                "startup recovery completed with errors"
            );
        }

        tracing::info!(
            recovery_id = %summary.recovery_id,
            processes_terminated = summary.processes_terminated,
            worktrees_cleaned = summary.worktrees_cleaned,
            integrations_recovered = summary.integrations_recovered,
            claims_released = summary.claims_released,
            artifacts_cleaned = summary.artifacts_cleaned,
            "startup recovery complete"
        );

        Ok(())
    }

    /// Request graceful shutdown.
    pub async fn stop(&self) -> Result<(), CoreError> {
        let instance_guard = self.instance.read().await;
        let instance = match instance_guard.as_ref() {
            Some(i) => i.clone(),
            None => {
                return Err(CoreError::new(
                    ErrorCode::InvalidState,
                    "supervisor not started",
                    ErrorSource::Harness,
                ));
            }
        };
        drop(instance_guard);

        // Transition to Draining
        self.transition_to(SupervisorState::Draining, &instance)
            .await?;

        // Stop heartbeat
        self.stop_heartbeat().await;

        // Transition to Stopping
        self.transition_to(SupervisorState::Stopping, &instance)
            .await?;

        // Release lease
        self.ownership.release_lease(&instance).await?;

        // Transition to Stopped (terminal)
        self.transition_to(SupervisorState::Stopped, &instance)
            .await?;

        tracing::info!(instance_id = %instance.instance_id, "supervisor stopped");
        Ok(())
    }

    /// Get current status snapshot.
    pub async fn status(&self) -> Result<SupervisorStatus, CoreError> {
        let instance_guard = self.instance.read().await;
        let instance = instance_guard.as_ref().ok_or_else(|| {
            CoreError::new(
                ErrorCode::InvalidState,
                "supervisor not started",
                ErrorSource::Harness,
            )
        })?;

        let now = Utc::now();
        let uptime_secs = (now - instance.started_at).num_seconds().max(0) as u64;

        Ok(SupervisorStatus {
            instance_id: instance.instance_id.clone(),
            state: instance.state,
            pid: instance.pid,
            process_started_at: instance.process_started_at,
            fencing_token: instance.fencing_token,
            started_at: instance.started_at,
            heartbeat_at: instance.heartbeat_at,
            lease_expires_at: instance.lease_expires_at,
            uptime_secs,
            protocol_version: instance.protocol_version.clone(),
            active_operations: 0,
            running_processes: 0,
            queue_depth: 0,
            active_claims: 0,
            active_leases: 0,
            recovery_state: None,
            last_recovery_summary: None,
            janitor_status: "not_started".to_string(),
            ipc_connections: 0,
            orphan_count: 0,
        })
    }

    /// Get a clone of the current instance (refreshing from in-memory state).
    #[allow(dead_code)]
    async fn current_instance(&self) -> SupervisorInstance {
        self.instance.read().await.as_ref().unwrap().clone()
    }

    /// Get current fencing token.
    pub async fn fencing_token(&self) -> Option<i64> {
        self.instance.read().await.as_ref().map(|i| i.fencing_token)
    }

    /// Get the supervisor config.
    pub fn config(&self) -> &SupervisorConfig {
        &self.config
    }

    /// Get the database pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Get the bundled production services.
    pub fn services(&self) -> &SupervisorServices {
        &self.services
    }

    // ── Private helpers ──────────────────────────────────────────────────

    async fn create_instance(
        &self,
        state_directory_id: &str,
    ) -> Result<SupervisorInstance, CoreError> {
        let instance_id = SupervisorInstanceId(uuid::Uuid::new_v4().to_string());
        let pid = std::process::id();
        let process_started_at = get_process_start_time();
        let boot_nonce = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let instance = SupervisorInstance {
            instance_id: instance_id.clone(),
            state_directory_id: state_directory_id.to_string(),
            pid,
            process_started_at,
            boot_nonce,
            state: SupervisorState::Created,
            fencing_token: 0,
            started_at: now,
            heartbeat_at: now,
            lease_expires_at: now + Duration::from_secs(self.config.lease_duration_secs),
            protocol_version: "1.0".to_string(),
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        };

        self.repo.insert_instance(&instance).await?;
        self.emit_event(
            &instance,
            SupervisorEvent::SupervisorStarting {
                instance_id: instance_id.clone(),
                pid,
                process_started_at,
                occurred_at: now,
            },
        )
        .await?;

        Ok(instance)
    }

    async fn transition_to(
        &self,
        new_state: SupervisorState,
        instance: &SupervisorInstance,
    ) -> Result<(), CoreError> {
        // Read current state from in-memory instance (may be newer than the parameter)
        let current_state = {
            let guard = self.instance.read().await;
            guard.as_ref().map(|i| i.state).unwrap_or(instance.state)
        };

        // Validate transition against the freshest state
        self.fsm
            .validate_transition(current_state, new_state)
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::InvalidStateTransition {
                        from: instance.state.to_string(),
                        to: new_state.to_string(),
                    },
                    format!("illegal supervisor state transition: {e}"),
                    ErrorSource::Harness,
                )
            })?;

        // Update in-memory instance state FIRST (before potentially skipping event)
        {
            let mut guard = self.instance.write().await;
            if let Some(ref mut inst) = *guard {
                inst.state = new_state;
            }
        }

        // Build event for this transition (some states don't need events)
        let event = match new_state {
            SupervisorState::Starting => SupervisorEvent::SupervisorStarting {
                instance_id: instance.instance_id.clone(),
                pid: instance.pid,
                process_started_at: instance.process_started_at,
                occurred_at: Utc::now(),
            },
            SupervisorState::AcquiringOwnership => SupervisorEvent::SupervisorStarting {
                instance_id: instance.instance_id.clone(),
                pid: instance.pid,
                process_started_at: instance.process_started_at,
                occurred_at: Utc::now(),
            },
            SupervisorState::Ready => SupervisorEvent::SupervisorReady {
                instance_id: instance.instance_id.clone(),
                occurred_at: Utc::now(),
            },
            SupervisorState::Draining => SupervisorEvent::SupervisorDraining {
                instance_id: instance.instance_id.clone(),
                reason: "stop requested".to_string(),
                occurred_at: Utc::now(),
            },
            SupervisorState::Stopping => SupervisorEvent::SupervisorStopping {
                instance_id: instance.instance_id.clone(),
                occurred_at: Utc::now(),
            },
            SupervisorState::Stopped => SupervisorEvent::SupervisorStopped {
                instance_id: instance.instance_id.clone(),
                occurred_at: Utc::now(),
            },
            SupervisorState::Failed => SupervisorEvent::SupervisorFailed {
                instance_id: instance.instance_id.clone(),
                reason: "ownership rejected".to_string(),
                occurred_at: Utc::now(),
            },
            SupervisorState::Recovering | SupervisorState::TakingOver => {
                // Persist state update without event
                self.repo
                    .update_state_no_event(&instance.instance_id, new_state)
                    .await?;
                return Ok(());
            }
            _ => {
                // Created or other states: persist state update without event
                self.repo
                    .update_state_no_event(&instance.instance_id, new_state)
                    .await?;
                return Ok(());
            }
        };

        // Persist state update + event in same transaction
        self.repo
            .update_state_and_append_event(&instance.instance_id, new_state, &event)
            .await?;

        Ok(())
    }

    async fn emit_event(
        &self,
        instance: &SupervisorInstance,
        event: SupervisorEvent,
    ) -> Result<(), CoreError> {
        let event_type = event_type_str(&event);
        let payload_json = serde_json::to_string(&event).map_err(|e| {
            CoreError::new(
                ErrorCode::SerializationError,
                format!("failed to serialize supervisor event: {e}"),
                ErrorSource::Harness,
            )
        })?;

        self.repo
            .append_event(&instance.instance_id, &event_type, &payload_json)
            .await
    }

    async fn start_heartbeat(&self, instance: &SupervisorInstance) -> Result<(), CoreError> {
        let handle = HeartbeatHandle::start(
            self.repo.clone(),
            instance.instance_id.clone(),
            instance.fencing_token,
            Duration::from_secs(self.config.heartbeat_interval_secs),
            Duration::from_secs(self.config.lease_duration_secs),
            self.instance.clone(),
        )
        .await;

        *self.heartbeat.write().await = Some(handle);
        Ok(())
    }

    async fn stop_heartbeat(&self) {
        if let Some(handle) = self.heartbeat.write().await.take() {
            handle.stop().await;
        }
    }
}

fn event_type_str(event: &SupervisorEvent) -> String {
    match event {
        SupervisorEvent::SupervisorStarting { .. } => "supervisor_starting",
        SupervisorEvent::SupervisorOwnershipAcquired { .. } => "supervisor_ownership_acquired",
        SupervisorEvent::SupervisorOwnershipRejected { .. } => "supervisor_ownership_rejected",
        SupervisorEvent::SupervisorHeartbeat { .. } => "supervisor_heartbeat",
        SupervisorEvent::SupervisorReady { .. } => "supervisor_ready",
        SupervisorEvent::SupervisorDraining { .. } => "supervisor_draining",
        SupervisorEvent::SupervisorStopping { .. } => "supervisor_stopping",
        SupervisorEvent::SupervisorStopped { .. } => "supervisor_stopped",
        SupervisorEvent::SupervisorLeaseLost { .. } => "supervisor_lease_lost",
        SupervisorEvent::SupervisorStaleOwnerDetected { .. } => "supervisor_stale_owner_detected",
        SupervisorEvent::SupervisorTakeoverStarted { .. } => "supervisor_takeover_started",
        SupervisorEvent::SupervisorTakeoverCompleted { .. } => "supervisor_takeover_completed",
        SupervisorEvent::SupervisorFailed { .. } => "supervisor_failed",
    }
    .to_string()
}

/// Get the OS process start time.
/// On Windows, uses `GetProcessTimes` via the process handle.
/// On Unix, reads `/proc/self/stat`.
#[allow(unsafe_code)]
fn get_process_start_time() -> chrono::DateTime<chrono::Utc> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{FILETIME, HANDLE};
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

        unsafe {
            let process: HANDLE = GetCurrentProcess();
            let mut _creation: FILETIME = std::mem::zeroed();
            let mut _exit: FILETIME = std::mem::zeroed();
            let mut _kernel: FILETIME = std::mem::zeroed();
            let mut _user: FILETIME = std::mem::zeroed();

            if GetProcessTimes(
                process,
                &mut _creation,
                &mut _exit,
                &mut _kernel,
                &mut _user,
            ) != 0
            {
                let ticks =
                    ((_creation.dwHighDateTime as u64) << 32) | (_creation.dwLowDateTime as u64);
                // FILETIME is 100-nanosecond intervals since 1601-01-01
                let unix_epoch_ticks = 11_644_473_600_000_000_000u64;
                if ticks > unix_epoch_ticks {
                    let secs = (ticks - unix_epoch_ticks) / 10_000_000;
                    let nanos = ((ticks - unix_epoch_ticks) % 10_000_000) * 100;
                    return chrono::DateTime::from_timestamp(secs as i64, nanos as u32)
                        .unwrap_or_else(chrono::Utc::now);
                }
            }
        }
    }

    #[cfg(not(windows))]
    {
        chrono::Utc::now()
    }

    chrono::Utc::now()
}
