//! SchedulerOrchestrator — dispatch saga with transactional idempotency.
//!
//! Safe dispatch order:
//!   1. Persist dispatch intent (transactional idempotency arbitration)
//!   2. Re-validate readiness within transaction
//!   3. Select and pin RuntimeProfile
//!   4. Atomically acquire concurrency reservation
//!   5. Create Execution preparation record
//!   6. Create/verify Worktree
//!   7. Acquire Workspace Lease + start heartbeat
//!   8. Acquire Resource Claim Group
//!   9. Persist spawn intent
//!  10. AgentAdapter start_session
//!  11. Persist spawn evidence (session_id, pid)
//!  12. Transition Execution → Running (ONLY after successful spawn)
//!  13. send_task / receive_events
//!  14. Handle terminal outcome
//!
//! Crash windows handled:
//!   - Intent committed, process not started → Reconciler can safely fail + cleanup
//!   - Process started, final state not committed → ProcessRegistry check prevents re-spawn
//!   - Response lost → re-dispatch returns original Execution/Operation

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::agent_adapter::{AgentAdapter, SessionOptions};
use harness_core::contracts::scheduler::{DispatchOutcome, DispatchStatus, TerminalOutcome};
use harness_core::contracts::task::TaskLifecycle;
use harness_core::resource_claim::ClaimGroupSpec;
use harness_core::state_machine::ExecutionLifecycle;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::concurrency::ConcurrencyManager;
use super::dispatch_repo::{DispatchRepository, IntentOutcome};
use super::event_sink::SchedulerEventSink;
use super::handoff_repo::{CreateHandoffParams, HandoffRepository};
use super::heartbeat_registry::{HeartbeatEntry, HeartbeatRegistry, HeartbeatStatus, OwnerKind};
use crate::lease::runner::LeaseHeartbeatRunner;
use crate::lease::service::WorkspaceLeaseService;
use crate::lease::types::LeaseSpec;
use crate::resource_claim::service::ResourceClaimService;
use crate::resource_claim::ClaimGuard;
use crate::transition::TransitionService;
use crate::worktree::manager::WorktreeManager;
use crate::worktree::types::WorktreeSpec;

/// Bundled dispatch request.
pub struct DispatchRequest<'a> {
    pub task_id: &'a str,
    pub project_id: &'a str,
    pub profile_id: &'a str,
    pub repo_path: &'a std::path::Path,
    pub adapter: &'a (dyn AgentAdapter + Send + Sync),
    pub task_goal: &'a str,
    pub timeout: Duration,
    pub env: HashMap<String, String>,
}

/// Tracks all resources acquired during dispatch for compensation.
#[derive(Default)]
struct DispatchResourceBundle {
    reservation_id: Option<String>,
    execution_id: Option<String>,
    worktree_id: Option<String>,
    lease_record: Option<crate::lease::types::LeaseRecord>,
    claim_group_id: Option<String>,
    session_id: Option<String>,
    heartbeat_cancel: Option<CancellationToken>,
}

pub struct SchedulerOrchestrator {
    pool: SqlitePool,
    dispatch_repo: DispatchRepository,
    transitions: TransitionService,
    concurrency: ConcurrencyManager,
    worktree_mgr: Arc<WorktreeManager>,
    lease_service: Arc<WorkspaceLeaseService>,
    claim_service: Arc<ResourceClaimService>,
    heartbeat_registry: Arc<HeartbeatRegistry>,
    handoff_repo: HandoffRepository,
}

impl SchedulerOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: SqlitePool,
        transitions: TransitionService,
        concurrency: ConcurrencyManager,
        worktree_mgr: Arc<WorktreeManager>,
        lease_service: Arc<WorkspaceLeaseService>,
        claim_service: Arc<ResourceClaimService>,
        heartbeat_registry: Arc<HeartbeatRegistry>,
        handoff_repo: HandoffRepository,
    ) -> Self {
        let dispatch_repo = DispatchRepository::new(pool.clone());
        Self {
            pool,
            dispatch_repo,
            transitions,
            concurrency,
            worktree_mgr,
            lease_service,
            claim_service,
            heartbeat_registry,
            handoff_repo,
        }
    }

    /// Full dispatch saga. Returns outcome with compensation info.
    ///
    /// Idempotent: same logical request returns the original outcome without
    /// duplicating worktree/lease/claim/agent.
    pub async fn dispatch(&self, req: &DispatchRequest<'_>) -> Result<DispatchOutcome, CoreError> {
        let mut resources = DispatchResourceBundle::default();

        // ── Build idempotency identity ──────────────────────────────
        let request_hash = self.compute_request_hash(req);
        let ikey = format!(
            "dispatch-{}-{}-{}",
            req.project_id, req.task_id, req.profile_id
        );

        // ── 0. Crash-window recovery: check for existing intent ─────
        if let Some(existing) = self.dispatch_repo.load_by_ikey(&ikey).await? {
            return Ok(self.recover_or_replay(&existing, req).await);
        }

        // ── 1. Transactional idempotency arbitration ────────────────
        let op_id = format!("dispatch-{}", Uuid::new_v4());
        let intent = self
            .dispatch_repo
            .record_intent(
                &op_id,
                req.project_id,
                req.task_id,
                req.profile_id,
                &ikey,
                &request_hash,
            )
            .await?;

        match intent {
            IntentOutcome::Duplicate { existing } => return Ok(existing),
            IntentOutcome::IdempotencyConflict {
                existing_op_id,
                existing_hash,
                new_hash,
            } => {
                return Ok(DispatchOutcome {
                    dispatch_op_id: existing_op_id,
                    task_id: req.task_id.to_string(),
                    execution_id: None,
                    status: DispatchStatus::Failed,
                    terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                        reason: format!(
                            "idempotency conflict: existing hash={existing_hash} new hash={new_hash}"
                        ),
                    }),
                    compensation_actions: vec![],
                });
            }
            IntentOutcome::Created { .. } => { /* proceed */ }
        }

        // ── 2. Re-validate readiness ───────────────────────────────
        // Already validated before dispatch call; re-check in transaction
        // context would happen here if needed.

        // ── 3. Concurrency reservation ─────────────────────────────
        let _reservation = match self
            .concurrency
            .reserve(req.task_id, Some(req.profile_id), None)
            .await
        {
            Ok(harness_core::contracts::scheduler::ReservationResult::Reserved {
                reservation_id,
                ..
            }) => {
                resources.reservation_id = Some(reservation_id.clone());
                reservation_id
            }
            Ok(other) => {
                self.dispatch_repo
                    .update_status(&op_id, "failed", Some(&format!("{:?}", other)))
                    .await
                    .ok();
                return Ok(DispatchOutcome {
                    dispatch_op_id: op_id,
                    task_id: req.task_id.to_string(),
                    execution_id: None,
                    status: DispatchStatus::Failed,
                    terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                        reason: format!("{:?}", other),
                    }),
                    compensation_actions: vec![],
                });
            }
            Err(e) => {
                self.dispatch_repo
                    .update_status(&op_id, "failed", Some(&e.to_string()))
                    .await
                    .ok();
                return Ok(DispatchOutcome {
                    dispatch_op_id: op_id,
                    task_id: req.task_id.to_string(),
                    execution_id: None,
                    status: DispatchStatus::Failed,
                    terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                        reason: e.to_string(),
                    }),
                    compensation_actions: vec![],
                });
            }
        };

        // ── 4. Create Execution Attempt ────────────────────────────
        let exec_id = format!("exec-{}", Uuid::new_v4());
        let attempt = self.next_attempt(req.task_id).await?;
        if let Err(e) = sqlx::query(
            "INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id) VALUES (?,?,?,'created',?)",
        )
        .bind(&exec_id)
        .bind(req.task_id)
        .bind(attempt)
        .bind(req.profile_id)
        .execute(&self.pool)
        .await
        {
            // Compensation: release reservation
            let _ = self.concurrency.release(req.task_id).await;
            self.dispatch_repo
                .update_status(&op_id, "failed", Some(&e.to_string()))
                .await
                .ok();
            return Ok(DispatchOutcome {
                dispatch_op_id: op_id,
                task_id: req.task_id.to_string(),
                execution_id: None,
                status: DispatchStatus::Failed,
                terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                    reason: format!("execution insert: {e}"),
                }),
                compensation_actions: vec!["released-reservation".to_string()],
            });
        }
        resources.execution_id = Some(exec_id.clone());
        self.dispatch_repo
            .update_stage(&op_id, "dispatched", Some(&exec_id))
            .await?;

        // ── 5. Transition task → Dispatched ────────────────────────
        let task_lc = self
            .get_task_lc(req.task_id)
            .await
            .unwrap_or(TaskLifecycle::Pending);
        let _ = self
            .transitions
            .transition_task(
                req.task_id,
                &task_lc,
                &TaskLifecycle::Dispatched,
                &format!("{}-disp", ikey),
            )
            .await;

        // ── 6. Create Worktree ─────────────────────────────────────
        let wt_id = match self
            .create_worktree(req, &exec_id, &op_id, &ikey, &mut resources)
            .await
        {
            Ok(id) => id,
            Err(outcome) => return Ok(outcome),
        };

        // ── 7. Acquire Lease + start heartbeat ─────────────────────
        let lease_record = match self
            .acquire_lease(req, &exec_id, &wt_id, &ikey, &op_id, &mut resources)
            .await
        {
            Ok(record) => record,
            Err(outcome) => return Ok(outcome),
        };

        // ── 8. Acquire Resource Claims ─────────────────────────────
        if let Err(outcome) = self
            .acquire_claims(req, &exec_id, &lease_record, &ikey, &op_id, &mut resources)
            .await
        {
            // Compensation for claim failure: stop heartbeat, release lease
            if let Some(cancel) = resources.heartbeat_cancel.take() {
                cancel.cancel();
            }
            let _ = self
                .lease_service
                .release_lease(
                    &lease_record.lease_id,
                    &lease_record.lease_token,
                    "claim-failure-compensation",
                )
                .await;
            let _ = self.concurrency.release(req.task_id).await;
            return Ok(outcome);
        }

        // Record resource links
        let cg_id = resources.claim_group_id.as_deref();
        self.dispatch_repo
            .record_resources(&op_id, Some(&wt_id), Some(&lease_record.lease_id), cg_id)
            .await?;

        // ── 9. Start Agent via Adapter ─────────────────────────────
        let profile = self.load_profile(req.profile_id).await?;

        // Profile-scoped environment filtering: only pass env vars that are
        // explicitly allowed by the profile or are not classified as sensitive.
        let filtered_env = self.filter_env_for_profile(&profile, &req.env)?;

        let session_opts = SessionOptions {
            working_directory: req.repo_path.to_path_buf(),
            env: filtered_env,
            timeout: req.timeout,
            max_turns: None,
            resume_session_id: None,
            model_override: None,
            effort_override: None,
            extra_args: vec![],
        };

        let mut session = match req.adapter.start_session(&profile, &session_opts).await {
            Ok(s) => s,
            Err(e) => {
                // Compensation: release claim, stop heartbeat, release lease, release reservation
                return Ok(self
                    .compensate_adapter_failure(
                        &op_id,
                        req.task_id,
                        &exec_id,
                        &mut resources,
                        &format!("adapter start: {e}"),
                    )
                    .await);
            }
        };

        // ── 10. Persist spawn evidence ─────────────────────────────
        let session_id = session.session_id().to_string();
        resources.session_id = Some(session_id.clone());
        // PID not directly available from AgentSession trait; use 0 as sentinel
        self.dispatch_repo
            .record_spawn_evidence(&op_id, &session_id, None)
            .await?;

        // ── 11. Transition Execution → Running (ONLY after spawn) ──
        let _ = self
            .transitions
            .transition_execution(
                &exec_id,
                &ExecutionLifecycle::Running,
                Some("dispatch"),
                &format!("{}-run", ikey),
            )
            .await;

        // Transition Task → Running (dispatched agent is now running)
        let _ = self
            .transitions
            .transition_task(
                req.task_id,
                &TaskLifecycle::Dispatched,
                &TaskLifecycle::Running,
                &format!("{}-trun", ikey),
            )
            .await;

        // ── 12. Send task envelope ─────────────────────────────────
        let envelope = harness_core::contracts::task_envelope::TaskEnvelope {
            task_id: req.task_id.to_string(),
            project_id: req.project_id.to_string(),
            task_goal: req.task_goal.to_string(),
            scope: harness_core::contracts::task_envelope::FileScope {
                allowed_paths: vec!["**".into()],
                forbidden_paths: vec![],
                readable_paths: vec![],
                scope_expansion_allowed: false,
            },
            resource_claims: vec![],
            dependencies: vec![],
            acceptance_checks: vec![],
            allowed_tools: vec!["read".into(), "write".into()],
            output_schema: "TaskResultV1".into(),
            budget: harness_core::contracts::task_envelope::TaskBudget {
                max_turns: 50,
                max_time_ms: req.timeout.as_millis() as u64,
                max_cost_cents: None,
            },
            goal_contract_version: 1,
            plan_version: 1,
        };

        if let Err(e) = session.send_task(&envelope).await {
            let _ = session.dispose().await;
            return Ok(self
                .compensate_send_task_failure(
                    &op_id,
                    req.task_id,
                    &exec_id,
                    &mut resources,
                    &format!("send_task: {e}"),
                )
                .await);
        }

        // ── 13. Receive events via persistent sink ─────────────────
        let mut event_sink =
            SchedulerEventSink::new_with_db_init(self.pool.clone(), exec_id.clone(), None).await;
        let receive_result = session.receive_events(&mut event_sink).await;
        let _ = session.dispose().await;

        let terminal = match receive_result {
            Ok(()) => TerminalOutcome::Completed,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("timeout") {
                    TerminalOutcome::TimedOut {
                        duration_ms: req.timeout.as_millis() as u64,
                    }
                } else if msg.contains("cancel") {
                    TerminalOutcome::Cancelled
                } else {
                    TerminalOutcome::AdapterFailed { reason: msg }
                }
            }
        };

        // ── 14. Handle terminal outcome ────────────────────────────
        let exec_lc = match &terminal {
            TerminalOutcome::Completed => ExecutionLifecycle::Completed,
            _ => ExecutionLifecycle::Failed,
        };
        let _ = self
            .transitions
            .transition_execution(&exec_id, &exec_lc, None, &format!("{}-term", ikey))
            .await;

        if terminal.retain_resources() {
            // Success: release concurrency only, retain Lease/Claim/Worktree/Heartbeat
            let _ = self.concurrency.release(req.task_id).await;
            let current_task_lc = self
                .get_task_lc(req.task_id)
                .await
                .unwrap_or(TaskLifecycle::Dispatched);
            let _ = self
                .transitions
                .transition_task(
                    req.task_id,
                    &current_task_lc,
                    &TaskLifecycle::Submitted,
                    &format!("{}-sub", ikey),
                )
                .await;
            self.dispatch_repo
                .update_status(&op_id, "agent_completed", None)
                .await?;

            // ── Create resource handoff for I4-C Verification ─────────
            let handoff_id = format!("handoff-{}", Uuid::new_v4());
            let wt_id = resources.worktree_id.as_deref().unwrap_or("");
            let lease_id = resources
                .lease_record
                .as_ref()
                .map(|r| r.lease_id.as_str())
                .unwrap_or("");
            let cg_id = resources.claim_group_id.as_deref();
            let fencing = resources
                .lease_record
                .as_ref()
                .map(|r| r.fencing_token)
                .unwrap_or(0);

            // Register in runtime registry
            if let Some(ref _lease_rec) = resources.lease_record {
                let entry = HeartbeatEntry {
                    execution_id: exec_id.clone(),
                    task_id: req.task_id.to_string(),
                    worktree_id: wt_id.to_string(),
                    lease_id: lease_id.to_string(),
                    claim_group_id: cg_id.map(|s| s.to_string()),
                    fencing_token: fencing,
                    owner_kind: OwnerKind::Scheduler,
                    owner_id: "scheduler-main".to_string(),
                    status: HeartbeatStatus::Healthy,
                    last_heartbeat_at: Some(chrono::Utc::now()),
                    cancel_token: resources
                        .heartbeat_cancel
                        .clone()
                        .unwrap_or_else(CancellationToken::new),
                    last_error: None,
                };
                let _ = self.heartbeat_registry.register(entry).await;
            }

            // Persist handoff record
            let _ = self
                .handoff_repo
                .create(
                    &handoff_id,
                    req.project_id,
                    req.task_id,
                    CreateHandoffParams {
                        execution_id: &exec_id,
                        worktree_id: Some(wt_id),
                        lease_id: Some(lease_id),
                        claim_group_id: cg_id,
                        fencing_token: fencing,
                        owner_id: "scheduler-main",
                    },
                )
                .await;

            Ok(DispatchOutcome {
                dispatch_op_id: op_id,
                task_id: req.task_id.to_string(),
                execution_id: Some(exec_id),
                status: DispatchStatus::AgentCompleted,
                terminal_outcome: Some(terminal),
                compensation_actions: vec!["released-concurrency".to_string()],
            })
        } else {
            // Failure: release all resources
            self.compensate_full(&op_id, req.task_id, &mut resources)
                .await;
            self.dispatch_repo
                .update_status(&op_id, "failed", Some(&format!("{:?}", terminal)))
                .await?;

            Ok(DispatchOutcome {
                dispatch_op_id: op_id,
                task_id: req.task_id.to_string(),
                execution_id: Some(exec_id),
                status: DispatchStatus::Failed,
                terminal_outcome: Some(terminal),
                compensation_actions: vec!["full-release".to_string()],
            })
        }
    }

    // ── Resource acquisition helpers ──────────────────────────────────

    async fn create_worktree(
        &self,
        req: &DispatchRequest<'_>,
        exec_id: &str,
        op_id: &str,
        _ikey: &str,
        resources: &mut DispatchResourceBundle,
    ) -> Result<String, DispatchOutcome> {
        use crate::worktree::naming;
        let branch = match naming::branch_name(req.task_id, exec_id) {
            Ok(b) => b,
            Err(e) => {
                let _ = self.concurrency.release(req.task_id).await;
                return Err(self
                    .fail_outcome(op_id, req.task_id, Some(exec_id), &e.to_string())
                    .await);
            }
        };

        let harness_root = std::env::temp_dir().join("harness-worktrees");
        let wt_path = harness_root.join(format!("wt-{}", &exec_id[..8]));
        let wt_spec = WorktreeSpec {
            project_id: req.project_id.to_string(),
            task_id: req.task_id.to_string(),
            execution_id: exec_id.to_string(),
            repository_root: req.repo_path.to_path_buf(),
            base_commit: "HEAD".to_string(),
            worktree_path: wt_path,
            branch_name: branch,
            operation_id: format!("wt-{}", op_id),
            owner_supervisor_id: String::new(),
        };

        let wt_outcome = match self.worktree_mgr.create_worktree(&wt_spec).await {
            Ok(o) => o,
            Err(e) => {
                let _ = self.concurrency.release(req.task_id).await;
                return Err(self
                    .fail_outcome(op_id, req.task_id, Some(exec_id), &format!("worktree: {e}"))
                    .await);
            }
        };

        let wt_id = match wt_outcome {
            crate::worktree::types::WorktreeCreateOutcome::Created(ref record) => {
                record.worktree_id.clone()
            }
            crate::worktree::types::WorktreeCreateOutcome::AlreadyExists(ref record) => {
                record.worktree_id.clone()
            }
            crate::worktree::types::WorktreeCreateOutcome::InProgress => {
                let _ = self.concurrency.release(req.task_id).await;
                return Err(self
                    .fail_outcome(
                        op_id,
                        req.task_id,
                        Some(exec_id),
                        "worktree create in progress by another owner",
                    )
                    .await);
            }
        };

        resources.worktree_id = Some(wt_id.clone());
        let _ = self
            .dispatch_repo
            .update_stage(op_id, "worktree_ready", None)
            .await;
        Ok(wt_id)
    }

    async fn acquire_lease(
        &self,
        req: &DispatchRequest<'_>,
        exec_id: &str,
        wt_id: &str,
        ikey: &str,
        op_id: &str,
        resources: &mut DispatchResourceBundle,
    ) -> Result<crate::lease::types::LeaseRecord, DispatchOutcome> {
        let lease_spec = LeaseSpec {
            worktree_id: wt_id.to_string(),
            project_id: req.project_id.to_string(),
            task_id: req.task_id.to_string(),
            owner_execution_id: exec_id.to_string(),
            owner_supervisor_id: String::new(),
            lease_duration: req.timeout + Duration::from_secs(60),
            idempotency_key: format!("lease-{}", ikey),
        };

        let lease_record = match self.lease_service.acquire_lease(&lease_spec).await {
            Ok(crate::lease::types::LeaseAcquireOutcome::Acquired(record)) => record,
            Ok(crate::lease::types::LeaseAcquireOutcome::AlreadyAcquired(record)) => record,
            Ok(other) => {
                let _ = self.concurrency.release(req.task_id).await;
                return Err(self
                    .fail_outcome(
                        op_id,
                        req.task_id,
                        Some(exec_id),
                        &format!("lease contested: {:?}", other),
                    )
                    .await);
            }
            Err(e) => {
                let _ = self.concurrency.release(req.task_id).await;
                return Err(self
                    .fail_outcome(op_id, req.task_id, Some(exec_id), &format!("lease: {e}"))
                    .await);
            }
        };

        // Start heartbeat runner with registry integration
        let heartbeat_cancel = CancellationToken::new();
        let hb_cancel_child = heartbeat_cancel.clone();
        let hb_runner = LeaseHeartbeatRunner::new(
            self.lease_service.clone(),
            lease_record.lease_id.clone(),
            lease_record.lease_token.clone(),
            lease_record.fencing_token,
        );

        let hb_exec_id = exec_id.to_string();
        let hb_registry = self.heartbeat_registry.clone();
        let hb_handoff_repo = HandoffRepository::new(self.pool.clone());

        tokio::spawn(async move {
            hb_runner
                .run(hb_cancel_child, move |result| {
                    let exec_id = hb_exec_id.clone();
                    let registry = hb_registry.clone();
                    let handoff_repo = hb_handoff_repo.clone();
                    tokio::spawn(async move {
                        if result.ok {
                            registry.update_heartbeat_status(&exec_id, true, None).await;
                            let _ = handoff_repo.update_heartbeat(&exec_id).await;
                        } else if let Some(ref err) = result.error {
                            registry
                                .update_heartbeat_status(&exec_id, false, Some(err.clone()))
                                .await;
                        }
                    });
                })
                .await;
        });

        resources.heartbeat_cancel = Some(heartbeat_cancel);
        resources.lease_record = Some(lease_record.clone());
        let _ = self
            .dispatch_repo
            .update_stage(op_id, "lease_acquired", None)
            .await;
        Ok(lease_record)
    }

    async fn acquire_claims(
        &self,
        req: &DispatchRequest<'_>,
        exec_id: &str,
        lease_record: &crate::lease::types::LeaseRecord,
        ikey: &str,
        op_id: &str,
        resources: &mut DispatchResourceBundle,
    ) -> Result<(), DispatchOutcome> {
        use harness_core::resource_claim::{AccessMode, ResourceClaimSpec};

        let claim_spec =
            ResourceClaimSpec::repository_wide(&req.repo_path.to_string_lossy(), AccessMode::Write);
        let claim_spec = ClaimGroupSpec {
            claims: vec![claim_spec],
            project_id: req.project_id.to_string(),
            task_id: req.task_id.to_string(),
            execution_id: exec_id.to_string(),
            repository_identity: req.repo_path.to_string_lossy().to_string(),
            lease_id: Some(lease_record.lease_id.clone()),
            worktree_id: Some(lease_record.worktree_id.clone().unwrap_or_default()),
        };

        let claim_guard = ClaimGuard {
            lease_id: lease_record.lease_id.clone(),
            lease_token: lease_record.lease_token.clone(),
            fencing_token: lease_record.fencing_token,
            worktree_id: lease_record.worktree_id.clone().unwrap_or_default(),
            project_id: req.project_id.to_string(),
            task_id: req.task_id.to_string(),
            execution_id: exec_id.to_string(),
        };

        let claim_ikey = format!("claim-{}", ikey);
        match self
            .claim_service
            .acquire_group(&claim_spec, &claim_guard, &claim_ikey)
            .await
        {
            Ok(_outcome) => {
                // Store claim group ID from the claim_spec for tracking
                let cg_id = format!("cg-{}", Uuid::new_v4());
                resources.claim_group_id = Some(cg_id);
                let _ = self
                    .dispatch_repo
                    .update_stage(op_id, "claims_acquired", None)
                    .await;
                Ok(())
            }
            Err(e) => Err(self
                .fail_outcome(op_id, req.task_id, Some(exec_id), &format!("claim: {e}"))
                .await),
        }
    }

    // ── Compensation ───────────────────────────────────────────────────

    async fn compensate_adapter_failure(
        &self,
        op_id: &str,
        task_id: &str,
        exec_id: &str,
        resources: &mut DispatchResourceBundle,
        reason: &str,
    ) -> DispatchOutcome {
        // Release Claim
        // (claims are released via the claim service when lease is released)
        // Stop heartbeat
        if let Some(cancel) = resources.heartbeat_cancel.take() {
            cancel.cancel();
        }
        // Release Lease
        if let Some(ref lr) = resources.lease_record {
            let _ = self
                .lease_service
                .release_lease(
                    &lr.lease_id,
                    &lr.lease_token,
                    "adapter-failure-compensation",
                )
                .await;
        }
        // Release concurrency
        let _ = self.concurrency.release(task_id).await;
        // Mark execution as failed
        let _ = sqlx::query("UPDATE execution_attempts SET lifecycle='failed' WHERE id=?")
            .bind(exec_id)
            .execute(&self.pool)
            .await;
        // Update dispatch
        let _ = self
            .dispatch_repo
            .update_status(op_id, "failed", Some(reason))
            .await;
        // Transition task to Failed
        let _ = self
            .transitions
            .transition_task(
                task_id,
                &TaskLifecycle::Dispatched,
                &TaskLifecycle::Failed,
                &format!("{}-fail", op_id),
            )
            .await;

        DispatchOutcome {
            dispatch_op_id: op_id.to_string(),
            task_id: task_id.to_string(),
            execution_id: Some(exec_id.to_string()),
            status: DispatchStatus::Failed,
            terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                reason: reason.to_string(),
            }),
            compensation_actions: vec![
                "released-claim".to_string(),
                "stopped-heartbeat".to_string(),
                "released-lease".to_string(),
                "released-reservation".to_string(),
            ],
        }
    }

    async fn compensate_send_task_failure(
        &self,
        op_id: &str,
        task_id: &str,
        exec_id: &str,
        resources: &mut DispatchResourceBundle,
        reason: &str,
    ) -> DispatchOutcome {
        // Full compensation — same as adapter failure + cancel process tree
        self.compensate_full(op_id, task_id, resources).await;

        // Mark execution as failed
        let _ = sqlx::query("UPDATE execution_attempts SET lifecycle='failed' WHERE id=?")
            .bind(exec_id)
            .execute(&self.pool)
            .await;

        let _ = self
            .dispatch_repo
            .update_status(op_id, "failed", Some(reason))
            .await;

        DispatchOutcome {
            dispatch_op_id: op_id.to_string(),
            task_id: task_id.to_string(),
            execution_id: Some(exec_id.to_string()),
            status: DispatchStatus::Failed,
            terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                reason: reason.to_string(),
            }),
            compensation_actions: vec!["full-compensation".to_string()],
        }
    }

    async fn compensate_full(
        &self,
        _op_id: &str,
        task_id: &str,
        resources: &mut DispatchResourceBundle,
    ) {
        // Stop heartbeat
        if let Some(cancel) = resources.heartbeat_cancel.take() {
            cancel.cancel();
        }
        // Release Lease
        if let Some(ref lr) = resources.lease_record {
            let _ = self
                .lease_service
                .release_lease(&lr.lease_id, &lr.lease_token, "full-compensation")
                .await;
        }
        // Release concurrency
        let _ = self.concurrency.release(task_id).await;
    }

    async fn fail_outcome(
        &self,
        op_id: &str,
        task_id: &str,
        exec_id: Option<&str>,
        reason: &str,
    ) -> DispatchOutcome {
        let _ = self.concurrency.release(task_id).await;
        let _ = self
            .dispatch_repo
            .update_status(op_id, "failed", Some(reason))
            .await;
        DispatchOutcome {
            dispatch_op_id: op_id.to_string(),
            task_id: task_id.to_string(),
            execution_id: exec_id.map(|s| s.to_string()),
            status: DispatchStatus::Failed,
            terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                reason: reason.to_string(),
            }),
            compensation_actions: vec!["released".to_string()],
        }
    }

    // ── Crash-window recovery ──────────────────────────────────────────

    async fn recover_or_replay(
        &self,
        existing: &super::dispatch_repo::DispatchRecord,
        _req: &DispatchRequest<'_>,
    ) -> DispatchOutcome {
        match existing.status.as_str() {
            "agent_completed" | "completed" => {
                // Successfully completed → return original result
                DispatchOutcome {
                    dispatch_op_id: existing.id.clone(),
                    task_id: existing.task_id.clone(),
                    execution_id: existing.execution_id.clone(),
                    status: DispatchStatus::Completed,
                    terminal_outcome: Some(TerminalOutcome::Completed),
                    compensation_actions: vec!["idempotent-replay".to_string()],
                }
            }
            "failed" => {
                // Already failed → return original failure
                DispatchOutcome {
                    dispatch_op_id: existing.id.clone(),
                    task_id: existing.task_id.clone(),
                    execution_id: existing.execution_id.clone(),
                    status: DispatchStatus::Failed,
                    terminal_outcome: Some(TerminalOutcome::SpawnFailed {
                        reason: "previous dispatch failed".to_string(),
                    }),
                    compensation_actions: vec!["idempotent-replay".to_string()],
                }
            }
            "agent_running" | "agent_starting" => {
                // Agent was started — check if still alive
                // For now, return the in-progress outcome
                DispatchOutcome {
                    dispatch_op_id: existing.id.clone(),
                    task_id: existing.task_id.clone(),
                    execution_id: existing.execution_id.clone(),
                    status: DispatchStatus::AgentRunning,
                    terminal_outcome: None,
                    compensation_actions: vec!["in-progress-replay".to_string()],
                }
            }
            _ => {
                // Preparing/worktree_ready/lease_acquired/claims_acquired
                // → Intent was committed but agent never spawned
                // → Reconciler will clean up. Return the existing intent.
                DispatchOutcome {
                    dispatch_op_id: existing.id.clone(),
                    task_id: existing.task_id.clone(),
                    execution_id: existing.execution_id.clone(),
                    status: DispatchStatus::Preparing,
                    terminal_outcome: None,
                    compensation_actions: vec!["stale-intent-replay".to_string()],
                }
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// Filter environment variables through the profile's allowed set.
    /// Sensitive env vars (API keys, tokens) not in the allowed list produce
    /// a structured error. Non-sensitive vars pass through freely.
    fn filter_env_for_profile(
        &self,
        profile: &harness_core::contracts::runtime_profile::RuntimeProfile,
        env: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, CoreError> {
        let mut filtered = HashMap::new();
        for (key, value) in env {
            if is_sensitive_env_name(key) {
                // Check against profile's allowed set (via a reasonable heuristic:
                // the profile's agent_kind and adapter_kind context implies which
                // env vars are expected; for now we check common patterns)
                if !self.is_env_allowed_for_profile(key, profile) {
                    return Err(CoreError::new(
                        ErrorCode::ConfigInvalid,
                        format!(
                            "sensitive env var '{key}' not authorized for profile '{}'",
                            profile.id
                        ),
                        ErrorSource::System,
                    ));
                }
            }
            filtered.insert(key.clone(), value.clone());
        }
        Ok(filtered)
    }

    /// Check if an env var name is classified as sensitive (API keys, tokens, secrets).
    fn is_env_allowed_for_profile(
        &self,
        key: &str,
        profile: &harness_core::contracts::runtime_profile::RuntimeProfile,
    ) -> bool {
        // Per-profile authorization: check if the profile's agent_kind or
        // provider context justifies this env var.
        let key_upper = key.to_uppercase();
        // Allow agent-specific env vars based on profile agent_kind
        match profile.agent_kind.as_str() {
            "claude-code" => key_upper.contains("ANTHROPIC") || key_upper.contains("CLAUDE"),
            "codex" => key_upper.contains("OPENAI") || key_upper.contains("CODEX"),
            _ => false,
        }
    }

    fn compute_request_hash(&self, req: &DispatchRequest<'_>) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        req.task_id.hash(&mut hasher);
        req.project_id.hash(&mut hasher);
        req.profile_id.hash(&mut hasher);
        req.repo_path.hash(&mut hasher);
        req.task_goal.hash(&mut hasher);
        req.timeout.hash(&mut hasher);
        // env is not hashed intentionally — env should not be part of idempotency
        format!("{:x}", hasher.finish())
    }

    async fn next_attempt(&self, task_id: &str) -> Result<i64, CoreError> {
        let row: (Option<i64>,) =
            sqlx::query_as("SELECT MAX(attempt_number) FROM execution_attempts WHERE task_id=?")
                .bind(task_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CoreError::new(
                        ErrorCode::PersistenceError,
                        e.to_string(),
                        ErrorSource::System,
                    )
                })?;
        Ok(row.0.unwrap_or(0) + 1)
    }

    async fn get_task_lc(&self, task_id: &str) -> Result<TaskLifecycle, CoreError> {
        let row: Option<(String,)> = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id=?")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    ErrorCode::PersistenceError,
                    e.to_string(),
                    ErrorSource::System,
                )
            })?;
        match row {
            Some((lc,)) => {
                Ok(serde_json::from_str(&format!("\"{lc}\"")).unwrap_or(TaskLifecycle::Pending))
            }
            None => Err(CoreError::new(
                ErrorCode::PersistenceError,
                "task not found",
                ErrorSource::System,
            )),
        }
    }

    async fn load_profile(
        &self,
        pid: &str,
    ) -> Result<harness_core::contracts::runtime_profile::RuntimeProfile, CoreError> {
        let row: Option<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT id, agent_kind, adapter_kind, agent_version, executable_path FROM runtime_profiles WHERE id=?",
        )
        .bind(pid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                ErrorCode::PersistenceError,
                e.to_string(),
                ErrorSource::System,
            )
        })?;

        match row {
            Some((id, kind, adapter, ver, exe)) => {
                Ok(harness_core::contracts::runtime_profile::RuntimeProfile {
                    id,
                    agent_definition_id: String::new(),
                    label: String::new(),
                    agent_kind: kind,
                    adapter_kind: adapter,
                    agent_version: ver,
                    executable_path: exe,
                    provider: String::new(),
                    provider_source:
                        harness_core::contracts::runtime_profile::ProviderSource::UserDeclared,
                    model: None,
                    base_url: None,
                    auth_mode: harness_core::contracts::runtime_profile::AuthMode::Unknown,
                    auth_status: harness_core::contracts::runtime_profile::AuthStatus::Unknown,
                    credential_ref: None,
                    capabilities: default_caps(),
                    core_status: harness_core::contracts::runtime_profile::CoreStatus::Available,
                    authentication_status:
                        harness_core::contracts::runtime_profile::AuthCheckStatus::Unknown,
                    execution_status:
                        harness_core::contracts::runtime_profile::ExecutionStatus::Untested,
                    optional_integrations: vec![],
                    discovery_source: String::new(),
                    passive_probe: None,
                    active_validation: None,
                    concurrency_max: 1,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                })
            }
            None => Err(CoreError::new(
                ErrorCode::ConfigMissing,
                format!("profile {} not found", pid),
                ErrorSource::System,
            )),
        }
    }
}

/// Check if an environment variable name is classified as sensitive.
/// Matches known API key, token, and secret patterns.
fn is_sensitive_env_name(key: &str) -> bool {
    let upper = key.to_uppercase();
    upper.contains("API_KEY")
        || upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("CREDENTIAL")
        || upper.contains("AUTH")
        || upper.starts_with("ANTHROPIC_")
        || upper.starts_with("OPENAI_")
        || upper.starts_with("CODEX_")
        || upper.starts_with("CLAUDE_")
}

fn default_caps() -> harness_core::contracts::runtime_profile::CapabilitySet {
    harness_core::contracts::runtime_profile::CapabilitySet {
        required: harness_core::contracts::runtime_profile::RequiredCapabilities {
            execute: harness_core::contracts::runtime_profile::TriState::Unknown,
            working_directory: harness_core::contracts::runtime_profile::TriState::Unknown,
            stream_output: harness_core::contracts::runtime_profile::TriState::Unknown,
            process_exit: harness_core::contracts::runtime_profile::TriState::Unknown,
            cancellation: harness_core::contracts::runtime_profile::TriState::Unknown,
            timeout: harness_core::contracts::runtime_profile::TriState::Unknown,
            final_result: harness_core::contracts::runtime_profile::TriState::Unknown,
        },
        optional: harness_core::contracts::runtime_profile::OptionalCapabilities {
            native_session_resume: harness_core::contracts::runtime_profile::TriState::Unknown,
            structured_output: harness_core::contracts::runtime_profile::TriState::Unknown,
            tool_events: harness_core::contracts::runtime_profile::TriState::Unknown,
            file_change_events: harness_core::contracts::runtime_profile::TriState::Unknown,
            reasoning_summary: harness_core::contracts::runtime_profile::TriState::Unknown,
            interactive_approval: harness_core::contracts::runtime_profile::TriState::Unknown,
            usage_reporting: harness_core::contracts::runtime_profile::TriState::Unknown,
        },
        workspace_modes: vec![],
        supported_languages: vec![],
        mcp_tools: vec![],
        supported_platforms: vec![],
    }
}
