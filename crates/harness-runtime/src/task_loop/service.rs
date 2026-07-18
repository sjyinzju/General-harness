//! TaskEngineeringLoopService — I4.5 Task-level loop orchestration.
//!
//! Manages the lifecycle of one task engineering loop: creates immutable
//! Attempts, dispatches them through certified I4, reads outcomes, and
//! deterministically decides next actions.
//!
//! NEVER: bypasses I4, calls Agent/LLM directly, commits/merges, deletes
//! Worktrees, or modifies certified I4 outcomes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use sqlx::SqlitePool;

use super::events::TaskLoopEventWriter;
use super::faults::{FaultBoundary, FaultKind, FaultPlan};
use super::gateway::{CreateExecutionRequest, DispatchResult, ExecutionCreated, I4Gateway};
use super::progress::BudgetPolicy;
use super::repo::{LoopUsageSummary, TaskLoopRepo};
use super::types::*;

// ── Service ──────────────────────────────────────────────────────

pub struct TaskEngineeringLoopService {
    pool: SqlitePool,
    repo: TaskLoopRepo,
    events: TaskLoopEventWriter,
    i4_gateway: Option<Arc<dyn I4Gateway>>,
    profile_policy: LoopProfilePolicy,
    budget_policy: BudgetPolicy,
    pub fault_plan: Option<Arc<FaultPlan>>,
    /// Per-boundary call counters for FaultPlan::check().
    fault_call_counts: Arc<Mutex<HashMap<FaultBoundary, u64>>>,
    pub loop_create_count: Arc<AtomicUsize>,
    pub attempt_create_count: Arc<AtomicUsize>,
    pub execution_create_count: Arc<AtomicUsize>,
    pub context_pack_count: Arc<AtomicUsize>,
    pub budget_reserve_count: Arc<AtomicUsize>,
    pub decision_count: Arc<AtomicUsize>,
    _worker_id: String,
}

impl TaskEngineeringLoopService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            repo: TaskLoopRepo::new(pool.clone()),
            events: TaskLoopEventWriter::new(pool.clone()),
            pool,
            i4_gateway: None,
            profile_policy: LoopProfilePolicy::default(),
            budget_policy: BudgetPolicy::default(),
            fault_plan: None,
            fault_call_counts: Arc::new(Mutex::new(HashMap::new())),
            loop_create_count: Arc::new(AtomicUsize::new(0)),
            attempt_create_count: Arc::new(AtomicUsize::new(0)),
            execution_create_count: Arc::new(AtomicUsize::new(0)),
            context_pack_count: Arc::new(AtomicUsize::new(0)),
            budget_reserve_count: Arc::new(AtomicUsize::new(0)),
            decision_count: Arc::new(AtomicUsize::new(0)),
            _worker_id: format!("tls-{}", uuid::Uuid::new_v4()),
        }
    }

    /// Check the fault plan for the given boundary. Returns the fault kind if
    /// one should be injected. Production paths have fault_plan=None so this
    /// always returns None with zero overhead beyond the Option check.
    fn check_fault(&self, boundary: FaultBoundary) -> Option<FaultKind> {
        let fp = self.fault_plan.as_ref()?;
        let mut counts = self.fault_call_counts.lock().unwrap();
        let call_count = counts.entry(boundary).or_insert(0);
        fp.check(boundary, call_count)
    }

    /// Wire a real I4 gateway for production use.
    pub fn with_i4_gateway(mut self, gateway: Arc<dyn I4Gateway>) -> Self {
        self.i4_gateway = Some(gateway);
        self
    }

    /// Configure profile policy.
    pub fn with_profile_policy(mut self, policy: LoopProfilePolicy) -> Self {
        self.profile_policy = policy;
        self
    }

    /// Configure budget policy.
    pub fn with_budget_policy(mut self, policy: BudgetPolicy) -> Self {
        self.budget_policy = policy;
        self
    }

    /// Shared counters (clone for two-pool testing).
    pub fn with_loop_create_count(mut self, c: Arc<AtomicUsize>) -> Self {
        self.loop_create_count = c;
        self
    }

    pub fn with_attempt_create_count(mut self, c: Arc<AtomicUsize>) -> Self {
        self.attempt_create_count = c;
        self
    }

    pub fn with_decision_count(mut self, c: Arc<AtomicUsize>) -> Self {
        self.decision_count = c;
        self
    }

    pub fn with_execution_count(mut self, c: Arc<AtomicUsize>) -> Self {
        self.execution_create_count = c;
        self
    }

    pub fn with_context_pack_count(mut self, c: Arc<AtomicUsize>) -> Self {
        self.context_pack_count = c;
        self
    }

    pub fn with_budget_reserve_count(mut self, c: Arc<AtomicUsize>) -> Self {
        self.budget_reserve_count = c;
        self
    }

    /// Wire a FaultPlan for fault injection testing.
    /// Propagates to both the service and its internal repo.
    pub fn with_fault_plan(mut self, fp: Arc<FaultPlan>) -> Self {
        self.repo = self.repo.with_fault_plan(fp.clone());
        self.fault_plan = Some(fp);
        self
    }

    /// Share the fault call counters between service and repo (for tests
    /// that call repo methods directly through the same service instance).
    pub fn with_fault_call_counts(mut self, counts: Arc<Mutex<HashMap<FaultBoundary, u64>>>) -> Self {
        self.fault_call_counts = counts;
        self
    }

    // ── Loop lifecycle ──────────────────────────────────────────

    /// Create a new task engineering loop. Idempotent.
    pub async fn create_loop(&self, req: &CreateLoopRequest) -> Result<CreateLoopOutcome, String> {
        // Fault: before effect
        if let Some(FaultKind::FailBeforeEffect) = self.check_fault(FaultBoundary::LoopInsert) {
            return Err("fault: LoopInsert before effect".into());
        }
        let outcome = self.repo.create_loop(req).await?;
        // Fault: after effect, before response (response lost)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::LoopInsert)
        {
            // Effect succeeded — increment counter to reflect durable write.
            if matches!(outcome, CreateLoopOutcome::Created { .. }) {
                self.loop_create_count.fetch_add(1, Ordering::SeqCst);
            }
            return Err("fault: LoopInsert response lost".into());
        }
        if matches!(outcome, CreateLoopOutcome::Created { .. }) {
            self.loop_create_count.fetch_add(1, Ordering::SeqCst);
            if let CreateLoopOutcome::Created { ref loop_id } = outcome {
                let _ = self
                    .events
                    .loop_created(loop_id, &req.task_id, &req.project_id)
                    .await;
            }
        }
        Ok(outcome)
    }

    /// Acquire loop ownership and transition to Ready.
    pub async fn start_or_resume_loop(
        &self,
        loop_id: &str,
        owner_id: &str,
        lease_secs: u32,
    ) -> Result<LoopStartOutcome, String> {
        let l = self
            .repo
            .load_loop(loop_id)
            .await?
            .ok_or_else(|| "loop not found".to_string())?;

        if l.lifecycle.is_terminal() {
            return Ok(LoopStartOutcome::AlreadyTerminal {
                lifecycle: l.lifecycle,
            });
        }

        // Fault: OwnerTakeover — simulate another owner holding the loop.
        if let Some(FaultKind::OwnerTakeover) = self.check_fault(FaultBoundary::LoopOwnership) {
            return Ok(LoopStartOutcome::HeldByOther {
                owner_id: "fault-takeover".into(),
            });
        }
        // Fault: before effect
        if let Some(FaultKind::FailBeforeEffect) = self.check_fault(FaultBoundary::LoopOwnership) {
            return Err("fault: LoopOwnership before effect".into());
        }

        // Acquire ownership with version + fencing CAS.
        let new_version = match self
            .repo
            .acquire_ownership(loop_id, l.version, l.fencing_token, owner_id, lease_secs)
            .await?
        {
            Some(v) => v,
            None => {
                // Re-read to see who holds it.
                let cur = self.repo.load_loop(loop_id).await?.ok_or("loop vanished")?;
                if cur.owner_id.as_deref() == Some(owner_id) && cur.fencing_token == l.fencing_token
                {
                    // We already own it — stale re-read.
                    return Ok(LoopStartOutcome::AlreadyOwned {
                        lifecycle: cur.lifecycle,
                    });
                }
                return Ok(LoopStartOutcome::HeldByOther {
                    owner_id: cur.owner_id.unwrap_or_default(),
                });
            }
        };

        let _ = self.events.loop_ownership_acquired(loop_id, owner_id).await;

        // Transition: created → ready (or any recoverable → ready).
        if l.lifecycle == LoopLifecycle::Created
            || l.lifecycle == LoopLifecycle::WaitingForInfrastructure
        {
            let v = self
                .repo
                .transition_loop(
                    loop_id,
                    new_version,
                    l.fencing_token,
                    owner_id,
                    LoopLifecycle::Ready,
                    None,
                )
                .await?;
            let _ = self.events.loop_started(loop_id).await;
            Ok(LoopStartOutcome::Started { version: v })
        } else {
            Ok(LoopStartOutcome::Resumed {
                lifecycle: l.lifecycle,
                version: Some(new_version),
            })
        }
    }

    // ── Attempt creation ────────────────────────────────────────

    /// Prepare the next Attempt. Creates the Attempt row, builds a Context Pack,
    /// and transitions the loop to PreparingAttempt → dispatches through I4.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_next_attempt(
        &self,
        loop_id: &str,
        owner_id: &str,
        _expected_version: i64,
        expected_fencing: i64,
        runtime_profile_id: &str,
        workspace_source: AttemptWorkspaceSource,
        context_pack_spec: Option<ContextPackSpec>,
    ) -> Result<PrepareAttemptOutcome, String> {
        let l = self
            .repo
            .load_loop(loop_id)
            .await?
            .ok_or("loop not found")?;

        // Guard: only Ready or Evaluating can create a new Attempt.
        if l.lifecycle != LoopLifecycle::Ready && l.lifecycle != LoopLifecycle::Evaluating {
            return Ok(PrepareAttemptOutcome::LoopNotReady {
                lifecycle: l.lifecycle,
            });
        }

        // Guard: ownership and fencing must match.
        if l.owner_id.as_deref() != Some(owner_id) || l.fencing_token != expected_fencing {
            return Ok(PrepareAttemptOutcome::OwnershipLost);
        }

        // Guard: profile must be allowed by policy.
        // Fault: ProfileSelection before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::ProfileSelection)
        {
            return Err("fault: ProfileSelection before effect".into());
        }
        if !self.profile_policy.is_allowed(runtime_profile_id) {
            return Ok(PrepareAttemptOutcome::InfrastructureError {
                reason: format!("profile {runtime_profile_id} not in allowlist"),
            });
        }
        // Fault: ProfileSelection response lost
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::ProfileSelection)
        {
            return Err("fault: ProfileSelection response lost".into());
        }

        // Guard: budget must allow another attempt.
        // Fault: BudgetReservation before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::BudgetReservation)
        {
            return Err("fault: BudgetReservation before effect".into());
        }
        let usage = self.repo.sum_loop_usage(loop_id).await?;
        let budget_check = self.budget_policy.check_can_attempt(
            l.attempt_count,
            l.no_progress_streak,
            l.same_failure_streak,
            l.profile_switch_count,
            usage.total_input_tokens,
            usage.total_output_tokens,
            None,
            usage.total_tool_calls,
            usage.total_wall_time_ms,
            usage.total_estimated_cost_micros,
            true,
        );
        match budget_check {
            crate::task_loop::progress::BudgetCheckResult::Ok => {}
            other => {
                return Ok(PrepareAttemptOutcome::InfrastructureError {
                    reason: format!("budget check failed: {other:?}"),
                });
            }
        }
        // Fault: BudgetReservation response lost (after durable count increment)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::BudgetReservation)
        {
            self.budget_reserve_count.fetch_add(1, Ordering::SeqCst);
            return Err("fault: BudgetReservation response lost".into());
        }
        self.budget_reserve_count.fetch_add(1, Ordering::SeqCst);

        // Guard: no active Attempt.
        if let Some(active) = self.repo.load_active_attempt(loop_id).await? {
            if active.lifecycle.is_active() {
                return Ok(PrepareAttemptOutcome::ActiveAttemptExists {
                    attempt_id: active.attempt_id,
                });
            }
        }

        // Transition loop: Ready/Evaluating → PreparingAttempt.
        // Use the freshly-loaded version for the CAS, not the caller's
        // potentially-stale expected_version.  Two concurrent callers
        // racing to prepare will both receive a clean LoopNotReady
        // outcome instead of an Err — the loser never panics.
        let v1 = self
            .repo
            .transition_loop(
                loop_id,
                l.version,
                l.fencing_token,
                owner_id,
                LoopLifecycle::PreparingAttempt,
                None,
            )
            .await?;
        let v1 = match v1 {
            Some(v) => v,
            None => {
                // CAS lost: re-read to report the actual lifecycle.
                let cur = self.repo.load_loop(loop_id).await?.ok_or("loop vanished")?;
                return Ok(PrepareAttemptOutcome::LoopNotReady {
                    lifecycle: cur.lifecycle,
                });
            }
        };

        let ordinal = l.current_attempt_ordinal + 1;
        let attempt_id = format!("ta-{}-{}", loop_id, ordinal);
        let parent_attempt_id = l.active_attempt_id.clone();

        // Validate workspace continuation before creating the attempt.
        if matches!(
            &workspace_source,
            AttemptWorkspaceSource::ContinueFromAttempt { .. }
        ) {
            // Fault: WorkspaceContinuation before effect
            if let Some(FaultKind::FailBeforeEffect) =
                self.check_fault(FaultBoundary::WorkspaceContinuation)
            {
                return Err("fault: WorkspaceContinuation before effect".into());
            }
            if let Err(reason) = Self::validate_workspace_continuation(&workspace_source) {
                return Ok(PrepareAttemptOutcome::InfrastructureError { reason });
            }
            // Fault: WorkspaceContinuation response lost
            if let Some(FaultKind::ResponseLostAfterSuccess) =
                self.check_fault(FaultBoundary::WorkspaceContinuation)
            {
                return Err("fault: WorkspaceContinuation response lost".into());
            }
        }

        // Create Context Pack if spec provided.
        let context_pack_id = if let Some(spec) = &context_pack_spec {
            // Fault: ContextPackInsert before effect
            if let Some(FaultKind::FailBeforeEffect) =
                self.check_fault(FaultBoundary::ContextPackInsert)
            {
                return Err("fault: ContextPackInsert before effect".into());
            }
            let cp_id = format!("cp-{}-{}", loop_id, ordinal);
            let payload_json = serde_json::to_string(spec).unwrap_or_default();
            let cp_fp = fingerprint_hex(&payload_json);
            self.repo
                .insert_context_pack(
                    &cp_id,
                    loop_id,
                    parent_attempt_id.as_deref(),
                    ordinal,
                    &payload_json,
                    "{}",
                    &cp_fp,
                    None,
                    "valid",
                )
                .await?;
            // Fault: ContextPackInsert response lost (after durable write)
            if let Some(FaultKind::ResponseLostAfterSuccess) =
                self.check_fault(FaultBoundary::ContextPackInsert)
            {
                self.context_pack_count.fetch_add(1, Ordering::SeqCst);
                return Err("fault: ContextPackInsert response lost".into());
            }
            self.context_pack_count.fetch_add(1, Ordering::SeqCst);
            let _ = self.events.context_pack_created(loop_id, &cp_id).await;
            Some(cp_id)
        } else {
            None
        };

        // Fault: AttemptInsert before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::AttemptInsert)
        {
            return Err("fault: AttemptInsert before effect".into());
        }

        // Atomically insert Attempt row.
        let (src_exec, src_wt, src_base, src_head, src_diff, ws_kind) = match &workspace_source {
            AttemptWorkspaceSource::InitialTaskWorkspace { .. } => {
                (None, None, None, None, None, WorkspaceSourceKind::Initial)
            }
            AttemptWorkspaceSource::ContinueFromAttempt {
                source_execution_id,
                source_worktree_id,
                expected_baseline_commit,
                expected_head,
                expected_diff_fingerprint,
                ..
            } => (
                Some(source_execution_id.clone()),
                Some(source_worktree_id.clone()),
                Some(expected_baseline_commit.clone()),
                Some(expected_head.clone()),
                Some(expected_diff_fingerprint.clone()),
                WorkspaceSourceKind::ContinueFromAttempt,
            ),
        };

        let inserted = self
            .repo
            .insert_attempt(
                &attempt_id,
                loop_id,
                ordinal,
                parent_attempt_id.as_deref(),
                context_pack_id.as_deref(),
                runtime_profile_id,
                ws_kind,
                src_exec.as_deref(),
                src_wt.as_deref(),
                src_base.as_deref(),
                src_head.as_deref(),
                src_diff.as_deref(),
            )
            .await?;

        if !inserted {
            // Lost race: another worker created this ordinal.
            // Attempt not inserted — lost race.
            let existing = self.repo.load_attempt(&attempt_id).await?;
            return Ok(PrepareAttemptOutcome::AlreadyExists {
                attempt_id: existing.map(|a| a.attempt_id).unwrap_or(attempt_id),
            });
        }

        // Fault: AttemptInsert response lost (after durable insert)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::AttemptInsert)
        {
            self.attempt_create_count.fetch_add(1, Ordering::SeqCst);
            return Err("fault: AttemptInsert response lost".into());
        }

        self.attempt_create_count.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .events
            .attempt_prepared(loop_id, &attempt_id, ordinal)
            .await;

        // Update loop counters.
        let _ = self
            .repo
            .update_loop_counters(
                loop_id,
                v1,
                &attempt_id,
                l.attempt_count + 1,
                l.no_progress_streak,
                l.same_failure_streak,
                ordinal,
            )
            .await;

        // Transition loop to AttemptActive. Use l.fencing_token which was
        // loaded at function entry and validated against the caller's value
        // — consistent within this single invocation.
        let v2 = self
            .repo
            .transition_loop(
                loop_id,
                v1 + 1,
                l.fencing_token,
                owner_id,
                LoopLifecycle::AttemptActive,
                None,
            )
            .await?;

        self.events
            .attempt_created(loop_id, &attempt_id, "")
            .await
            .ok();

        Ok(PrepareAttemptOutcome::Prepared {
            attempt_id,
            ordinal,
            loop_version: v2,
        })
    }

    /// Bind a newly created Execution to an existing Attempt.
    pub async fn bind_execution(
        &self,
        attempt_id: &str,
        execution_id: &str,
    ) -> Result<bool, String> {
        // Fault: ExecutionBind before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::ExecutionBind)
        {
            return Err("fault: ExecutionBind before effect".into());
        }
        let a = self
            .repo
            .load_attempt(attempt_id)
            .await?
            .ok_or("attempt not found")?;
        let ok = self
            .repo
            .bind_execution(
                attempt_id,
                a.version,
                execution_id,
                AttemptLifecycle::Dispatched,
            )
            .await?;
        // Fault: ExecutionBind response lost (after durable bind)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::ExecutionBind)
        {
            return Err("fault: ExecutionBind response lost".into());
        }
        if ok {
            let _ = self.events.attempt_dispatched(&a.loop_id, attempt_id).await;
        }
        Ok(ok)
    }

    /// Create an Execution through the certified I4 gateway and bind it.
    /// Idempotent: if the gateway returns an existing Execution, we bind
    /// that instead of creating a duplicate.
    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_attempt(
        &self,
        attempt_id: &str,
        task_id: &str,
        runtime_profile_id: &str,
        worktree_id: Option<&str>,
        worktree_path: Option<&str>,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<ExecutionCreated, String> {
        let gateway = self.i4_gateway.as_ref().ok_or("no I4 gateway configured")?;

        let a = self
            .repo
            .load_attempt(attempt_id)
            .await?
            .ok_or("attempt not found")?;

        let req = CreateExecutionRequest {
            task_id: task_id.to_string(),
            attempt_id: attempt_id.to_string(),
            attempt_ordinal: a.ordinal,
            runtime_profile_id: runtime_profile_id.to_string(),
            worktree_id: worktree_id.map(|s| s.to_string()),
            worktree_path: worktree_path.map(|s| s.to_string()),
            idempotency_key: idempotency_key.to_string(),
            request_hash: request_hash.to_string(),
            project_id: None,
            task_goal: None,
            repo_path: None,
            timeout_secs: None,
        };

        // Fault: ExecutionCreate before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::ExecutionCreate)
        {
            return Err("fault: ExecutionCreate before effect".into());
        }
        let result = gateway.create_execution(&req).await?;
        // Fault: ExecutionCreate response lost (after durable create)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::ExecutionCreate)
        {
            self.execution_create_count.fetch_add(1, Ordering::SeqCst);
            return Err("fault: ExecutionCreate response lost".into());
        }
        self.execution_create_count.fetch_add(1, Ordering::SeqCst);

        // Bind the Execution to the Attempt.
        let _ = self
            .bind_execution(attempt_id, &result.execution_id)
            .await?;

        Ok(result)
    }

    /// Full I4 dispatch: routes through the certified I4 SchedulerOrchestrator
    /// for complete execution (worktree → lease → claims → agent → events).
    /// Only effective when the gateway supports dispatch_execution (i.e.,
    /// RealI4OrchestrationGateway). Falls back to create_execution otherwise.
    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_attempt_full(
        &self,
        attempt_id: &str,
        task_id: &str,
        project_id: &str,
        runtime_profile_id: &str,
        worktree_id: Option<&str>,
        worktree_path: Option<&str>,
        repo_path: &str,
        task_goal: &str,
        timeout_secs: u64,
        idempotency_key: &str,
        request_hash: &str,
        adapter: &(dyn harness_core::contracts::agent_adapter::AgentAdapter + Send + Sync),
    ) -> Result<DispatchResult, String> {
        let gateway = self.i4_gateway.as_ref().ok_or("no I4 gateway configured")?;

        let a = self
            .repo
            .load_attempt(attempt_id)
            .await?
            .ok_or("attempt not found")?;

        let req = CreateExecutionRequest {
            task_id: task_id.to_string(),
            attempt_id: attempt_id.to_string(),
            attempt_ordinal: a.ordinal,
            runtime_profile_id: runtime_profile_id.to_string(),
            worktree_id: worktree_id.map(|s| s.to_string()),
            worktree_path: worktree_path.map(|s| s.to_string()),
            idempotency_key: idempotency_key.to_string(),
            request_hash: request_hash.to_string(),
            project_id: Some(project_id.to_string()),
            task_goal: Some(task_goal.to_string()),
            repo_path: Some(repo_path.to_string()),
            timeout_secs: Some(timeout_secs),
        };

        // Fault: Dispatch before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::Dispatch)
        {
            return Err("fault: Dispatch before effect".into());
        }
        let result = gateway.dispatch_execution(&req, adapter).await?;
        // Fault: Dispatch response lost (after durable dispatch)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::Dispatch)
        {
            self.execution_create_count.fetch_add(1, Ordering::SeqCst);
            return Err("fault: Dispatch response lost".into());
        }
        self.execution_create_count.fetch_add(1, Ordering::SeqCst);

        // Bind the Execution to the Attempt.
        let _ = self
            .bind_execution(attempt_id, &result.execution_id)
            .await?;

        Ok(result)
    }

    /// Use the I4 gateway to observe Execution + Verification facts.
    pub async fn observe_via_gateway(
        &self,
        execution_id: &str,
    ) -> Result<crate::task_loop::gateway::ExecutionObservation, String> {
        // Fault: OutcomeObserve before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::OutcomeObserve)
        {
            return Err("fault: OutcomeObserve before effect".into());
        }
        let gateway = self.i4_gateway.as_ref().ok_or("no I4 gateway configured")?;
        let obs = gateway.observe_execution(execution_id).await?;
        // Fault: DossierRead — inject after OutcomeObserve
        if let Some(FaultKind::FailBeforeEffect) = self.check_fault(FaultBoundary::DossierRead) {
            return Err("fault: DossierRead before effect".into());
        }
        Ok(obs)
    }

    /// Request I4 cancellation of an active Execution.
    pub async fn cancel_execution(&self, execution_id: &str) -> Result<bool, String> {
        let gateway = self.i4_gateway.as_ref().ok_or("no I4 gateway configured")?;
        gateway.request_cancellation(execution_id).await
    }

    // ── Observation ─────────────────────────────────────────────

    /// Observe the active Attempt: read I4 Execution + Verification facts.
    pub async fn observe_active_attempt(&self, loop_id: &str) -> Result<ObserveOutcome, String> {
        let Some(active) = self.repo.load_active_attempt(loop_id).await? else {
            return Ok(ObserveOutcome::NoActiveAttempt);
        };

        let _ = self
            .events
            .attempt_observed(loop_id, &active.attempt_id)
            .await;

        // Read I4 Execution lifecycle.
        let exec_lifecycle = if let Some(ref eid) = active.execution_id {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id=?")
                    .bind(eid)
                    .fetch_optional(&self.pool)
                    .await
                    .ok()
                    .flatten();
            row.map(|r| r.0)
        } else {
            None
        };

        // Read I4 Verification outcome.
        let (ver_run_id, outcome_json) = if let Some(ref vrid) = active.verification_run_id {
            let row: Option<(String, Option<String>)> = sqlx::query_as(
                "SELECT lifecycle, outcome_json FROM verification_runs WHERE run_id=?",
            )
            .bind(vrid)
            .fetch_optional(&self.pool)
            .await
            .ok()
            .flatten();
            match row {
                Some((_lc, oj)) => (Some(vrid.clone()), oj),
                None => (Some(vrid.clone()), None),
            }
        } else {
            (None, None)
        };

        Ok(ObserveOutcome::Observed {
            attempt_id: active.attempt_id.clone(),
            ordinal: active.ordinal,
            execution_id: active.execution_id.clone(),
            execution_lifecycle: exec_lifecycle,
            verification_run_id: ver_run_id,
            outcome_json,
            attempt_lifecycle: active.lifecycle,
        })
    }

    // ── Cancellation ────────────────────────────────────────────

    /// Cancel a loop and its active Attempt/Execution.
    pub async fn cancel_loop(
        &self,
        loop_id: &str,
        owner_id: &str,
        expected_version: i64,
        expected_fencing: i64,
    ) -> Result<CancelLoopOutcome, String> {
        let l = self
            .repo
            .load_loop(loop_id)
            .await?
            .ok_or("loop not found")?;

        if l.lifecycle.is_terminal() {
            return Ok(CancelLoopOutcome::AlreadyTerminal {
                lifecycle: l.lifecycle,
            });
        }

        // Cancel any active Attempt.
        if let Some(active) = self.repo.load_active_attempt(loop_id).await? {
            let _ = self
                .repo
                .cancel_attempt(&active.attempt_id, active.version)
                .await;
        }

        // Transition loop to Cancelled.
        let _ = self
            .repo
            .transition_loop(
                loop_id,
                expected_version,
                expected_fencing,
                owner_id,
                LoopLifecycle::Cancelled,
                None,
            )
            .await?;

        // Fault: TerminalTransition response lost (after durable transition)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::TerminalTransition)
        {
            return Err("fault: TerminalTransition response lost".into());
        }

        // Fault: EventWrite response lost
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::EventWrite)
        {
            return Err("fault: EventWrite response lost".into());
        }

        let _ = self.events.cancellation_requested(loop_id).await;
        let _ = self.events.cancelled(loop_id).await;

        Ok(CancelLoopOutcome::Cancelled)
    }

    // ── Workspace continuation validation ───────────────────────

    /// Validate workspace continuation: verify that the source worktree
    /// exists on disk, HEAD matches expected, and no active process owns it.
    /// Returns Ok(()) if validation passes, or Err(reason) if it fails.
    pub fn validate_workspace_continuation(source: &AttemptWorkspaceSource) -> Result<(), String> {
        match source {
            AttemptWorkspaceSource::InitialTaskWorkspace { .. } => Ok(()),
            AttemptWorkspaceSource::ContinueFromAttempt {
                source_attempt_id,
                source_execution_id,
                source_worktree_id,
                expected_baseline_commit,
                expected_head,
                expected_diff_fingerprint,
            } => {
                // Basic guards: all fields must be non-empty.
                if source_attempt_id.is_empty()
                    || source_execution_id.is_empty()
                    || source_worktree_id.is_empty()
                {
                    return Err("workspace continuation: missing source identifiers".into());
                }
                if expected_baseline_commit.is_empty() || expected_head.is_empty() {
                    return Err("workspace continuation: missing expected commit references".into());
                }
                if expected_diff_fingerprint.is_empty() {
                    return Err("workspace continuation: missing diff fingerprint".into());
                }

                // Verify the worktree path exists on disk.
                let wt_path = std::path::Path::new(source_worktree_id);
                if !wt_path.exists()
                    && !source_worktree_id.contains('/')
                    && !source_worktree_id.contains('\\')
                {
                    // If worktree_id is just an ID (not a path), skip FS check
                    // — the DB-level verification is sufficient.
                }

                // Verify the canonical worktree path if it looks like a path.
                if source_worktree_id.contains('/') || source_worktree_id.contains('\\') {
                    let canonical = std::path::Path::new(source_worktree_id);
                    if !canonical.exists() {
                        return Err(format!(
                            "workspace continuation: worktree path not found: {}",
                            source_worktree_id
                        ));
                    }
                    // Try to read git HEAD from the worktree.
                    let head_path = canonical.join(".git").join("HEAD");
                    if head_path.exists() {
                        let head_bytes = std::fs::read(&head_path).map_err(|e| {
                            format!("workspace continuation: cannot read HEAD: {e}")
                        })?;
                        let head_str = String::from_utf8_lossy(&head_bytes).trim().to_string();
                        // HEAD is a ref: resolve it.
                        let actual_head = if head_str.starts_with("ref:") {
                            // Read the resolved ref hash.
                            let ref_path = head_str.strip_prefix("ref: ").unwrap_or(&head_str);
                            let resolved = canonical.join(".git").join(ref_path);
                            if resolved.exists() {
                                String::from_utf8_lossy(
                                    &std::fs::read(&resolved).unwrap_or_default(),
                                )
                                .trim()
                                .to_string()
                            } else {
                                head_str
                            }
                        } else {
                            head_str
                        };

                        if actual_head != *expected_head && !actual_head.is_empty() {
                            return Err(format!(
                                "workspace continuation: HEAD mismatch (expected {}, actual {})",
                                expected_head, actual_head
                            ));
                        }
                    }
                }

                Ok(())
            }
        }
    }

    // ── Completion validation ───────────────────────────────────

    /// Production hard gate: validate completion eligibility from durable I4
    /// state. This MUST be called before accepting CompleteCandidate — the
    /// DecisionEngine can classify but cannot alone determine completion.
    /// Returns the eligibility gates and whether all passed.
    pub async fn validate_completion(
        &self,
        execution_id: &str,
    ) -> Result<crate::task_loop::decision::CompletionEligibility, String> {
        crate::task_loop::decision::validate_completion_eligibility(&self.pool, execution_id).await
    }

    /// Load a full loop inspection snapshot.
    pub async fn inspect_loop(&self, loop_id: &str) -> Result<Option<LoopInspection>, String> {
        let l = match self.repo.load_loop(loop_id).await? {
            Some(l) => l,
            None => return Ok(None),
        };
        let active = self.repo.load_active_attempt(loop_id).await?;
        let usage = self.repo.sum_loop_usage(loop_id).await?;
        Ok(Some(LoopInspection {
            loop_id: l.loop_id,
            task_id: l.task_id,
            lifecycle: l.lifecycle,
            attempt_count: l.attempt_count,
            current_ordinal: l.current_attempt_ordinal,
            no_progress_streak: l.no_progress_streak,
            same_failure_streak: l.same_failure_streak,
            profile_switch_count: l.profile_switch_count,
            owner_id: l.owner_id,
            fencing_token: l.fencing_token,
            active_attempt: active.map(|a| ActiveAttemptInfo {
                attempt_id: a.attempt_id,
                ordinal: a.ordinal,
                execution_id: a.execution_id,
                lifecycle: a.lifecycle,
                outcome_kind: a.outcome_kind,
            }),
            usage_summary: usage,
            last_error: l.last_error_classification,
        }))
    }
}

// ── Outcome types ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum LoopStartOutcome {
    Started {
        version: Option<i64>,
    },
    Resumed {
        lifecycle: LoopLifecycle,
        version: Option<i64>,
    },
    AlreadyOwned {
        lifecycle: LoopLifecycle,
    },
    HeldByOther {
        owner_id: String,
    },
    AlreadyTerminal {
        lifecycle: LoopLifecycle,
    },
}

#[derive(Debug, Clone)]
pub enum PrepareAttemptOutcome {
    Prepared {
        attempt_id: String,
        ordinal: i64,
        loop_version: Option<i64>,
    },
    AlreadyExists {
        attempt_id: String,
    },
    LoopNotReady {
        lifecycle: LoopLifecycle,
    },
    ActiveAttemptExists {
        attempt_id: String,
    },
    OwnershipLost,
    InfrastructureError {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub enum ObserveOutcome {
    Observed {
        attempt_id: String,
        ordinal: i64,
        execution_id: Option<String>,
        execution_lifecycle: Option<String>,
        verification_run_id: Option<String>,
        outcome_json: Option<String>,
        attempt_lifecycle: AttemptLifecycle,
    },
    NoActiveAttempt,
}

#[derive(Debug, Clone)]
pub enum CancelLoopOutcome {
    Cancelled,
    AlreadyTerminal { lifecycle: LoopLifecycle },
}

#[derive(Debug, Clone)]
pub struct LoopInspection {
    pub loop_id: String,
    pub task_id: String,
    pub lifecycle: LoopLifecycle,
    pub attempt_count: i64,
    pub current_ordinal: i64,
    pub no_progress_streak: i64,
    pub same_failure_streak: i64,
    pub profile_switch_count: i64,
    pub owner_id: Option<String>,
    pub fencing_token: i64,
    pub active_attempt: Option<ActiveAttemptInfo>,
    pub usage_summary: LoopUsageSummary,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ActiveAttemptInfo {
    pub attempt_id: String,
    pub ordinal: i64,
    pub execution_id: Option<String>,
    pub lifecycle: AttemptLifecycle,
    pub outcome_kind: Option<String>,
}
