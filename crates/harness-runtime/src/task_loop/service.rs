//! TaskEngineeringLoopService — I4.5 Task-level loop orchestration.
//!
//! Manages the lifecycle of one task engineering loop: creates immutable
//! Attempts, dispatches them through certified I4, reads outcomes, and
//! deterministically decides next actions.
//!
//! NEVER: bypasses I4, calls Agent/LLM directly, commits/merges, deletes
//! Worktrees, or modifies certified I4 outcomes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sqlx::SqlitePool;

use super::events::TaskLoopEventWriter;
use super::gateway::{CreateExecutionRequest, ExecutionCreated, I4Gateway};
use super::repo::{LoopUsageSummary, TaskLoopRepo};
use super::types::*;

// ── Service ──────────────────────────────────────────────────────

pub struct TaskEngineeringLoopService {
    pool: SqlitePool,
    repo: TaskLoopRepo,
    events: TaskLoopEventWriter,
    i4_gateway: Option<Arc<dyn I4Gateway>>,
    pub loop_create_count: Arc<AtomicUsize>,
    pub attempt_create_count: Arc<AtomicUsize>,
    pub execution_create_count: Arc<AtomicUsize>,
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
            loop_create_count: Arc::new(AtomicUsize::new(0)),
            attempt_create_count: Arc::new(AtomicUsize::new(0)),
            execution_create_count: Arc::new(AtomicUsize::new(0)),
            decision_count: Arc::new(AtomicUsize::new(0)),
            _worker_id: format!("tls-{}", uuid::Uuid::new_v4()),
        }
    }

    /// Wire a real I4 gateway for production use.
    pub fn with_i4_gateway(mut self, gateway: Arc<dyn I4Gateway>) -> Self {
        self.i4_gateway = Some(gateway);
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

    // ── Loop lifecycle ──────────────────────────────────────────

    /// Create a new task engineering loop. Idempotent.
    pub async fn create_loop(&self, req: &CreateLoopRequest) -> Result<CreateLoopOutcome, String> {
        let outcome = self.repo.create_loop(req).await?;
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
        expected_version: i64,
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

        // Guard: no active Attempt.
        if let Some(active) = self.repo.load_active_attempt(loop_id).await? {
            if active.lifecycle.is_active() {
                return Ok(PrepareAttemptOutcome::ActiveAttemptExists {
                    attempt_id: active.attempt_id,
                });
            }
        }

        // Transition loop: Ready/Evaluating → PreparingAttempt.
        let v1 = self
            .repo
            .transition_loop(
                loop_id,
                expected_version,
                expected_fencing,
                owner_id,
                LoopLifecycle::PreparingAttempt,
                None,
            )
            .await?
            .ok_or("loop transition CAS lost")?;

        let ordinal = l.current_attempt_ordinal + 1;
        let attempt_id = format!("ta-{}-{}", loop_id, ordinal);
        let parent_attempt_id = l.active_attempt_id.clone();

        // Create Context Pack if spec provided.
        let context_pack_id = if let Some(spec) = &context_pack_spec {
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
            let _ = self.events.context_pack_created(loop_id, &cp_id).await;
            Some(cp_id)
        } else {
            None
        };

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

        // Transition loop to AttemptActive.
        let v2 = self
            .repo
            .transition_loop(
                loop_id,
                v1 + 1,
                expected_fencing,
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
        if ok {
            let _ = self
                .events
                .attempt_dispatched(&a.loop_id, attempt_id)
                .await;
        }
        Ok(ok)
    }

    /// Create an Execution through the certified I4 gateway and bind it.
    /// Idempotent: if the gateway returns an existing Execution, we bind
    /// that instead of creating a duplicate.
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
        let gateway = self
            .i4_gateway
            .as_ref()
            .ok_or("no I4 gateway configured")?;

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
        };

        let result = gateway.create_execution(&req).await?;
        self.execution_create_count
            .fetch_add(1, Ordering::SeqCst);

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
        let gateway = self
            .i4_gateway
            .as_ref()
            .ok_or("no I4 gateway configured")?;
        gateway.observe_execution(execution_id).await
    }

    /// Request I4 cancellation of an active Execution.
    pub async fn cancel_execution(&self, execution_id: &str) -> Result<bool, String> {
        let gateway = self
            .i4_gateway
            .as_ref()
            .ok_or("no I4 gateway configured")?;
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

        let _ = self.events.cancellation_requested(loop_id).await;
        let _ = self.events.cancelled(loop_id).await;

        Ok(CancelLoopOutcome::Cancelled)
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
