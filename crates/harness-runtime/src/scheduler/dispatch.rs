//! SchedulerOrchestrator — dispatch saga.
//! Coordinates: reservation → execution → worktree → lease → claim → adapter → events → outcome.
//! Handles compensation on failure, idempotency on response loss.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use harness_core::contracts::agent_adapter::{AgentAdapter, SessionOptions};
use harness_core::contracts::scheduler::{DispatchOutcome, DispatchStatus, TerminalOutcome};
use harness_core::contracts::task::TaskLifecycle;
use harness_core::state_machine::ExecutionLifecycle;
use harness_core::resource_claim::ClaimGroupSpec;
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use super::concurrency::ConcurrencyManager;
use super::event_sink::SchedulerEventSink;
use crate::lease::service::WorkspaceLeaseService;
use crate::lease::types::LeaseSpec;
use crate::resource_claim::service::ResourceClaimService;
use crate::resource_claim::ClaimGuard;
use crate::transition::TransitionService;
use crate::worktree::manager::WorktreeManager;
use crate::worktree::types::WorktreeSpec;

pub struct SchedulerOrchestrator {
    pool: SqlitePool,
    transitions: TransitionService,
    concurrency: ConcurrencyManager,
    worktree_mgr: Arc<WorktreeManager>,
    lease_service: Arc<WorkspaceLeaseService>,
    claim_service: Arc<ResourceClaimService>,
}

impl SchedulerOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: SqlitePool, transitions: TransitionService, concurrency: ConcurrencyManager,
        worktree_mgr: Arc<WorktreeManager>, lease_service: Arc<WorkspaceLeaseService>,
        claim_service: Arc<ResourceClaimService>,
    ) -> Self {
        Self { pool, transitions, concurrency, worktree_mgr, lease_service, claim_service }
    }

    /// Full dispatch saga. Returns outcome with compensation info.
    pub async fn dispatch(
        &self, task_id: &str, project_id: &str, profile_id: &str,
        repo_path: &std::path::Path,
        adapter: &(dyn AgentAdapter + Send + Sync),
        task_goal: &str, timeout: Duration, env: HashMap<String, String>,
    ) -> Result<DispatchOutcome, CoreError> {
        let op_id = format!("dispatch-{}", Uuid::new_v4());
        let ikey = format!("dispatch-{}-v1", task_id);

        if let Some(existing) = self.check_duplicate(&ikey).await? { return Ok(existing); }
        self.record_intent(&op_id, project_id, task_id, profile_id, &ikey).await?;

        // 1. Reserve concurrency
        let reservation = self.concurrency.reserve(task_id, Some(profile_id), None).await?;
        let _reservation_id = match reservation {
            harness_core::contracts::scheduler::ReservationResult::Reserved { reservation_id, .. } => reservation_id,
            other => return Ok(self.fail(&op_id, task_id, None, &format!("{:?}", other)).await),
        };

        // 2. Create Execution Attempt
        let exec_id = format!("exec-{}", Uuid::new_v4());
        let attempt = self.next_attempt(task_id).await?;
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id) VALUES (?,?,?,'created',?)")
            .bind(&exec_id).bind(task_id).bind(attempt).bind(profile_id)
            .execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        // 3. Transition task
        let task_lc = self.get_task_lc(task_id).await.unwrap_or(TaskLifecycle::Pending);
        let _ = self.transitions.transition_task(task_id, &task_lc, &TaskLifecycle::Dispatched, &format!("{}-disp", ikey)).await;
        self.update_stage(&op_id, "dispatched", Some(&exec_id)).await?;

        // 4. Create Worktree
        use crate::worktree::naming;
        let branch = naming::branch_name(task_id, &exec_id).map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        let harness_root = std::env::temp_dir().join("harness-worktrees");
        let wt_path = harness_root.join(format!("wt-{}", &exec_id[..8]));
        let wt_spec = WorktreeSpec {
            project_id: project_id.to_string(),
            task_id: task_id.to_string(),
            execution_id: exec_id.clone(),
            repository_root: repo_path.to_path_buf(),
            base_commit: "HEAD".to_string(),
            worktree_path: wt_path,
            branch_name: branch,
            operation_id: format!("wt-{}", op_id),
            owner_supervisor_id: String::new(),
        };
        let wt_outcome = self.worktree_mgr.create_worktree(&wt_spec).await.map_err(|e| {
            CoreError::new(ErrorCode::PersistenceError, format!("worktree: {e}"), ErrorSource::System)
        })?;
        let wt_id = match wt_outcome {
            crate::worktree::types::WorktreeCreateOutcome::Created(ref record) => record.worktree_id.clone(),
            crate::worktree::types::WorktreeCreateOutcome::AlreadyExists(ref record) => record.worktree_id.clone(),
            crate::worktree::types::WorktreeCreateOutcome::InProgress => {
                return Ok(self.fail(&op_id, task_id, Some(&exec_id), "worktree create in progress by another owner").await);
            }
        };
        self.update_stage(&op_id, "worktree_ready", None).await?;

        // 5. Acquire Lease
        let lease_spec = LeaseSpec {
            worktree_id: wt_id,
            project_id: project_id.to_string(),
            task_id: task_id.to_string(),
            owner_execution_id: exec_id.clone(),
            owner_supervisor_id: String::new(),
            lease_duration: timeout + Duration::from_secs(60),
            idempotency_key: format!("lease-{}", ikey),
        };
        let lease_record = match self.lease_service.acquire_lease(&lease_spec).await {
            Ok(crate::lease::types::LeaseAcquireOutcome::Acquired(record)) => record,
            Ok(crate::lease::types::LeaseAcquireOutcome::AlreadyAcquired(record)) => record,
            Ok(other) => {
                let _ = self.concurrency.release(task_id).await;
                return Ok(self.fail(&op_id, task_id, Some(&exec_id), &format!("lease contested: {:?}", other)).await);
            }
            Err(e) => {
                let _ = self.concurrency.release(task_id).await;
                return Ok(self.fail(&op_id, task_id, Some(&exec_id), &format!("lease: {e}")).await);
            }
        };
        self.update_stage(&op_id, "lease_acquired", None).await?;

        // 6. Acquire Resource Claims
        use harness_core::resource_claim::{AccessMode, ResourceClaimSpec};
        let claim_spec = ResourceClaimSpec::repository_wide(
            &repo_path.to_string_lossy(),
            AccessMode::Write,
        );
        let claim_spec = ClaimGroupSpec {
            claims: vec![claim_spec],
            project_id: project_id.to_string(),
            task_id: task_id.to_string(),
            execution_id: exec_id.clone(),
            repository_identity: repo_path.to_string_lossy().to_string(),
            lease_id: Some(lease_record.lease_id.clone()),
            worktree_id: Some(lease_record.worktree_id.clone().unwrap_or_default()),
        };
        let claim_guard = ClaimGuard {
            lease_id: lease_record.lease_id.clone(),
            lease_token: lease_record.lease_token,
            fencing_token: lease_record.fencing_token,
            worktree_id: lease_record.worktree_id.clone().unwrap_or_default(),
            project_id: project_id.to_string(),
            task_id: task_id.to_string(),
            execution_id: exec_id.clone(),
        };
        let claim_ikey = format!("claim-{}", ikey);
        let claim_result = self.claim_service.acquire_group(&claim_spec, &claim_guard, &claim_ikey).await;
        if let Err(e) = claim_result {
            let _ = self.concurrency.release(task_id).await;
            return Ok(self.fail(&op_id, task_id, Some(&exec_id), &format!("claim: {e}")).await);
        }
        self.update_stage(&op_id, "claims_acquired", None).await?;

        // 7. Transition Execution → Running
        let _ = self.transitions.transition_execution(&exec_id, &ExecutionLifecycle::Running, Some("dispatch"), &format!("{}-run", ikey)).await;
        self.update_stage(&op_id, "agent_starting", None).await?;

        // 8. Start Agent via Adapter
        let profile = self.load_profile(profile_id).await?;
        let session_opts = SessionOptions {
            working_directory: repo_path.to_path_buf(), env, timeout, max_turns: None,
            resume_session_id: None, model_override: None, effort_override: None, extra_args: vec![],
        };
        let mut session = adapter.start_session(&profile, &session_opts).await.map_err(|e| {
            CoreError::new(ErrorCode::ProcessSpawnFailed, format!("adapter: {e}"), ErrorSource::Agent)
        })?;
        self.update_stage(&op_id, "agent_running", None).await?;

        let envelope = harness_core::contracts::task_envelope::TaskEnvelope {
            task_id: task_id.to_string(), project_id: project_id.to_string(), task_goal: task_goal.to_string(),
            scope: harness_core::contracts::task_envelope::FileScope { allowed_paths: vec!["**".into()], forbidden_paths: vec![], readable_paths: vec![], scope_expansion_allowed: false },
            resource_claims: vec![], dependencies: vec![], acceptance_checks: vec![],
            allowed_tools: vec!["read".into(),"write".into()], output_schema: "TaskResultV1".into(),
            budget: harness_core::contracts::task_envelope::TaskBudget { max_turns: 50, max_time_ms: timeout.as_millis() as u64, max_cost_cents: None },
            goal_contract_version: 1, plan_version: 1,
        };
        if let Err(e) = session.send_task(&envelope).await {
            let _ = session.dispose().await;
            let _ = self.concurrency.release(task_id).await;
            return Ok(self.fail(&op_id, task_id, Some(&exec_id), &format!("send_task: {e}")).await);
        }

        // 9. Receive events via persistent sink
        let mut event_sink = SchedulerEventSink::new(self.pool.clone(), exec_id.clone(), None);
        let receive_result = session.receive_events(&mut event_sink).await;
        let _ = session.dispose().await;

        let terminal = match receive_result {
            Ok(()) => TerminalOutcome::Completed,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("timeout") { TerminalOutcome::TimedOut { duration_ms: timeout.as_millis() as u64 } }
                else if msg.contains("cancel") { TerminalOutcome::Cancelled }
                else { TerminalOutcome::AdapterFailed { reason: msg } }
            }
        };

        // 10. Handle outcome
        let exec_lc = match &terminal { TerminalOutcome::Completed => ExecutionLifecycle::Completed, _ => ExecutionLifecycle::Failed };
        let _ = self.transitions.transition_execution(&exec_id, &exec_lc, None, &format!("{}-term", ikey)).await;

        if terminal.retain_resources() {
            let _ = self.transitions.transition_task(task_id, &TaskLifecycle::Running, &TaskLifecycle::Submitted, &format!("{}-sub", ikey)).await;
            self.update_status(&op_id, "agent_completed").await?;
        } else {
            let _ = self.concurrency.release(task_id).await;
            self.update_status(&op_id, "failed").await?;
        }

        Ok(DispatchOutcome { dispatch_op_id: op_id, task_id: task_id.to_string(), execution_id: Some(exec_id), status: DispatchStatus::AgentCompleted, terminal_outcome: Some(terminal), compensation_actions: vec![] })
    }

    async fn check_duplicate(&self, ikey: &str) -> Result<Option<DispatchOutcome>, CoreError> {
        let row: Option<(String, String, Option<String>)> = sqlx::query_as("SELECT id, task_id, execution_id FROM dispatch_operations WHERE idempotency_key = ? AND status IN ('agent_completed','failed','completed')").bind(ikey).fetch_optional(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(row.map(|(id, tid, eid)| DispatchOutcome { dispatch_op_id: id, task_id: tid, execution_id: eid, status: DispatchStatus::Completed, terminal_outcome: None, compensation_actions: vec!["duplicate".to_string()] }))
    }

    async fn record_intent(&self, id: &str, project_id: &str, task_id: &str, profile_id: &str, ikey: &str) -> Result<(), CoreError> {
        sqlx::query("INSERT INTO dispatch_operations (id, project_id, task_id, selected_profile_id, request_hash, idempotency_key, status, stage) VALUES (?,?,?,?,?,?,'preparing','init')").bind(id).bind(project_id).bind(task_id).bind(profile_id).bind(ikey).bind(ikey).execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?; Ok(())
    }

    async fn update_stage(&self, op_id: &str, stage: &str, exec_id: Option<&str>) -> Result<(), CoreError> {
        if let Some(eid) = exec_id { sqlx::query("UPDATE dispatch_operations SET stage=?, execution_id=? WHERE id=?").bind(stage).bind(eid).bind(op_id).execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?; }
        else { sqlx::query("UPDATE dispatch_operations SET stage=? WHERE id=?").bind(stage).bind(op_id).execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?; }
        Ok(())
    }

    async fn update_status(&self, op_id: &str, status: &str) -> Result<(), CoreError> {
        sqlx::query("UPDATE dispatch_operations SET status=?, completed_at=datetime('now') WHERE id=?").bind(status).bind(op_id).execute(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?; Ok(())
    }

    async fn next_attempt(&self, task_id: &str) -> Result<i64, CoreError> {
        let row: (Option<i64>,) = sqlx::query_as("SELECT MAX(attempt_number) FROM execution_attempts WHERE task_id=?").bind(task_id).fetch_one(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        Ok(row.0.unwrap_or(0) + 1)
    }

    async fn get_task_lc(&self, task_id: &str) -> Result<TaskLifecycle, CoreError> {
        let row: Option<(String,)> = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id=?").bind(task_id).fetch_optional(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        match row { Some((lc,)) => Ok(serde_json::from_str(&format!("\"{lc}\"")).unwrap_or(TaskLifecycle::Pending)), None => Err(CoreError::new(ErrorCode::PersistenceError, "task not found", ErrorSource::System)) }
    }

    async fn load_profile(&self, pid: &str) -> Result<harness_core::contracts::runtime_profile::RuntimeProfile, CoreError> {
        let row: Option<(String, String, String, String, String)> = sqlx::query_as("SELECT id, agent_kind, adapter_kind, agent_version, executable_path FROM runtime_profiles WHERE id=?").bind(pid).fetch_optional(&self.pool).await.map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;
        match row {
            Some((id, kind, adapter, ver, exe)) => Ok(harness_core::contracts::runtime_profile::RuntimeProfile {
                id, agent_definition_id: String::new(), label: String::new(), agent_kind: kind, adapter_kind: adapter, agent_version: ver, executable_path: exe,
                provider: String::new(), provider_source: harness_core::contracts::runtime_profile::ProviderSource::UserDeclared, model: None, base_url: None,
                auth_mode: harness_core::contracts::runtime_profile::AuthMode::Unknown, auth_status: harness_core::contracts::runtime_profile::AuthStatus::Unknown,
                credential_ref: None, capabilities: default_caps(),
                core_status: harness_core::contracts::runtime_profile::CoreStatus::Available,
                authentication_status: harness_core::contracts::runtime_profile::AuthCheckStatus::Unknown,
                execution_status: harness_core::contracts::runtime_profile::ExecutionStatus::Untested,
                optional_integrations: vec![], discovery_source: String::new(), passive_probe: None, active_validation: None,
                concurrency_max: 1, created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
            }),
            None => Err(CoreError::new(ErrorCode::ConfigMissing, format!("profile {} not found", pid), ErrorSource::System)),
        }
    }

    async fn fail(&self, op_id: &str, task_id: &str, exec_id: Option<&str>, reason: &str) -> DispatchOutcome {
        let _ = self.concurrency.release(task_id).await;
        let _ = sqlx::query("UPDATE dispatch_operations SET status='failed', outcome_json=? WHERE id=?").bind(reason).bind(op_id).execute(&self.pool).await;
        DispatchOutcome { dispatch_op_id: op_id.to_string(), task_id: task_id.to_string(), execution_id: exec_id.map(|s| s.to_string()), status: DispatchStatus::Failed, terminal_outcome: Some(TerminalOutcome::SpawnFailed { reason: reason.to_string() }), compensation_actions: vec!["released".to_string()] }
    }
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
        workspace_modes: vec![], supported_languages: vec![], mcp_tools: vec![], supported_platforms: vec![],
    }
}
