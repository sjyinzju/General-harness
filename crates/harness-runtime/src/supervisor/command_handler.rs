//! Supervisor IPC command handler — bridges IPC commands to existing
//! production services through the deterministic control loop.
//!
//! Each mutating command is:
//! 1. Validated against the command whitelist
//! 2. Persisted as a durable OperationIntent
//! 3. Routed to the appropriate production service
//! 4. Response is returned through IPC
//!
//! Read-only commands route directly to repositories/services.
//! Commands without backing production services return UnsupportedCommand.

use harness_core::contracts::ipc::{IpcCommand, IpcResponseStatus};
use harness_core::contracts::presentation as pres;
use harness_core::contracts::supervisor::SupervisorInstanceId;
use harness_core::CoreError;
use sha2::Digest;
use sqlx::SqlitePool;

use crate::idempotency;
use crate::ipc::{IpcCommandHandler, IpcHandlerOutcome, IpcRequestContext};
use crate::supervisor::SupervisorServices;
use crate::task_loop::types::CreateLoopRequest;

/// Production command handler that routes IPC commands to real services.
///
/// Each command is dispatched to the corresponding production service
/// from the SupervisorServices bundle. Commands without backing services
/// return structured UnsupportedCommand errors — never placeholder success.
pub struct SupervisorCommandHandler {
    pool: SqlitePool,
    services: SupervisorServices,
    /// Current supervisor instance for fencing validation.
    instance_id: Option<SupervisorInstanceId>,
    fencing_token: i64,
}

impl SupervisorCommandHandler {
    pub fn new(
        pool: SqlitePool,
        services: SupervisorServices,
        instance_id: Option<SupervisorInstanceId>,
        fencing_token: i64,
    ) -> Self {
        Self {
            pool,
            services,
            instance_id,
            fencing_token,
        }
    }

    /// Update the fencing context after takeover.
    pub fn update_fencing(&mut self, instance_id: SupervisorInstanceId, fencing_token: i64) {
        self.instance_id = Some(instance_id);
        self.fencing_token = fencing_token;
    }

    /// Persist an OperationIntent for a mutating command and return the operation_id.
    pub(crate) async fn persist_operation_intent(
        &self,
        request_id: &str,
        idempotency_key: &str,
        command_name: &str,
        aggregate_id: &str,
        payload: &serde_json::Value,
    ) -> Result<String, CoreError> {
        let operation_id = uuid::Uuid::new_v4().to_string();
        let payload_json = serde_json::to_string(payload).unwrap_or_default();
        let owner_id = self
            .instance_id
            .as_ref()
            .map(|i| i.0.clone())
            .unwrap_or_default();

        // Idempotency check: same key + same payload → return existing
        if !idempotency_key.is_empty() {
            let existing: Option<(String, String)> = sqlx::query_as(
                r#"SELECT operation_id, state FROM operation_intents
                   WHERE idempotency_key = ? AND payload_json = ?
                   ORDER BY created_at DESC LIMIT 1"#,
            )
            .bind(idempotency_key)
            .bind(&payload_json)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("idempotency check: {e}"),
                    harness_core::ErrorSource::System,
                )
            })?;

            if let Some((existing_id, _state)) = existing {
                return Ok(existing_id);
            }

            // Check for idempotency conflict: same key, different payload
            let conflict: Option<(String,)> = sqlx::query_as(
                r#"SELECT operation_id FROM operation_intents
                   WHERE idempotency_key = ? AND payload_json != ?
                   LIMIT 1"#,
            )
            .bind(idempotency_key)
            .bind(&payload_json)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("idempotency conflict check: {e}"),
                    harness_core::ErrorSource::System,
                )
            })?;

            if conflict.is_some() {
                return Err(CoreError::new(
                    harness_core::ErrorCode::Conflict,
                    format!(
                        "idempotency conflict: key '{idempotency_key}' used with different payload"
                    ),
                    harness_core::ErrorSource::Harness,
                ));
            }
        }

        sqlx::query(
            r#"INSERT INTO operation_intents
               (operation_id, request_id, idempotency_key, operation_kind, aggregate_id,
                desired_action, state, owner_instance_id, owner_fencing_token,
                attempt, payload_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, 1, ?, datetime('now'), datetime('now'))"#,
        )
        .bind(&operation_id)
        .bind(request_id)
        .bind(idempotency_key)
        .bind(command_name)
        .bind(aggregate_id)
        .bind(command_name)
        .bind(&owner_id)
        .bind(self.fencing_token)
        .bind(&payload_json)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("persist operation intent: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;

        Ok(operation_id)
    }

    /// Build an UnsupportedCommand response.
    fn unsupported(command: &IpcCommand) -> serde_json::Value {
        serde_json::json!({
            "supported": false,
            "command": command.as_str(),
            "error": "unsupported_command",
            "message": format!("Command '{}' is not supported in this supervisor version", command.as_str())
        })
    }
}

#[async_trait::async_trait]
impl IpcCommandHandler for SupervisorCommandHandler {
    async fn handle_command(
        &self,
        command: &IpcCommand,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        match command {
            // ── Supervisor lifecycle ──────────────────────────
            IpcCommand::SupervisorStatus => self.cmd_supervisor_status(payload).await,
            IpcCommand::SupervisorStop => self.cmd_supervisor_stop(payload).await,

            // ── Health / Diagnostics ─────────────────────────
            IpcCommand::Health => self.cmd_health(payload).await,
            IpcCommand::Diagnostics => self.cmd_diagnostics(payload).await,

            // ── Inspection ────────────────────────────────────
            IpcCommand::Inspect => self.cmd_inspect(payload).await,

            // ── Task loop ─────────────────────────────────────
            IpcCommand::TaskStart => self.cmd_task_start(payload).await,
            IpcCommand::TaskStatus => self.cmd_task_status(payload).await,
            IpcCommand::TaskResume => self.cmd_task_resume(payload).await,
            IpcCommand::TaskCancel => self.cmd_task_cancel(payload).await,
            IpcCommand::TaskInspect => self.cmd_task_inspect(payload).await,
            IpcCommand::TaskDryRunDecision => self.cmd_task_dry_run(payload).await,

            // ── Review ────────────────────────────────────────
            IpcCommand::ReviewCreate => self.cmd_review_create(payload).await,
            IpcCommand::ReviewShow => self.cmd_review_show(payload).await,
            IpcCommand::ReviewRun => self.cmd_review_run(payload).await,
            IpcCommand::ReviewList => self.cmd_review_list(payload).await,

            // ── Integration ───────────────────────────────────
            IpcCommand::IntegrationEnqueue => self.cmd_integration_enqueue(payload).await,
            IpcCommand::IntegrationRunNext => self.cmd_integration_run_next(payload).await,
            IpcCommand::IntegrationShow => self.cmd_integration_show(payload).await,
            IpcCommand::IntegrationList => self.cmd_integration_list(payload).await,
            IpcCommand::IntegrationCancel => self.cmd_integration_cancel(payload).await,
            IpcCommand::IntegrationRecover => self.cmd_integration_recover(payload).await,

            // ── Cancellation ──────────────────────────────────
            IpcCommand::Cancel => self.cmd_cancel(payload).await,

            // ── Event streaming ───────────────────────────────
            IpcCommand::Subscribe | IpcCommand::Unsubscribe => Ok(Self::unsupported(command)),

            // ── Goal loop ────────────────────────────────────
            IpcCommand::GoalCreate => self.cmd_goal_create(payload).await,
            IpcCommand::GoalStart => self.cmd_goal_start(payload).await,
            IpcCommand::GoalShow => self.cmd_goal_show(payload).await,
            IpcCommand::GoalList => self.cmd_goal_list(payload).await,
            IpcCommand::GoalStatus => self.cmd_goal_status(payload).await,
            IpcCommand::GoalPause => self.cmd_goal_pause(payload).await,
            IpcCommand::GoalResume => self.cmd_goal_resume(payload).await,
            IpcCommand::GoalCancel => self.cmd_goal_cancel(payload).await,
            IpcCommand::GoalReplan => self.cmd_goal_replan(payload).await,
            IpcCommand::GoalApprovals => self.cmd_goal_approvals(payload).await,
            IpcCommand::GoalApprove => self.cmd_goal_approve(payload).await,
            IpcCommand::GoalReject => self.cmd_goal_reject(payload).await,
            IpcCommand::GoalAnswer => self.cmd_goal_answer(payload).await,
            IpcCommand::GoalEvents => self.cmd_goal_events(payload).await,
            IpcCommand::GoalSnapshot => self.cmd_goal_snapshot(payload).await,
            IpcCommand::GoalRequestChanges => self.cmd_goal_request_changes(payload).await,
            IpcCommand::GoalIntervene => self.cmd_goal_intervene(payload).await,
        }
    }

    /// I8A: interaction mutations are wrapped in the durable request ledger.
    ///
    /// key  = "ipc-" + envelope.idempotency_key
    /// hash = sha256(command + canonical payload)
    ///
    /// A replay with the same key returns the stored result with
    /// status=Duplicate and produces no second business effect. The same key
    /// with a different payload is a Conflict.
    async fn handle_request(
        &self,
        ctx: &IpcRequestContext,
        command: &IpcCommand,
        payload: &serde_json::Value,
    ) -> Result<IpcHandlerOutcome, CoreError> {
        let ledgered = matches!(
            command,
            IpcCommand::GoalAnswer
                | IpcCommand::GoalApprove
                | IpcCommand::GoalRequestChanges
                | IpcCommand::GoalReject
                | IpcCommand::GoalIntervene
                | IpcCommand::GoalPause
                | IpcCommand::GoalResume
                | IpcCommand::GoalCancel
        );
        if !ledgered || ctx.idempotency_key.is_empty() {
            let result = self.handle_command(command, payload).await?;
            return Ok(IpcHandlerOutcome::success(result));
        }

        // Thread the request identity into interventions for provenance —
        // the hash stays bound to the client payload so retries match.
        let mut effective = payload.clone();
        if matches!(command, IpcCommand::GoalIntervene) {
            if let Some(obj) = effective.as_object_mut() {
                obj.entry("request_id".to_string())
                    .or_insert_with(|| serde_json::Value::String(ctx.request_id.clone()));
            }
        }

        let key = format!("ipc-{}", ctx.idempotency_key);
        let canonical = serde_json::to_string(payload).unwrap_or_default();
        let hash = {
            let mut hasher = sha2::Sha256::new();
            hasher.update(command.as_str().as_bytes());
            hasher.update(canonical.as_bytes());
            format!("{:x}", hasher.finalize())
        };

        match idempotency::try_claim(&self.pool, &key, &hash, 600).await {
            Ok(Some(token)) => match self.handle_command(command, &effective).await {
                Ok(result) => {
                    idempotency::complete_claim(&self.pool, &key, &token, &result.to_string())
                        .await?;
                    Ok(IpcHandlerOutcome::success(result))
                }
                Err(e) => {
                    let error_json = serde_json::json!({
                        "code": format!("{:?}", e.code),
                        "message": e.to_string(),
                    })
                    .to_string();
                    let _ =
                        idempotency::fail_claim(&self.pool, &key, &token, &error_json, false).await;
                    Err(e)
                }
            },
            Ok(None) => {
                // The claim is held or terminal. A replay is only valid when
                // the stored hash matches — a completed key reused with a
                // different payload is a conflict, never a silent replay.
                let stored_hash: Option<(String,)> =
                    sqlx::query_as("SELECT request_hash FROM idempotency_records WHERE key = ?")
                        .bind(&key)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| {
                            CoreError::new(
                                harness_core::ErrorCode::PersistenceError,
                                format!("idempotency hash read: {e}"),
                                harness_core::ErrorSource::System,
                            )
                        })?;
                if let Some((stored,)) = stored_hash {
                    if stored != hash {
                        return Err(CoreError::new(
                            harness_core::ErrorCode::Conflict,
                            format!(
                                "idempotency conflict: key '{}' reused with a different payload",
                                ctx.idempotency_key
                            ),
                            harness_core::ErrorSource::Harness,
                        ));
                    }
                }
                match idempotency::get_result(&self.pool, &key).await? {
                    Some(stored) => {
                        let replay = serde_json::from_str(&stored)
                            .unwrap_or(serde_json::Value::String(stored));
                        Ok(IpcHandlerOutcome::duplicate(replay))
                    }
                    None => Ok(IpcHandlerOutcome {
                        status: IpcResponseStatus::Accepted,
                        payload: serde_json::json!({
                            "state": "in_flight",
                            "message": "a request with this idempotency key is still executing",
                        }),
                    }),
                }
            }
            Err(e) => {
                if e.to_string().contains("idempotency_request_mismatch") {
                    Err(CoreError::new(
                        harness_core::ErrorCode::Conflict,
                        format!(
                            "idempotency conflict: key '{}' reused with a different payload",
                            ctx.idempotency_key
                        ),
                        harness_core::ErrorSource::Harness,
                    ))
                } else {
                    Err(e)
                }
            }
        }
    }
}

// ── Command implementations ──────────────────────────────────────

impl SupervisorCommandHandler {
    // ── Supervisor lifecycle ──────────────────────────────────────

    async fn cmd_supervisor_status(
        &self,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let instance = match &self.instance_id {
            Some(id) => self.services.supervisor_repo.get_instance(id).await?,
            None => {
                return Ok(serde_json::json!({
                    "status": "no_supervisor",
                    "message": "Supervisor instance not yet assigned"
                }))
            }
        };

        match instance {
            Some(inst) => Ok(serde_json::json!({
                "instance_id": inst.instance_id.0,
                "state": inst.state.to_string(),
                "pid": inst.pid,
                "process_started_at": inst.process_started_at.to_rfc3339(),
                "fencing_token": inst.fencing_token,
                "started_at": inst.started_at.to_rfc3339(),
                "heartbeat_at": inst.heartbeat_at.to_rfc3339(),
                "lease_expires_at": inst.lease_expires_at.to_rfc3339(),
                "protocol_version": inst.protocol_version,
                "binary_version": inst.binary_version,
            })),
            None => Ok(serde_json::json!({
                "status": "unknown",
                "message": "Instance record not found"
            })),
        }
    }

    async fn cmd_supervisor_stop(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        // Persist durable OperationIntent
        let request_id = uuid::Uuid::new_v4().to_string();
        let _op_id = self
            .persist_operation_intent(
                &request_id,
                &format!("supervisor-stop-{}", request_id),
                "supervisor.stop",
                "supervisor",
                payload,
            )
            .await?;

        if let Some(ref id) = self.instance_id {
            self.services
                .supervisor_repo
                .force_deactivate_lease(&id.0)
                .await
                .map_err(|e| {
                    CoreError::new(
                        harness_core::ErrorCode::Internal,
                        format!("stop supervisor: {e}"),
                        harness_core::ErrorSource::Harness,
                    )
                })?;
        }

        Ok(serde_json::json!({
            "acknowledged": true,
            "message": "Shutdown signal sent. Supervisor will drain and stop."
        }))
    }

    // ── Health / Diagnostics ─────────────────────────────────────

    async fn cmd_health(
        &self,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let db_ok = sqlx::query("SELECT 1").execute(&self.pool).await.is_ok();

        Ok(serde_json::json!({
            "healthy": db_ok,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    }

    async fn cmd_diagnostics(
        &self,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let tables_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                .fetch_one(&self.pool)
                .await
                .unwrap_or(-1);

        let active_leases: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM supervisor_leases WHERE is_active = 1")
                .fetch_one(&self.pool)
                .await
                .unwrap_or(-1);

        let pending_ops: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM operation_intents WHERE state NOT IN ('succeeded', 'failed', 'cancelled', 'abandoned')",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(-1);

        Ok(serde_json::json!({
            "binary_version": env!("CARGO_PKG_VERSION"),
            "database_connected": true,
            "tables_count": tables_count,
            "active_leases": active_leases,
            "pending_operations": pending_ops,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    }

    // ── Inspection ────────────────────────────────────────────────

    async fn cmd_inspect(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let aggregate_type = payload
            .get("aggregate_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let aggregate_id = payload
            .get("aggregate_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let rows: Vec<(String, String, String)> = sqlx::query_as(
            r#"SELECT operation_id, operation_kind, state
               FROM operation_intents
               WHERE aggregate_id = ?
               ORDER BY created_at DESC LIMIT 20"#,
        )
        .bind(aggregate_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("inspect query: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;

        let operations: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|(id, kind, state)| {
                serde_json::json!({
                    "operation_id": id,
                    "kind": kind,
                    "state": state,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "operations": operations,
            "count": operations.len(),
        }))
    }

    // ── Task loop commands ────────────────────────────────────────

    async fn cmd_task_start(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let project = payload
            .get("project")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let task = payload.get("task").and_then(|v| v.as_str()).unwrap_or("");
        let owner = payload
            .get("owner")
            .and_then(|v| v.as_str())
            .unwrap_or("ipc");
        let policy = payload
            .get("policy")
            .and_then(|v| v.as_str())
            .unwrap_or("{}");

        if task.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "task identifier is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Persist durable OperationIntent before executing
        let request_id = uuid::Uuid::new_v4().to_string();
        let aggregate_id = format!("task-{}-{}", project, task);
        let _op_id = self
            .persist_operation_intent(
                &request_id,
                &format!("task-start-{}-{}", project, task),
                "task.start",
                &aggregate_id,
                payload,
            )
            .await?;

        let req = CreateLoopRequest {
            project_id: project.to_string(),
            task_id: task.to_string(),
            policy_json: policy.to_string(),
            policy_fingerprint: String::new(),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            request_hash: String::new(),
            owner_id: owner.to_string(),
            lease_secs: 300,
        };

        let outcome = self
            .services
            .task_loop_service
            .create_loop(&req)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("task start: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        let loop_id = match outcome {
            crate::task_loop::types::CreateLoopOutcome::Created { loop_id } => loop_id,
            crate::task_loop::types::CreateLoopOutcome::Duplicate { loop_id } => loop_id,
            crate::task_loop::types::CreateLoopOutcome::IdempotencyConflict { .. } => {
                return Err(CoreError::new(
                    harness_core::ErrorCode::Conflict,
                    "idempotency conflict on task loop create",
                    harness_core::ErrorSource::Harness,
                ));
            }
            crate::task_loop::types::CreateLoopOutcome::TaskAlreadyHasActiveLoop {
                existing_loop_id,
            } => {
                return Ok(serde_json::json!({
                    "loop_id": existing_loop_id,
                    "status": "already_active",
                    "message": "Task already has an active loop",
                }));
            }
            crate::task_loop::types::CreateLoopOutcome::InfrastructureError { reason } => {
                return Err(CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("infrastructure error: {reason}"),
                    harness_core::ErrorSource::Harness,
                ));
            }
        };

        Ok(serde_json::json!({
            "loop_id": loop_id,
            "project": project,
            "task": task,
            "owner": owner,
            "status": "started",
        }))
    }

    async fn cmd_task_status(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let loop_id = payload
            .get("loop_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if loop_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "loop_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let inspection = self
            .services
            .task_loop_service
            .inspect_loop(loop_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("task status: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        match inspection {
            Some(info) => Ok(serde_json::json!({
                "loop_id": info.loop_id,
                "task_id": info.task_id,
                "lifecycle": info.lifecycle.as_str(),
                "attempt_count": info.attempt_count,
                "current_ordinal": info.current_ordinal,
            })),
            None => Ok(serde_json::json!({
                "loop_id": loop_id,
                "status": "not_found",
            })),
        }
    }

    async fn cmd_task_resume(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let loop_id = payload
            .get("loop_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let owner = payload
            .get("owner")
            .and_then(|v| v.as_str())
            .unwrap_or("ipc");

        if loop_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "loop_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let outcome = self
            .services
            .task_loop_service
            .start_or_resume_loop(loop_id, owner, 300)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("task resume: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "loop_id": loop_id,
            "status": format!("{:?}", outcome),
        }))
    }

    async fn cmd_task_cancel(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let loop_id = payload
            .get("loop_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let owner = payload
            .get("owner")
            .and_then(|v| v.as_str())
            .unwrap_or("ipc");

        if loop_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "loop_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let outcome = self
            .services
            .task_loop_service
            .cancel_loop(loop_id, owner, 0, 0)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("task cancel: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "loop_id": loop_id,
            "status": format!("{:?}", outcome),
        }))
    }

    async fn cmd_task_inspect(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let loop_id = payload
            .get("loop_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if loop_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "loop_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let inspection = self
            .services
            .task_loop_service
            .inspect_loop(loop_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("task inspect: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        match inspection {
            Some(info) => Ok(serde_json::json!({
                "loop_id": info.loop_id,
                "task_id": info.task_id,
                "lifecycle": info.lifecycle.as_str(),
                "attempt_count": info.attempt_count,
                "current_ordinal": info.current_ordinal,
                "no_progress_streak": info.no_progress_streak,
            })),
            None => Ok(serde_json::json!({
                "loop_id": loop_id,
                "status": "not_found",
            })),
        }
    }

    async fn cmd_task_dry_run(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let loop_id = payload
            .get("loop_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if loop_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "loop_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Observe the active attempt to get the current decision state
        let outcome = self
            .services
            .task_loop_service
            .observe_active_attempt(loop_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("task dry-run: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "loop_id": loop_id,
            "outcome": format!("{:?}", outcome),
        }))
    }

    // ── Review commands ───────────────────────────────────────────

    async fn cmd_review_create(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let candidate_id = payload
            .get("candidate_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let reviewer = payload
            .get("reviewer")
            .and_then(|v| v.as_str())
            .unwrap_or("default-reviewer");

        if candidate_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "candidate_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let review = self
            .services
            .review_service
            .create_review(&candidate_id.to_string(), reviewer)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("review create: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "review_id": review.review_id,
            "candidate_id": review.candidate_id,
            "reviewer": review.reviewer_profile_id,
            "state": format!("{:?}", review.state),
            "status": "created",
        }))
    }

    async fn cmd_review_show(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let review_id = payload
            .get("review_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if review_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "review_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let review = self
            .services
            .review_service
            .get_review(review_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("review show: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        match review {
            Some(r) => Ok(serde_json::json!({
                "review_id": r.review_id,
                "candidate_id": r.candidate_id,
                "state": format!("{:?}", r.state),
                "reviewer": r.reviewer_profile_id,
            })),
            None => Ok(serde_json::json!({
                "review_id": review_id,
                "status": "not_found",
            })),
        }
    }

    async fn cmd_review_run(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let review_id = payload
            .get("review_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if review_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "review_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Get the review and report its current state.
        // Full review orchestration (freeze → precheck → dossier → decision)
        // requires the complete pipeline with CandidateSnapshot, VerificationOutcome, etc.
        // The CLI `review run` command handles this through the production graph.
        let review = self
            .services
            .review_service
            .get_review(review_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("review run: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        match review {
            Some(r) => Ok(serde_json::json!({
                "review_id": r.review_id,
                "state": format!("{:?}", r.state),
                "message": "Review orchestration requires full pipeline. Use 'harness review run <id>' from CLI.",
            })),
            None => Ok(serde_json::json!({
                "review_id": review_id,
                "status": "not_found",
            })),
        }
    }

    async fn cmd_review_list(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let state_filter = payload.get("state").and_then(|v| v.as_str());

        let reviews = self
            .services
            .review_service
            .list_reviews(state_filter)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("review list: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        let items: Vec<serde_json::Value> = reviews
            .iter()
            .map(|r| {
                serde_json::json!({
                    "review_id": r.review_id,
                    "candidate_id": r.candidate_id,
                    "state": format!("{:?}", r.state),
                })
            })
            .collect();

        Ok(serde_json::json!({
            "reviews": items,
            "count": items.len(),
        }))
    }

    // ── Integration commands ──────────────────────────────────────

    async fn cmd_integration_enqueue(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let candidate_id = payload
            .get("candidate_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let repo_id = payload
            .get("repo_id")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let target_ref = payload
            .get("target_ref")
            .and_then(|v| v.as_str())
            .unwrap_or("refs/heads/main");
        let priority: i32 = payload
            .get("priority")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        if candidate_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "candidate_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Persist durable OperationIntent
        let request_id = uuid::Uuid::new_v4().to_string();
        let _op_id = self
            .persist_operation_intent(
                &request_id,
                &format!("integration-enqueue-{}", candidate_id),
                "integration.enqueue",
                candidate_id,
                payload,
            )
            .await?;

        let commit_request_id = uuid::Uuid::new_v4().to_string();
        let integration_id: harness_core::contracts::integration::IntegrationId =
            uuid::Uuid::new_v4().to_string();

        let request = self
            .services
            .integration_queue
            .enqueue(
                &integration_id,
                &commit_request_id,
                candidate_id,
                "via-ipc", // review_id
                repo_id,
                target_ref,
                "", // expected_target_head
                priority,
            )
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("integration enqueue: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "integration_id": request.integration_id,
            "candidate_id": candidate_id,
            "target_ref": target_ref,
            "target_ref": target_ref,
            "priority": priority,
        }))
    }

    async fn cmd_integration_run_next(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let repo_id = payload
            .get("repo_id")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        let target_ref = payload
            .get("target_ref")
            .and_then(|v| v.as_str())
            .unwrap_or("refs/heads/main");

        // Persist durable OperationIntent
        let request_id = uuid::Uuid::new_v4().to_string();
        let _op_id = self
            .persist_operation_intent(
                &request_id,
                &format!("integration-run-next-{}-{}", repo_id, target_ref),
                "integration.run_next",
                &format!("{}/{}", repo_id, target_ref),
                payload,
            )
            .await?;

        let policy = harness_core::contracts::integration::IntegrationVerificationPolicy::default();

        let outcome = self
            .services
            .integration_queue
            .run_next(
                repo_id,
                target_ref,
                &self.services.repo_root,
                &self.services.integration_root,
                &policy,
            )
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("integration run-next: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        match outcome {
            Some(o) => Ok(serde_json::json!({
                "integration_id": o.integration_id,
                "state": format!("{:?}", o.state),
                "attempt_id": o.attempt_id,
                "published": o.published,
            })),
            None => Ok(serde_json::json!({
                "status": "empty_queue",
                "message": "No queued integrations available",
            })),
        }
    }

    async fn cmd_integration_show(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let integration_id = payload
            .get("integration_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if integration_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "integration_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let id: harness_core::contracts::integration::IntegrationId = integration_id.to_string();
        let request = self
            .services
            .integration_queue
            .get(&id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("integration show: {e}"),
                    harness_core::ErrorSource::System,
                )
            })?;

        match request {
            Some(r) => Ok(serde_json::json!({
                "integration_id": r.integration_id,
                "candidate_id": r.candidate_id,
                "target_ref": r.target_ref,
                "priority": r.priority,
            })),
            None => Ok(serde_json::json!({
                "integration_id": integration_id,
                "status": "not_found",
            })),
        }
    }

    async fn cmd_integration_list(
        &self,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let requests = self
            .services
            .integration_queue
            .list_all()
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("integration list: {e}"),
                    harness_core::ErrorSource::System,
                )
            })?;

        let items: Vec<serde_json::Value> = requests
            .iter()
            .map(|r| {
                serde_json::json!({
                    "integration_id": r.integration_id,
                    "candidate_id": r.candidate_id,
                    "target_ref": r.target_ref,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "integrations": items,
            "count": items.len(),
        }))
    }

    async fn cmd_integration_cancel(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let integration_id = payload
            .get("integration_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if integration_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "integration_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let id: harness_core::contracts::integration::IntegrationId = integration_id.to_string();
        self.services
            .integration_queue
            .cancel(&id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("integration cancel: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "integration_id": integration_id,
            "status": "cancelled",
        }))
    }

    async fn cmd_integration_recover(
        &self,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let outcome = self
            .services
            .integration_recovery
            .reconcile(&self.services.repo_root, &self.services.integration_root)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("integration recover: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "scanned": outcome.scanned,
            "requeued": outcome.requeued,
            "recovered_integrated": outcome.recovered_integrated,
            "failed_attempts": outcome.failed_attempts,
            "blocked": outcome.blocked,
            "leases_closed": outcome.leases_closed,
            "worktrees_cleaned": outcome.worktrees_cleaned,
            "processes_terminated": outcome.processes_terminated,
        }))
    }

    // ── Cancellation ──────────────────────────────────────────────

    async fn cmd_cancel(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let operation_id = payload
            .get("operation_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let aggregate_id = payload
            .get("aggregate_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if operation_id.is_empty() && aggregate_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "operation_id or aggregate_id is required for cancellation",
                harness_core::ErrorSource::Harness,
            ));
        }

        if !operation_id.is_empty() {
            sqlx::query(
                r#"UPDATE operation_intents
                   SET state = 'cancelled', completed_at = datetime('now'), updated_at = datetime('now')
                   WHERE operation_id = ? AND state NOT IN ('succeeded', 'failed', 'cancelled', 'abandoned')"#,
            )
            .bind(operation_id)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("cancel operation: {e}"),
                harness_core::ErrorSource::System,
            ))?;
        }

        if !aggregate_id.is_empty() {
            sqlx::query(
                r#"UPDATE operation_intents
                   SET state = 'cancelled', completed_at = datetime('now'), updated_at = datetime('now')
                   WHERE aggregate_id = ? AND state NOT IN ('succeeded', 'failed', 'cancelled', 'abandoned')"#,
            )
            .bind(aggregate_id)
            .execute(&self.pool)
            .await
            .map_err(|e| CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("cancel aggregate operations: {e}"),
                harness_core::ErrorSource::System,
            ))?;
        }

        Ok(serde_json::json!({
            "cancelled": true,
            "operation_id": operation_id,
            "aggregate_id": aggregate_id,
        }))
    }

    // ── Goal command implementations ──────────────────────────────────

    async fn cmd_goal_create(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        // CLI sends {"goal_spec": "<json>"}. Extract or use directly.
        let goal_value = match payload.get("goal_spec") {
            Some(sv) if sv.is_string() => {
                serde_json::from_str::<serde_json::Value>(sv.as_str().unwrap())
                    .unwrap_or_else(|_| sv.clone())
            }
            Some(sv) => sv.clone(),
            None => payload.clone(),
        };
        let goal: harness_core::contracts::goal::GoalSpec = serde_json::from_value(goal_value)
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::SerializationError,
                    format!("invalid goal spec: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        self.services
            .goal_loop_service
            .create_goal(goal)
            .await
            .map(|g| {
                serde_json::json!({
                    "goal_id": g.goal_id,
                    "revision": g.revision,
                    "title": g.title,
                    "state": "draft",
                    "status": "created"
                })
            })
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("goal create: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })
    }

    async fn cmd_goal_start(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Validate profile separation before starting
        if let Err(sep_err) = self
            .services
            .goal_loop_service
            .validate_profile_separation(goal_id)
        {
            return Err(CoreError::new(
                harness_core::ErrorCode::ProfileSeparationViolation,
                sep_err.to_string(),
                harness_core::ErrorSource::Harness,
            ));
        }

        // Transition to Planning → try to plan
        self.services
            .goal_loop_service
            .transition_goal(goal_id, harness_core::contracts::goal::GoalState::Planning)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("goal start: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        let run_id = self
            .services
            .goal_loop_service
            .start_loop_run(goal_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::Internal,
                    format!("goal start loop: {e}"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        Ok(serde_json::json!({
            "goal_id": goal_id,
            "run_id": run_id,
            "status": "started"
        }))
    }

    async fn cmd_goal_show(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        match goal_repo.get_goal(goal_id).await.map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("goal show: {e}"),
                harness_core::ErrorSource::System,
            )
        })? {
            Some(g) => Ok(serde_json::json!({
                "goal_id": g.goal_id,
                "revision": g.revision,
                "title": g.title,
                "objective": g.objective,
                "repository_id": g.repository_id,
                "target_ref": g.target_ref,
                "criteria_count": g.success_criteria.len(),
                "constraints_count": g.constraints.len(),
            })),
            None => Ok(serde_json::json!({
                "goal_id": goal_id,
                "status": "not_found"
            })),
        }
    }

    async fn cmd_goal_list(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let state_filter = payload.get("state").and_then(|v| v.as_str());

        // Direct projection query: the TUI goal list needs state and
        // timestamps in addition to identity (additive presentation,
        // I8B §71 — no schema change).
        let list_err = |e: sqlx::Error| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("goal list: {e}"),
                harness_core::ErrorSource::System,
            )
        };
        type ListRow = (String, String, i64, String, String, String);
        let rows: Vec<ListRow> = if let Some(s) = state_filter {
            sqlx::query_as(
                r#"SELECT goal_id, title, revision, state, created_at, updated_at
                   FROM goals WHERE state = ? ORDER BY created_at DESC"#,
            )
            .bind(s)
            .fetch_all(&self.pool)
            .await
            .map_err(list_err)?
        } else {
            sqlx::query_as(
                r#"SELECT goal_id, title, revision, state, created_at, updated_at
                   FROM goals ORDER BY created_at DESC"#,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(list_err)?
        };

        let items: Vec<serde_json::Value> = rows
            .iter()
            .map(|g| {
                serde_json::json!({
                    "goal_id": g.0,
                    "title": g.1,
                    "revision": g.2,
                    "state": g.3,
                    "created_at": g.4,
                    "updated_at": g.5,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "goals": items,
            "count": items.len()
        }))
    }

    async fn cmd_goal_status(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        match goal_repo.get_goal(goal_id).await.map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("goal status: {e}"),
                harness_core::ErrorSource::System,
            )
        })? {
            Some(g) => {
                let plan = goal_repo.get_active_plan(goal_id).await.map_err(|e| {
                    CoreError::new(
                        harness_core::ErrorCode::PersistenceError,
                        format!("goal status plan: {e}"),
                        harness_core::ErrorSource::System,
                    )
                })?;
                Ok(serde_json::json!({
                    "goal_id": g.goal_id,
                    "revision": g.revision,
                    "title": g.title,
                    "has_active_plan": plan.is_some(),
                    "plan_revision": plan.map(|p| p.revision_number),
                }))
            }
            None => Ok(serde_json::json!({
                "goal_id": goal_id,
                "status": "not_found"
            })),
        }
    }

    async fn cmd_goal_pause(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Production interaction path: durable event + dispatch gate,
        // idempotent when the goal is already paused.
        let outcome = self.services.goal_loop_service.pause_goal(goal_id).await?;

        Ok(serde_json::json!({
            "goal_id": goal_id,
            "status": "paused",
            "applied": outcome.applied(),
        }))
    }

    async fn cmd_goal_resume(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let outcome = self.services.goal_loop_service.resume_goal(goal_id).await?;

        Ok(serde_json::json!({
            "goal_id": goal_id,
            "status": "resumed",
            "applied": outcome.applied(),
        }))
    }

    async fn cmd_goal_cancel(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        // Idempotent replay: cancelling an already-cancelled goal is a no-op.
        let state: Option<(String,)> = sqlx::query_as("SELECT state FROM goals WHERE goal_id = ?")
            .bind(goal_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("goal cancel state read: {e}"),
                    harness_core::ErrorSource::System,
                )
            })?;
        match state.as_ref().map(|(s,)| s.as_str()) {
            None => {
                return Err(CoreError::new(
                    harness_core::ErrorCode::NotFound,
                    format!("goal {goal_id} not found"),
                    harness_core::ErrorSource::Harness,
                ))
            }
            Some("cancelled") => {
                return Ok(serde_json::json!({
                    "goal_id": goal_id,
                    "status": "cancelled",
                    "applied": false,
                }))
            }
            _ => {}
        }

        self.services
            .goal_loop_service
            .transition_goal(goal_id, harness_core::contracts::goal::GoalState::Cancelled)
            .await?;

        Ok(serde_json::json!({
            "goal_id": goal_id,
            "status": "cancelled",
            "applied": true,
        }))
    }

    async fn cmd_goal_replan(
        &self,
        _payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        Ok(serde_json::json!({
            "supported": true,
            "message": "goal replan uses the GoalLoopService replanning path"
        }))
    }

    async fn cmd_goal_approvals(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        let approvals = goal_repo
            .list_pending_approvals(goal_id)
            .await
            .map_err(|e| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("goal approvals: {e}"),
                    harness_core::ErrorSource::System,
                )
            })?;

        let items: Vec<serde_json::Value> = approvals
            .iter()
            .map(|a| {
                serde_json::json!({
                    "approval_id": a.approval_id,
                    "type": a.approval_type.as_str(),
                    "state": format!("{:?}", a.state),
                    "reason": a.reason,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "approvals": items,
            "count": items.len()
        }))
    }

    async fn cmd_goal_approve(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let approval_id = payload
            .get("approval_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if approval_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "approval_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }
        let expected_plan_revision_id = payload
            .get("expected_plan_revision_id")
            .and_then(|v| v.as_str());

        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        let approval = goal_repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                harness_core::ErrorCode::NotFound,
                format!("approval {approval_id} not found"),
                harness_core::ErrorSource::Harness,
            )
        })?;

        // Plan approvals go through the stale-guarded activation path;
        // other approval kinds keep the legacy generic resolution.
        if approval.approval_type == crate::goal::ApprovalType::ApproveInitialPlan {
            let outcome = self
                .services
                .goal_loop_service
                .approve_plan(
                    &approval.goal_id,
                    approval_id,
                    "ipc-user",
                    expected_plan_revision_id,
                )
                .await?;
            Ok(serde_json::json!({
                "approval_id": approval_id,
                "goal_id": approval.goal_id,
                "plan_revision_id": approval.plan_revision_id,
                "status": "approved",
                "applied": outcome.applied(),
            }))
        } else {
            self.services
                .goal_loop_service
                .approve(approval_id, "ipc-user")
                .await?;
            Ok(serde_json::json!({
                "approval_id": approval_id,
                "goal_id": approval.goal_id,
                "status": "approved",
                "applied": true,
            }))
        }
    }

    async fn cmd_goal_reject(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let approval_id = payload
            .get("approval_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if approval_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "approval_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }
        let expected_plan_revision_id = payload
            .get("expected_plan_revision_id")
            .and_then(|v| v.as_str());

        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        let approval = goal_repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                harness_core::ErrorCode::NotFound,
                format!("approval {approval_id} not found"),
                harness_core::ErrorSource::Harness,
            )
        })?;

        if approval.approval_type == crate::goal::ApprovalType::ApproveInitialPlan {
            // Terminal reject: revision → Rejected, goal → Cancelled.
            let outcome = self
                .services
                .goal_loop_service
                .reject_plan(
                    &approval.goal_id,
                    approval_id,
                    "ipc-user",
                    expected_plan_revision_id,
                )
                .await?;
            Ok(serde_json::json!({
                "approval_id": approval_id,
                "goal_id": approval.goal_id,
                "plan_revision_id": approval.plan_revision_id,
                "status": "rejected",
                "applied": outcome.applied(),
            }))
        } else {
            self.services
                .goal_loop_service
                .reject_approval(approval_id, "ipc-user")
                .await?;
            Ok(serde_json::json!({
                "approval_id": approval_id,
                "goal_id": approval.goal_id,
                "status": "rejected",
                "applied": true,
            }))
        }
    }

    async fn cmd_goal_answer(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let approval_id = payload
            .get("approval_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if approval_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "approval_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }
        let answers = payload.get("answers").cloned().ok_or_else(|| {
            CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "answers is required",
                harness_core::ErrorSource::Harness,
            )
        })?;

        // The goal id comes from the approval itself — the client cannot
        // answer across goals.
        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        let approval = goal_repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                harness_core::ErrorCode::NotFound,
                format!("approval {approval_id} not found"),
                harness_core::ErrorSource::Harness,
            )
        })?;

        let outcome = self
            .services
            .goal_loop_service
            .answer_clarification(&approval.goal_id, approval_id, &answers, "ipc-user")
            .await?;

        Ok(serde_json::json!({
            "approval_id": approval_id,
            "goal_id": approval.goal_id,
            "status": "answered",
            "applied": outcome.applied(),
        }))
    }

    async fn cmd_goal_events(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let after: i64 = payload
            .get("after_sequence")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        // Optional long-poll: wait up to wait_ms for the next event before
        // returning empty. Capped so a client cannot pin a connection.
        let wait_ms: u64 = payload
            .get("wait_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            .min(30_000);

        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
        let events = loop {
            let batch = self.fetch_goal_events(goal_id, after).await?;
            if !batch.is_empty() || std::time::Instant::now() >= deadline {
                break batch;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };

        let count = events.len();
        let last_sequence = events.last().map(|e| e.sequence).unwrap_or(after);
        Ok(serde_json::json!({
            "goal_id": goal_id,
            "events": events,
            "count": count,
            "last_sequence": last_sequence,
        }))
    }

    /// Fetch up to 100 events after the given sequence as presentation DTOs.
    async fn fetch_goal_events(
        &self,
        goal_id: &str,
        after: i64,
    ) -> Result<Vec<pres::PresentationEvent>, CoreError> {
        let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
            r#"SELECT sequence_num, event_type, payload_json, occurred_at
               FROM goal_events WHERE goal_id = ? AND sequence_num > ?
               ORDER BY sequence_num ASC LIMIT 100"#,
        )
        .bind(goal_id)
        .bind(after)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("goal events: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(sequence, event_type, payload_json, occurred_at)| pres::PresentationEvent {
                    sequence,
                    goal_id: goal_id.to_string(),
                    event_type,
                    occurred_at,
                    payload: serde_json::from_str(&payload_json)
                        .unwrap_or(serde_json::Value::String(payload_json)),
                },
            )
            .collect())
    }

    // ── I8A interaction commands ─────────────────────────────────────

    async fn cmd_goal_request_changes(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let approval_id = payload
            .get("approval_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let feedback = payload
            .get("feedback")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if approval_id.is_empty() || feedback.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "approval_id and feedback are required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        let approval = goal_repo.get_approval(approval_id).await?.ok_or_else(|| {
            CoreError::new(
                harness_core::ErrorCode::NotFound,
                format!("approval {approval_id} not found"),
                harness_core::ErrorSource::Harness,
            )
        })?;

        let outcome = self
            .services
            .goal_loop_service
            .request_plan_changes(&approval.goal_id, approval_id, feedback, "ipc-user")
            .await?;

        Ok(serde_json::json!({
            "approval_id": approval_id,
            "goal_id": approval.goal_id,
            "plan_revision_id": approval.plan_revision_id,
            "status": "changes_requested",
            "applied": outcome.applied(),
        }))
    }

    async fn cmd_goal_intervene(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let message = payload
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() || message.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id and message are required",
                harness_core::ErrorSource::Harness,
            ));
        }
        // Threaded in by handle_request for provenance.
        let request_id = payload.get("request_id").and_then(|v| v.as_str());

        let intervention = self
            .services
            .goal_loop_service
            .record_intervention(goal_id, message, request_id, "user")
            .await?;

        Ok(serde_json::json!({
            "goal_id": goal_id,
            "intervention_id": intervention.intervention_id,
            "classification": intervention.classification.as_str(),
            "state": intervention.state.as_str(),
            "status": "recorded",
        }))
    }

    /// One-read-pass projection of the full goal state (I8A `goal.snapshot`).
    async fn cmd_goal_snapshot(
        &self,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CoreError> {
        let goal_id = payload
            .get("goal_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if goal_id.is_empty() {
            return Err(CoreError::new(
                harness_core::ErrorCode::InvalidState,
                "goal_id is required",
                harness_core::ErrorSource::Harness,
            ));
        }

        let db_err = |ctx: &str| {
            let ctx = ctx.to_string();
            move |e: sqlx::Error| {
                CoreError::new(
                    harness_core::ErrorCode::PersistenceError,
                    format!("goal snapshot {ctx}: {e}"),
                    harness_core::ErrorSource::System,
                )
            }
        };

        // Goal header (state lives in the goals table, not the spec DTO).
        type GoalRow = (
            String,
            i64,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        );
        let goal_row: Option<GoalRow> = sqlx::query_as(
            r#"SELECT goal_id, revision, title, objective, state,
               budget_json, approval_policy_json, created_at, updated_at
               FROM goals WHERE goal_id = ?"#,
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err("goal"))?;

        let (gid, revision, title, objective, state, budget_json, policy_json, created, updated) =
            goal_row.ok_or_else(|| {
                CoreError::new(
                    harness_core::ErrorCode::NotFound,
                    format!("goal {goal_id} not found"),
                    harness_core::ErrorSource::Harness,
                )
            })?;

        let goal = pres::SnapshotGoal {
            goal_id: gid,
            revision,
            title,
            objective,
            state,
            budget: serde_json::from_str(&budget_json).unwrap_or(serde_json::Value::Null),
            approval_policy: serde_json::from_str(&policy_json).unwrap_or(serde_json::Value::Null),
            created_at: created,
            updated_at: updated,
        };

        // Plan revisions: active + latest.
        let plan_row = |row: (String, i64, String)| pres::SnapshotPlan {
            plan_revision_id: row.0,
            revision_number: row.1,
            state: row.2,
        };
        let active_plan: Option<(String, i64, String)> = sqlx::query_as(
            r#"SELECT plan_revision_id, revision_number, state FROM plan_revisions
               WHERE goal_id = ? AND state = 'active'"#,
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err("active plan"))?;
        let latest_plan: Option<(String, i64, String)> = sqlx::query_as(
            r#"SELECT plan_revision_id, revision_number, state FROM plan_revisions
               WHERE goal_id = ? ORDER BY revision_number DESC LIMIT 1"#,
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err("latest plan"))?;
        let active_plan = active_plan.map(plan_row);
        let latest_plan = latest_plan.map(plan_row);

        // Tasks of the active plan (falling back to the latest revision so
        // pending approvals can render the proposed work).
        let tasks_plan_id = active_plan
            .as_ref()
            .or(latest_plan.as_ref())
            .map(|p| p.plan_revision_id.clone());
        let mut tasks: Vec<pres::SnapshotTask> = Vec::new();
        if let Some(ref plan_id) = tasks_plan_id {
            type TaskRow = (
                String,
                String,
                String,
                String,
                String,
                String,
                String,
                bool,
                String,
                Option<String>,
                Option<String>,
            );
            let rows: Vec<TaskRow> = sqlx::query_as(
                r#"SELECT planned_task_id, milestone_id, client_ref, title, state,
                   dependency_refs_json, risk_level, requires_approval,
                   expected_evidence_json, materialized_task_id, materialized_loop_id
                   FROM planned_tasks WHERE plan_revision_id = ?
                   ORDER BY client_ref ASC"#,
            )
            .bind(plan_id)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err("tasks"))?;

            // Real runtime assignments for materialized tasks (I8B §67):
            // task → current execution → runtime profile. Absent values stay
            // None ("unknown"), never fabricated.
            let materialized: Vec<&String> = rows.iter().filter_map(|r| r.9.as_ref()).collect();
            type Assignment = (Option<String>, Option<String>, Option<String>);
            let mut assignments: std::collections::HashMap<String, Assignment> =
                std::collections::HashMap::new();
            if !materialized.is_empty() {
                let placeholders = vec!["?"; materialized.len()].join(",");
                let sql = format!(
                    "SELECT t.id, NULLIF(rp.agent_kind, ''), rp.model, \
                     NULLIF(rp.provider, '') \
                     FROM tasks t \
                     LEFT JOIN execution_attempts ea ON ea.id = t.current_execution_id \
                     LEFT JOIN runtime_profiles rp ON rp.id = ea.profile_id \
                     WHERE t.id IN ({placeholders})"
                );
                let mut query = sqlx::query_as::<
                    _,
                    (String, Option<String>, Option<String>, Option<String>),
                >(&sql);
                for id in &materialized {
                    query = query.bind(id);
                }
                let assignment_rows = query
                    .fetch_all(&self.pool)
                    .await
                    .map_err(db_err("task assignments"))?;
                for (task_id, agent_kind, model, provider) in assignment_rows {
                    assignments.insert(task_id, (agent_kind, model, provider));
                }
            }

            tasks = rows
                .into_iter()
                .map(|r| {
                    let (agent_kind, model, provider) =
                        r.9.as_ref()
                            .and_then(|tid| assignments.get(tid))
                            .cloned()
                            .unwrap_or((None, None, None));
                    pres::SnapshotTask {
                        planned_task_id: r.0,
                        milestone_id: r.1,
                        client_ref: r.2,
                        title: r.3,
                        state: r.4,
                        dependencies: serde_json::from_str(&r.5).unwrap_or_default(),
                        risk_level: r.6,
                        requires_approval: r.7,
                        expected_evidence: serde_json::from_str(&r.8).unwrap_or_default(),
                        materialized_task_id: r.9,
                        materialized_loop_id: r.10,
                        agent_kind,
                        model,
                        provider,
                    }
                })
                .collect();
        }

        // Pending interactions + recent interventions via the repo.
        let goal_repo = crate::goal::repo::GoalRepo::new(self.pool.clone());
        let pending_interactions: Vec<pres::PendingInteraction> = goal_repo
            .list_pending_approvals(goal_id)
            .await?
            .into_iter()
            .map(|a| pres::PendingInteraction {
                approval_id: a.approval_id,
                kind: a.approval_type.as_str().to_string(),
                plan_revision_id: a.plan_revision_id,
                reason: a.reason,
                requested_action: a.requested_action,
                created_at: a.created_at.to_rfc3339(),
            })
            .collect();
        let interventions: Vec<pres::SnapshotIntervention> = goal_repo
            .list_interventions(goal_id, None)
            .await?
            .into_iter()
            .take(20)
            .map(|i| pres::SnapshotIntervention {
                intervention_id: i.intervention_id,
                message: i.message,
                classification: i.classification.as_str().to_string(),
                state: i.state.as_str().to_string(),
                created_at: i.created_at.to_rfc3339(),
                applied_plan_revision_id: i.applied_plan_revision_id,
            })
            .collect();

        // Active loop runs.
        let run_rows: Vec<(String, String, i64, Option<String>)> = sqlx::query_as(
            r#"SELECT run_id, state, iteration_number, plan_revision_id
               FROM goal_loop_runs WHERE goal_id = ?
               AND state NOT IN ('completed','failed','cancelled')"#,
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err("loop runs"))?;
        let running_activities = {
            // Currently executing planned task + its real runtime assignment
            // (I8B §67). Absent values stay None ("unknown").
            let current: Option<(String, Option<String>, Option<String>)> =
                match tasks_plan_id.as_ref() {
                    Some(plan_id) => sqlx::query_as(
                        r#"SELECT pt.title, NULLIF(rp.agent_kind, ''), rp.model
                           FROM planned_tasks pt
                           LEFT JOIN tasks t ON t.id = pt.materialized_task_id
                           LEFT JOIN execution_attempts ea ON ea.id = t.current_execution_id
                           LEFT JOIN runtime_profiles rp ON rp.id = ea.profile_id
                           WHERE pt.plan_revision_id = ? AND pt.state = 'running'
                           ORDER BY pt.client_ref ASC LIMIT 1"#,
                    )
                    .bind(plan_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(db_err("current task"))?,
                    None => None,
                };
            run_rows
                .into_iter()
                .map(|r| {
                    let (task_title, agent_kind, model) = current
                        .clone()
                        .map(|(t, a, m)| (Some(t), a, m))
                        .unwrap_or((None, None, None));
                    pres::RunningActivity {
                        run_id: r.0,
                        state: r.1,
                        iteration_number: r.2,
                        plan_revision_id: r.3,
                        task_title,
                        agent_kind,
                        model,
                    }
                })
                .collect()
        };

        let usage = self.project_usage_summary(goal_id).await?;

        // Resume cursor.
        let last_seq: Option<(i64,)> = sqlx::query_as(
            "SELECT COALESCE(MAX(sequence_num), 0) FROM goal_events WHERE goal_id = ?",
        )
        .bind(goal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err("event cursor"))?;

        let snapshot = pres::GoalSnapshot {
            goal,
            active_plan,
            latest_plan,
            tasks,
            pending_interactions,
            interventions,
            running_activities,
            usage,
            last_event_sequence: last_seq.map(|(s,)| s).unwrap_or(0),
        };

        serde_json::to_value(&snapshot).map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::Internal,
                format!("goal snapshot serialize: {e}"),
                harness_core::ErrorSource::System,
            )
        })
    }

    /// Boundary-only usage projection (design §2.8): AND semantics for
    /// usage_known, absent metrics stay null — never fabricated.
    async fn project_usage_summary(&self, goal_id: &str) -> Result<pres::UsageSummary, CoreError> {
        type UsageRow = (
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            String,
            bool,
        );
        let rows: Vec<UsageRow> = sqlx::query_as(
            r#"SELECT u.runtime_profile_id, u.model_identifier, u.provider_identifier,
               u.input_tokens, u.output_tokens, u.cached_input_tokens, u.tool_calls,
               u.wall_time_ms, u.estimated_cost_micros, u.usage_source, u.usage_known
               FROM task_usage_ledger u
               WHERE u.loop_id IN (
                   SELECT pt.materialized_loop_id FROM planned_tasks pt
                   JOIN plan_revisions pr ON pr.plan_revision_id = pt.plan_revision_id
                   WHERE pr.goal_id = ? AND pt.materialized_loop_id IS NOT NULL)"#,
        )
        .bind(goal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("goal snapshot usage: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;

        // Sum an optional metric: absent everywhere → None (unknown).
        fn add(total: &mut Option<i64>, v: Option<i64>) {
            if let Some(v) = v {
                *total = Some(total.unwrap_or(0) + v);
            }
        }
        fn accumulate(totals: &mut pres::UsageTotals, row: &UsageRow) {
            add(&mut totals.input_tokens, row.3);
            add(&mut totals.output_tokens, row.4);
            add(&mut totals.cached_input_tokens, row.5);
            add(&mut totals.tool_calls, row.6);
            add(&mut totals.wall_time_ms, row.7);
            add(&mut totals.estimated_cost_micros, row.8);
        }

        let mut summary = pres::UsageSummary {
            usage_known: !rows.is_empty(),
            ..Default::default()
        };
        let mut per_profile: Vec<pres::ProfileUsage> = Vec::new();
        for row in &rows {
            accumulate(&mut summary.totals, row);
            summary.usage_known &= row.10;
            if !summary.sources.contains(&row.9) {
                summary.sources.push(row.9.clone());
            }
            match per_profile
                .iter_mut()
                .find(|p| p.profile_id == row.0 && p.model == row.1 && p.provider == row.2)
            {
                Some(entry) => accumulate(&mut entry.totals, row),
                None => {
                    let mut entry = pres::ProfileUsage {
                        profile_id: row.0.clone(),
                        model: row.1.clone(),
                        provider: row.2.clone(),
                        totals: pres::UsageTotals::default(),
                    };
                    accumulate(&mut entry.totals, row);
                    per_profile.push(entry);
                }
            }
        }
        summary.per_profile = per_profile;
        Ok(summary)
    }
}
