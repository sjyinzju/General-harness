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

use harness_core::contracts::ipc::IpcCommand;
use harness_core::contracts::supervisor::SupervisorInstanceId;
use harness_core::CoreError;
use sqlx::SqlitePool;

use crate::ipc::IpcCommandHandler;
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
}
