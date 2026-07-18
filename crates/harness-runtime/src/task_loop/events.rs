//! Domain event writer for I4.5 Task Engineering Loop.
//!
//! All events are append-only, exactly-once (deterministic idempotency key),
//! and contain no secrets or full raw logs. Secret-bearing fields are
//! redacted or rejected before persistence.

use sqlx::SqlitePool;

use super::types::DecisionClassification;

/// Writes exactly-once events into the existing `event_log` table.
/// Every event uses a deterministic idempotency key so that response-lost
/// replay cannot duplicate.
pub struct TaskLoopEventWriter {
    pool: SqlitePool,
}

impl TaskLoopEventWriter {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Write one event with deterministic idempotency.
    async fn write_one(
        &self,
        loop_id: &str,
        event_type: &str,
        payload_json: &str,
    ) -> Result<bool, String> {
        let ikey = format!("tl-ev-{}-{event_type}", loop_id);
        let eid = format!("tl-{}-{}", uuid::Uuid::new_v4(), event_type);
        // Idempotency key is on the stream correlation — use INSERT OR IGNORE.
        // We insert the correlation id as the idempotency_key directly; if the
        // event_log table only has UNIQUE on idempotency_key, this is sufficient.
        // We map stream_id=loop_id, stream_version=0 (synthetic).
        let r = sqlx::query(
            "INSERT OR IGNORE INTO event_log \
             (id, stream_id, stream_version, event_type, payload_json, \
              schema_version, correlation_id, idempotency_key, source) \
             VALUES (?,?,0,?,?,1,?,?,'harness-i4-5')",
        )
        .bind(&eid)
        .bind(loop_id)
        .bind(event_type)
        .bind(payload_json)
        .bind(loop_id)
        .bind(&ikey)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("event {event_type}: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    // ── Lifecycle events ──────────────────────────────────────────

    pub async fn loop_created(
        &self,
        loop_id: &str,
        task_id: &str,
        project_id: &str,
    ) -> Result<bool, String> {
        let payload =
            serde_json::json!({"loop_id":loop_id,"task_id":task_id,"project_id":project_id});
        self.write_one(loop_id, "TaskEngineeringLoopCreated", &payload.to_string())
            .await
    }

    pub async fn loop_started(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringLoopStarted", "{}")
            .await
    }

    pub async fn loop_ownership_acquired(
        &self,
        loop_id: &str,
        owner_id: &str,
    ) -> Result<bool, String> {
        let payload = serde_json::json!({"owner_id":owner_id});
        self.write_one(
            loop_id,
            "TaskEngineeringLoopOwnershipAcquired",
            &payload.to_string(),
        )
        .await
    }

    pub async fn attempt_prepared(
        &self,
        loop_id: &str,
        attempt_id: &str,
        ordinal: i64,
    ) -> Result<bool, String> {
        let payload =
            serde_json::json!({"loop_id":loop_id,"attempt_id":attempt_id,"ordinal":ordinal});
        self.write_one(
            loop_id,
            "TaskEngineeringAttemptPrepared",
            &payload.to_string(),
        )
        .await
    }

    pub async fn attempt_created(
        &self,
        loop_id: &str,
        attempt_id: &str,
        execution_id: &str,
    ) -> Result<bool, String> {
        let payload = serde_json::json!({"loop_id":loop_id,"attempt_id":attempt_id,"execution_id":execution_id});
        self.write_one(
            loop_id,
            "TaskEngineeringAttemptCreated",
            &payload.to_string(),
        )
        .await
    }

    pub async fn attempt_dispatched(
        &self,
        loop_id: &str,
        attempt_id: &str,
    ) -> Result<bool, String> {
        let payload = serde_json::json!({"loop_id":loop_id,"attempt_id":attempt_id});
        self.write_one(
            loop_id,
            "TaskEngineeringAttemptDispatched",
            &payload.to_string(),
        )
        .await
    }

    pub async fn attempt_observed(&self, loop_id: &str, attempt_id: &str) -> Result<bool, String> {
        let payload = serde_json::json!({"loop_id":loop_id,"attempt_id":attempt_id});
        self.write_one(
            loop_id,
            "TaskEngineeringAttemptObserved",
            &payload.to_string(),
        )
        .await
    }

    pub async fn decision_recorded(
        &self,
        loop_id: &str,
        attempt_id: &str,
        classification: DecisionClassification,
    ) -> Result<bool, String> {
        let payload = serde_json::json!({
            "loop_id":loop_id,
            "attempt_id":attempt_id,
            "classification":classification.as_str()
        });
        self.write_one(
            loop_id,
            "TaskEngineeringDecisionRecorded",
            &payload.to_string(),
        )
        .await
    }

    pub async fn context_pack_created(
        &self,
        loop_id: &str,
        context_pack_id: &str,
    ) -> Result<bool, String> {
        let payload = serde_json::json!({"loop_id":loop_id,"context_pack_id":context_pack_id});
        self.write_one(
            loop_id,
            "TaskEngineeringContextPackCreated",
            &payload.to_string(),
        )
        .await
    }

    // ── Terminal events ────────────────────────────────────────────

    pub async fn complete_candidate(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringCompleteCandidate", "{}")
            .await
    }

    pub async fn reconciliation_waiting(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringAwaitingReconciliation", "{}")
            .await
    }

    pub async fn infrastructure_blocked(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringInfrastructureBlocked", "{}")
            .await
    }

    pub async fn awaiting_human(&self, loop_id: &str, reason: &str) -> Result<bool, String> {
        let payload = serde_json::json!({"reason":reason});
        self.write_one(
            loop_id,
            "TaskEngineeringAwaitingHuman",
            &payload.to_string(),
        )
        .await
    }

    pub async fn budget_exhausted(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringBudgetExhausted", "{}")
            .await
    }

    pub async fn no_progress(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringNoProgress", "{}")
            .await
    }

    pub async fn non_retryable(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringNonRetryable", "{}")
            .await
    }

    pub async fn escalated(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringEscalated", "{}")
            .await
    }

    pub async fn cancelled(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringCancelled", "{}")
            .await
    }

    pub async fn loop_failed(&self, loop_id: &str, reason: &str) -> Result<bool, String> {
        let payload = serde_json::json!({"reason":reason});
        self.write_one(loop_id, "TaskEngineeringLoopFailed", &payload.to_string())
            .await
    }

    // ── Reconciler events ─────────────────────────────────────────

    pub async fn reconciliation_started(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringLoopReconciliationStarted", "{}")
            .await
    }

    pub async fn reconciliation_completed(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringLoopReconciliationCompleted", "{}")
            .await
    }

    pub async fn reconciliation_blocked(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringLoopReconciliationBlocked", "{}")
            .await
    }

    pub async fn profile_selected(&self, loop_id: &str, profile_id: &str) -> Result<bool, String> {
        let payload = serde_json::json!({"profile_id":profile_id});
        self.write_one(
            loop_id,
            "TaskEngineeringProfileSelected",
            &payload.to_string(),
        )
        .await
    }

    pub async fn profile_changed(
        &self,
        loop_id: &str,
        from_profile: &str,
        to_profile: &str,
        reason: &str,
    ) -> Result<bool, String> {
        let payload = serde_json::json!({
            "from_profile":from_profile,
            "to_profile":to_profile,
            "reason":reason
        });
        self.write_one(
            loop_id,
            "TaskEngineeringProfileChanged",
            &payload.to_string(),
        )
        .await
    }

    pub async fn budget_reserved(&self, loop_id: &str, attempt_id: &str) -> Result<bool, String> {
        let payload = serde_json::json!({"loop_id":loop_id,"attempt_id":attempt_id});
        self.write_one(
            loop_id,
            "TaskEngineeringBudgetReserved",
            &payload.to_string(),
        )
        .await
    }

    pub async fn usage_recorded(&self, loop_id: &str, attempt_id: &str) -> Result<bool, String> {
        let payload = serde_json::json!({"loop_id":loop_id,"attempt_id":attempt_id});
        self.write_one(
            loop_id,
            "TaskEngineeringUsageRecorded",
            &payload.to_string(),
        )
        .await
    }

    pub async fn cancellation_requested(&self, loop_id: &str) -> Result<bool, String> {
        self.write_one(loop_id, "TaskEngineeringCancellationRequested", "{}")
            .await
    }
}
