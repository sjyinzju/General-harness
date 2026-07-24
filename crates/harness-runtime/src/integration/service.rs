//! IntegrationQueueService — enqueue, dequeue, run, and publish integrations.
//!
//! I5.2/I5.3: Durable integration queue with lease/fencing, sandboxed integration,
//! verification, and atomic git update-ref publish.

use chrono::Utc;
use harness_core::contracts::integration::{
    IntegrationAttempt, IntegrationId, IntegrationRequest, IntegrationState,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;
use uuid::Uuid;

use super::repo::IntegrationRepo;

pub struct IntegrationQueueService {
    pool: SqlitePool,
    integration_repo: IntegrationRepo,
}

impl IntegrationQueueService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            integration_repo: IntegrationRepo::new(pool.clone()),
            pool,
        }
    }

    // ── Enqueue ──────────────────────────────────────────────────────

    /// Enqueue an integration request for a commit candidate.
    /// Idempotent: same candidate + repo + target_ref returns existing request.
    pub async fn enqueue(
        &self,
        integration_id: &IntegrationId,
        commit_request_id: &str,
        candidate_id: &str,
        review_id: &str,
        repository_id: &str,
        target_ref: &str,
        expected_target_head: &str,
        priority: i32,
    ) -> Result<IntegrationRequest, CoreError> {
        // Validate target ref
        IntegrationRequest::validate_target_ref(target_ref).map_err(|e| {
            CoreError::new(ErrorCode::InvalidState, e, ErrorSource::System)
        })?;

        // Idempotency: check existing
        let ikey = format!("integrate-{}-{}-{}", candidate_id, repository_id, target_ref);
        if let Some(existing) = self
            .integration_repo
            .find_by_idempotency_key(&ikey)
            .await?
        {
            return Ok(existing);
        }

        // Check for active request in same scope
        if let Some(active) = self
            .integration_repo
            .find_active_by_scope(candidate_id, repository_id, target_ref)
            .await?
        {
            return Ok(active);
        }

        let req = IntegrationRequest {
            integration_id: integration_id.clone(),
            commit_request_id: commit_request_id.into(),
            candidate_id: candidate_id.into(),
            review_id: review_id.into(),
            repository_id: repository_id.into(),
            target_ref: target_ref.into(),
            expected_target_head: expected_target_head.into(),
            priority,
            idempotency_key: ikey,
            created_at: Utc::now(),
        };

        self.integration_repo.insert_request(&req).await?;

        self.emit_event(integration_id, None, "IntegrationEnqueued", "{}")
            .await;

        Ok(req)
    }

    /// Dequeue the next integration for a (repo, target_ref) scope.
    /// Returns the highest-priority, earliest-created queued request.
    /// Transitions it to WaitingForLease.
    pub async fn dequeue(
        &self,
        repository_id: &str,
        target_ref: &str,
    ) -> Result<Option<IntegrationRequest>, CoreError> {
        let queued = self
            .integration_repo
            .list_queued_for_scope(repository_id, target_ref, 1)
            .await?;

        if queued.is_empty() {
            return Ok(None);
        }

        let req = &queued[0];

        // CAS: Queued → WaitingForLease
        let ok = self
            .integration_repo
            .transition_state(
                &req.integration_id,
                &IntegrationState::Queued,
                &IntegrationState::WaitingForLease,
            )
            .await?;

        if ok {
            self.emit_event(
                &req.integration_id,
                None,
                "IntegrationDequeued",
                &serde_json::json!({"repository_id": repository_id, "target_ref": target_ref}).to_string(),
            )
            .await;
            Ok(Some(req.clone()))
        } else {
            // Lost the race — another dequeue won
            Ok(None)
        }
    }

    /// Start a new integration attempt. Transitions from WaitingForLease → Preparing.
    pub async fn start_attempt(
        &self,
        integration_id: &IntegrationId,
        target_head: &str,
        commit_oid: &str,
        parent_oid: &str,
    ) -> Result<IntegrationAttempt, CoreError> {
        let attempt_count = self
            .integration_repo
            .count_attempts(integration_id)
            .await?;
        let attempt_number = attempt_count + 1;
        let attempt_id = format!("iatt-{}", Uuid::new_v4());

        let attempt = IntegrationAttempt {
            attempt_id: attempt_id.clone(),
            integration_id: integration_id.clone(),
            attempt_number,
            state: IntegrationState::Preparing,
            commit_oid: commit_oid.into(),
            parent_oid: parent_oid.into(),
            target_head_at_start: target_head.into(),
            integration_tree_oid: None,
            integration_commit_oid: None,
            lease_id: None,
            fencing_token: None,
            started_at: Some(Utc::now()),
            completed_at: None,
            created_at: Utc::now(),
        };

        self.integration_repo.insert_attempt(&attempt).await?;

        self.emit_event(
            integration_id,
            Some(&attempt_id),
            "IntegrationStarted",
            &serde_json::json!({"attempt_number": attempt_number}).to_string(),
        )
        .await;

        Ok(attempt)
    }

    /// Cancel an integration request. Only allowed from non-terminal states.
    pub async fn cancel(&self, integration_id: &IntegrationId) -> Result<bool, CoreError> {
        // Get current state from DB
        let state_str = self
            .integration_repo
            .get_state(integration_id)
            .await?
            .ok_or_else(|| {
                CoreError::new(
                    ErrorCode::NotFound,
                    format!("Integration not found: {integration_id}"),
                    ErrorSource::System,
                )
            })?;

        let current_state = parse_integration_state(&state_str);
        if current_state.is_terminal() {
            return Err(CoreError::new(
                ErrorCode::InvalidState,
                format!("Cannot cancel terminal integration: {}", state_str),
                ErrorSource::System,
            ));
        }

        let ok = self
            .integration_repo
            .transition_state(integration_id, &current_state, &IntegrationState::Cancelled)
            .await?;

        if ok {
            self.emit_event(integration_id, None, "IntegrationCancelled", "{}")
                .await;
        }

        Ok(ok)
    }

    /// Get an integration request by ID.
    pub async fn get(&self, integration_id: &IntegrationId) -> Result<Option<IntegrationRequest>, CoreError> {
        self.integration_repo.get_request(integration_id).await
    }

    /// List all integration requests.
    pub async fn list_all(&self) -> Result<Vec<IntegrationRequest>, CoreError> {
        // Default listing — limit to 100 most recent
        let rows: Vec<ListRow> = sqlx::query_as(
            "SELECT integration_id, commit_request_id, candidate_id, review_id, repository_id, target_ref, expected_target_head, priority, state, idempotency_key, created_at FROM integration_requests ORDER BY created_at DESC LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        Ok(rows.into_iter().map(|r| IntegrationRequest {
            integration_id: r.integration_id,
            commit_request_id: r.commit_request_id,
            candidate_id: r.candidate_id,
            review_id: r.review_id,
            repository_id: r.repository_id,
            target_ref: r.target_ref,
            expected_target_head: r.expected_target_head,
            priority: r.priority as i32,
            idempotency_key: r.idempotency_key,
            created_at: parse_dt(&r.created_at),
        }).collect())
    }

    // ── Events ────────────────────────────────────────────────────────

    async fn emit_event(&self, integration_id: &str, attempt_id: Option<&str>, event_type: &str, payload_json: &str) {
        let event_id = format!("evt-{}", Uuid::new_v4());
        let _ = self
            .integration_repo
            .write_event(&event_id, integration_id, attempt_id, event_type, payload_json)
            .await;
    }
}

fn parse_dt(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .ok()
        .and_then(|dt| dt.and_utc().into())
        .unwrap_or_else(chrono::Utc::now)
}

#[derive(sqlx::FromRow)]
struct ListRow {
    integration_id: String,
    commit_request_id: String,
    candidate_id: String,
    review_id: String,
    repository_id: String,
    target_ref: String,
    expected_target_head: String,
    priority: i64,
    #[allow(dead_code)]
    state: String,
    idempotency_key: String,
    created_at: String,
}

fn parse_integration_state(s: &str) -> IntegrationState {
    match s {
        "queued" => IntegrationState::Queued,
        "waiting_for_lease" => IntegrationState::WaitingForLease,
        "preparing" => IntegrationState::Preparing,
        "applying" => IntegrationState::Applying,
        "verifying" => IntegrationState::Verifying,
        "ready_to_publish" => IntegrationState::ReadyToPublish,
        "integrated" => IntegrationState::Integrated,
        "conflict" => IntegrationState::Conflict,
        "blocked" => IntegrationState::Blocked,
        "failed" => IntegrationState::Failed,
        "cancelled" => IntegrationState::Cancelled,
        "stale" => IntegrationState::Stale,
        _ => IntegrationState::Queued,
    }
}
