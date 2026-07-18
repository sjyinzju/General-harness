//! TaskLoopRepo — persistence for I4.5 task engineering loops.
//!
//! All idempotent entry points use `INSERT ... ON CONFLICT DO NOTHING` or
//! version-CAS UPDATE to guarantee exactly-one winner semantics.
//! Ownership is acquired via version+fencing CAS; losers re-read durable
//! state and never execute side effects.
//!
//! Queries with >16 columns use `sqlx::Row::get` instead of `query_as`
//! because sqlx FromRow is capped at 16-element tuples.
//!

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqlx::{Row, SqlitePool};

use super::faults::{FaultBoundary, FaultKind, FaultPlan};
use super::types::*;

/// Helper: extract a loop row from a sqlx Row.
fn row_to_loop(r: &sqlx::sqlite::SqliteRow) -> TaskLoopRow {
    TaskLoopRow {
        loop_id: r.get("loop_id"),
        project_id: r.get("project_id"),
        task_id: r.get("task_id"),
        lifecycle: LoopLifecycle::parse(r.get("lifecycle")),
        policy_json: r.get("policy_json"),
        policy_fingerprint: r.get("policy_fingerprint"),
        idempotency_key: r.get("idempotency_key"),
        request_hash: r.get("request_hash"),
        owner_id: r.get("owner_id"),
        fencing_token: r.get("fencing_token"),
        lease_expires_at: r.get("lease_expires_at"),
        active_attempt_id: r.get("active_attempt_id"),
        current_attempt_ordinal: r.get("current_attempt_ordinal"),
        attempt_count: r.get("attempt_count"),
        no_progress_streak: r.get("no_progress_streak"),
        same_failure_streak: r.get("same_failure_streak"),
        profile_switch_count: r.get("profile_switch_count"),
        started_at: r.get("started_at"),
        updated_at: r.get("updated_at"),
        terminal_at: r.get("terminal_at"),
        last_error_classification: r.get("last_error_classification"),
        version: r.get("version"),
    }
}

/// Helper: extract an attempt row from a sqlx Row.
fn row_to_attempt(r: &sqlx::sqlite::SqliteRow) -> TaskAttemptRow {
    TaskAttemptRow {
        attempt_id: r.get("attempt_id"),
        loop_id: r.get("loop_id"),
        ordinal: r.get("ordinal"),
        parent_attempt_id: r.get("parent_attempt_id"),
        execution_id: r.get("execution_id"),
        verification_run_id: r.get("verification_run_id"),
        context_pack_id: r.get("context_pack_id"),
        runtime_profile_id: r.get("runtime_profile_id"),
        workspace_source_kind: WorkspaceSourceKind::parse(r.get("workspace_source_kind")),
        source_execution_id: r.get("source_execution_id"),
        source_worktree_id: r.get("source_worktree_id"),
        source_baseline_commit: r.get("source_baseline_commit"),
        source_head: r.get("source_head"),
        source_diff_fingerprint: r.get("source_diff_fingerprint"),
        lifecycle: AttemptLifecycle::parse(r.get("lifecycle")),
        outcome_kind: r.get("outcome_kind"),
        outcome_fingerprint: r.get("outcome_fingerprint"),
        dossier_fingerprint: r.get("dossier_fingerprint"),
        decision_id: r.get("decision_id"),
        started_at: r.get("started_at"),
        terminal_at: r.get("terminal_at"),
        version: r.get("version"),
    }
}

const LOOP_COLS: &str = "\
    loop_id, project_id, task_id, lifecycle, policy_json, policy_fingerprint, \
    idempotency_key, request_hash, owner_id, fencing_token, \
    lease_expires_at, active_attempt_id, current_attempt_ordinal, \
    attempt_count, no_progress_streak, same_failure_streak, \
    profile_switch_count, started_at, updated_at, terminal_at, \
    last_error_classification, version";

const ATTEMPT_COLS: &str = "\
    attempt_id, loop_id, ordinal, parent_attempt_id, execution_id, \
    verification_run_id, context_pack_id, runtime_profile_id, \
    workspace_source_kind, source_execution_id, source_worktree_id, \
    source_baseline_commit, source_head, source_diff_fingerprint, \
    lifecycle, outcome_kind, outcome_fingerprint, dossier_fingerprint, \
    decision_id, started_at, terminal_at, version";

pub struct TaskLoopRepo {
    pool: SqlitePool,
    /// Optional fault plan for injecting faults at repo-level boundaries.
    pub fault_plan: Option<Arc<FaultPlan>>,
    fault_call_counts: Arc<Mutex<HashMap<FaultBoundary, u64>>>,
}

#[allow(clippy::too_many_arguments)]
impl TaskLoopRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            fault_plan: None,
            fault_call_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Wire a fault plan for fault injection testing.
    pub fn with_fault_plan(mut self, fp: Arc<FaultPlan>) -> Self {
        self.fault_plan = Some(fp);
        self
    }

    fn check_fault(&self, boundary: FaultBoundary) -> Option<FaultKind> {
        let fp = self.fault_plan.as_ref()?;
        let mut counts = self.fault_call_counts.lock().unwrap();
        let call_count = counts.entry(boundary).or_insert(0);
        fp.check(boundary, call_count)
    }

    // ── Loop CRUD ──────────────────────────────────────────────────

    /// Atomically create a loop or detect duplicate/conflict.
    pub async fn create_loop(&self, req: &CreateLoopRequest) -> Result<CreateLoopOutcome, String> {
        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT loop_id, request_hash FROM task_engineering_loops WHERE idempotency_key=?",
        )
        .bind(&req.idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("check existing loop: {e}"))?;

        if let Some((lid, eh)) = existing {
            if eh == req.request_hash {
                return Ok(CreateLoopOutcome::Duplicate { loop_id: lid });
            }
            return Ok(CreateLoopOutcome::IdempotencyConflict { existing_hash: eh });
        }

        let other: Option<(String,)> = sqlx::query_as(
            "SELECT loop_id FROM task_engineering_loops \
             WHERE task_id=? AND lifecycle NOT IN (\
              'complete_candidate','budget_exhausted','no_progress',\
              'non_retryable','escalated','cancelled','failed')",
        )
        .bind(&req.task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("check active loop: {e}"))?;
        if let Some((other_id,)) = other {
            return Ok(CreateLoopOutcome::TaskAlreadyHasActiveLoop {
                existing_loop_id: other_id,
            });
        }

        let loop_id = format!("tl-{}", uuid::Uuid::new_v4());
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let expires = (chrono::Utc::now() + chrono::Duration::seconds(req.lease_secs as i64))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        let r = sqlx::query(
            "INSERT INTO task_engineering_loops \
             (loop_id,project_id,task_id,lifecycle,policy_json,policy_fingerprint,\
              idempotency_key,request_hash,owner_id,fencing_token,\
              lease_expires_at,updated_at,version) \
             VALUES (?,?,?,'created',?,?,?,?,?,1,?,?,1) \
             ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(&loop_id)
        .bind(&req.project_id)
        .bind(&req.task_id)
        .bind(&req.policy_json)
        .bind(&req.policy_fingerprint)
        .bind(&req.idempotency_key)
        .bind(&req.request_hash)
        .bind(&req.owner_id)
        .bind(&expires)
        .bind(&now)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("insert loop: {e}"))?;

        if r.rows_affected() == 0 {
            let w: Option<(String, String)> = sqlx::query_as(
                "SELECT loop_id, request_hash FROM task_engineering_loops WHERE idempotency_key=?",
            )
            .bind(&req.idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("reread: {e}"))?;
            match w {
                Some((lid, eh)) if eh == req.request_hash => {
                    Ok(CreateLoopOutcome::Duplicate { loop_id: lid })
                }
                Some((_, eh)) => Ok(CreateLoopOutcome::IdempotencyConflict { existing_hash: eh }),
                None => Ok(CreateLoopOutcome::InfrastructureError {
                    reason: "loop row vanished after conflict".into(),
                }),
            }
        } else {
            Ok(CreateLoopOutcome::Created { loop_id })
        }
    }

    /// Load a loop row by id (>16 cols, uses manual Row extraction).
    pub async fn load_loop(&self, loop_id: &str) -> Result<Option<TaskLoopRow>, String> {
        let sql = format!("SELECT {LOOP_COLS} FROM task_engineering_loops WHERE loop_id=?");
        let row = sqlx::query(&sql)
            .bind(loop_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("load loop: {e}"))?;
        Ok(row.as_ref().map(row_to_loop))
    }

    /// Load the active loop for a task.
    pub async fn load_active_for_task(&self, task_id: &str) -> Result<Option<TaskLoopRow>, String> {
        let sql = format!(
            "SELECT {LOOP_COLS} FROM task_engineering_loops \
             WHERE task_id=? AND lifecycle NOT IN (\
              'complete_candidate','budget_exhausted','no_progress',\
              'non_retryable','escalated','cancelled','failed')"
        );
        let row = sqlx::query(&sql)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("load active loop: {e}"))?;
        Ok(row.as_ref().map(row_to_loop))
    }

    // ── Loop ownership ────────────────────────────────────────────

    /// Acquire or renew ownership. Only current owner with matching fencing
    /// may renew. Stale-lease takeover uses `takeover_ownership`.
    pub async fn acquire_ownership(
        &self,
        loop_id: &str,
        expected_version: i64,
        expected_fencing: i64,
        owner_id: &str,
        lease_secs: u32,
    ) -> Result<Option<i64>, String> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let expires = (chrono::Utc::now() + chrono::Duration::seconds(lease_secs as i64))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let r = sqlx::query(
            "UPDATE task_engineering_loops \
             SET lease_expires_at=?, version=version+1, updated_at=? \
             WHERE loop_id=? AND version=? AND owner_id=? AND fencing_token=? \
               AND lifecycle NOT IN (\
                'complete_candidate','budget_exhausted','no_progress',\
                'non_retryable','escalated','cancelled','failed')",
        )
        .bind(&expires)
        .bind(&now)
        .bind(loop_id)
        .bind(expected_version)
        .bind(owner_id)
        .bind(expected_fencing)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("acquire ownership: {e}"))?;
        if r.rows_affected() == 1 {
            Ok(Some(expected_version + 1))
        } else {
            Ok(None)
        }
    }

    /// Take over a loop whose lease has expired. Version-CAS ensures exactly
    /// one winner; fencing monotonically increases so old owner writes fail.
    pub async fn takeover_ownership(
        &self,
        loop_id: &str,
        expected_version: i64,
        new_owner_id: &str,
        lease_secs: u32,
    ) -> Result<Option<i64>, String> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let expires = (chrono::Utc::now() + chrono::Duration::seconds(lease_secs as i64))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let r = sqlx::query(
            "UPDATE task_engineering_loops \
             SET owner_id=?, fencing_token=fencing_token+1, \
                 lease_expires_at=?, version=version+1, updated_at=? \
             WHERE loop_id=? AND version=? \
               AND lease_expires_at < datetime('now') \
               AND lifecycle NOT IN (\
                'complete_candidate','budget_exhausted','no_progress',\
                'non_retryable','escalated','cancelled','failed')",
        )
        .bind(new_owner_id)
        .bind(&expires)
        .bind(&now)
        .bind(loop_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("takeover ownership: {e}"))?;
        if r.rows_affected() == 1 {
            Ok(Some(expected_version + 1))
        } else {
            Ok(None)
        }
    }

    /// CAS loop lifecycle transition.
    pub async fn transition_loop(
        &self,
        loop_id: &str,
        expected_version: i64,
        expected_fencing: i64,
        owner_id: &str,
        new_lifecycle: LoopLifecycle,
        error_classification: Option<&str>,
    ) -> Result<Option<i64>, String> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let terminal_at = if new_lifecycle.is_terminal() {
            Some(now.clone())
        } else {
            None
        };

        let r = if let Some(ta) = &terminal_at {
            sqlx::query(
                "UPDATE task_engineering_loops \
                 SET lifecycle=?, terminal_at=?, last_error_classification=?,\
                     version=version+1, updated_at=? \
                 WHERE loop_id=? AND version=? AND fencing_token=? AND owner_id=?",
            )
            .bind(new_lifecycle.as_str())
            .bind(ta)
            .bind(error_classification)
            .bind(&now)
            .bind(loop_id)
            .bind(expected_version)
            .bind(expected_fencing)
            .bind(owner_id)
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(
                "UPDATE task_engineering_loops \
                 SET lifecycle=?, last_error_classification=?,\
                     version=version+1, updated_at=? \
                 WHERE loop_id=? AND version=? AND fencing_token=? AND owner_id=?",
            )
            .bind(new_lifecycle.as_str())
            .bind(error_classification)
            .bind(&now)
            .bind(loop_id)
            .bind(expected_version)
            .bind(expected_fencing)
            .bind(owner_id)
            .execute(&self.pool)
            .await
        }
        .map_err(|e| format!("transition loop: {e}"))?;

        if r.rows_affected() == 1 {
            Ok(Some(expected_version + 1))
        } else {
            Ok(None)
        }
    }

    pub async fn update_loop_counters(
        &self,
        loop_id: &str,
        expected_version: i64,
        active_attempt_id: &str,
        attempt_count: i64,
        no_progress_streak: i64,
        same_failure_streak: i64,
        current_attempt_ordinal: i64,
    ) -> Result<bool, String> {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let r = sqlx::query(
            "UPDATE task_engineering_loops \
             SET active_attempt_id=?, attempt_count=?, no_progress_streak=?,\
                 same_failure_streak=?, current_attempt_ordinal=?,\
                 version=version+1, updated_at=? \
             WHERE loop_id=? AND version=?",
        )
        .bind(active_attempt_id)
        .bind(attempt_count)
        .bind(no_progress_streak)
        .bind(same_failure_streak)
        .bind(current_attempt_ordinal)
        .bind(&now)
        .bind(loop_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("update counters: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    // ── Attempt persistence ────────────────────────────────────────

    pub async fn insert_attempt(
        &self,
        attempt_id: &str,
        loop_id: &str,
        ordinal: i64,
        parent_attempt_id: Option<&str>,
        context_pack_id: Option<&str>,
        runtime_profile_id: &str,
        workspace_source_kind: WorkspaceSourceKind,
        source_execution_id: Option<&str>,
        source_worktree_id: Option<&str>,
        source_baseline_commit: Option<&str>,
        source_head: Option<&str>,
        source_diff_fingerprint: Option<&str>,
    ) -> Result<bool, String> {
        let r = sqlx::query(
            "INSERT INTO task_engineering_attempts \
             (attempt_id,loop_id,ordinal,parent_attempt_id,context_pack_id,\
              runtime_profile_id,workspace_source_kind,source_execution_id,\
              source_worktree_id,source_baseline_commit,source_head,\
              source_diff_fingerprint,lifecycle) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'created') \
             ON CONFLICT(loop_id,ordinal) DO NOTHING",
        )
        .bind(attempt_id)
        .bind(loop_id)
        .bind(ordinal)
        .bind(parent_attempt_id)
        .bind(context_pack_id)
        .bind(runtime_profile_id)
        .bind(workspace_source_kind.as_str())
        .bind(source_execution_id)
        .bind(source_worktree_id)
        .bind(source_baseline_commit)
        .bind(source_head)
        .bind(source_diff_fingerprint)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("insert attempt: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    /// Load an attempt by id (>16 cols, uses manual Row extraction).
    pub async fn load_attempt(&self, attempt_id: &str) -> Result<Option<TaskAttemptRow>, String> {
        let sql =
            format!("SELECT {ATTEMPT_COLS} FROM task_engineering_attempts WHERE attempt_id=?");
        let row = sqlx::query(&sql)
            .bind(attempt_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("load attempt: {e}"))?;
        Ok(row.as_ref().map(row_to_attempt))
    }

    /// Load the active (non-terminal) attempt for a loop.
    pub async fn load_active_attempt(
        &self,
        loop_id: &str,
    ) -> Result<Option<TaskAttemptRow>, String> {
        let sql = format!(
            "SELECT {ATTEMPT_COLS} FROM task_engineering_attempts \
             WHERE loop_id=? AND lifecycle NOT IN ('terminal','cancelled','failed')"
        );
        let row = sqlx::query(&sql)
            .bind(loop_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("load active attempt: {e}"))?;
        Ok(row.as_ref().map(row_to_attempt))
    }

    pub async fn bind_execution(
        &self,
        attempt_id: &str,
        expected_version: i64,
        execution_id: &str,
        new_lifecycle: AttemptLifecycle,
    ) -> Result<bool, String> {
        let r = sqlx::query(
            "UPDATE task_engineering_attempts \
             SET execution_id=?, lifecycle=?, started_at=datetime('now'), version=version+1 \
             WHERE attempt_id=? AND version=?",
        )
        .bind(execution_id)
        .bind(new_lifecycle.as_str())
        .bind(attempt_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("bind execution: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn terminal_attempt(
        &self,
        attempt_id: &str,
        expected_version: i64,
        verification_run_id: &str,
        outcome_kind: &str,
        outcome_fingerprint: &str,
        dossier_fingerprint: &str,
        decision_id: &str,
    ) -> Result<bool, String> {
        let r = sqlx::query(
            "UPDATE task_engineering_attempts \
             SET lifecycle='terminal', verification_run_id=?, outcome_kind=?,\
                 outcome_fingerprint=?, dossier_fingerprint=?, decision_id=?,\
                 terminal_at=datetime('now'), version=version+1 \
             WHERE attempt_id=? AND version=?",
        )
        .bind(verification_run_id)
        .bind(outcome_kind)
        .bind(outcome_fingerprint)
        .bind(dossier_fingerprint)
        .bind(decision_id)
        .bind(attempt_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("terminal attempt: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn cancel_attempt(
        &self,
        attempt_id: &str,
        expected_version: i64,
    ) -> Result<bool, String> {
        let r = sqlx::query(
            "UPDATE task_engineering_attempts \
             SET lifecycle='cancelled', terminal_at=datetime('now'), version=version+1 \
             WHERE attempt_id=? AND version=?",
        )
        .bind(attempt_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("cancel attempt: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    // ── Decision persistence ──────────────────────────────────────

    pub async fn insert_decision(
        &self,
        decision_id: &str,
        loop_id: &str,
        attempt_id: &str,
        classification: DecisionClassification,
        reason_codes_json: &str,
        observed_state_fingerprint: &str,
        outcome_fingerprint: &str,
        dossier_fingerprint: &str,
        progress_fingerprint: &str,
        budget_snapshot_fingerprint: &str,
        selected_profile_id: Option<&str>,
        next_context_pack_id: Option<&str>,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<bool, String> {
        // Fault: DecisionInsert before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::DecisionInsert)
        {
            return Err("fault: DecisionInsert before effect".into());
        }
        let r = sqlx::query(
            "INSERT INTO task_attempt_decisions \
             (decision_id,loop_id,attempt_id,classification,action,\
              reason_codes_json,observed_state_fingerprint,outcome_fingerprint,\
              dossier_fingerprint,progress_fingerprint,budget_snapshot_fingerprint,\
              selected_profile_id,next_context_pack_id,idempotency_key,request_hash) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(decision_id)
        .bind(loop_id)
        .bind(attempt_id)
        .bind(classification.as_str())
        .bind(classification.action())
        .bind(reason_codes_json)
        .bind(observed_state_fingerprint)
        .bind(outcome_fingerprint)
        .bind(dossier_fingerprint)
        .bind(progress_fingerprint)
        .bind(budget_snapshot_fingerprint)
        .bind(selected_profile_id)
        .bind(next_context_pack_id)
        .bind(idempotency_key)
        .bind(request_hash)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("insert decision: {e}"))?;
        // Fault: DecisionInsert response lost (after durable write)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::DecisionInsert)
        {
            return Err("fault: DecisionInsert response lost".into());
        }
        Ok(r.rows_affected() == 1)
    }

    pub async fn load_decision_by_ikey(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<DecisionRow>, String> {
        let row = sqlx::query(
            "SELECT decision_id,loop_id,attempt_id,classification,action,\
                    reason_codes_json,observed_state_fingerprint,outcome_fingerprint,\
                    dossier_fingerprint,progress_fingerprint,budget_snapshot_fingerprint,\
                    selected_profile_id,next_context_pack_id,idempotency_key,\
                    request_hash,created_at \
             FROM task_attempt_decisions WHERE idempotency_key=?",
        )
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("load decision: {e}"))?;
        Ok(row.map(|r| DecisionRow {
            decision_id: r.get("decision_id"),
            loop_id: r.get("loop_id"),
            attempt_id: r.get("attempt_id"),
            classification: DecisionClassification::parse(r.get("classification"))
                .unwrap_or(DecisionClassification::AwaitingHuman),
            action: r.get("action"),
            reason_codes_json: r.get("reason_codes_json"),
            observed_state_fingerprint: r.get("observed_state_fingerprint"),
            outcome_fingerprint: r.get("outcome_fingerprint"),
            dossier_fingerprint: r.get("dossier_fingerprint"),
            progress_fingerprint: r.get("progress_fingerprint"),
            budget_snapshot_fingerprint: r.get("budget_snapshot_fingerprint"),
            selected_profile_id: r.get("selected_profile_id"),
            next_context_pack_id: r.get("next_context_pack_id"),
            idempotency_key: r.get("idempotency_key"),
            request_hash: r.get("request_hash"),
            created_at: r.get("created_at"),
        }))
    }

    pub async fn decision_exists_for_attempt(&self, attempt_id: &str) -> Result<bool, String> {
        let n: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM task_attempt_decisions WHERE attempt_id=?")
                .bind(attempt_id)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("check decision: {e}"))?;
        Ok(n.0 > 0)
    }

    // ── Context pack persistence ──────────────────────────────────

    /// Insert an immutable context pack. Idempotent: the same
    /// (loop_id, target_attempt_ordinal) or the same context_fingerprint
    /// will hit a UNIQUE constraint and DO NOTHING, returning Ok(false)
    /// so callers can re-read the existing row. Never returns a bare
    /// UNIQUE error.
    pub async fn insert_context_pack(
        &self,
        context_pack_id: &str,
        loop_id: &str,
        source_attempt_id: Option<&str>,
        target_attempt_ordinal: i64,
        payload_json: &str,
        source_fingerprints_json: &str,
        context_fingerprint: &str,
        estimated_input_tokens: Option<i64>,
        validation_status: &str,
    ) -> Result<bool, String> {
        // Fault: ContextPackInsert before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::ContextPackInsert)
        {
            return Err("fault: ContextPackInsert before effect".into());
        }
        let r = sqlx::query(
            "INSERT INTO task_context_packs \
             (context_pack_id,loop_id,source_attempt_id,target_attempt_ordinal,\
              payload_json,source_fingerprints_json,context_fingerprint,\
              estimated_input_tokens,validation_status) \
             VALUES (?,?,?,?,?,?,?,?,?) \
             ON CONFLICT DO NOTHING",
        )
        .bind(context_pack_id)
        .bind(loop_id)
        .bind(source_attempt_id)
        .bind(target_attempt_ordinal)
        .bind(payload_json)
        .bind(source_fingerprints_json)
        .bind(context_fingerprint)
        .bind(estimated_input_tokens)
        .bind(validation_status)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("insert context pack: {e}"))?;
        // Fault: ContextPackInsert response lost (after durable write)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::ContextPackInsert)
        {
            return Err("fault: ContextPackInsert response lost".into());
        }
        Ok(r.rows_affected() == 1)
    }

    /// Load a Context Pack by idempotent (loop, ordinal) key.
    pub async fn load_context_pack_by_ordinal(
        &self,
        loop_id: &str,
        ordinal: i64,
    ) -> Result<Option<ContextPackRow>, String> {
        let row = sqlx::query(
            "SELECT context_pack_id,loop_id,source_attempt_id,target_attempt_ordinal,\
                    schema_version,payload_json,source_fingerprints_json,\
                    context_fingerprint,estimated_input_tokens,validation_status,\
                    created_at \
             FROM task_context_packs WHERE loop_id=? AND target_attempt_ordinal=?",
        )
        .bind(loop_id)
        .bind(ordinal)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("load cp by ordinal: {e}"))?;
        Ok(row.map(|r| ContextPackRow {
            context_pack_id: r.get("context_pack_id"),
            loop_id: r.get("loop_id"),
            source_attempt_id: r.get("source_attempt_id"),
            target_attempt_ordinal: r.get("target_attempt_ordinal"),
            schema_version: r.get("schema_version"),
            payload_json: r.get("payload_json"),
            source_fingerprints_json: r.get("source_fingerprints_json"),
            context_fingerprint: r.get("context_fingerprint"),
            estimated_input_tokens: r.get("estimated_input_tokens"),
            validation_status: r.get("validation_status"),
            created_at: r.get("created_at"),
        }))
    }

    pub async fn load_context_pack(
        &self,
        context_pack_id: &str,
    ) -> Result<Option<ContextPackRow>, String> {
        let row = sqlx::query(
            "SELECT context_pack_id,loop_id,source_attempt_id,target_attempt_ordinal,\
                    schema_version,payload_json,source_fingerprints_json,\
                    context_fingerprint,estimated_input_tokens,validation_status,\
                    created_at \
             FROM task_context_packs WHERE context_pack_id=?",
        )
        .bind(context_pack_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("load context pack: {e}"))?;
        Ok(row.map(|r| ContextPackRow {
            context_pack_id: r.get("context_pack_id"),
            loop_id: r.get("loop_id"),
            source_attempt_id: r.get("source_attempt_id"),
            target_attempt_ordinal: r.get("target_attempt_ordinal"),
            schema_version: r.get("schema_version"),
            payload_json: r.get("payload_json"),
            source_fingerprints_json: r.get("source_fingerprints_json"),
            context_fingerprint: r.get("context_fingerprint"),
            estimated_input_tokens: r.get("estimated_input_tokens"),
            validation_status: r.get("validation_status"),
            created_at: r.get("created_at"),
        }))
    }

    // ── Usage ledger ──────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_usage(
        &self,
        usage_id: &str,
        loop_id: &str,
        attempt_id: &str,
        execution_id: Option<&str>,
        runtime_profile_id: &str,
        model_identifier: Option<&str>,
        provider_identifier: Option<&str>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cached_input_tokens: Option<i64>,
        tool_calls: Option<i64>,
        wall_time_ms: Option<i64>,
        estimated_cost_micros: Option<i64>,
        usage_source: &str,
        usage_known: bool,
        usage_fingerprint: Option<&str>,
        idempotency_key: &str,
    ) -> Result<bool, String> {
        // Fault: UsageWrite before effect
        if let Some(FaultKind::FailBeforeEffect) =
            self.check_fault(FaultBoundary::UsageWrite)
        {
            return Err("fault: UsageWrite before effect".into());
        }
        let r = sqlx::query(
            "INSERT INTO task_usage_ledger \
             (usage_id,loop_id,attempt_id,execution_id,runtime_profile_id,\
              model_identifier,provider_identifier,input_tokens,output_tokens,\
              cached_input_tokens,tool_calls,wall_time_ms,estimated_cost_micros,\
              usage_source,usage_known,usage_fingerprint,idempotency_key) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(usage_id)
        .bind(loop_id)
        .bind(attempt_id)
        .bind(execution_id)
        .bind(runtime_profile_id)
        .bind(model_identifier)
        .bind(provider_identifier)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cached_input_tokens)
        .bind(tool_calls)
        .bind(wall_time_ms)
        .bind(estimated_cost_micros)
        .bind(usage_source)
        .bind(usage_known as i64)
        .bind(usage_fingerprint)
        .bind(idempotency_key)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("insert usage: {e}"))?;
        // Fault: UsageWrite response lost (after durable write)
        if let Some(FaultKind::ResponseLostAfterSuccess) =
            self.check_fault(FaultBoundary::UsageWrite)
        {
            return Err("fault: UsageWrite response lost".into());
        }
        Ok(r.rows_affected() == 1)
    }

    pub async fn sum_loop_usage(&self, loop_id: &str) -> Result<LoopUsageSummary, String> {
        let row = sqlx::query(
            "SELECT SUM(input_tokens) as total_input_tokens, \
                    SUM(output_tokens) as total_output_tokens, \
                    SUM(cached_input_tokens) as total_cached_input_tokens, \
                    SUM(tool_calls) as total_tool_calls, \
                    SUM(wall_time_ms) as total_wall_time_ms, \
                    SUM(estimated_cost_micros) as total_estimated_cost_micros \
             FROM task_usage_ledger WHERE loop_id=?",
        )
        .bind(loop_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("sum usage: {e}"))?;
        match row {
            Some(r) => Ok(LoopUsageSummary {
                total_input_tokens: r.get("total_input_tokens"),
                total_output_tokens: r.get("total_output_tokens"),
                total_cached_input_tokens: r.get("total_cached_input_tokens"),
                total_tool_calls: r.get("total_tool_calls"),
                total_wall_time_ms: r.get("total_wall_time_ms"),
                total_estimated_cost_micros: r.get("total_estimated_cost_micros"),
            }),
            None => Ok(LoopUsageSummary::default()),
        }
    }

    // ── Loop operations ───────────────────────────────────────────

    pub async fn insert_loop_operation(
        &self,
        operation_id: &str,
        loop_id: &str,
        operation_kind: LoopOperationKind,
        idempotency_key: &str,
        request_hash: &str,
        observed_state_fingerprint: Option<&str>,
        owner_id: Option<&str>,
        fencing_token: Option<i64>,
    ) -> Result<bool, String> {
        let r = sqlx::query(
            "INSERT INTO task_loop_operations \
             (operation_id,loop_id,operation_kind,idempotency_key,request_hash,\
              observed_state_fingerprint,lifecycle,owner_id,fencing_token) \
             VALUES (?,?,?,?,?,?,'running',?,?) \
             ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(operation_id)
        .bind(loop_id)
        .bind(operation_kind.as_str())
        .bind(idempotency_key)
        .bind(request_hash)
        .bind(observed_state_fingerprint)
        .bind(owner_id)
        .bind(fencing_token)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("insert loop op: {e}"))?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn complete_loop_operation(
        &self,
        operation_id: &str,
        result_fingerprint: &str,
    ) -> Result<bool, String> {
        let r = sqlx::query(
            "UPDATE task_loop_operations \
             SET lifecycle='completed', terminal_at=datetime('now'), result_fingerprint=? \
             WHERE operation_id=? AND lifecycle='running'",
        )
        .bind(result_fingerprint)
        .bind(operation_id)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("complete loop op: {e}"))?;
        Ok(r.rows_affected() == 1)
    }
}

#[derive(Debug, Clone, Default)]
pub struct LoopUsageSummary {
    pub total_input_tokens: Option<i64>,
    pub total_output_tokens: Option<i64>,
    pub total_cached_input_tokens: Option<i64>,
    pub total_tool_calls: Option<i64>,
    pub total_wall_time_ms: Option<i64>,
    pub total_estimated_cost_micros: Option<i64>,
}
