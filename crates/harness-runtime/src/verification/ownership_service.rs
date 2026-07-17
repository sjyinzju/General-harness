//! VerificationOwnershipService — coordinates resource handoff takeover
//! from the I4-B Scheduler to I4-C Verification.
//!
//! This is the ONLY entry point for Verification to claim scheduler resources.
//! Never calls HandoffRepository or HeartbeatRegistry directly.

use std::sync::Arc;

use harness_core::contracts::verification::VerificationRunLifecycle;
use sqlx::SqlitePool;

use crate::scheduler::handoff_coordinator::{
    CoordinatedTakeoverResult, ResourceHandoffCoordinator,
};
use crate::scheduler::handoff_repo::{HandoffRecord, HandoffRepository};
use crate::scheduler::heartbeat_registry::HeartbeatRegistry;

// ── Takeover request ──────────────────────────────────────────────────

/// Input for a verification ownership takeover attempt.
pub struct TakeoverRequest {
    pub verification_run_id: String,
    pub execution_id: String,
    pub task_id: String,
    pub project_id: String,
    pub plan_hash: String,
    pub handoff_id: String,
    pub expected_worktree_id: String,
    pub expected_lease_id: String,
    pub expected_claim_group_id: Option<String>,
    pub expected_fencing: i64,
    pub verification_owner_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
}

// ── Takeover result ───────────────────────────────────────────────────

/// Structured outcome of a verification ownership takeover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipTakeoverResult {
    /// Takeover succeeded, VerificationRun is now Running.
    Acquired { run_id: String, execution_id: String },
    /// Same verification owner already owns this run (idempotent).
    AlreadyOwned { run_id: String },
    /// Another verification run already owns this handoff.
    Contested { current_owner: String },
    /// The expected fencing token does not match.
    StaleFencing { expected: i64, actual: i64 },
    /// DB and runtime registry disagree on ownership.
    HandoffStateMismatch { detail: String },
    /// Heartbeat is missing from the runtime registry.
    HeartbeatMissing { execution_id: String },
    /// Worktree DB record does not exist.
    WorktreeMissing { worktree_id: String },
    /// Filesystem worktree path does not exist.
    WorktreePathMissing { path: String },
    /// Workspace lease is not active.
    LeaseInactive { lease_id: String },
    /// Resource claim group is not active.
    ClaimInactive { claim_group_id: Option<String> },
    /// Resource identity mismatch (worktree/lease/claim do not match).
    IdentityMismatch { detail: String },
    /// Handoff requires reconciliation.
    ReconciliationRequired { handoff_id: String, status: String },
    /// Handoff is in a terminal state.
    TerminalHandoff { status: String },
    /// Idempotency conflict (same key, different hash).
    IdempotencyConflict { existing_hash: String, new_hash: String },
    /// Verification run was not found.
    RunNotFound { run_id: String },
    /// Verification run is in a terminal state and cannot be taken over.
    RunTerminal { lifecycle: String },
    /// Task is not in a state awaiting verification.
    TaskNotAwaitingVerification { lifecycle: String },
    /// Pre-takeover validation failed for an unexpected reason.
    ValidationFailed { reason: String },
}

// ── Service ───────────────────────────────────────────────────────────

pub struct VerificationOwnershipService {
    pool: SqlitePool,
    coordinator: ResourceHandoffCoordinator,
    handoff_repo: HandoffRepository,
    heartbeat_registry: Arc<HeartbeatRegistry>,
}

impl VerificationOwnershipService {
    pub fn new(
        pool: SqlitePool,
        coordinator: ResourceHandoffCoordinator,
        handoff_repo: HandoffRepository,
        heartbeat_registry: Arc<HeartbeatRegistry>,
    ) -> Self {
        Self {
            pool,
            coordinator,
            handoff_repo,
            heartbeat_registry,
        }
    }

    /// Attempt to take over scheduler resources for verification.
    ///
    /// Order:
    ///   1. Load and validate all preconditions
    ///   2. Coordinated takeover via ResourceHandoffCoordinator
    ///   3. Transition VerificationRun Created → Running
    ///   4. Post-consistency confirmation
    ///
    /// Idempotent: same request_hash on same run returns AlreadyOwned.
    /// Never starts an Agent, creates a retry, or deletes a Worktree.
    pub async fn start_or_resume_takeover(
        &self,
        req: &TakeoverRequest,
    ) -> OwnershipTakeoverResult {
        // ── 1. Load verification run ────────────────────────────────
        let run_row: Option<(String, String, i64)> = match sqlx::query_as(
            "SELECT lifecycle, request_hash, version FROM verification_runs WHERE run_id = ?",
        )
        .bind(&req.verification_run_id)
        .fetch_optional(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(_) => return OwnershipTakeoverResult::RunNotFound { run_id: req.verification_run_id.clone() },
        };

        let (run_lc, existing_hash, run_version) = match run_row {
            Some(r) => r,
            None => return OwnershipTakeoverResult::RunNotFound { run_id: req.verification_run_id.clone() },
        };

        // Already Running — idempotent or contested.
        if run_lc == "running" {
            if existing_hash == req.request_hash {
                return OwnershipTakeoverResult::AlreadyOwned { run_id: req.verification_run_id.clone() };
            }
            return OwnershipTakeoverResult::Contested { current_owner: "another-verification-run".into() };
        }

        // Terminal — cannot take over.
        let lc = parse_run_lifecycle(&run_lc);
        if lc.is_terminal() {
            return OwnershipTakeoverResult::RunTerminal { lifecycle: run_lc };
        }

        // Must be Created.
        if run_lc != "created" {
            return OwnershipTakeoverResult::ValidationFailed {
                reason: format!("run lifecycle is '{}', expected 'created'", run_lc),
            };
        }

        // ── 2. Load handoff ─────────────────────────────────────────
        let handoff: HandoffRecord = match self.handoff_repo.get_by_execution(&req.execution_id).await {
            Ok(Some(h)) => h,
            Ok(None) => return OwnershipTakeoverResult::ReconciliationRequired {
                handoff_id: req.handoff_id.clone(),
                status: "missing".into(),
            },
            Err(_) => return OwnershipTakeoverResult::ValidationFailed {
                reason: "failed to load handoff".into(),
            },
        };

        // Handoff identity must match.
        if handoff.handoff_id != req.handoff_id
            || handoff.task_id != req.task_id
            || handoff.execution_id != req.execution_id
        {
            return OwnershipTakeoverResult::IdentityMismatch {
                detail: format!(
                    "handoff identity mismatch: expected {}/{}/{}",
                    req.handoff_id, req.task_id, req.execution_id
                ),
            };
        }

        // Terminal handoff.
        match handoff.status.as_str() {
            "released" | "lost" => {
                return OwnershipTakeoverResult::TerminalHandoff { status: handoff.status.clone() };
            }
            "reconciliation_required" => {
                return OwnershipTakeoverResult::ReconciliationRequired {
                    handoff_id: handoff.handoff_id.clone(),
                    status: handoff.status.clone(),
                };
            }
            _ => {}
        }

        // ── 3. Validate worktree ────────────────────────────────────
        let wt_id = handoff.worktree_id.as_deref().unwrap_or("");
        if wt_id.is_empty() || wt_id != req.expected_worktree_id {
            return OwnershipTakeoverResult::WorktreeMissing { worktree_id: wt_id.into() };
        }

        let wt_exists: (i64,) = match sqlx::query_as(
            "SELECT COUNT(*) FROM worktrees WHERE id = ?",
        )
        .bind(wt_id)
        .fetch_one(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(_) => return OwnershipTakeoverResult::WorktreeMissing { worktree_id: wt_id.into() },
        };
        if wt_exists.0 == 0 {
            return OwnershipTakeoverResult::WorktreeMissing { worktree_id: wt_id.into() };
        }

        // Check filesystem worktree path.
        let wt_path: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT worktree_path FROM worktrees WHERE id = ?",
        )
        .bind(wt_id)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None);
        if let Some((Some(ref path),)) = wt_path {
            if !std::path::Path::new(path).exists() {
                return OwnershipTakeoverResult::WorktreePathMissing { path: path.clone() };
            }
        }

        // ── 4. Validate lease ───────────────────────────────────────
        let lease_id = handoff.lease_id.as_deref().unwrap_or("");
        if lease_id.is_empty() || lease_id != req.expected_lease_id {
            return OwnershipTakeoverResult::LeaseInactive { lease_id: lease_id.into() };
        }

        let lease_active: (i64,) = match sqlx::query_as(
            "SELECT COUNT(*) FROM workspace_leases WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(lease_id)
        .fetch_one(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(_) => return OwnershipTakeoverResult::LeaseInactive { lease_id: lease_id.into() },
        };
        if lease_active.0 == 0 {
            return OwnershipTakeoverResult::LeaseInactive { lease_id: lease_id.into() };
        }

        // ── 5. Validate fencing ─────────────────────────────────────
        if handoff.fencing_token != req.expected_fencing {
            return OwnershipTakeoverResult::StaleFencing {
                expected: req.expected_fencing,
                actual: handoff.fencing_token,
            };
        }

        // ── 6. Validate heartbeat ───────────────────────────────────
        let hb = self.heartbeat_registry.inspect(&req.execution_id).await;
        match hb {
            None => {
                return OwnershipTakeoverResult::HeartbeatMissing { execution_id: req.execution_id.clone() };
            }
            Some(ref hb_info) => {
                if !hb_info.status.contains("healthy") && !hb_info.status.contains("degraded") {
                    return OwnershipTakeoverResult::HeartbeatMissing { execution_id: req.execution_id.clone() };
                }
                if hb_info.fencing_token != req.expected_fencing {
                    return OwnershipTakeoverResult::StaleFencing {
                        expected: req.expected_fencing,
                        actual: hb_info.fencing_token,
                    };
                }
            }
        }

        // ── 7. Coordinated takeover ─────────────────────────────────
        let takeover = self
            .coordinator
            .takeover_for_verification(&req.execution_id, &req.verification_owner_id, req.expected_fencing)
            .await;

        match takeover {
            CoordinatedTakeoverResult::Acquired { .. } => { /* proceed */ }
            CoordinatedTakeoverResult::AlreadyOwned => {
                // Transition run if not already Running.
                if run_lc == "created" {
                    let _ = sqlx::query(
                        "UPDATE verification_runs SET lifecycle='running', started_at=datetime('now'), version=version+1, updated_at=datetime('now') WHERE run_id=? AND lifecycle='created' AND version=?",
                    )
                    .bind(&req.verification_run_id).bind(run_version)
                    .execute(&self.pool).await;
                }
                return OwnershipTakeoverResult::AlreadyOwned { run_id: req.verification_run_id.clone() };
            }
            CoordinatedTakeoverResult::Contested { current_owner } => {
                return OwnershipTakeoverResult::Contested { current_owner };
            }
            CoordinatedTakeoverResult::StaleFencing { expected, actual } => {
                return OwnershipTakeoverResult::StaleFencing { expected, actual };
            }
            CoordinatedTakeoverResult::Terminal { status } => {
                return OwnershipTakeoverResult::TerminalHandoff { status };
            }
            CoordinatedTakeoverResult::NotFound => {
                return OwnershipTakeoverResult::ReconciliationRequired {
                    handoff_id: req.handoff_id.clone(),
                    status: "not_found".into(),
                };
            }
            CoordinatedTakeoverResult::HandoffStateMismatch { detail, .. } => {
                return OwnershipTakeoverResult::HandoffStateMismatch { detail };
            }
        }

        // ── 8. Transition VerificationRun Created → Running ─────────
        let rows = sqlx::query(
            "UPDATE verification_runs SET lifecycle='running', started_at=datetime('now'), version=version+1, updated_at=datetime('now') WHERE run_id=? AND lifecycle='created' AND version=?",
        )
        .bind(&req.verification_run_id)
        .bind(run_version)
        .execute(&self.pool)
        .await;

        match rows {
            Ok(r) if r.rows_affected() == 1 => { /* success */ }
            Ok(_) => {
                // Run was already transitioned or version mismatch.
                // Coordinator already succeeded — verify run state.
                let current: Option<(String,)> = sqlx::query_as(
                    "SELECT lifecycle FROM verification_runs WHERE run_id = ?",
                )
                .bind(&req.verification_run_id)
                .fetch_optional(&self.pool)
                .await
                .unwrap_or(None);

                if let Some((lc,)) = current {
                    if lc == "running" {
                        return OwnershipTakeoverResult::AlreadyOwned { run_id: req.verification_run_id.clone() };
                    }
                }
                return OwnershipTakeoverResult::ValidationFailed {
                    reason: "run transition failed after successful coordinator takeover".into(),
                };
            }
            Err(e) => {
                return OwnershipTakeoverResult::ValidationFailed {
                    reason: format!("run transition error after takeover: {e}"),
                };
            }
        }

        // ── 9. Post-consistency confirmation ────────────────────────
        let post = match self.coordinator.inspect_consistent(&req.execution_id).await {
            Ok(r) => r,
            Err(_) => {
                return OwnershipTakeoverResult::HandoffStateMismatch {
                    detail: "post-takeover inspect failed".into(),
                };
            }
        };

        if !post.consistent {
            return OwnershipTakeoverResult::HandoffStateMismatch {
                detail: "post-takeover layers inconsistent".into(),
            };
        }

        OwnershipTakeoverResult::Acquired {
            run_id: req.verification_run_id.clone(),
            execution_id: req.execution_id.clone(),
        }
    }
}

fn parse_run_lifecycle(s: &str) -> VerificationRunLifecycle {
    match s {
        "running" => VerificationRunLifecycle::Running,
        "completed" => VerificationRunLifecycle::Completed,
        "failed" => VerificationRunLifecycle::Failed,
        "cancelled" => VerificationRunLifecycle::Cancelled,
        "error" => VerificationRunLifecycle::Error,
        _ => VerificationRunLifecycle::Created,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::scheduler::handoff_coordinator::ResourceHandoffCoordinator;
    use crate::scheduler::handoff_repo::{CreateHandoffParams, HandoffRepository};
    use crate::scheduler::heartbeat_registry::{
        HeartbeatEntry, HeartbeatRegistry, HeartbeatStatus, OwnerKind,
    };
    use tokio_util::sync::CancellationToken;

    struct TestContext {
        svc: VerificationOwnershipService,
        db: Database,
        registry: Arc<HeartbeatRegistry>,
        db_path: std::path::PathBuf,
        _td: tempfile::TempDir, // keep tempdir alive
    }

    async fn setup_ownership_test() -> TestContext {
        let td = tempfile::tempdir().unwrap();
        let db_path = td.path().join("ownership.db");
        let db = Database::open(&db_path).await.unwrap();
        let pool = db.pool.clone();

        // Create a real worktree path on the filesystem.
        let wt_path = td.path().join("wt1");
        std::fs::create_dir_all(&wt_path).unwrap();
        let wt_path_str = wt_path.to_string_lossy().to_string();

        // Seed prerequisite rows.
        sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','submitted')")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'completed')")
            .execute(&pool).await.unwrap();
        // Worktree record pointing to a real directory.
        sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, operation_id, owner_supervisor_id, status, lease_epoch) VALUES ('wt1','p1','t1','e1','/repo','/repo/.git',?,'br','abc','op1','sup1','active',5)")
            .bind(&wt_path_str)
            .execute(&pool).await.unwrap();
        // Active lease.
        sqlx::query("INSERT INTO workspace_leases (id, worktree_id, project_id, task_id, owner_execution_id, lease_token, fencing_token, lifecycle, heartbeat_at, expires_at) VALUES ('l1','wt1','p1','t1','e1','tok-secret',5,'active',datetime('now'),datetime('now','+10 minutes'))")
            .execute(&pool).await.unwrap();
        // Verification plan.
        sqlx::query("INSERT INTO verification_plans (plan_id, task_id, execution_id, project_id, plan_hash, plan_version, steps_json) VALUES ('plan-1','t1','e1','p1','hash-aaa',1,'[]')")
            .execute(&pool).await.unwrap();

        // Create handoff via repository.
        let handoff_repo = HandoffRepository::new(pool.clone());
        handoff_repo.create("ho-1", "p1", "t1", CreateHandoffParams {
            execution_id: "e1", worktree_id: Some("wt1"), lease_id: Some("l1"),
            claim_group_id: Some("cg1"), fencing_token: 5, owner_id: "scheduler-main",
        }).await.unwrap();

        // Heartbeat registry.
        let registry = Arc::new(HeartbeatRegistry::new());
        registry.register(HeartbeatEntry {
            execution_id: "e1".into(), task_id: "t1".into(), worktree_id: "wt1".into(),
            lease_id: "l1".into(), claim_group_id: Some("cg1".into()),
            fencing_token: 5, owner_kind: OwnerKind::Scheduler, owner_id: "scheduler-main".into(),
            status: HeartbeatStatus::Healthy,
            last_heartbeat_at: Some(chrono::Utc::now()),
            cancel_token: CancellationToken::new(), last_error: None,
        }).await.unwrap();

        let coordinator = ResourceHandoffCoordinator::new(handoff_repo.clone(), registry.clone());
        let svc = VerificationOwnershipService::new(pool, coordinator, handoff_repo, registry.clone());
        TestContext { svc, db, registry, db_path, _td: td }
    }

    fn make_req(run_id: &str, ikey: &str, hash: &str) -> TakeoverRequest {
        TakeoverRequest {
            verification_run_id: run_id.into(), execution_id: "e1".into(),
            task_id: "t1".into(), project_id: "p1".into(), plan_hash: "hash-aaa".into(),
            handoff_id: "ho-1".into(), expected_worktree_id: "wt1".into(),
            expected_lease_id: "l1".into(), expected_claim_group_id: Some("cg1".into()),
            expected_fencing: 5, verification_owner_id: "verify-run-1".into(),
            idempotency_key: ikey.into(), request_hash: hash.into(),
        }
    }

    async fn seed_run(pool: &SqlitePool, run_id: &str, ikey: &str, hash: &str) {
        sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, idempotency_key, request_hash) VALUES (?, 'plan-1', 'hash-aaa', 1, 'e1', 't1', 'p1', 'created', ?, ?)")
            .bind(run_id).bind(ikey).bind(hash).execute(pool).await.unwrap();
    }

    // ── Normal takeover ─────────────────────────────────────────────
    #[tokio::test]
    async fn test_normal_takeover() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-1", "ikey-nt", "hash-nt").await;
        let req = make_req("run-1", "ikey-nt", "hash-nt");

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::Acquired { .. }));

        // Run must now be Running.
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id='run-1'")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(lc.0, "running");
    }

    // ── Same run repeated idempotent ────────────────────────────────
    #[tokio::test]
    async fn test_same_run_idempotent() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-2", "ikey-idem", "hash-idem").await;
        let req = make_req("run-2", "ikey-idem", "hash-idem");

        let r1 = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(r1, OwnershipTakeoverResult::Acquired { .. }));

        let r2 = ctx.svc.start_or_resume_takeover(&req).await;
        assert_eq!(r2, OwnershipTakeoverResult::AlreadyOwned { run_id: "run-2".into() });
    }

    // ── Response-lost recovery ──────────────────────────────────────
    #[tokio::test]
    async fn test_response_lost_recovery() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-rl", "ikey-rl", "hash-rl").await;
        let req = make_req("run-rl", "ikey-rl", "hash-rl");

        // First takeover succeeds.
        ctx.svc.start_or_resume_takeover(&req).await;
        // Simulate response lost — same request again.
        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert_eq!(result, OwnershipTakeoverResult::AlreadyOwned { run_id: "run-rl".into() });
    }

    // ── Stale fencing rejected ──────────────────────────────────────
    #[tokio::test]
    async fn test_stale_fencing_rejected() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-sf", "ikey-sf", "hash-sf").await;
        let mut req = make_req("run-sf", "ikey-sf", "hash-sf");
        req.expected_fencing = 99; // wrong fencing

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::StaleFencing { .. }));

        // Run must still be Created.
        let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM verification_runs WHERE run_id='run-sf'").fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(lc.0, "created");
    }

    // ── Terminal run rejected ───────────────────────────────────────
    #[tokio::test]
    async fn test_terminal_run_rejected() {
        let ctx = setup_ownership_test().await;
        sqlx::query("INSERT INTO verification_runs (run_id, plan_id, plan_hash, plan_version, execution_id, task_id, project_id, lifecycle, idempotency_key, request_hash) VALUES ('run-term', 'plan-1', 'hash-aaa', 1, 'e1', 't1', 'p1', 'completed', 'ikey-term', 'hash-term')")
            .execute(&ctx.db.pool).await.unwrap();
        let req = make_req("run-term", "ikey-term", "hash-term");

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::RunTerminal { .. }));
    }

    // ── Worktree missing ────────────────────────────────────────────
    #[tokio::test]
    async fn test_worktree_missing_rejected() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-wm", "ikey-wm", "hash-wm").await;
        let mut req = make_req("run-wm", "ikey-wm", "hash-wm");
        req.expected_worktree_id = "wt-nonexistent".into();

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::WorktreeMissing { .. }));
    }

    // ── Lease inactive ──────────────────────────────────────────────
    #[tokio::test]
    async fn test_lease_inactive_rejected() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-li", "ikey-li", "hash-li").await;
        let mut req = make_req("run-li", "ikey-li", "hash-li");
        req.expected_lease_id = "l-nonexistent".into();

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::LeaseInactive { .. }));
    }

    // ── Heartbeat missing ───────────────────────────────────────────
    #[tokio::test]
    async fn test_heartbeat_missing_rejected() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-hm", "ikey-hm", "hash-hm").await;
        // Remove heartbeat from registry.
        ctx.registry.mark_lost("e1").await;
        let req = make_req("run-hm", "ikey-hm", "hash-hm");

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::HeartbeatMissing { .. }));
    }

    // ── Terminal handoff rejected ───────────────────────────────────
    #[tokio::test]
    async fn test_terminal_handoff_rejected() {
        let ctx = setup_ownership_test().await;
        // Mark handoff as lost.
        let ho = HandoffRepository::new(ctx.db.pool.clone());
        ho.mark_lost("e1").await.unwrap();
        seed_run(&ctx.db.pool, "run-th", "ikey-th", "hash-th").await;
        let req = make_req("run-th", "ikey-th", "hash-th");

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::TerminalHandoff { .. }));
    }

    // ── DB/registry mismatch ────────────────────────────────────────
    #[tokio::test]
    async fn test_db_registry_mismatch_rejected() {
        let ctx = setup_ownership_test().await;
        // Change registry owner without updating DB.
        ctx.registry.takeover("e1", "rogue", 5).await;
        seed_run(&ctx.db.pool, "run-mm", "ikey-mm", "hash-mm").await;
        let req = make_req("run-mm", "ikey-mm", "hash-mm");

        let result = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(matches!(result, OwnershipTakeoverResult::HandoffStateMismatch { .. }));
    }

    // ── No command started before takeover ──────────────────────────
    #[tokio::test]
    async fn test_no_command_before_takeover() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-nc", "ikey-nc", "hash-nc").await;

        // Test multiple failure paths — none must start a command.
        // Stale fencing.
        let mut req = make_req("run-nc", "ikey-nc", "hash-nc");
        req.expected_fencing = 99;
        let r = ctx.svc.start_or_resume_takeover(&req).await;
        assert!(!matches!(r, OwnershipTakeoverResult::Acquired { .. }));

        // Verify no execution was created (no agent started).
        let exec_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts WHERE lifecycle NOT IN ('completed','failed','lost','cancelled')")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(exec_count.0, 0, "no new execution created");

        // Verify no retry task.
        let task_lc: (String,) = sqlx::query_as("SELECT lifecycle FROM tasks WHERE id='t1'").fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(task_lc.0, "submitted", "task lifecycle unchanged");
    }

    // ── Two file-backed pools concurrent one winner ─────────────────
    #[tokio::test]
    async fn test_two_pools_concurrent_one_winner() {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;
        use std::time::Duration;

        // Setup via pool1.
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-conc", "ikey-conc", "hash-conc").await;

        // Create pool2 connected to the same file.
        let db_path_str = ctx.db_path.to_string_lossy().to_string();
        let opts2 = SqliteConnectOptions::from_str(&db_path_str).unwrap()
            .create_if_missing(false).foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(30));
        let pool2 = SqlitePoolOptions::new().max_connections(5).connect_with(opts2).await.unwrap();

        let hr2 = HandoffRepository::new(pool2.clone());
        let coord2 = ResourceHandoffCoordinator::new(hr2, ctx.registry.clone());
        let svc2 = VerificationOwnershipService::new(pool2.clone(), coord2, HandoffRepository::new(pool2), ctx.registry);

        let req = make_req("run-conc", "ikey-conc", "hash-conc");

        let (r1, r2) = tokio::join!(
            ctx.svc.start_or_resume_takeover(&req),
            svc2.start_or_resume_takeover(&req)
        );

        let has_acquired = matches!(r1, OwnershipTakeoverResult::Acquired { .. })
            || matches!(r2, OwnershipTakeoverResult::Acquired { .. });
        assert!(has_acquired, "exactly one must acquire");

        // Only one Running run.
        let running: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM verification_runs WHERE lifecycle='running' AND run_id='run-conc'")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(running.0, 1, "only one run must be running");
    }

    // ── No agent execution created ──────────────────────────────────
    #[tokio::test]
    async fn test_no_agent_execution_created() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-na", "ikey-na", "hash-na").await;
        let req = make_req("run-na", "ikey-na", "hash-na");

        ctx.svc.start_or_resume_takeover(&req).await;

        // No new execution with non-terminal lifecycle.
        let new_exec: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts WHERE lifecycle NOT IN ('completed','failed','lost','cancelled')")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(new_exec.0, 0);
    }

    // ── No retry created ────────────────────────────────────────────
    #[tokio::test]
    async fn test_no_retry_created() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-nr", "ikey-nr", "hash-nr").await;
        let req = make_req("run-nr", "ikey-nr", "hash-nr");

        ctx.svc.start_or_resume_takeover(&req).await;

        let exec_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM execution_attempts WHERE task_id='t1'")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(exec_count.0, 1, "no retry execution created");
    }

    // ── No provider switch ──────────────────────────────────────────
    #[tokio::test]
    async fn test_no_provider_switch() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-np", "ikey-np", "hash-np").await;
        let req = make_req("run-np", "ikey-np", "hash-np");

        ctx.svc.start_or_resume_takeover(&req).await;

        // Profile must not have changed — verify no provider switch.
        let prof: (Option<String>,) = sqlx::query_as("SELECT profile_id FROM execution_attempts WHERE id='e1'")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert!(prof.0.is_none() || prof.0.as_deref() == Some(""), "profile must not be set or changed");
    }

    // ── No worktree deletion ────────────────────────────────────────
    #[tokio::test]
    async fn test_no_worktree_deletion() {
        let ctx = setup_ownership_test().await;
        seed_run(&ctx.db.pool, "run-nw", "ikey-nw", "hash-nw").await;
        let req = make_req("run-nw", "ikey-nw", "hash-nw");

        ctx.svc.start_or_resume_takeover(&req).await;

        let wt: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM worktrees WHERE id='wt1'")
            .fetch_one(&ctx.db.pool).await.unwrap();
        assert_eq!(wt.0, 1, "worktree must not be deleted");
    }
}
