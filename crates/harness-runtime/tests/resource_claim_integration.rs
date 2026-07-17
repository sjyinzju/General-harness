//! I3-C integration tests: lease/fencing validation, claim TTL, and reconciliation.

use harness_core::contracts::task_envelope::{FileScope, TaskBudget, TaskEnvelope};
use harness_core::resource_claim::{AccessMode, ClaimGroupSpec, ClaimLifecycle, ResourceClaimSpec};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use harness_runtime::db::Database;
use harness_runtime::resource_claim::adapter::derive_claims_from_envelope;
use harness_runtime::resource_claim::{
    ClaimAnomaly, ClaimGuard, ResourceClaimLeaseValidator, ResourceClaimReconciler,
    ResourceClaimRepo, ResourceClaimService,
};
use sqlx::SqlitePool;
use std::sync::Arc;

// ── Helpers ──────────────────────────────────────────────────────────

async fn seed_world(pool: &SqlitePool, project_id: &str, task_id: &str, execution_id: &str) {
    sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?, 'test', 'created')")
        .bind(project_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?, ?, 'test', 'pending')",
    )
    .bind(task_id)
    .bind(project_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle, profile_id) VALUES (?, ?, 1, 'created', '')")
        .bind(execution_id).bind(task_id).execute(pool).await.unwrap();
}

fn guard() -> ClaimGuard {
    ClaimGuard {
        lease_id: "lease-1".into(),
        lease_token: "tok-secret".into(),
        fencing_token: 1,
        worktree_id: "wt-1".into(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
    }
}

fn spec_exact(path: &str, mode: AccessMode) -> ClaimGroupSpec {
    ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file("repo", path, mode)],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: Some("lease-1".into()),
    }
}

/// Mock lease validator that always succeeds.
struct AlwaysValidValidator;
#[async_trait::async_trait]
impl ResourceClaimLeaseValidator for AlwaysValidValidator {
    async fn validate_lease(
        &self,
        _lease_id: &str,
        _token: &str,
        _fencing: i64,
    ) -> Result<(), CoreError> {
        Ok(())
    }
    async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
        let far = "2099-01-01 00:00:00".to_string();
        Ok(Some(far))
    }
}

/// Mock lease validator that rejects stale fencing tokens.
struct FencingRejector;
#[async_trait::async_trait]
impl ResourceClaimLeaseValidator for FencingRejector {
    async fn validate_lease(
        &self,
        _lease_id: &str,
        _token: &str,
        fencing: i64,
    ) -> Result<(), CoreError> {
        if fencing < 2 {
            Err(CoreError::new(
                ErrorCode::WorkspaceLeaseExpired,
                "stale fencing token",
                ErrorSource::System,
            ))
        } else {
            Ok(())
        }
    }
    async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
        Ok(Some("2099-01-01 00:00:00".to_string()))
    }
}

/// Mock lease validator that rejects wrong tokens.
struct TokenRejector;
#[async_trait::async_trait]
impl ResourceClaimLeaseValidator for TokenRejector {
    async fn validate_lease(
        &self,
        _lease_id: &str,
        token: &str,
        _fencing: i64,
    ) -> Result<(), CoreError> {
        if token != "tok-secret" {
            Err(CoreError::new(
                ErrorCode::WorkspaceLeaseExpired,
                "wrong token",
                ErrorSource::System,
            ))
        } else {
            Ok(())
        }
    }
    async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
        Ok(Some("2099-01-01 00:00:00".to_string()))
    }
}

/// Mock lease validator for expired lease.
struct ExpiredLeaseValidator;
#[async_trait::async_trait]
impl ResourceClaimLeaseValidator for ExpiredLeaseValidator {
    async fn validate_lease(
        &self,
        _lease_id: &str,
        _token: &str,
        _fencing: i64,
    ) -> Result<(), CoreError> {
        Err(CoreError::new(
            ErrorCode::WorkspaceLeaseExpired,
            "lease expired",
            ErrorSource::System,
        ))
    }
    async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
        Ok(Some("2020-01-01 00:00:00".to_string()))
    }
}

fn make_service(
    pool: SqlitePool,
    validator: Box<dyn ResourceClaimLeaseValidator + Send + Sync>,
) -> ResourceClaimService {
    use harness_runtime::lease::clock::SystemClock;
    let repo = ResourceClaimRepo::new(pool);
    ResourceClaimService::new(repo, validator, Arc::new(SystemClock))
}

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_valid_guard_acquire_succeeds() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc.acquire_group(&spec, &guard(), "ikey-1").await.unwrap();
    assert!(matches!(
        result,
        harness_runtime::resource_claim::AcquireOutcome::Acquired(_)
    ));
}

#[tokio::test]
async fn test_stale_fencing_acquire_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(FencingRejector));

    // Guard has fencing_token = 1; FencingRejector requires >= 2.
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc.acquire_group(&spec, &guard(), "ikey-fence").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_wrong_lease_token_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(TokenRejector));

    let bad_guard = ClaimGuard {
        lease_token: "wrong-token".into(),
        ..guard()
    };
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc.acquire_group(&spec, &bad_guard, "ikey-wrong").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_released_lease_acquire_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(ExpiredLeaseValidator));

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc.acquire_group(&spec, &guard(), "ikey-expired").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_stale_fencing_renew_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    // First acquire with valid validator.
    let svc_ok = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc_ok
        .acquire_group(&spec, &guard(), "ikey-renew")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Now try renew with stale fencing.
    let svc_stale = make_service(db.pool.clone(), Box::new(FencingRejector));
    let result = svc_stale.renew_group(&group_id, &guard(), 300).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_old_owner_release_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-rel")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Wrong token → rejection.
    let bad_guard = ClaimGuard {
        lease_token: "stolen-token".into(),
        ..guard()
    };
    let svc_bad = make_service(db.pool.clone(), Box::new(TokenRejector));
    let result = svc_bad.release_group(&group_id, &bad_guard, "done").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execution_terminal_claims_expired_by_reconciler() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    // Acquire with valid validator.
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-rec")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Mark execution as completed (terminal).
    sqlx::query("UPDATE execution_attempts SET lifecycle = 'completed' WHERE id = 'e1'")
        .execute(&db.pool)
        .await
        .unwrap();

    // Run reconciler.
    let reconciler = ResourceClaimReconciler::new(db.pool.clone());
    let report = reconciler.reconcile().await.unwrap();
    assert!(report.expired.contains(&group_id));

    // Verify group is expired.
    let record = svc.get_group(&group_id).await.unwrap();
    assert_eq!(record.lifecycle, ClaimLifecycle::Expired);
}

#[tokio::test]
async fn test_worktree_removed_claims_expired() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    // Need a worktrees row for the claim group reference.
    sqlx::query("INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status) VALUES ('wt-1', 'p1', 't1', 'e1', '/repo', 'repo', '/repo/wt', 'br', 'abc123', 's1', 'op1', 'removed')")
        .execute(&db.pool).await.unwrap();

    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/a.rs",
            AccessMode::Write,
        )],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: Some("wt-1".into()),
        lease_id: Some("lease-1".into()),
    };
    let result = svc.acquire_group(&spec, &guard(), "ikey-wt").await.unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    let reconciler = ResourceClaimReconciler::new(db.pool.clone());
    let report = reconciler.reconcile().await.unwrap();
    assert!(report.expired.contains(&group_id));
}

#[tokio::test]
async fn test_concurrent_reconcilers_no_duplicates() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-conc")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Mark execution terminal.
    sqlx::query("UPDATE execution_attempts SET lifecycle = 'completed' WHERE id = 'e1'")
        .execute(&db.pool)
        .await
        .unwrap();

    // Run reconciler twice.
    let r1 = ResourceClaimReconciler::new(db.pool.clone());
    let r2 = ResourceClaimReconciler::new(db.pool.clone());
    let report1 = r1.reconcile().await.unwrap();
    let report2 = r2.reconcile().await.unwrap();

    // First run expires the group; second run should be idempotent.
    assert!(!report1.expired.is_empty());
    // Second run should find the group already expired, no new expiration.
    let dupe_expire = report2.expired.contains(&group_id);
    assert!(!dupe_expire, "second reconciler should not re-expire");
}

#[tokio::test]
async fn test_terminal_group_cannot_reactivate() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-term")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    svc.release_group(&group_id, &guard(), "done")
        .await
        .unwrap();

    // Try to renew a released group — should fail.
    let result = svc.renew_group(&group_id, &guard(), 300).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_reacquire_creates_new_group_id() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);

    let r1 = svc
        .acquire_group(&spec, &guard(), "ikey-reacq-1")
        .await
        .unwrap();
    let gid1 = match r1 {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    svc.release_group(&gid1, &guard(), "done").await.unwrap();

    let r2 = svc
        .acquire_group(&spec, &guard(), "ikey-reacq-2")
        .await
        .unwrap();
    let gid2 = match r2 {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    assert_ne!(gid1, gid2, "re-acquire must create a new group ID");
}

#[tokio::test]
async fn test_lease_token_absent_from_events_and_errors() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-tok")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Check event log for any lease token leaks.
    let events: Vec<(String, String)> =
        sqlx::query_as("SELECT event_type, payload_json FROM event_log WHERE stream_id = ?")
            .bind(&group_id)
            .fetch_all(&db.pool)
            .await
            .unwrap();

    for (_etype, payload) in &events {
        assert!(
            !payload.contains("tok-secret"),
            "event payload must not contain lease token: {payload}"
        );
        assert!(
            !payload.contains("lease_token"),
            "event payload must not reference lease_token"
        );
    }

    // Check that ClaimGuard debug output doesn't leak the token.
    let debug_str = format!("{:?}", guard());
    assert!(!debug_str.contains("tok-secret"));
    assert!(debug_str.contains("[REDACTED]"));
}

#[tokio::test]
async fn test_replace_with_current_fencing_succeeds() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let svc = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));

    let spec_old = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec_old, &guard(), "ikey-rep-old")
        .await
        .unwrap();
    let old_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    let spec_new = ClaimGroupSpec {
        claims: vec![
            ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Write),
            ResourceClaimSpec::exact_file("repo", "src/b.rs", AccessMode::Read),
        ],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: Some("lease-1".into()),
    };
    let rep = svc
        .replace_group(&old_id, &spec_new, &guard(), "ikey-rep-new")
        .await
        .unwrap();
    assert!(matches!(
        rep,
        harness_runtime::resource_claim::AcquireOutcome::Acquired(_)
    ));
}

#[tokio::test]
async fn test_replace_with_stale_fencing_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    // Acquire with valid validator.
    let svc_ok = make_service(db.pool.clone(), Box::new(AlwaysValidValidator));
    let spec_old = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc_ok
        .acquire_group(&spec_old, &guard(), "ikey-rep-stale")
        .await
        .unwrap();
    let old_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Replace with stale fencing validator.
    let svc_stale = make_service(db.pool.clone(), Box::new(FencingRejector));
    let spec_new = spec_exact("src/b.rs", AccessMode::Read);
    let result = svc_stale
        .replace_group(&old_id, &spec_new, &guard(), "ikey-rep-stale-2")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_claim_expiry_bounded_by_lease() {
    // Verify that renew_group bounds the duration by lease expiry.
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    struct ShortLeaseValidator;
    #[async_trait::async_trait]
    impl ResourceClaimLeaseValidator for ShortLeaseValidator {
        async fn validate_lease(
            &self,
            _lease_id: &str,
            _token: &str,
            _fencing: i64,
        ) -> Result<(), CoreError> {
            Ok(())
        }
        async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
            // Lease expires in 30 seconds.
            let exp = (chrono::Utc::now() + chrono::Duration::seconds(30))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            Ok(Some(exp))
        }
    }

    let svc = make_service(db.pool.clone(), Box::new(ShortLeaseValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-bound")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Try to renew for 300 seconds — should be bounded to 30.
    let result = svc.renew_group(&group_id, &guard(), 300).await;
    assert!(result.is_ok(), "renew should succeed with bounded duration");
}

#[tokio::test]
async fn test_claim_cannot_exceed_lease_expiry() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;

    // Use a validator with a short-lived lease for acquire.
    struct ShortLeaseValidator;
    #[async_trait::async_trait]
    impl ResourceClaimLeaseValidator for ShortLeaseValidator {
        async fn validate_lease(
            &self,
            _lease_id: &str,
            _token: &str,
            _fencing: i64,
        ) -> Result<(), CoreError> {
            Ok(())
        }
        async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
            // Lease expires in 2 seconds.
            let exp = (chrono::Utc::now() + chrono::Duration::seconds(2))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            Ok(Some(exp))
        }
    }

    let svc = make_service(db.pool.clone(), Box::new(ShortLeaseValidator));
    let spec = spec_exact("src/a.rs", AccessMode::Write);

    // Acquire succeeds with bounded (short) expiry.
    let result = svc
        .acquire_group(&spec, &guard(), "ikey-past")
        .await
        .unwrap();
    let group_id = match result {
        harness_runtime::resource_claim::AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Wait for the lease to expire.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Now create a service with an already-expired lease validator.
    struct AlreadyExpiredLease;
    #[async_trait::async_trait]
    impl ResourceClaimLeaseValidator for AlreadyExpiredLease {
        async fn validate_lease(
            &self,
            _lease_id: &str,
            _token: &str,
            _fencing: i64,
        ) -> Result<(), CoreError> {
            Ok(())
        }
        async fn get_lease_expires_at(&self, _lease_id: &str) -> Result<Option<String>, CoreError> {
            Ok(Some("2020-01-01 00:00:00".to_string()))
        }
    }

    let svc_expired = make_service(db.pool.clone(), Box::new(AlreadyExpiredLease));

    // Renew should fail because lease already expired.
    let result = svc_expired.renew_group(&group_id, &guard(), 300).await;
    assert!(result.is_err(), "renew with expired lease should fail");
}

#[tokio::test]
async fn test_reconciler_detects_conflicting_active_invariant() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;

    // Directly insert two conflicting active groups to test reconciler detection.
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

    sqlx::query(
        "INSERT INTO resource_claim_groups (group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, created_at, updated_at) VALUES ('g1', 'p1', 't1', 'e1', 'repo', NULL, NULL, 1, 'h1', 'active', ?, ?, ?)",
    )
    .bind(&now).bind(&now).bind(&now)
    .execute(&db.pool).await.unwrap();

    sqlx::query(
        "INSERT INTO resource_claim_groups (group_id, project_id, task_id, execution_id, repository_identity, worktree_id, lease_id, fencing_token, request_hash, lifecycle, acquired_at, created_at, updated_at) VALUES ('g2', 'p2', 't2', 'e2', 'repo', NULL, NULL, 1, 'h2', 'active', ?, ?, ?)",
    )
    .bind(&now).bind(&now).bind(&now)
    .execute(&db.pool).await.unwrap();

    // Insert conflicting claims: Write on src/x.rs from g1, Read on src/x.rs from g2.
    sqlx::query(
        "INSERT INTO resource_claims (id, project_id, task_id, resource_kind, normalized_resource, access_mode, status, group_id, lifecycle, acquired_at, created_at) VALUES ('c1', 'p1', 't1', 'exact_file', 'src/x.rs', 'write', 'active', 'g1', 'active', ?, ?)",
    )
    .bind(&now).bind(&now)
    .execute(&db.pool).await.unwrap();

    sqlx::query(
        "INSERT INTO resource_claims (id, project_id, task_id, resource_kind, normalized_resource, access_mode, status, group_id, lifecycle, acquired_at, created_at) VALUES ('c2', 'p2', 't2', 'exact_file', 'src/x.rs', 'read', 'active', 'g2', 'active', ?, ?)",
    )
    .bind(&now).bind(&now)
    .execute(&db.pool).await.unwrap();

    // Reconciler should detect the conflicting active groups (Write vs Read on same file).
    let reconciler = ResourceClaimReconciler::new(db.pool.clone());
    let report = reconciler.reconcile().await.unwrap();
    let has_conflict = report
        .anomalies
        .iter()
        .any(|a| matches!(a, ClaimAnomaly::MultipleConflictingActiveGroups { .. }));
    assert!(
        has_conflict,
        "reconciler should detect conflicting active groups, got: {:?}",
        report.anomalies
    );
}

// ── Adapter tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_adapter_exact_write_from_envelope() {
    let env = TaskEnvelope {
        task_id: "t1".into(),
        project_id: "p1".into(),
        task_goal: "test".into(),
        scope: FileScope {
            allowed_paths: vec!["src/auth/callback.rs".into()],
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec![],
        allowed_tools: vec![],
        output_schema: String::new(),
        budget: TaskBudget {
            max_turns: 10,
            max_time_ms: 60000,
            max_cost_cents: None,
        },
        goal_contract_version: 1,
        plan_version: 1,
    };
    let result = derive_claims_from_envelope(&env, "repo");
    assert!(matches!(
        result,
        harness_runtime::resource_claim::adapter::DeriveClaimsOutcome::Claims(_)
    ));
}

#[tokio::test]
async fn test_adapter_glob_directory_prefix() {
    let env = TaskEnvelope {
        task_id: "t1".into(),
        project_id: "p1".into(),
        task_goal: "test".into(),
        scope: FileScope {
            allowed_paths: vec!["src/**".into()],
            forbidden_paths: vec![],
            readable_paths: vec![],
            scope_expansion_allowed: false,
        },
        resource_claims: vec![],
        dependencies: vec![],
        acceptance_checks: vec![],
        allowed_tools: vec![],
        output_schema: String::new(),
        budget: TaskBudget {
            max_turns: 10,
            max_time_ms: 60000,
            max_cost_cents: None,
        },
        goal_contract_version: 1,
        plan_version: 1,
    };
    let result = derive_claims_from_envelope(&env, "repo");
    assert!(matches!(
        result,
        harness_runtime::resource_claim::adapter::DeriveClaimsOutcome::Claims(_)
    ));
}
