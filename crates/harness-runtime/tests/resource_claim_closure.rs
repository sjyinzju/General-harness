//! I3 Pre-I4 Closure tests: cross-connection concurrency, atomic events,
//! idempotency TOCTOU, and replace atomicity.
//!
//! These tests use a shared temp-file SQLite database to verify that
//! SQLite-level serialization works correctly across independent connections
//! and pools.

use harness_core::resource_claim::{AccessMode, ClaimGroupSpec, ClaimLifecycle, ResourceClaimSpec};
use harness_runtime::resource_claim::{
    AcquireOutcome, ClaimGuard, ResourceClaimReconciler, ResourceClaimRepo,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;
use std::time::Duration;

// ── Helpers ──────────────────────────────────────────────────────────

fn far_future() -> String {
    "2099-01-01 00:00:00".to_string()
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
        lease_id: None,
    }
}

/// Open a temp-file database with configurable max_connections.
async fn open_file_db(max_conns: u32) -> (SqlitePool, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .unwrap()
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));

    let pool = SqlitePoolOptions::new()
        .max_connections(max_conns)
        .connect_with(opts)
        .await
        .unwrap();

    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    (pool, dir)
}

/// Seed minimal project/task/execution rows for a task.
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
        .bind(execution_id)
        .bind(task_id)
        .execute(pool)
        .await
        .unwrap();
}

/// Open a second pool to the same file (for cross-pool tests).
async fn open_second_pool(dir: &tempfile::TempDir, max_conns: u32) -> SqlitePool {
    let path = dir.path().join("test.db");
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .unwrap()
        .create_if_missing(false)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));

    SqlitePoolOptions::new()
        .max_connections(max_conns)
        .connect_with(opts)
        .await
        .unwrap()
}

// ── Cross-connection concurrency tests ───────────────────────────────

#[tokio::test]
async fn test_cross_pool_concurrent_exact_write_one_winner() {
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    // Seed both pools with different tasks.
    seed_world(&pool1, "p1", "t1", "e1").await;
    seed_world(&pool1, "p2", "t2", "e2").await;

    // Race acquires from two independent pools.
    let (r1, r2) = tokio::join!(
        async {
            let repo = ResourceClaimRepo::new(pool1);
            let g = ClaimGuard {
                task_id: "t1".into(),
                execution_id: "e1".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo",
                    "src/x.rs",
                    AccessMode::Write,
                )],
                project_id: "p1".into(),
                task_id: "t1".into(),
                execution_id: "e1".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-cross-1", &far_future())
                .await
        },
        async {
            let repo = ResourceClaimRepo::new(pool2);
            let g = ClaimGuard {
                task_id: "t2".into(),
                execution_id: "e2".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo",
                    "src/x.rs",
                    AccessMode::Write,
                )],
                project_id: "p2".into(),
                task_id: "t2".into(),
                execution_id: "e2".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-cross-2", &far_future())
                .await
        },
    );

    let ok_count = r1.is_ok() as u8 + r2.is_ok() as u8;
    assert_eq!(ok_count, 2, "both should complete without transport error");

    let acquired_count = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(
        acquired_count, 1,
        "cross-pool concurrent exact write: exactly one should acquire"
    );
}

#[tokio::test]
async fn test_cross_pool_directory_vs_exact_one_winner() {
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    seed_world(&pool1, "p1", "t1", "e1").await;
    seed_world(&pool1, "p2", "t2", "e2").await;

    let (r1, r2) = tokio::join!(
        async {
            let repo = ResourceClaimRepo::new(pool1);
            let g = ClaimGuard {
                task_id: "t1".into(),
                execution_id: "e1".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::directory_prefix(
                    "repo",
                    "src/auth",
                    AccessMode::Write,
                )],
                project_id: "p1".into(),
                task_id: "t1".into(),
                execution_id: "e1".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-dir-cross-1", &far_future())
                .await
        },
        async {
            let repo = ResourceClaimRepo::new(pool2);
            let g = ClaimGuard {
                task_id: "t2".into(),
                execution_id: "e2".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo",
                    "src/auth/login.rs",
                    AccessMode::Write,
                )],
                project_id: "p2".into(),
                task_id: "t2".into(),
                execution_id: "e2".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-dir-cross-2", &far_future())
                .await
        },
    );

    let acquired_count = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(
        acquired_count, 1,
        "cross-pool directory vs exact: exactly one should acquire"
    );
}

#[tokio::test]
async fn test_cross_pool_read_read_both_succeed() {
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    seed_world(&pool1, "p1", "t1", "e1").await;
    seed_world(&pool1, "p2", "t2", "e2").await;

    let (r1, r2) = tokio::join!(
        async {
            let repo = ResourceClaimRepo::new(pool1);
            let g = ClaimGuard {
                task_id: "t1".into(),
                execution_id: "e1".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo",
                    "src/shared.rs",
                    AccessMode::Read,
                )],
                project_id: "p1".into(),
                task_id: "t1".into(),
                execution_id: "e1".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-rr-cross-1", &far_future())
                .await
        },
        async {
            let repo = ResourceClaimRepo::new(pool2);
            let g = ClaimGuard {
                task_id: "t2".into(),
                execution_id: "e2".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo",
                    "src/shared.rs",
                    AccessMode::Read,
                )],
                project_id: "p2".into(),
                task_id: "t2".into(),
                execution_id: "e2".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-rr-cross-2", &far_future())
                .await
        },
    );

    let acquired_count = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(
        acquired_count, 2,
        "cross-pool read/read: both readers should succeed"
    );
}

#[tokio::test]
async fn test_cross_pool_different_repos_both_succeed() {
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    seed_world(&pool1, "p1", "t1", "e1").await;
    seed_world(&pool1, "p2", "t2", "e2").await;

    let (r1, r2) = tokio::join!(
        async {
            let repo = ResourceClaimRepo::new(pool1);
            let g = ClaimGuard {
                task_id: "t1".into(),
                execution_id: "e1".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo-A",
                    "src/a.rs",
                    AccessMode::Write,
                )],
                project_id: "p1".into(),
                task_id: "t1".into(),
                execution_id: "e1".into(),
                repository_identity: "repo-A".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-diff-cross-1", &far_future())
                .await
        },
        async {
            let repo = ResourceClaimRepo::new(pool2);
            let g = ClaimGuard {
                task_id: "t2".into(),
                execution_id: "e2".into(),
                ..guard()
            };
            let spec = ClaimGroupSpec {
                claims: vec![ResourceClaimSpec::exact_file(
                    "repo-B",
                    "src/a.rs",
                    AccessMode::Write,
                )],
                project_id: "p2".into(),
                task_id: "t2".into(),
                execution_id: "e2".into(),
                repository_identity: "repo-B".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-diff-cross-2", &far_future())
                .await
        },
    );

    let acquired_count = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(
        acquired_count, 2,
        "cross-pool different repos: both should acquire"
    );
}

// ── Idempotency TOCTOU closure ───────────────────────────────────────

#[tokio::test]
async fn test_cross_pool_same_ikey_hash_idempotent() {
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    seed_world(&pool1, "p1", "t1", "e1").await;

    // First: acquire on pool1.
    let repo1 = ResourceClaimRepo::new(pool1);
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let r1 = repo1
        .acquire_group(&spec, &guard(), "ikey-tx-1", &far_future())
        .await
        .unwrap();
    assert!(matches!(r1, AcquireOutcome::Acquired(_)));

    // Second: retry with same ikey on pool2 — should get AlreadyAcquired.
    let repo2 = ResourceClaimRepo::new(pool2);
    let r2 = repo2
        .acquire_group(&spec, &guard(), "ikey-tx-1", &far_future())
        .await
        .unwrap();
    assert!(
        matches!(r2, AcquireOutcome::AlreadyAcquired(_)),
        "cross-pool same ikey+hash: should return AlreadyAcquired, got {r2:?}"
    );
}

#[tokio::test]
async fn test_cross_pool_same_ikey_different_hash_conflict() {
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    seed_world(&pool1, "p1", "t1", "e1").await;

    // Acquire one spec on pool1.
    let repo1 = ResourceClaimRepo::new(pool1);
    let spec1 = spec_exact("src/a.rs", AccessMode::Write);
    repo1
        .acquire_group(&spec1, &guard(), "ikey-diff-hash", &far_future())
        .await
        .unwrap();

    // Different spec with same ikey on pool2 — should get IdempotencyConflict.
    let repo2 = ResourceClaimRepo::new(pool2);
    let spec2 = spec_exact("src/b.rs", AccessMode::Read);
    let r2 = repo2
        .acquire_group(&spec2, &guard(), "ikey-diff-hash", &far_future())
        .await
        .unwrap();
    assert!(
        matches!(r2, AcquireOutcome::IdempotencyConflict),
        "cross-pool same ikey different hash: should return IdempotencyConflict, got {r2:?}"
    );
}

#[tokio::test]
async fn test_concurrent_same_ikey_no_db_error() {
    // Verify that concurrent requests with the same ikey produce clean
    // AlreadyAcquired/IdempotencyConflict, not a raw UNIQUE constraint error.
    let (pool1, dir) = open_file_db(2).await;
    let pool2 = open_second_pool(&dir, 2).await;

    seed_world(&pool1, "p1", "t1", "e1").await;
    seed_world(&pool1, "p2", "t2", "e2").await;

    let spec = spec_exact("src/z.rs", AccessMode::Write);
    let spec_owned = spec.clone();

    let (r1, r2) = tokio::join!(
        async {
            let repo = ResourceClaimRepo::new(pool1);
            let g = ClaimGuard {
                task_id: "t1".into(),
                execution_id: "e1".into(),
                ..guard()
            };
            repo.acquire_group(&spec, &g, "ikey-concurrent-same", &far_future())
                .await
        },
        async {
            // Small delay to ensure ordering varies.
            tokio::time::sleep(Duration::from_millis(5)).await;
            let repo = ResourceClaimRepo::new(pool2);
            let g = ClaimGuard {
                task_id: "t2".into(),
                execution_id: "e2".into(),
                ..guard()
            };
            repo.acquire_group(&spec_owned, &g, "ikey-concurrent-same", &far_future())
                .await
        },
    );

    // Both should complete without transport error (no UNIQUE constraint panic).
    assert!(r1.is_ok(), "r1 should not error: {:?}", r1.err());
    assert!(r2.is_ok(), "r2 should not error: {:?}", r2.err());

    // One should be Acquired, the other AlreadyAcquired (or IdempotencyConflict).
    let r1_is_acquired = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_));
    let r2_is_acquired = matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_));
    let r1_is_already = matches!(r1.as_ref().unwrap(), AcquireOutcome::AlreadyAcquired(_));
    let r2_is_already = matches!(r2.as_ref().unwrap(), AcquireOutcome::AlreadyAcquired(_));

    assert!(
        (r1_is_acquired && r2_is_already) || (r2_is_acquired && r1_is_already),
        "one should Acquire, the other AlreadyAcquired. r1={:?} r2={:?}",
        r1.unwrap(),
        r2.unwrap()
    );
}

// ── Atomic event + state ─────────────────────────────────────────────

#[tokio::test]
async fn test_no_partial_group_on_any_failure() {
    let (pool, _dir) = open_file_db(2).await;
    seed_world(&pool, "p1", "t1", "e1").await;
    seed_world(&pool, "p2", "t2", "e2").await;
    let repo = ResourceClaimRepo::new(pool.clone());

    // Acquire a conflicting claim first.
    let spec_conflict = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/a.rs",
            AccessMode::Write,
        )],
        project_id: "p2".into(),
        task_id: "t2".into(),
        execution_id: "e2".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };
    let g2 = ClaimGuard {
        task_id: "t2".into(),
        execution_id: "e2".into(),
        ..guard()
    };
    repo.acquire_group(
        &spec_conflict,
        &g2,
        "ikey-no-partial-conflict",
        &far_future(),
    )
    .await
    .unwrap();

    // Try multi-resource acquire where one conflicts.
    let spec = ClaimGroupSpec {
        claims: vec![
            ResourceClaimSpec::exact_file("repo", "src/a.rs", AccessMode::Read),
            ResourceClaimSpec::exact_file("repo", "src/b.rs", AccessMode::Write),
        ],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-no-partial", &far_future())
        .await
        .unwrap();
    assert!(matches!(result, AcquireOutcome::Conflict { .. }));

    // Verify: no partial rows, no partial group, no leaked idempotency record.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert!(active.is_empty(), "no active claims for t1");

    // Verify no idempotency record was inserted for the failed attempt.
    let idem_row: Option<(String,)> =
        sqlx::query_as("SELECT key FROM idempotency_records WHERE key = 'ikey-no-partial'")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert!(
        idem_row.is_none(),
        "no idempotency record for failed attempt"
    );

    // Verify no event was emitted for the failed attempt.
    let event_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_log WHERE idempotency_key LIKE '%ikey-no-partial%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count.0, 0, "no events for failed attempt");
}

#[tokio::test]
async fn test_response_lost_retry_no_duplicate_event() {
    let (pool, _dir) = open_file_db(2).await;
    seed_world(&pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);

    // First acquire.
    let r1 = repo
        .acquire_group(&spec, &guard(), "ikey-retry-event", &far_future())
        .await
        .unwrap();
    let group_id = match r1 {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Count events after first acquire.
    let count1: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_log WHERE stream_id = ? AND event_type = 'resource_claim_group_acquired'",
    )
    .bind(&group_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count1.0, 1, "exactly one acquire event after first call");

    // Retry with same key (response lost).
    let r2 = repo
        .acquire_group(&spec, &guard(), "ikey-retry-event", &far_future())
        .await
        .unwrap();
    assert!(matches!(r2, AcquireOutcome::AlreadyAcquired(_)));

    // Count events after retry — should still be 1.
    let count2: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_log WHERE stream_id = ? AND event_type = 'resource_claim_group_acquired'",
    )
    .bind(&group_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count2.0, 1, "no duplicate event after idempotent retry");
}

#[tokio::test]
async fn test_replace_atomic_old_released_new_active() {
    let (pool, _dir) = open_file_db(2).await;
    seed_world(&pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(pool.clone());

    // Acquire old group.
    let spec_old = spec_exact("src/a.rs", AccessMode::Write);
    let r = repo
        .acquire_group(
            &spec_old,
            &guard(),
            "ikey-replace-atomic-old",
            &far_future(),
        )
        .await
        .unwrap();
    let old_id = match r {
        AcquireOutcome::Acquired(ref rec) => rec.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Replace.
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
        lease_id: None,
    };
    let rep = repo
        .replace_group(
            &old_id,
            &spec_new,
            &guard(),
            "ikey-replace-atomic-new",
            &far_future(),
        )
        .await
        .unwrap();
    let new_id = match rep {
        AcquireOutcome::Acquired(ref rec) => rec.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Old group is Released.
    let old_rec = repo.get_group(&old_id).await.unwrap();
    assert_eq!(old_rec.lifecycle, ClaimLifecycle::Released);

    // New group is Active.
    let new_rec = repo.get_group(&new_id).await.unwrap();
    assert_eq!(new_rec.lifecycle, ClaimLifecycle::Active);
    assert_eq!(new_rec.claims.len(), 2);

    // Replaced event exists for new group.
    let event_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_log WHERE stream_id = ? AND event_type = 'resource_claim_group_replaced'",
    )
    .bind(&new_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count.0, 1, "replaced event emitted");
}

#[tokio::test]
async fn test_replace_response_lost_retry_idempotent() {
    let (pool, _dir) = open_file_db(2).await;
    seed_world(&pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(pool.clone());

    let spec_old = spec_exact("src/a.rs", AccessMode::Write);
    let r = repo
        .acquire_group(&spec_old, &guard(), "ikey-rep-retry-old", &far_future())
        .await
        .unwrap();
    let old_id = match r {
        AcquireOutcome::Acquired(ref rec) => rec.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    let spec_new = spec_exact("src/b.rs", AccessMode::Write);

    // First replace.
    let rep1 = repo
        .replace_group(
            &old_id,
            &spec_new,
            &guard(),
            "ikey-rep-retry-new",
            &far_future(),
        )
        .await
        .unwrap();
    assert!(matches!(rep1, AcquireOutcome::Acquired(_)));

    // Retry with same ikey — AlreadyAcquired.
    let rep2 = repo
        .replace_group(
            &old_id,
            &spec_new,
            &guard(),
            "ikey-rep-retry-new",
            &far_future(),
        )
        .await
        .unwrap();
    assert!(
        matches!(rep2, AcquireOutcome::AlreadyAcquired(_)),
        "replace retry should be AlreadyAcquired, got {rep2:?}"
    );
}

// ── Reconciler closure tests ─────────────────────────────────────────

#[tokio::test]
async fn test_reconciler_detects_incomplete_claim_group() {
    let (pool, _dir) = open_file_db(2).await;
    seed_world(&pool, "p1", "t1", "e1").await;

    // Create a workspace_leases row so bad-lease detection doesn't expire first.
    let far = far_future();
    sqlx::query(
        "INSERT INTO workspace_leases (id, task_id, owner_execution_id, lifecycle, worktree_path, branch_name, expires_at) VALUES ('lease-1', 't1', 'e1', 'acquired', '/repo/wt', 'br', ?)",
    )
    .bind(&far)
    .execute(&pool)
    .await
    .unwrap();

    let repo = ResourceClaimRepo::new(pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let r = repo
        .acquire_group(&spec, &guard(), "ikey-inc-group", &far_future())
        .await
        .unwrap();
    let group_id = match r {
        AcquireOutcome::Acquired(ref rec) => rec.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Simulate an incomplete state: manually set one claim row to 'expired'.
    sqlx::query(
        "UPDATE resource_claims SET lifecycle = 'expired' WHERE rowid = (SELECT rowid FROM resource_claims WHERE group_id = ? LIMIT 1)",
    )
    .bind(&group_id)
    .execute(&pool)
    .await
    .unwrap();

    let reconciler = ResourceClaimReconciler::new(pool.clone());
    let report = reconciler.reconcile().await.unwrap();
    let has_incomplete = report.anomalies.iter().any(|a| {
        matches!(
            a,
            harness_runtime::resource_claim::ClaimAnomaly::IncompleteClaimGroup { .. }
        )
    });
    assert!(
        has_incomplete,
        "reconciler should detect incomplete claim group"
    );
    assert!(
        report.expired.contains(&group_id),
        "incomplete group should be expired"
    );
}

#[tokio::test]
async fn test_reconciler_detects_repository_identity_mismatch() {
    let (pool, _dir) = open_file_db(2).await;
    seed_world(&pool, "p1", "t1", "e1").await;

    // Create a workspace_leases row to prevent bad-lease detection.
    let far_future_ts = far_future();
    sqlx::query(
        "INSERT INTO workspace_leases (id, task_id, owner_execution_id, lifecycle, worktree_path, branch_name, expires_at) VALUES ('lease-1', 't1', 'e1', 'acquired', '/repo/wt', 'br', ?)",
    )
    .bind(&far_future_ts)
    .execute(&pool)
    .await
    .unwrap();

    // Need a worktree row with a different repository_identity.
    sqlx::query(
        "INSERT INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status, lease_epoch) VALUES ('wt-mismatch', 'p1', 't1', 'e1', '/repo', 'repo-OTHER', '/repo/wt', 'br', 'abc123', 's1', 'op1', 'active', 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let repo = ResourceClaimRepo::new(pool.clone());
    let spec = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo-MY",
            "src/a.rs",
            AccessMode::Write,
        )],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo-MY".into(),
        worktree_id: Some("wt-mismatch".into()),
        lease_id: None,
    };
    let r = repo
        .acquire_group(&spec, &guard(), "ikey-repo-mismatch", &far_future())
        .await
        .unwrap();
    assert!(matches!(r, AcquireOutcome::Acquired(_)));

    let reconciler = ResourceClaimReconciler::new(pool.clone());
    let report = reconciler.reconcile().await.unwrap();
    let has_mismatch = report.anomalies.iter().any(|a| {
        matches!(
            a,
            harness_runtime::resource_claim::ClaimAnomaly::RepositoryIdentityMismatch { .. }
        )
    });
    assert!(
        has_mismatch,
        "reconciler should detect repository identity mismatch"
    );
}
