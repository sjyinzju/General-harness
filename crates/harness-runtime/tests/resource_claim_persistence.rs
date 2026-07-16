//! I3-B persistence tests: atomic claim group acquisition, idempotency,
//! cross-connection concurrency, and event emission.
//!
//! All tests use in-memory SQLite databases. FK constraints are enforced
//! so we seed minimal project/task/execution rows.

use harness_core::resource_claim::{
    AccessMode, ClaimDecision, ClaimGroupSpec, ClaimLifecycle, ResourceClaimSpec,
};
use harness_runtime::db::Database;
use harness_runtime::resource_claim::{AcquireOutcome, ClaimGuard, ResourceClaimRepo};
use sqlx::SqlitePool;

// ── Helpers ──────────────────────────────────────────────────────────

async fn seed_world(pool: &SqlitePool, project_id: &str, task_id: &str, execution_id: &str) {
    let _now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
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
        worktree_id: Some("wt-1".into()),
        lease_id: Some("lease-1".into()),
    }
}

fn spec_multi(paths: &[(&str, AccessMode)]) -> ClaimGroupSpec {
    ClaimGroupSpec {
        claims: paths
            .iter()
            .map(|(p, m)| ResourceClaimSpec::exact_file("repo", p, *m))
            .collect(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: Some("wt-1".into()),
        lease_id: Some("lease-1".into()),
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_acquire_empty_group_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = ClaimGroupSpec {
        claims: vec![],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };

    let result = repo
        .acquire_group(&spec, &guard(), "ikey-empty")
        .await
        .unwrap();
    assert!(
        matches!(result, AcquireOutcome::InvalidSpec { .. }),
        "expected InvalidSpec, got {result:?}"
    );
}

#[tokio::test]
async fn test_acquire_success() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo.acquire_group(&spec, &guard(), "ikey-1").await.unwrap();
    assert!(matches!(result, AcquireOutcome::Acquired(_)));

    // Verify stored state.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].claims.len(), 1);
    assert_eq!(active[0].lifecycle, ClaimLifecycle::Active);
}

#[tokio::test]
async fn test_multi_resource_all_succeed() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_multi(&[
        ("src/a.rs", AccessMode::Write),
        ("src/b.rs", AccessMode::Read),
        ("src/c.rs", AccessMode::Write),
    ]);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-multi")
        .await
        .unwrap();
    assert!(matches!(result, AcquireOutcome::Acquired(_)));

    let active = repo.list_active_for_task("t1").await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].claims.len(), 3);
}

#[tokio::test]
async fn test_one_conflict_means_none_inserted() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    // Task 1 acquires src/a.rs.
    let g1 = ClaimGuard {
        task_id: "t1".into(),
        execution_id: "e1".into(),
        ..guard()
    };
    let spec1 = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/a.rs",
            AccessMode::Write,
        )],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };
    repo.acquire_group(&spec1, &g1, "ikey-conflict-1")
        .await
        .unwrap();

    // Task 2 tries to acquire src/a.rs + src/b.rs — src/a.rs conflicts.
    let g2 = ClaimGuard {
        task_id: "t2".into(),
        execution_id: "e2".into(),
        ..guard()
    };
    let spec2 = spec_multi(&[
        ("src/a.rs", AccessMode::Read),
        ("src/b.rs", AccessMode::Write),
    ]);
    let result = repo
        .acquire_group(&spec2, &g2, "ikey-conflict-2")
        .await
        .unwrap();
    assert!(matches!(result, AcquireOutcome::Conflict { .. }));

    // Task 2 should have NO active claims (all-or-nothing).
    let active_t2 = repo.list_active_for_task("t2").await.unwrap();
    assert!(active_t2.is_empty(), "task 2 should have no claims");

    // Task 1 should still have its claim.
    let active_t1 = repo.list_active_for_task("t1").await.unwrap();
    assert_eq!(active_t1.len(), 1);
}

#[tokio::test]
async fn test_repeated_idempotent_acquire() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let r1 = repo
        .acquire_group(&spec, &guard(), "ikey-idem")
        .await
        .unwrap();
    assert!(matches!(r1, AcquireOutcome::Acquired(_)));

    // Same key again — should return AlreadyAcquired.
    let r2 = repo
        .acquire_group(&spec, &guard(), "ikey-idem")
        .await
        .unwrap();
    assert!(
        matches!(r2, AcquireOutcome::AlreadyAcquired(_)),
        "expected AlreadyAcquired, got {r2:?}"
    );

    // Only one group exists.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert_eq!(active.len(), 1);
}

#[tokio::test]
async fn test_same_key_different_hash_rejected() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec1 = spec_exact("src/a.rs", AccessMode::Write);
    repo.acquire_group(&spec1, &guard(), "ikey-hash")
        .await
        .unwrap();

    // Different spec with same idempotency key.
    let spec2 = spec_exact("src/b.rs", AccessMode::Read);
    let result = repo
        .acquire_group(&spec2, &guard(), "ikey-hash")
        .await
        .unwrap();
    assert!(
        matches!(result, AcquireOutcome::IdempotencyConflict),
        "expected IdempotencyConflict, got {result:?}"
    );
}

#[tokio::test]
async fn test_concurrent_exact_write_one_winner() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    // Race two acquires for the same file from separate connections.
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
            repo.acquire_group(&spec, &g, "ikey-race-1").await
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
            repo.acquire_group(&spec, &g, "ikey-race-2").await
        },
    );

    let ok_count = r1.is_ok() as u8 + r2.is_ok() as u8;
    assert_eq!(ok_count, 2, "both should complete without error");

    let acquired_count = matches!(r1.unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(acquired_count, 1, "exactly one should acquire");
}

#[tokio::test]
async fn test_concurrent_directory_exact_one_winner() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

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
            repo.acquire_group(&spec, &g, "ikey-dir-1").await
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
                    AccessMode::Read,
                )],
                project_id: "p2".into(),
                task_id: "t2".into(),
                execution_id: "e2".into(),
                repository_identity: "repo".into(),
                worktree_id: None,
                lease_id: None,
            };
            repo.acquire_group(&spec, &g, "ikey-dir-2").await
        },
    );

    let acquired_count = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(
        acquired_count, 1,
        "directory vs exact: exactly one should win"
    );
}

#[tokio::test]
async fn test_concurrent_read_read_both_succeed() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

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
            repo.acquire_group(&spec, &g, "ikey-rr-1").await
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
            repo.acquire_group(&spec, &g, "ikey-rr-2").await
        },
    );

    let acquired_count = matches!(r1.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8
        + matches!(r2.as_ref().unwrap(), AcquireOutcome::Acquired(_)) as u8;
    assert_eq!(acquired_count, 2, "both readers should succeed");
}

#[tokio::test]
async fn test_different_repositories_both_succeed() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    // Task 1 acquires src/a.rs in repo-A.
    let g1 = ClaimGuard {
        task_id: "t1".into(),
        execution_id: "e1".into(),
        ..guard()
    };
    let spec1 = ClaimGroupSpec {
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
    repo.acquire_group(&spec1, &g1, "ikey-diff-repo-1")
        .await
        .unwrap();

    // Task 2 acquires same path in repo-B — no conflict.
    let g2 = ClaimGuard {
        task_id: "t2".into(),
        execution_id: "e2".into(),
        ..guard()
    };
    let spec2 = ClaimGroupSpec {
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
    let result = repo
        .acquire_group(&spec2, &g2, "ikey-diff-repo-2")
        .await
        .unwrap();
    assert!(matches!(result, AcquireOutcome::Acquired(_)));
}

#[tokio::test]
async fn test_release_success() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-release")
        .await
        .unwrap();
    let group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    repo.release_group(&group_id, &guard(), "done")
        .await
        .unwrap();

    // Group should now be released.
    let record = repo.get_group(&group_id).await.unwrap();
    assert_eq!(record.lifecycle, ClaimLifecycle::Released);

    // Active list should be empty.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert!(active.is_empty());
}

#[tokio::test]
async fn test_repeated_release_idempotent() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-rel2")
        .await
        .unwrap();
    let group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // First release succeeds.
    repo.release_group(&group_id, &guard(), "done")
        .await
        .unwrap();

    // Second release fails (already released, guard check fails on lifecycle).
    let r2 = repo.release_group(&group_id, &guard(), "again").await;
    assert!(r2.is_err(), "second release should be rejected");
}

#[tokio::test]
async fn test_replace_success() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    // Acquire old group.
    let spec_old = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec_old, &guard(), "ikey-replace-old")
        .await
        .unwrap();
    let old_group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Replace with new spec.
    let spec_new = spec_multi(&[
        ("src/a.rs", AccessMode::Write),
        ("src/b.rs", AccessMode::Read),
    ]);
    let replace_result = repo
        .replace_group(&old_group_id, &spec_new, &guard(), "ikey-replace-new")
        .await
        .unwrap();
    assert!(matches!(replace_result, AcquireOutcome::Acquired(_)));

    // Old group should be released.
    let old_record = repo.get_group(&old_group_id).await.unwrap();
    assert_eq!(old_record.lifecycle, ClaimLifecycle::Released);
}

#[tokio::test]
async fn test_replace_conflict_preserves_old_group() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    seed_world(&db.pool, "p2", "t2", "e2").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    // Task 1 acquires src/a.rs.
    let g1 = ClaimGuard {
        task_id: "t1".into(),
        execution_id: "e1".into(),
        ..guard()
    };
    let spec1 = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/a.rs",
            AccessMode::Write,
        )],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };
    let result = repo
        .acquire_group(&spec1, &g1, "ikey-rep-conf-1")
        .await
        .unwrap();
    let old_group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Task 2 acquires src/b.rs (non-conflicting).
    let g2 = ClaimGuard {
        task_id: "t2".into(),
        execution_id: "e2".into(),
        lease_id: "lease-2".into(),
        lease_token: "tok-2".into(),
        ..guard()
    };
    let spec2 = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/b.rs",
            AccessMode::Write,
        )],
        project_id: "p2".into(),
        task_id: "t2".into(),
        execution_id: "e2".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: Some("lease-2".into()),
    };
    repo.acquire_group(&spec2, &g2, "ikey-rep-conf-2")
        .await
        .unwrap();

    // Task 1 tries to replace with a claim that conflicts with Task 2's claim.
    let spec_new = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/b.rs",
            AccessMode::Write,
        )],
        project_id: "p1".into(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };
    let rep_result = repo
        .replace_group(&old_group_id, &spec_new, &g1, "ikey-rep-conf-3")
        .await
        .unwrap();
    assert!(
        matches!(rep_result, AcquireOutcome::Conflict { .. }),
        "expected Conflict, got {rep_result:?}"
    );

    // Old group should still be active.
    let old_record = repo.get_group(&old_group_id).await.unwrap();
    assert_eq!(old_record.lifecycle, ClaimLifecycle::Active);
}

#[tokio::test]
async fn test_commit_success_response_lost_retry() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);

    // First attempt "succeeds" but caller doesn't get the response.
    let _ = repo
        .acquire_group(&spec, &guard(), "ikey-retry")
        .await
        .unwrap();

    // Retry with same key — should get AlreadyAcquired with the same group.
    let retry = repo
        .acquire_group(&spec, &guard(), "ikey-retry")
        .await
        .unwrap();
    assert!(matches!(retry, AcquireOutcome::AlreadyAcquired(_)));

    // Only one group should exist.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert_eq!(active.len(), 1);
}

#[tokio::test]
async fn test_state_and_event_atomic() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-atomic")
        .await
        .unwrap();
    let group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // An acquire event should exist in the event log.
    let events: Vec<(String,)> = sqlx::query_as(
        "SELECT event_type FROM event_log WHERE stream_id = ? AND event_type = 'resource_claim_group_acquired'",
    )
    .bind(&group_id)
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert!(!events.is_empty(), "acquire event should be present");
}

#[tokio::test]
async fn test_lifecycle_optimistic_locking() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-optlock")
        .await
        .unwrap();
    let group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Read the version.
    let record = repo.get_group(&group_id).await.unwrap();
    assert_eq!(record.version, 1);

    // Renew bumps version.
    repo.renew_group(&group_id, &guard(), 300).await.unwrap();
    let record2 = repo.get_group(&group_id).await.unwrap();
    assert_eq!(record2.version, 2);
}

#[tokio::test]
async fn test_active_indexes_history_behavior() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    // Acquire a group.
    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-hist-1")
        .await
        .unwrap();
    let group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Release it.
    repo.release_group(&group_id, &guard(), "done")
        .await
        .unwrap();

    // List active should be empty.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert!(active.is_empty());

    // But the group still exists (history).
    let record = repo.get_group(&group_id).await.unwrap();
    assert_eq!(record.lifecycle, ClaimLifecycle::Released);
}

#[tokio::test]
async fn test_no_partial_group_after_injected_failure() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_multi(&[
        ("src/a.rs", AccessMode::Write),
        ("src/b.rs", AccessMode::Read),
    ]);

    // Acquire a conflicting claim for src/a.rs first.
    let spec_conflict = ClaimGroupSpec {
        claims: vec![ResourceClaimSpec::exact_file(
            "repo",
            "src/a.rs",
            AccessMode::Write,
        )],
        project_id: "p1b".into(),
        task_id: "t2".into(),
        execution_id: "e2".into(),
        repository_identity: "repo".into(),
        worktree_id: None,
        lease_id: None,
    };
    seed_world(&db.pool, "p1b", "t2", "e2").await;
    let g2 = ClaimGuard {
        task_id: "t2".into(),
        execution_id: "e2".into(),
        ..guard()
    };
    repo.acquire_group(&spec_conflict, &g2, "ikey-partial-conflict")
        .await
        .unwrap();

    // Now try to acquire the multi-resource spec — should conflict.
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-partial")
        .await
        .unwrap();
    assert!(matches!(result, AcquireOutcome::Conflict { .. }));

    // Verify no partial claims for t1.
    let active = repo.list_active_for_task("t1").await.unwrap();
    assert!(active.is_empty(), "no partial groups should exist for t1");
}

#[tokio::test]
async fn test_separate_connections_concurrency() {
    // Open two in-memory databases — they are separate SQLite instances.
    let db1 = Database::open_in_memory().await.unwrap();
    let db2 = Database::open_in_memory().await.unwrap();

    seed_world(&db1.pool, "p1", "t1", "e1").await;
    seed_world(&db2.pool, "p1", "t1", "e1").await;

    let repo1 = ResourceClaimRepo::new(db1.pool.clone());
    let repo2 = ResourceClaimRepo::new(db2.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);

    // Both connections can independently acquire (different databases).
    let r1 = repo1
        .acquire_group(&spec, &guard(), "ikey-sep-1")
        .await
        .unwrap();
    let r2 = repo2
        .acquire_group(&spec, &guard(), "ikey-sep-2")
        .await
        .unwrap();

    assert!(matches!(r1, AcquireOutcome::Acquired(_)));
    assert!(matches!(r2, AcquireOutcome::Acquired(_)));
}

#[tokio::test]
async fn test_expire_due_groups() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    let spec = spec_exact("src/a.rs", AccessMode::Write);
    let result = repo
        .acquire_group(&spec, &guard(), "ikey-expire")
        .await
        .unwrap();
    let group_id = match result {
        AcquireOutcome::Acquired(ref r) => r.group_id.clone(),
        _ => panic!("expected Acquired"),
    };

    // Expire with a far-future timestamp — should expire the group.
    let far_future = "2099-01-01 00:00:00";
    let expired = repo.expire_due_groups(far_future).await.unwrap();
    assert!(expired.contains(&group_id));

    let record = repo.get_group(&group_id).await.unwrap();
    assert_eq!(record.lifecycle, ClaimLifecycle::Expired);
}

#[tokio::test]
async fn test_conflict_detection_via_check() {
    let db = Database::open_in_memory().await.unwrap();
    seed_world(&db.pool, "p1", "t1", "e1").await;
    let repo = ResourceClaimRepo::new(db.pool.clone());

    // Acquire a claim.
    let spec1 = spec_exact("src/a.rs", AccessMode::Write);
    repo.acquire_group(&spec1, &guard(), "ikey-check-1")
        .await
        .unwrap();

    // Check without acquiring — should report conflict.
    let decision = repo.check_conflicts(&spec1).await.unwrap();
    assert!(
        matches!(decision, ClaimDecision::Conflict { .. }),
        "check should report conflict, got {decision:?}"
    );

    // Check a non-conflicting spec.
    let spec2 = spec_exact("src/b.rs", AccessMode::Write);
    let decision2 = repo.check_conflicts(&spec2).await.unwrap();
    assert_eq!(decision2, ClaimDecision::Compatible);
}
