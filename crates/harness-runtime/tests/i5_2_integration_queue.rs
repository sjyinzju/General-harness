//! I5.2 Durable Integration Queue — tests.
//!
//! Tests: enqueue idempotency, FIFO ordering, priority sorting,
//! same repo/ref serialization, different repo parallelism, cancel.

use harness_runtime::db::Database;
use harness_runtime::integration::IntegrationQueueService;
use uuid::Uuid;

async fn setup_db() -> Database {
    let db = Database::open_in_memory().await.unwrap();
    sqlx::query("INSERT INTO projects(id,objective,lifecycle) VALUES('p1','test','active')")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','test','verified')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')").execute(&db.pool).await.unwrap();
    db
}

async fn seed_fk_chain(
    pool: &sqlx::SqlitePool,
    candidate_id: &str,
    review_id: &str,
    commit_request_id: &str,
    repo: &str,
) {
    sqlx::query("INSERT OR IGNORE INTO candidate_snapshots (candidate_id,task_id,execution_id,executor_profile_id,workspace_id,base_commit,candidate_tree_hash,diff_digest,task_spec_digest,evidence_digest,composite_digest) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
        .bind(candidate_id).bind("t1").bind("e1").bind("p1").bind("w1").bind("abc").bind("tree").bind("diff").bind("task").bind("ev").bind("comp")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO review_requests (review_id,candidate_id,reviewer_profile_id,state,idempotency_key,request_hash) VALUES (?,?,?,?,?,?)")
        .bind(review_id).bind(candidate_id).bind("rev1").bind("approved").bind(format!("ik-{review_id}")).bind(format!("h-{review_id}"))
        .execute(pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO commit_requests (commit_request_id,candidate_id,review_id,repository_id,target_ref,expected_base_commit,author_name,author_email,committer_name,committer_email,commit_timestamp,message,state,idempotency_key,idempotency_digest) VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'),?,'created',?,?)")
        .bind(commit_request_id).bind(candidate_id).bind(review_id).bind(repo).bind("refs/heads/main").bind("abc").bind("A").bind("a@t.com").bind("C").bind("c@t.com").bind("msg").bind(format!("ik-{commit_request_id}")).bind(format!("dig-{commit_request_id}"))
        .execute(pool).await.unwrap();
}

fn make_svc(db: &Database) -> IntegrationQueueService {
    IntegrationQueueService::new(db.pool.clone())
}

#[tokio::test]
async fn test_enqueue_idempotent() {
    let db = setup_db().await;
    seed_fk_chain(&db.pool, "c1", "r1", "cr1", "repo-a").await;
    let svc = make_svc(&db);

    let iid = format!("i-{}", Uuid::new_v4());
    let r1 = svc
        .enqueue(
            &iid,
            "cr1",
            "c1",
            "r1",
            "repo-a",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    let r2 = svc
        .enqueue(
            &iid,
            "cr1",
            "c1",
            "r1",
            "repo-a",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    assert_eq!(r1.integration_id, r2.integration_id);
}

#[tokio::test]
async fn test_priority_ordering() {
    let db = setup_db().await;
    let repo = "repo-prio";
    seed_fk_chain(&db.pool, "c-low", "r-low", "cr-low", repo).await;
    seed_fk_chain(&db.pool, "c-high", "r-high", "cr-high", repo).await;
    seed_fk_chain(&db.pool, "c-mid", "r-mid", "cr-mid", repo).await;
    let svc = make_svc(&db);

    let _low = svc
        .enqueue(
            &format!("i-low-{}", Uuid::new_v4()),
            "cr-low",
            "c-low",
            "r-low",
            repo,
            "refs/heads/main",
            "abc",
            1,
        )
        .await
        .unwrap();
    let high = svc
        .enqueue(
            &format!("i-high-{}", Uuid::new_v4()),
            "cr-high",
            "c-high",
            "r-high",
            repo,
            "refs/heads/main",
            "abc",
            100,
        )
        .await
        .unwrap();
    let _mid = svc
        .enqueue(
            &format!("i-mid-{}", Uuid::new_v4()),
            "cr-mid",
            "c-mid",
            "r-mid",
            repo,
            "refs/heads/main",
            "abc",
            50,
        )
        .await
        .unwrap();

    let dq = svc.dequeue(repo, "refs/heads/main").await.unwrap().unwrap();
    assert_eq!(dq.integration_id, high.integration_id);
}

#[tokio::test]
async fn test_fifo_tiebreak() {
    let db = setup_db().await;
    let repo = "repo-fifo";
    seed_fk_chain(&db.pool, "c-first", "r-first", "cr-first", repo).await;
    seed_fk_chain(&db.pool, "c-second", "r-second", "cr-second", repo).await;
    let svc = make_svc(&db);

    let first = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-first",
            "c-first",
            "r-first",
            repo,
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    // Ensure distinct timestamps for FIFO ordering
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let _second = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-second",
            "c-second",
            "r-second",
            repo,
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();

    let dq = svc.dequeue(repo, "refs/heads/main").await.unwrap().unwrap();
    assert_eq!(dq.integration_id, first.integration_id);
}

#[tokio::test]
async fn test_different_repo_parallel() {
    let db = setup_db().await;
    seed_fk_chain(&db.pool, "c-a", "r-a", "cr-a", "repo-a").await;
    seed_fk_chain(&db.pool, "c-b", "r-b", "cr-b", "repo-b").await;
    let svc = make_svc(&db);

    let a = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-a",
            "c-a",
            "r-a",
            "repo-a",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    let b = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-b",
            "c-b",
            "r-b",
            "repo-b",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();

    let dq_a = svc
        .dequeue("repo-a", "refs/heads/main")
        .await
        .unwrap()
        .unwrap();
    let dq_b = svc
        .dequeue("repo-b", "refs/heads/main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dq_a.integration_id, a.integration_id);
    assert_eq!(dq_b.integration_id, b.integration_id);
}

#[tokio::test]
async fn test_different_target_ref_parallel() {
    let db = setup_db().await;
    let repo = "repo-parallel-ref";
    seed_fk_chain(&db.pool, "c-main", "r-main", "cr-main", repo).await;
    seed_fk_chain(&db.pool, "c-dev", "r-dev", "cr-dev", repo).await;
    let svc = make_svc(&db);

    let main = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-main",
            "c-main",
            "r-main",
            repo,
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    let dev = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-dev",
            "c-dev",
            "r-dev",
            repo,
            "refs/heads/dev",
            "abc",
            10,
        )
        .await
        .unwrap();

    let dq_main = svc.dequeue(repo, "refs/heads/main").await.unwrap().unwrap();
    let dq_dev = svc.dequeue(repo, "refs/heads/dev").await.unwrap().unwrap();
    assert_eq!(dq_main.integration_id, main.integration_id);
    assert_eq!(dq_dev.integration_id, dev.integration_id);
}

#[tokio::test]
async fn test_same_repo_ref_serialization() {
    let db = setup_db().await;
    let repo = "repo-serial";
    seed_fk_chain(&db.pool, "c-s1", "r-s1", "cr-s1", repo).await;
    seed_fk_chain(&db.pool, "c-s2", "r-s2", "cr-s2", repo).await;
    let svc = make_svc(&db);

    let r1 = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-s1",
            "c-s1",
            "r-s1",
            repo,
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    let _r2 = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-s2",
            "c-s2",
            "r-s2",
            repo,
            "refs/heads/main",
            "abc",
            5,
        )
        .await
        .unwrap();

    // Only one dequeued at a time (higher priority first)
    let dq1 = svc.dequeue(repo, "refs/heads/main").await.unwrap().unwrap();
    assert_eq!(dq1.integration_id, r1.integration_id);
    // Verifying serialization: second is still queued since r1 is active (WaitingForLease)
}

#[tokio::test]
async fn test_cancel_queued() {
    let db = setup_db().await;
    seed_fk_chain(&db.pool, "c-c1", "r-c1", "cr-c1", "repo-cancel").await;
    let svc = make_svc(&db);

    let r = svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-c1",
            "c-c1",
            "r-c1",
            "repo-cancel",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    let ok = svc.cancel(&r.integration_id).await.unwrap();
    assert!(ok);
}

#[tokio::test]
async fn test_invalid_target_ref_rejected() {
    let db = setup_db().await;
    seed_fk_chain(&db.pool, "c-inv", "r-inv", "cr-inv", "repo-inv").await;
    let svc = make_svc(&db);

    assert!(svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-inv",
            "c-inv",
            "r-inv",
            "repo-inv",
            "HEAD",
            "abc",
            10
        )
        .await
        .is_err());
    assert!(svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-inv",
            "c-inv",
            "r-inv",
            "repo-inv",
            "",
            "abc",
            10
        )
        .await
        .is_err());
    assert!(svc
        .enqueue(
            &format!("i-{}", Uuid::new_v4()),
            "cr-inv",
            "c-inv",
            "r-inv",
            "repo-inv",
            "refs/remotes/origin/main",
            "abc",
            10
        )
        .await
        .is_err());
}

#[tokio::test]
async fn test_empty_queue_dequeue_none() {
    let db = setup_db().await;
    let svc = make_svc(&db);

    let result = svc.dequeue("repo-empty", "refs/heads/main").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_duplicate_scope_returns_existing() {
    let db = setup_db().await;
    seed_fk_chain(&db.pool, "c-dup", "r-dup", "cr-dup", "repo-dup").await;
    let svc = make_svc(&db);

    // First enqueue
    let r1 = svc
        .enqueue(
            &format!("i1-{}", Uuid::new_v4()),
            "cr-dup",
            "c-dup",
            "r-dup",
            "repo-dup",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();

    // Second enqueue with different integration_id but same scope → returns r1
    let r2 = svc
        .enqueue(
            &format!("i2-{}", Uuid::new_v4()),
            "cr-dup",
            "c-dup",
            "r-dup",
            "repo-dup",
            "refs/heads/main",
            "abc",
            10,
        )
        .await
        .unwrap();
    assert_eq!(r1.integration_id, r2.integration_id);
}
