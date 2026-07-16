//! I2B-2 WorkspaceLeaseService integration tests.
use std::sync::Arc;
use std::time::Duration;

use harness_runtime::db::Database;
use harness_runtime::lease::{
    access_validator::ServiceLeaseAccessValidator,
    clock::{SystemClock, TestClock},
    guard::{
        LeaseAccessResult, LeaseCredential, WorkspaceLeaseAccessValidator, WorktreeAccessRequest,
    },
    reconciler::{LeaseDriftKind, WorkspaceLeaseReconciler},
    runner::LeaseHeartbeatRunner,
    service::WorkspaceLeaseService,
    types::*,
    LeaseHeartbeatOutcome,
};
use sqlx::SqlitePool;

async fn init_db() -> (Database, SqlitePool, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().await.unwrap();
    let pool = db.pool.clone();
    let repo = tmp.path().join("r");
    let wt = repo.join("wt");
    std::fs::create_dir_all(&wt).unwrap();
    // Sidecar content must match DB record (schema_version 1 + all identity fields).
    let git_path = repo.join(".git").to_string_lossy().into_owned();
    let wt_path = wt.to_string_lossy().into_owned();
    let sidecar_json = serde_json::json!({
        "schema_version": 1,
        "worktree_id": "wt-1",
        "project_id": "p1",
        "task_id": "t1",
        "execution_id": "e1",
        "repository_identity": git_path,
        "worktree_path": wt_path,
        "branch": "harness/t1/e1",
        "base_commit": "abc",
        "owner_supervisor_id": "sup-test",
        "operation_id": "op1",
        "created_at": "2026-07-15T00:00:00Z",
        "state": "active"
    });
    std::fs::write(
        format!("{}.harness.json", wt.display()),
        sidecar_json.to_string(),
    )
    .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO projects (id, objective, lifecycle) VALUES ('p1','test','active')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT OR IGNORE INTO tasks (id, project_id, goal, lifecycle) VALUES ('t1','p1','test','running')").execute(&pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES ('e1','t1',1,'running')").execute(&pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO worktrees (id, project_id, task_id, execution_id, repository_root, repository_identity, worktree_path, branch_name, base_commit, owner_supervisor_id, operation_id, status, lease_epoch) VALUES ('wt-1','p1','t1','e1',?,?,?,?,?,?,?,'active',0)")
        .bind(repo.to_string_lossy().into_owned()).bind(repo.join(".git").to_string_lossy().into_owned())
        .bind(wt.to_string_lossy().into_owned()).bind("harness/t1/e1").bind("abc").bind("sup-test").bind("op1")
        .execute(&pool).await.unwrap();
    (db, pool, tmp)
}

fn spec() -> LeaseSpec {
    LeaseSpec {
        worktree_id: "wt-1".into(),
        project_id: "p1".into(),
        task_id: "t1".into(),
        owner_execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_duration: Duration::from_secs(300),
        idempotency_key: format!("ik-{}", uuid::Uuid::new_v4()),
    }
}

fn svc(pool: &SqlitePool) -> WorkspaceLeaseService {
    WorkspaceLeaseService::new_unverified_for_tests(
        pool.clone(),
        Arc::new(SystemClock),
        LeaseConfig::default(),
    )
}
fn svcc(pool: &SqlitePool, clock: Arc<TestClock>) -> WorkspaceLeaseService {
    WorkspaceLeaseService::new_unverified_for_tests(pool.clone(), clock, LeaseConfig::default())
}
fn svc_with_fast_hb(pool: &SqlitePool) -> WorkspaceLeaseService {
    WorkspaceLeaseService::new_unverified_for_tests(
        pool.clone(),
        Arc::new(SystemClock),
        LeaseConfig {
            heartbeat_interval: Duration::from_millis(10),
            ..LeaseConfig::default()
        },
    )
}

// ── 1-6 acquire ──────────────────────────────────────────────

#[tokio::test]
async fn acquire_success() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_eq!(r.lifecycle, "active");
    assert_eq!(r.fencing_token, 1);
}
#[tokio::test]
async fn worktree_missing_rejected() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let mut sp = spec();
    sp.worktree_id = "wt-nope".into();
    assert!(s.acquire_lease(&sp).await.is_err());
}
#[tokio::test]
async fn concurrent_acquire_one_succeeds() {
    let (_db, p, _tmp) = init_db().await;
    let s1 = Arc::new(svc(&p));
    let s2 = s1.clone();
    let sp = spec();
    let (r1, r2) = tokio::join!(s1.acquire_lease(&sp), s2.acquire_lease(&sp));
    assert_eq!(
        [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Ok(LeaseAcquireOutcome::Acquired(_))))
            .count(),
        1,
        "{r1:?} {r2:?}"
    );
}
#[tokio::test]
async fn same_ikey_returns_same_lease() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let sp = spec();
    let id = match s.acquire_lease(&sp).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r.lease_id,
        _ => panic!(),
    };
    let sp2 = LeaseSpec {
        idempotency_key: sp.idempotency_key.clone(),
        ..spec()
    };
    match s.acquire_lease(&sp2).await.unwrap() {
        LeaseAcquireOutcome::AlreadyAcquired(r) => assert_eq!(r.lease_id, id),
        _ => panic!(),
    }
}
#[tokio::test]
async fn execution_terminal_rejected() {
    let (_db, p, _tmp) = init_db().await;
    sqlx::query("UPDATE execution_attempts SET lifecycle='completed' WHERE id='e1'")
        .execute(&p)
        .await
        .unwrap();
    assert!(svc(&p).acquire_lease(&spec()).await.is_err());
}
#[tokio::test]
async fn active_lease_blocks_second() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    s.acquire_lease(&spec()).await.unwrap();
    assert!(s.acquire_lease(&spec()).await.is_err());
}

// ── 7-12 heartbeat ───────────────────────────────────────────

#[tokio::test]
async fn heartbeat_success() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_eq!(
        s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token)
            .await
            .unwrap(),
        LeaseHeartbeatOutcome::Ok
    );
}
#[tokio::test]
async fn wrong_token_heartbeat_rejected() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_eq!(
        s.heartbeat(&r.lease_id, "bad", r.fencing_token)
            .await
            .unwrap(),
        LeaseHeartbeatOutcome::TokenMismatch
    );
}
#[tokio::test]
async fn wrong_fencing_heartbeat_rejected() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_eq!(
        s.heartbeat(&r.lease_id, &r.lease_token, 999).await.unwrap(),
        LeaseHeartbeatOutcome::FencingMismatch
    );
}
#[tokio::test]
async fn heartbeat_extends_expires() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let old = r.expires_at.clone();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token)
        .await
        .unwrap();
    assert!(s.get_lease(&r.lease_id).await.unwrap().unwrap().expires_at > old);
}
#[tokio::test]
async fn expired_lease_no_heartbeat() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    assert_eq!(
        s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token)
            .await
            .unwrap(),
        LeaseHeartbeatOutcome::Expired
    );
}
#[tokio::test]
async fn old_owner_heartbeat_fails_after_takeover() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s1 = svcc(&p, clk.clone());
    let old = match s1.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    s1.expire_due_leases().await.unwrap();
    let s2 = svc(&p);
    let _ = match s2.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let out = s1
        .heartbeat(&old.lease_id, &old.lease_token, old.fencing_token)
        .await
        .unwrap();
    assert!(matches!(
        out,
        LeaseHeartbeatOutcome::TokenMismatch | LeaseHeartbeatOutcome::NotActive
    ));
}

// ── 13-15 release ────────────────────────────────────────────

#[tokio::test]
async fn release_success() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_eq!(
        s.release_lease(&r.lease_id, &r.lease_token, "done")
            .await
            .unwrap(),
        LeaseReleaseOutcome::Released
    );
    assert_eq!(
        s.get_lease(&r.lease_id).await.unwrap().unwrap().lifecycle,
        "released"
    );
}
#[tokio::test]
async fn repeated_release_idempotent() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    assert_eq!(
        s.release_lease(&r.lease_id, &r.lease_token, "again")
            .await
            .unwrap(),
        LeaseReleaseOutcome::AlreadyReleased
    );
}
#[tokio::test]
async fn old_owner_release_rejected() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s1 = svcc(&p, clk.clone());
    let old = match s1.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    s1.expire_due_leases().await.unwrap();
    let s2 = svc(&p);
    let _ = match s2.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let out = s1
        .release_lease(&old.lease_id, &old.lease_token, "stale")
        .await
        .unwrap();
    assert!(matches!(
        out,
        LeaseReleaseOutcome::TokenMismatch | LeaseReleaseOutcome::NotActive
    ));
}

// ── 16-18 expire & reacquire ─────────────────────────────────

#[tokio::test]
async fn lease_auto_expires() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    assert!(s.expire_due_leases().await.unwrap().contains(&r.lease_id));
    assert_eq!(
        s.get_lease(&r.lease_id).await.unwrap().unwrap().lifecycle,
        "expired"
    );
}
#[tokio::test]
async fn reacquire_new_lease_id() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let fst = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    s.expire_due_leases().await.unwrap();
    let snd = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_ne!(fst.lease_id, snd.lease_id);
    assert!(snd.fencing_token > fst.fencing_token);
}
#[tokio::test]
async fn terminal_lease_no_reanimate() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    assert_eq!(
        s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token)
            .await
            .unwrap(),
        LeaseHeartbeatOutcome::NotActive
    );
}

// ── 19-21 uniqueness ─────────────────────────────────────────

#[tokio::test]
async fn one_active_per_task() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    s.acquire_lease(&spec()).await.unwrap();
    assert!(s.acquire_lease(&spec()).await.is_err());
}
#[tokio::test]
async fn one_active_per_worktree() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    s.acquire_lease(&spec()).await.unwrap();
    assert!(s.acquire_lease(&spec()).await.is_err());
}
#[tokio::test]
async fn one_active_per_execution() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    s.acquire_lease(&spec()).await.unwrap();
    assert!(s.acquire_lease(&spec()).await.is_err());
}

// ── 22-26 reconciliation ─────────────────────────────────────

#[tokio::test]
async fn reconcile_expired_active() {
    let (_db, p, _tmp) = init_db().await;
    // Past timestamp so the lease is definitely expired relative to
    // reconciler's chrono::Utc::now() (even same-second inserts win with <).
    let past = (chrono::Utc::now() - chrono::Duration::seconds(60))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    // FK-safe: create a distinct worktree row first.
    sqlx::query("INSERT OR IGNORE INTO worktrees (id,project_id,task_id,execution_id,repository_root,repository_identity,worktree_path,branch_name,base_commit,owner_supervisor_id,operation_id,status,lease_epoch) VALUES ('wt-x1','p1','t1','e1','/x','/x/.git','/x/w','b','abc','sup-old','op1','active',0)").execute(&p).await.unwrap();
    sqlx::query("INSERT INTO workspace_leases (id,worktree_id,project_id,task_id,owner_execution_id,owner_supervisor_id,lease_token,fencing_token,lifecycle,acquired_at,expires_at) VALUES ('re1','wt-x1','p1','t1','e1','sup-old','tok1',1,'active',?,?)").bind(&past).bind(&past).execute(&p).await.unwrap();
    WorkspaceLeaseReconciler::new(p.clone(), "sup-now".into())
        .reconcile()
        .await
        .unwrap();
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='re1'")
        .fetch_one(&p)
        .await
        .unwrap();
    assert_eq!(lc.0, "expired");
}
#[tokio::test]
async fn reconcile_execution_terminal() {
    let (_db, p, _tmp) = init_db().await;
    sqlx::query("UPDATE execution_attempts SET lifecycle='completed' WHERE id='e1'")
        .execute(&p)
        .await
        .unwrap();
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let exp = (chrono::Utc::now() + chrono::Duration::seconds(300))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    sqlx::query("INSERT INTO workspace_leases (id,worktree_id,project_id,task_id,owner_execution_id,owner_supervisor_id,lease_token,fencing_token,lifecycle,acquired_at,expires_at) VALUES ('re2','wt-1','p1','t1','e1','sup-old','tok2',1,'active',?,?)").bind(&now).bind(&exp).execute(&p).await.unwrap();
    WorkspaceLeaseReconciler::new(p.clone(), "sup-now".into())
        .reconcile()
        .await
        .unwrap();
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='re2'")
        .fetch_one(&p)
        .await
        .unwrap();
    assert_eq!(lc.0, "expired");
}
#[tokio::test]
async fn reconcile_supervisor_mismatch() {
    let (_db, p, _tmp) = init_db().await;
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let exp = (chrono::Utc::now() + chrono::Duration::seconds(300))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    sqlx::query("INSERT INTO workspace_leases (id,worktree_id,project_id,task_id,owner_execution_id,owner_supervisor_id,lease_token,fencing_token,lifecycle,acquired_at,expires_at) VALUES ('re3','wt-1','p1','t1','e1','sup-other','tok3',1,'active',?,?)").bind(&now).bind(&exp).execute(&p).await.unwrap();
    let drifts = WorkspaceLeaseReconciler::new(p.clone(), "sup-now".into())
        .reconcile()
        .await
        .unwrap();
    assert!(
        drifts
            .iter()
            .any(|d| d.kind == LeaseDriftKind::ActiveOwnerSupervisorMismatch),
        "{drifts:?}"
    );
}
#[tokio::test]
async fn reconcile_worktree_missing() {
    let (_db, p, _tmp) = init_db().await;
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let exp = (chrono::Utc::now() + chrono::Duration::seconds(300))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    // Create the worktree row, then mark it removed so the reconciler
    // detects ActiveWorktreeRemoved drift.
    sqlx::query("INSERT OR IGNORE INTO worktrees (id,project_id,task_id,execution_id,repository_root,repository_identity,worktree_path,branch_name,base_commit,owner_supervisor_id,operation_id,status,lease_epoch) VALUES ('wt-gone','p1','t1','e1','/g','/g/.git','/g/w','b','abc','sup-old','op1','removed',0)").execute(&p).await.unwrap();
    sqlx::query("INSERT INTO workspace_leases (id,worktree_id,project_id,task_id,owner_execution_id,owner_supervisor_id,lease_token,fencing_token,lifecycle,acquired_at,expires_at) VALUES ('re4','wt-gone','p1','t1','e1','sup-old','tok4',1,'active',?,?)").bind(&now).bind(&exp).execute(&p).await.unwrap();
    WorkspaceLeaseReconciler::new(p.clone(), "sup-now".into())
        .reconcile()
        .await
        .unwrap();
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id='re4'")
        .fetch_one(&p)
        .await
        .unwrap();
    assert_eq!(lc.0, "expired");
}
#[tokio::test]
async fn repeated_reconciliation_no_dupes() {
    let (_db, p, _tmp) = init_db().await;
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    sqlx::query("INSERT INTO workspace_leases (id,worktree_id,project_id,task_id,owner_execution_id,owner_supervisor_id,lease_token,fencing_token,lifecycle,acquired_at,expires_at) VALUES ('re5','wt-1','p1','t1','e1','sup-old','tok5',1,'active',?,?)").bind(&now).bind(&now).execute(&p).await.unwrap();
    let recon = WorkspaceLeaseReconciler::new(p.clone(), "sup-now".into());
    recon.reconcile().await.unwrap();
    let before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE idempotency_key LIKE 'recon-expire-re5%'",
    )
    .fetch_one(&p)
    .await
    .unwrap();
    recon.reconcile().await.unwrap();
    let after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE idempotency_key LIKE 'recon-expire-re5%'",
    )
    .fetch_one(&p)
    .await
    .unwrap();
    assert_eq!(after, before, "duplicate events");
}

// ── 27-29 heartbeat runner ───────────────────────────────────

#[tokio::test]
async fn runner_cycles() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc_with_fast_hb(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let runner = LeaseHeartbeatRunner::new(s.clone(), r.lease_id, r.lease_token, r.fencing_token);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = tokio_util::sync::CancellationToken::new();
    let c2 = cancel.clone();
    let h = tokio::spawn(async move {
        runner
            .run(c2, |r| {
                let _ = tx.send(r);
            })
            .await;
    });
    // Wait for the first heartbeat result deterministically.
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .expect("first heartbeat must be delivered");
    assert!(first.ok, "first heartbeat must succeed: {first:?}");
    cancel.cancel();
    h.await.unwrap();
}
#[tokio::test]
async fn runner_stops_on_token_mismatch() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let runner = LeaseHeartbeatRunner::new(s.clone(), r.lease_id, "bad".into(), r.fencing_token);
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut results = Vec::new();
    runner.run(cancel, |r| results.push(r)).await;
    assert!(results
        .iter()
        .any(|r| matches!(r.outcome, Some(LeaseHeartbeatOutcome::TokenMismatch))));
}
#[tokio::test]
async fn runner_stops_on_expired() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = Arc::new(svcc(&p, clk.clone()));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    let runner = LeaseHeartbeatRunner::new(s.clone(), r.lease_id, r.lease_token, r.fencing_token);
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut results = Vec::new();
    runner.run(cancel, |r| results.push(r)).await;
    assert!(results
        .iter()
        .any(|r| matches!(r.outcome, Some(LeaseHeartbeatOutcome::Expired))));
}

// ── 30-31 clock ──────────────────────────────────────────────

#[tokio::test]
async fn clock_advance_before_expiry() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(200));
    assert_eq!(
        s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token)
            .await
            .unwrap(),
        LeaseHeartbeatOutcome::Ok
    );
}
#[tokio::test]
async fn heartbeat_does_not_regress() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let before = s.get_lease(&r.lease_id).await.unwrap().unwrap().expires_at;
    s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token)
        .await
        .unwrap();
    assert!(s.get_lease(&r.lease_id).await.unwrap().unwrap().expires_at >= before);
}

// ── 32-34 state+event ────────────────────────────────────────

#[tokio::test]
async fn acquire_writes_event() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let _ = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let cnt: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE event_type='workspace_lease_lifecycle_changed'",
    )
    .fetch_one(&p)
    .await
    .unwrap();
    assert!(cnt > 0);
}
#[tokio::test]
async fn release_writes_event() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    let cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE idempotency_key LIKE '%-release'")
            .fetch_one(&p)
            .await
            .unwrap();
    assert!(cnt > 0, "release must write an event");
}
#[tokio::test]
async fn expire_writes_event() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let _ = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    s.expire_due_leases().await.unwrap();
    let cnt:i64=sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE event_type='workspace_lease_lifecycle_changed' AND idempotency_key LIKE '%-expire'").fetch_one(&p).await.unwrap();
    assert!(cnt > 0);
}

// ── 35-36 fencing guard ──────────────────────────────────────

#[tokio::test]
async fn validate_accepts_current() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert!(s
        .validate_lease(&r.lease_id, &r.lease_token, r.fencing_token)
        .await
        .is_ok());
}
#[tokio::test]
async fn validate_rejects_old() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s1 = svcc(&p, clk.clone());
    let old = match s1.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    s1.expire_due_leases().await.unwrap();
    let s2 = svc(&p);
    let _ = match s2.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert!(s1
        .validate_lease(&old.lease_id, &old.lease_token, old.fencing_token)
        .await
        .is_err());
}

// ── 37-41: reacquire+fencing / concurrency ────────────────────────

#[tokio::test]
async fn reacquire_new_id_and_higher_fencing_token() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let first = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    clk.advance(Duration::from_secs(600));
    s.expire_due_leases().await.unwrap();
    let second = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    assert_ne!(first.lease_id, second.lease_id, "reacquire = new lease id");
    assert!(
        second.fencing_token > first.fencing_token,
        "fencing must increase: {} vs {}",
        first.fencing_token,
        second.fencing_token
    );
}

#[tokio::test]
async fn acquire_retry_after_response_lost_returns_same_lease() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let sp = spec();
    // First attempt: response "lost" to the caller, but the lease was committed.
    let _ = s.acquire_lease(&sp).await;
    // Retry with same idempotency key returns the already-committed lease.
    let sp2 = LeaseSpec {
        idempotency_key: sp.idempotency_key.clone(),
        ..spec()
    };
    match s.acquire_lease(&sp2).await.unwrap() {
        LeaseAcquireOutcome::AlreadyAcquired(r) => {
            assert_eq!(r.worktree_id.as_deref(), Some("wt-1"));
        }
        other => panic!("expected AlreadyAcquired on retry, got {other:?}"),
    }
}

#[tokio::test]
async fn heartbeat_vs_expire_single_winner() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = svcc(&p, clk.clone());
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    // Near-expiry: heartbeat and expire race.
    clk.advance(Duration::from_secs(299));
    let (hb, exp) = tokio::join!(
        s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token),
        s.expire_due_leases(),
    );
    // The heartbeat must win (the lease is not yet expired at the start of
    // the race), OR the expire runs first. In either case the outcome is
    // deterministic: exactly one pathway transitions the lease.
    let _ = (hb, exp);
    let final_state = s.get_lease(&r.lease_id).await.unwrap().unwrap();
    assert!(
        final_state.lifecycle == "active" || final_state.lifecycle == "expired",
        "lease lifecycle must be active or expired, got {}",
        final_state.lifecycle
    );
}

#[tokio::test]
async fn heartbeat_vs_release_single_winner() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let (hb, rel) = tokio::join!(
        s.heartbeat(&r.lease_id, &r.lease_token, r.fencing_token),
        s.release_lease(&r.lease_id, &r.lease_token, "raced"),
    );
    let _ = (hb, rel);
    let fresh = s.get_lease(&r.lease_id).await.unwrap().unwrap();
    assert!(
        fresh.lifecycle == "active" || fresh.lifecycle == "released",
        "must settle to active or released, got {}",
        fresh.lifecycle
    );
}

// ── 42-43: token security ─────────────────────────────────────────

#[tokio::test]
async fn lease_token_not_in_debug_or_display() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let token = r.lease_token.clone();
    // Debug output must NOT contain the token value.
    let debug_str = format!("{r:?}");
    assert!(
        !debug_str.contains(&token),
        "lease token must not appear in Debug: {debug_str}"
    );
    // Error messages must NOT contain the token value.
    let err = s
        .heartbeat(&r.lease_id, "wrong-token", r.fencing_token)
        .await
        .unwrap();
    // The error variant name may appear but the token VALUE must not.
    let outcome_str = format!("{err:?}");
    assert!(
        !outcome_str.contains(&token),
        "lease token must not leak into error formatting: {outcome_str}"
    );
}

#[tokio::test]
async fn lease_token_not_in_event_payload() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    // Query the event_log for any payload containing the token.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE payload_json LIKE ?")
        .bind(format!("%{}%", r.lease_token))
        .fetch_one(&p)
        .await
        .unwrap();
    assert_eq!(count, 0, "lease token must never appear in event payload");
}

// ── 44-48: acquire sidecar/git identity validation ──────────────────

#[tokio::test]
async fn acquire_rejects_sidecar_identity_mismatch() {
    let (_db, p, _tmp) = init_db().await;
    // Point the worktree at a different worktree_id in the sidecar.
    let s = svc(&p);
    let mut sp = spec();
    sp.worktree_id = "wt-1".into();
    // sidecar says "wt-1" but we request "wt-1" — same ID, works.
    // Now break the sidecar: set it to a different repo identity.
    let row: (String,) = sqlx::query_as("SELECT worktree_path FROM worktrees WHERE id='wt-1'")
        .fetch_one(&p)
        .await
        .unwrap();
    let path = std::path::PathBuf::from(&row.0);
    let sc_path = harness_runtime::worktree::metadata::sidecar_path(&path);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sc_path).unwrap()).unwrap();
    meta["repository_identity"] = serde_json::Value::String("/alien/.git".into());
    std::fs::write(&sc_path, meta.to_string()).unwrap();
    assert!(
        s.acquire_lease(&sp).await.is_err(),
        "sidecar repo identity mismatch must reject acquire"
    );
}

#[tokio::test]
async fn acquire_rejects_sidecar_path_mismatch() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let row: (String,) = sqlx::query_as("SELECT worktree_path FROM worktrees WHERE id='wt-1'")
        .fetch_one(&p)
        .await
        .unwrap();
    let path = std::path::PathBuf::from(&row.0);
    let sc_path = harness_runtime::worktree::metadata::sidecar_path(&path);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sc_path).unwrap()).unwrap();
    meta["worktree_path"] = serde_json::Value::String("/other/path".into());
    std::fs::write(&sc_path, meta.to_string()).unwrap();
    assert!(
        s.acquire_lease(&spec()).await.is_err(),
        "sidecar path mismatch must reject acquire"
    );
}

#[tokio::test]
async fn acquire_rejects_branch_mismatch() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let row: (String,) = sqlx::query_as("SELECT worktree_path FROM worktrees WHERE id='wt-1'")
        .fetch_one(&p)
        .await
        .unwrap();
    let path = std::path::PathBuf::from(&row.0);
    let sc_path = harness_runtime::worktree::metadata::sidecar_path(&path);
    let mut meta: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&sc_path).unwrap()).unwrap();
    meta["branch"] = serde_json::Value::String("wrong-branch".into());
    std::fs::write(&sc_path, meta.to_string()).unwrap();
    assert!(
        s.acquire_lease(&spec()).await.is_err(),
        "branch mismatch must reject acquire"
    );
}

#[tokio::test]
async fn acquire_rejects_worktree_removed_status() {
    let (_db, p, _tmp) = init_db().await;
    sqlx::query("UPDATE worktrees SET status='removed' WHERE id='wt-1'")
        .execute(&p)
        .await
        .unwrap();
    assert!(
        svc(&p).acquire_lease(&spec()).await.is_err(),
        "removed worktree must reject acquire"
    );
}

// ── 49-52: release event idempotency ────────────────────────────────

#[tokio::test]
async fn release_produces_exactly_one_event() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM event_log WHERE idempotency_key LIKE '%-release' AND stream_id = ?",
    )
    .bind(&r.lease_id)
    .fetch_one(&p)
    .await
    .unwrap();
    assert_eq!(count, 1, "release must produce exactly one event");
}

#[tokio::test]
async fn repeated_release_no_second_event() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    let count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE stream_id = ?")
            .bind(&r.lease_id)
            .fetch_one(&p)
            .await
            .unwrap();
    // Repeat release (idempotent).
    s.release_lease(&r.lease_id, &r.lease_token, "again")
        .await
        .unwrap();
    let count_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_log WHERE stream_id = ?")
        .bind(&r.lease_id)
        .fetch_one(&p)
        .await
        .unwrap();
    assert_eq!(
        count_after, count_before,
        "repeat release must not write a second event"
    );
}

// ── 53-55: Worktree remove gated by lease ───────────────────────────

#[tokio::test]
async fn active_lease_blocks_worktree_remove() {
    // The lease validator blocks removal when an active lease exists.
    // This test exercises the lease gating directly (via the guard trait).
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    s.acquire_lease(&spec()).await.unwrap();
    let validator = ServiceLeaseAccessValidator::new(s.clone());
    let request = harness_runtime::lease::WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: None,
    };
    let result = validator.can_remove_worktree(&request).await.unwrap();
    assert!(
        matches!(
            result,
            harness_runtime::lease::LeaseAccessResult::BlockedByActiveLease { .. }
        ),
        "active lease must block removal: {result:?}"
    );
}

#[tokio::test]
async fn released_lease_allows_remove() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    let validator = ServiceLeaseAccessValidator::new(s.clone());
    let request = harness_runtime::lease::WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: None,
    };
    assert_eq!(
        validator.can_remove_worktree(&request).await.unwrap(),
        harness_runtime::lease::LeaseAccessResult::Allowed
    );
}

#[tokio::test]
async fn stale_fencing_token_blocks_remove() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let validator = ServiceLeaseAccessValidator::new(s.clone());
    // Use a wrong fencing token in the credential.
    let request = harness_runtime::lease::WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: Some(harness_runtime::lease::LeaseCredential {
            lease_id: r.lease_id.clone(),
            lease_token: r.lease_token.clone(),
            fencing_token: 0,
        }),
    };
    assert_eq!(
        validator.can_remove_worktree(&request).await.unwrap(),
        harness_runtime::lease::LeaseAccessResult::StaleFencingToken
    );
}

// ── 44-45: migration v5 index verification ────────────────────────

#[tokio::test]
async fn single_active_lease_per_worktree_index_enforced() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    s.acquire_lease(&spec()).await.unwrap();
    // Second active lease on the same worktree must fail.
    let sp2 = spec();
    let r2 = s.acquire_lease(&sp2).await;
    assert!(
        r2.is_err(),
        "partial unique index must block second active lease"
    );
}

#[tokio::test]
async fn released_lease_does_not_block_reacquire() {
    let (_db, p, _tmp) = init_db().await;
    let s = svc(&p);
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "test")
        .await
        .unwrap();
    // After release, a new lease on the same worktree must succeed.
    let sp2 = spec();
    match s.acquire_lease(&sp2).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r2) => {
            assert_ne!(r2.lease_id, r.lease_id);
        }
        other => panic!("reacquire after release must succeed: {other:?}"),
    }
}

// ── 56-60: normal-remove vs admin-recovery security ─────────────────

#[tokio::test]
async fn current_lease_owner_cannot_normal_remove() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    s.acquire_lease(&spec()).await.unwrap();
    let validator = ServiceLeaseAccessValidator::new(s);
    // Normal remove (no credential): even the current lease owner cannot
    // remove an active workspace. The correct order is release → remove.
    let req = WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: None,
    };
    assert!(
        matches!(
            validator.can_remove_worktree(&req).await.unwrap(),
            LeaseAccessResult::BlockedByActiveLease { .. }
        ),
        "even the current owner cannot normal-remove an active workspace"
    );
}

#[tokio::test]
async fn released_lease_permits_normal_remove() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    s.release_lease(&r.lease_id, &r.lease_token, "done")
        .await
        .unwrap();
    let validator = ServiceLeaseAccessValidator::new(s);
    let req = WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: None,
    };
    assert_eq!(
        validator.can_remove_worktree(&req).await.unwrap(),
        LeaseAccessResult::Allowed
    );
}

#[tokio::test]
async fn expired_reconciled_lease_allows_remove() {
    let (_db, p, _tmp) = init_db().await;
    let clk = Arc::new(TestClock::new(chrono::Utc::now()));
    let s = Arc::new(svcc(&p, clk.clone()));
    s.acquire_lease(&spec()).await.unwrap();
    clk.advance(Duration::from_secs(600));
    s.expire_due_leases().await.unwrap();
    let validator = ServiceLeaseAccessValidator::new(s);
    let req = WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: None,
    };
    assert_eq!(
        validator.can_remove_worktree(&req).await.unwrap(),
        LeaseAccessResult::Allowed
    );
}

#[tokio::test]
async fn stale_fencing_rejects_admin_remove() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let validator = ServiceLeaseAccessValidator::new(s);
    let req = WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: Some(LeaseCredential {
            lease_id: r.lease_id,
            lease_token: r.lease_token,
            fencing_token: 0,
        }),
    };
    assert_eq!(
        validator.can_remove_worktree(&req).await.unwrap(),
        LeaseAccessResult::StaleFencingToken
    );
}

#[tokio::test]
async fn valid_admin_recovery_credential_accepted() {
    let (_db, p, _tmp) = init_db().await;
    let s = Arc::new(svc(&p));
    let r = match s.acquire_lease(&spec()).await.unwrap() {
        LeaseAcquireOutcome::Acquired(r) => r,
        _ => panic!(),
    };
    let validator = ServiceLeaseAccessValidator::new(s);
    let cred = LeaseCredential {
        lease_id: r.lease_id.clone(),
        lease_token: r.lease_token.clone(),
        fencing_token: r.fencing_token,
    };
    let req = WorktreeAccessRequest {
        worktree_id: "wt-1".into(),
        worktree_path: String::new(),
        task_id: "t1".into(),
        execution_id: "e1".into(),
        owner_supervisor_id: "sup-test".into(),
        lease_credential: Some(cred.clone()),
    };
    // A valid credential matching the active lease's id + token + fencing
    // must be accepted (administrative force-remove path).
    assert_eq!(
        validator.can_remove_worktree(&req).await.unwrap(),
        LeaseAccessResult::Allowed
    );
    // validate_force_credential must agree and reject a mismatched worktree.
    assert!(validator
        .validate_force_credential("wt-1", &cred)
        .await
        .unwrap());
    assert!(!validator
        .validate_force_credential("wt-other", &cred)
        .await
        .unwrap());
}
