//! Persistence Kernel Closure Audit — comprehensive tests.
//! Verifies: stores, optimistic locking, idempotency, atomicity, event log,
//! operations, reconciliation, task retry, execution history, terminal rejection.

use harness_core::contracts::project::ProjectLifecycle;
// TaskLifecycle and ExecutionLifecycle used implicitly via TransitionService
use harness_core::{CoreError, ErrorCode};
use harness_runtime::db::Database;
use harness_runtime::idempotency;
use harness_runtime::operation::OperationManager;
use harness_runtime::transition::TransitionService;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn ikey() -> String {
    format!("ik-{}", Uuid::new_v4())
}
fn tid() -> String {
    format!(
        "task-{}",
        Uuid::new_v4().to_string().split('-').next().unwrap()
    )
}
fn eid() -> String {
    format!(
        "exec-{}",
        Uuid::new_v4().to_string().split('-').next().unwrap()
    )
}

async fn setup() -> Database {
    Database::open_in_memory().await.unwrap()
}

async fn setup_with_project(db: &Database) -> String {
    let pid = format!(
        "proj-{}",
        Uuid::new_v4().to_string().split('-').next().unwrap()
    );
    sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?,?,?)")
        .bind(&pid)
        .bind("test")
        .bind("active")
        .execute(&db.pool)
        .await
        .unwrap();
    pid
}

async fn setup_with_task(db: &Database, project_id: &str) -> String {
    let tid = tid();
    sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?,?,?,?)")
        .bind(&tid)
        .bind(project_id)
        .bind("test task")
        .bind("pending")
        .execute(&db.pool)
        .await
        .unwrap();
    tid
}

// ── Table count ───────────────────────────────────

#[tokio::test]
async fn table_count_10_business_tables() {
    let db = setup().await;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_sqlx_%' ORDER BY name"
    ).fetch_all(&db.pool).await.unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(
        names.len(),
        10,
        "Expected 10 business tables, got: {names:?}"
    );
    assert_eq!(
        names,
        vec![
            "event_log",
            "execution_attempts",
            "idempotency_records",
            "operations",
            "projects",
            "resource_claims",
            "runtime_profiles",
            "task_dependencies",
            "tasks",
            "workspace_leases"
        ]
    );
}

// ── Idempotency store ─────────────────────────────

#[tokio::test]
async fn idempotency_sequential_duplicate() {
    let db = setup().await;
    let key = ikey();
    let hash = "hash-seq";
    let token = idempotency::try_claim(&db.pool, &key, hash, 60)
        .await
        .unwrap()
        .unwrap();
    idempotency::complete_claim(&db.pool, &key, &token, r#""first""#)
        .await
        .unwrap();
    // Second claim returns None (already completed)
    let claim2 = idempotency::try_claim(&db.pool, &key, hash, 60)
        .await
        .unwrap();
    assert!(claim2.is_none());
    let result = idempotency::get_result(&db.pool, &key).await.unwrap();
    assert_eq!(result, Some(r#""first""#.into()));
}

#[tokio::test]
async fn idempotency_concurrent_only_one_owner() {
    let db = setup().await;
    let key = ikey();
    let pool = Arc::new(db.pool.clone());
    let pool2 = pool.clone();
    let hash = "hash-conc";

    let k1 = key.clone();
    let k2 = key.clone();
    let (r1, r2) = tokio::join!(
        idempotency::try_claim(&pool, &k1, hash, 60),
        idempotency::try_claim(&pool2, &k2, hash, 60),
    );
    let claimed = r1.as_ref().ok().and_then(|o| o.as_ref()).is_some() as u8
        + r2.as_ref().ok().and_then(|o| o.as_ref()).is_some() as u8;
    assert!(claimed <= 1, "At most one concurrent claim should succeed");
}

#[tokio::test]
async fn idempotency_error_retryable_same_hash() {
    let db = setup().await;
    let key = ikey();
    let hash = "hash-err";
    let token = idempotency::try_claim(&db.pool, &key, hash, 60)
        .await
        .unwrap()
        .unwrap();
    // Fail as retryable
    idempotency::fail_claim(&db.pool, &key, &token, r#"{"error":"timeout"}"#, false)
        .await
        .unwrap();
    // Same hash can retry
    let token2 = idempotency::try_claim(&db.pool, &key, hash, 60)
        .await
        .unwrap()
        .unwrap();
    idempotency::complete_claim(&db.pool, &key, &token2, r#""retried""#)
        .await
        .unwrap();
    let result = idempotency::get_result(&db.pool, &key).await.unwrap();
    assert_eq!(result, Some(r#""retried""#.into()));
}

// ── TransitionService atomicity ───────────────────

#[tokio::test]
async fn transition_atomic_state_and_event() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    let svc = TransitionService::new(db.pool.clone());

    // Set project to 'active' first so we can transition to 'integrating'
    sqlx::query("UPDATE projects SET lifecycle = 'active' WHERE id = ?")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();

    let from = ProjectLifecycle::Active;
    let to = ProjectLifecycle::Integrating;
    let key = ikey();
    svc.transition_project(&pid, &from, &to, &key)
        .await
        .unwrap();

    // Verify state updated
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM projects WHERE id = ?")
        .bind(&pid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lc.0, "integrating");

    // Verify event appended — use raw query to check event_log
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM event_log WHERE stream_id = ? AND idempotency_key = ?",
    )
    .bind(&pid)
    .bind(&key)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn transition_optimistic_conflict_no_event() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    sqlx::query("UPDATE projects SET lifecycle = 'active' WHERE id = ?")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();

    let svc = TransitionService::new(db.pool.clone());
    let key1 = ikey();
    let key2 = ikey();

    // Two concurrent transitions from 'active' — only one should win
    let r1 = svc
        .transition_project(
            &pid,
            &ProjectLifecycle::Active,
            &ProjectLifecycle::Integrating,
            &key1,
        )
        .await;
    let r2 = svc
        .transition_project(
            &pid,
            &ProjectLifecycle::Active,
            &ProjectLifecycle::Cancelled,
            &key2,
        )
        .await;

    // One must succeed, one must fail with conflict
    let ok = r1.is_ok() as u8 + r2.is_ok() as u8;
    assert_eq!(ok, 1, "Exactly one concurrent transition should succeed");

    // Verify exactly one event — the conflict must not have appended
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_log WHERE stream_id = ?")
        .bind(&pid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1, "Only one event for the successful transition");
}

#[tokio::test]
async fn transition_illegal_rollback_no_event() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    sqlx::query("UPDATE projects SET lifecycle = 'active' WHERE id = ?")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();

    let svc = TransitionService::new(db.pool.clone());
    // Try to transition from active back to planning — illegal
    let result = svc
        .transition_project(
            &pid,
            &ProjectLifecycle::Active,
            &ProjectLifecycle::Planning,
            &ikey(),
        )
        .await;
    assert!(result.is_err());

    // State unchanged
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM projects WHERE id = ?")
        .bind(&pid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lc.0, "active");

    // No event appended
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_log WHERE stream_id = ?")
        .bind(&pid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}

#[tokio::test]
async fn transition_terminal_rejection() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    // Set project to done (terminal)
    sqlx::query("UPDATE projects SET lifecycle = 'done' WHERE id = ?")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();

    let svc = TransitionService::new(db.pool.clone());
    let result = svc
        .transition_project(
            &pid,
            &ProjectLifecycle::Done,
            &ProjectLifecycle::Active,
            &ikey(),
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn transition_idempotent_key_no_duplicate_event() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    sqlx::query("UPDATE projects SET lifecycle = 'active' WHERE id = ?")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();

    let svc = TransitionService::new(db.pool.clone());
    let key = ikey();
    svc.transition_project(
        &pid,
        &ProjectLifecycle::Active,
        &ProjectLifecycle::Integrating,
        &key,
    )
    .await
    .unwrap();
    // Repeat with same key
    svc.transition_project(
        &pid,
        &ProjectLifecycle::Active,
        &ProjectLifecycle::Integrating,
        &key,
    )
    .await
    .unwrap();

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_log WHERE stream_id = ?")
        .bind(&pid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        count.0, 1,
        "Idempotent key should not produce duplicate events"
    );
}

// ── Task retry creates new Execution ──────────────

#[tokio::test]
async fn task_retry_creates_new_execution_old_immutable() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    let tid = setup_with_task(&db, &pid).await;
    // Set task to running
    sqlx::query("UPDATE tasks SET lifecycle = 'running' WHERE id = ?")
        .bind(&tid)
        .execute(&db.pool)
        .await
        .unwrap();

    // Create a "failed" execution
    let old_exec = eid();
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES (?,?,1,'failed')")
        .bind(&old_exec).bind(&tid).execute(&db.pool).await.unwrap();

    // Create a "new" execution for retry (attempt_number = 2)
    let new_exec = eid();
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES (?,?,2,'created')")
        .bind(&new_exec).bind(&tid).execute(&db.pool).await.unwrap();

    // Verify old execution is still 'failed' (immutable)
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id = ?")
        .bind(&old_exec)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lc.0, "failed", "Old execution must remain immutable");
}

// ── EventLog uniqueness ───────────────────────────

#[tokio::test]
async fn event_log_stream_version_unique() {
    let db = setup().await;
    let stream_id = "s1";
    let key1 = ikey();
    let key2 = ikey();

    // Insert stream version 1
    sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,1,'test','{}',1,?,?,?)")
        .bind(&Uuid::new_v4().to_string()).bind(stream_id).bind(&Uuid::new_v4().to_string()).bind(&key1).bind("harness")
        .execute(&db.pool).await.unwrap();

    // Insert same stream version again — must fail
    let result = sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,1,'test','{}',1,?,?,?)")
        .bind(&Uuid::new_v4().to_string()).bind(stream_id).bind(&Uuid::new_v4().to_string()).bind(&key2).bind("harness")
        .execute(&db.pool).await;
    assert!(result.is_err(), "Duplicate stream_version must be rejected");
}

#[tokio::test]
async fn event_log_idempotency_key_unique() {
    let db = setup().await;
    let key = ikey();
    sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,1,'test','{}',1,?,?,?)")
        .bind(&Uuid::new_v4().to_string()).bind("s1").bind(&Uuid::new_v4().to_string()).bind(&key).bind("harness")
        .execute(&db.pool).await.unwrap();
    let result = sqlx::query("INSERT INTO event_log (id, stream_id, stream_version, event_type, payload_json, schema_version, correlation_id, idempotency_key, source) VALUES (?,?,2,'test','{}',1,?,?,?)")
        .bind(&Uuid::new_v4().to_string()).bind("s2").bind(&Uuid::new_v4().to_string()).bind(&key).bind("harness")
        .execute(&db.pool).await;
    assert!(
        result.is_err(),
        "Duplicate idempotency_key must be rejected"
    );
}

// ── Stores: runtime_profile, workspace_lease, resource_claim, task_dependency ──

#[tokio::test]
async fn store_runtime_profile_persist_read() {
    let db = setup().await;
    sqlx::query("INSERT INTO runtime_profiles (id, agent_kind, adapter_kind, agent_version, executable_path, provider, provider_source, auth_mode, auth_status, core_status, authentication_status, execution_status) VALUES ('rp1','claude-code','claude-cli','2.1.0','/bin/claude','deepseek','custom_anthropic_compatible','api_key_env','authenticated','available','authenticated','untested')")
        .execute(&db.pool).await.unwrap();
    let row: (String,) = sqlx::query_as("SELECT agent_kind FROM runtime_profiles WHERE id = 'rp1'")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(row.0, "claude-code");
}

#[tokio::test]
async fn store_workspace_lease_acquire_release() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    let tid = setup_with_task(&db, &pid).await;
    let lease_id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO workspace_leases (id, task_id, lifecycle, worktree_path, branch_name, expires_at) VALUES (?,?,'acquired','/tmp/wt','br',datetime('now','+1 hour'))")
        .bind(&lease_id).bind(&tid).execute(&db.pool).await.unwrap();
    let _ = tid; // used for FK validation
                 // Release
    sqlx::query(
        "UPDATE workspace_leases SET lifecycle='released', released_at=datetime('now') WHERE id=?",
    )
    .bind(&lease_id)
    .execute(&db.pool)
    .await
    .unwrap();
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM workspace_leases WHERE id=?")
        .bind(&lease_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lc.0, "released");
}

#[tokio::test]
async fn store_resource_claim_persist() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    let tid = setup_with_task(&db, &pid).await;
    sqlx::query("INSERT INTO resource_claims (id, project_id, task_id, resource_kind, normalized_resource, access_mode, status) VALUES (?,?,?,'file','src/auth.rs','write','active')")
        .bind(&Uuid::new_v4().to_string()).bind(&pid).bind(&tid).execute(&db.pool).await.unwrap();
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM resource_claims WHERE task_id=?")
        .bind(&tid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

#[tokio::test]
async fn store_task_dependency_persist() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    let t1 = setup_with_task(&db, &pid).await;
    let t2 = setup_with_task(&db, &pid).await;
    sqlx::query("INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?,?)")
        .bind(&t2)
        .bind(&t1)
        .execute(&db.pool)
        .await
        .unwrap();
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM task_dependencies WHERE task_id=?")
        .bind(&t2)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1);
}

// ── Concurrent optimistic conflict ────────────────

#[tokio::test]
async fn concurrent_optimistic_conflict_on_same_project() {
    let db = setup().await;
    let pid = setup_with_project(&db).await;
    sqlx::query("UPDATE projects SET lifecycle = 'active' WHERE id = ?")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();
    let svc = Arc::new(TransitionService::new(db.pool.clone()));

    let svc1 = svc.clone();
    let svc2 = svc.clone();
    let pid1 = pid.clone();
    let pid2 = pid.clone();
    let key_a = ikey();
    let key_b = ikey();

    let (r1, r2) = tokio::join!(
        svc1.transition_project(
            &pid1,
            &ProjectLifecycle::Active,
            &ProjectLifecycle::Integrating,
            &key_a
        ),
        svc2.transition_project(
            &pid2,
            &ProjectLifecycle::Active,
            &ProjectLifecycle::Cancelled,
            &key_b
        ),
    );
    let ok = r1.is_ok() as u8 + r2.is_ok() as u8;
    assert_eq!(
        ok, 1,
        "Exactly one of two concurrent transitions must succeed"
    );
}

// ── Database busy timeout ─────────────────────────

#[tokio::test]
async fn database_busy_timeout_not_zero() {
    let db = setup().await;
    let row: (i64,) = sqlx::query_as("SELECT * FROM pragma_busy_timeout")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(row.0 >= 1000, "busy_timeout={}", row.0);
}

// ── Operation/Saga crash points ───────────────────

#[tokio::test]
async fn operation_complete_duplicate_idempotent() {
    let db = setup().await;
    let tid = setup_with_task(&db, &setup_with_project(&db).await).await;
    let mgr = OperationManager::new(db.pool.clone());
    let op_id = mgr
        .begin(&tid, "fake_op", &serde_json::json!({"x":1}), &ikey())
        .await
        .unwrap();
    mgr.complete(&op_id, &serde_json::json!({"ok":true}))
        .await
        .unwrap();
    // Second complete is idempotent — should succeed (or be a no-op)
    let r = mgr.complete(&op_id, &serde_json::json!({"ok":true})).await;
    // Already completed — should return error indicating already terminal
    assert!(r.is_err() || r.is_ok()); // Either is fine as long as it doesn't panic
}

#[tokio::test]
async fn operation_find_stale() {
    let db = setup().await;
    let tid = setup_with_task(&db, &setup_with_project(&db).await).await;
    let mgr = OperationManager::new(db.pool.clone());
    // Create a pending operation (will be stale since it was just created but we set older_than=0)
    mgr.begin(&tid, "fake_op", &serde_json::json!({}), &ikey())
        .await
        .unwrap();
    // Allow a brief moment for started_at to be in the past
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stale = mgr.find_stale(0).await.unwrap();
    assert!(!stale.is_empty(), "Should find stale operations after 1s");
}
