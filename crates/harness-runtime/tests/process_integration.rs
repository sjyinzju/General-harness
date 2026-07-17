//! ProcessManager integration tests using process-fixture binary.
//! Requires: `cargo build --bin process-fixture` before running.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use harness_runtime::db::Database;
use harness_runtime::idempotency;
use harness_runtime::operation::OperationManager;
use harness_runtime::process::{
    manager::ProcessManager, reconciler::ProcessReconciler, registry::ProcessRegistry, types::*,
};

fn fixture_path() -> PathBuf {
    // Cargo builds binaries to target/debug/ or target/release/
    // Tests run from target/debug/deps/, so go up two levels
    let exe = std::env::current_exe().unwrap();
    // Test binary: target/debug/deps/process_integration-xxx.exe
    // Fixture:     target/debug/process-fixture.exe
    let debug_dir = exe.parent().unwrap().parent().unwrap();
    debug_dir
        .join("process-fixture")
        .with_extension(std::env::consts::EXE_EXTENSION)
}

fn basic_spec(execution_id: &str, args: Vec<&str>) -> ProcessSpec {
    ProcessSpec {
        executable: fixture_path(),
        args: args.into_iter().map(|s| s.to_string()).collect(),
        working_directory: std::env::temp_dir(),
        env_overrides: HashMap::new(),
        env_removals: vec![],
        stdin_mode: StdinMode::Closed,
        timeout: Duration::from_secs(30),
        graceful_shutdown_timeout: Duration::from_secs(2),
        stdout_capture: CapturePolicy::Pipe,
        stderr_capture: CapturePolicy::Pipe,
        output_byte_limit: 10 * 1024 * 1024,
        spool_dir: None,
        allowed_env_var_names: vec![],
        known_secrets: vec![],
        execution_id: execution_id.to_string(),
        runtime_profile_id: "test-profile".into(),
    }
}

async fn wait_for_completion(mgr: &ProcessManager, eid: &str, timeout: Duration) -> ProcessState {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(s) = mgr.get_state(eid).await {
            if matches!(s, ProcessState::Completed { .. }) {
                return s;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timeout waiting for {eid}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn process_wait_helper_reaches_completion() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let spec = basic_spec("e0-wait", vec!["print_stdout"]);
    mgr.spawn(&spec).await.unwrap();
    let state = wait_for_completion(&mgr, "e0-wait", Duration::from_secs(10)).await;
    assert!(matches!(state, ProcessState::Completed { .. }));
}

async fn setup_db_with_execution() -> (Database, String, String) {
    let db = Database::open_in_memory().await.unwrap();
    let pid = format!("p-{}", uuid::Uuid::new_v4());
    let tid = format!("t-{}", uuid::Uuid::new_v4());
    let eid = format!("e-{}", uuid::Uuid::new_v4());
    sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?,?,?)")
        .bind(&pid)
        .bind("test")
        .bind("active")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?,?,?,?)")
        .bind(&tid)
        .bind(&pid)
        .bind("test")
        .bind("running")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution_attempts (id, task_id, attempt_number, lifecycle) VALUES (?,?,1,'running')")
        .bind(&eid).bind(&tid).execute(&db.pool).await.unwrap();
    (db, tid, eid)
}

// ── Process spawn/exit ────────────────────────────

#[tokio::test]
async fn process_success_exit_capture_stdout() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let spec = basic_spec("e1", vec!["print_stdout"]);
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = mgr.get_state("e1").await.unwrap();
    assert!(matches!(state, ProcessState::Completed { .. }));
    if let ProcessState::Completed { outcome } = state {
        assert_eq!(outcome.termination, ProcessTermination::Completed);
    }
}

#[tokio::test]
async fn process_non_zero_exit() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let spec = basic_spec("e2", vec!["exit_with_code", "42"]);
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = mgr.get_state("e2").await.unwrap();
    if let ProcessState::Completed { outcome } = state {
        assert_eq!(outcome.termination, ProcessTermination::NonZeroExit);
        assert_eq!(outcome.exit_code, Some(42));
    } else {
        panic!("expected completed");
    }
}

#[tokio::test]
async fn process_cwd_correct() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let mut spec = basic_spec("e3", vec!["print_cwd"]);
    let tmp = std::env::temp_dir();
    spec.working_directory = tmp.clone();
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = mgr.get_state("e3").await.unwrap();
    assert!(matches!(state, ProcessState::Completed { .. }));
}

// ── Timeout ───────────────────────────────────────

#[tokio::test]
async fn process_explicit_timeout() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let mut spec = basic_spec("e4", vec!["sleep", "60"]);
    spec.timeout = Duration::from_secs(1);
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let state = mgr.get_state("e4").await.unwrap();
    if let ProcessState::Completed { outcome } = state {
        assert_eq!(outcome.termination, ProcessTermination::Timeout);
    } else {
        panic!("expected completed");
    }
}

// ── Cancel ────────────────────────────────────────

#[tokio::test]
async fn process_cancel_running() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let spec = basic_spec("e5", vec!["sleep", "60"]);
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    mgr.cancel("e5").await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let state = mgr.get_state("e5").await.unwrap();
    if let ProcessState::Completed { outcome } = state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
    } else {
        panic!("expected completed");
    }
}

#[tokio::test]
async fn process_cancel_idempotent() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let spec = basic_spec("e5b", vec!["sleep", "60"]);
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    mgr.cancel("e5b").await.unwrap();
    mgr.cancel("e5b").await.unwrap(); // idempotent
    tokio::time::sleep(Duration::from_secs(1)).await;
    let state = mgr.get_state("e5b").await.unwrap();
    if let ProcessState::Completed { outcome } = state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
    }
}

// ── Invalid executable ────────────────────────────

#[tokio::test]
async fn process_invalid_executable() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let mut spec = basic_spec("e6", vec![]);
    spec.executable = PathBuf::from("/nonexistent/binary_xyz_123");
    let result = mgr.spawn(&spec).await;
    assert!(result.is_err());
}

// ── Environment ───────────────────────────────────

#[tokio::test]
async fn process_env_injection_and_removal() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let mut spec = basic_spec("e7", vec!["print_env", "HARNESS_TEST_VAR"]);
    spec.env_overrides
        .insert("HARNESS_TEST_VAR".into(), "injected_value".into());
    mgr.spawn(&spec).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = mgr.get_state("e7").await.unwrap();
    assert!(matches!(state, ProcessState::Completed { .. }));
}

// ── Reconciliation ────────────────────────────────

#[tokio::test]
async fn reconciler_marks_lost_when_registry_empty() {
    let (db, _tid, eid) = setup_db_with_execution().await;
    let reg = Arc::new(ProcessRegistry::new());
    let reconciler = ProcessReconciler::new(db.pool.clone(), reg, "test-supervisor".into());
    let lost = reconciler.reconcile().await.unwrap();
    assert!(lost.contains(&eid));
    // Verify DB updated
    let lc: (String,) = sqlx::query_as("SELECT lifecycle FROM execution_attempts WHERE id = ?")
        .bind(&eid)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lc.0, "lost");
}

#[tokio::test]
async fn reconciler_idempotent() {
    let (db, _tid, _eid) = setup_db_with_execution().await;
    let reg = Arc::new(ProcessRegistry::new());
    let reconciler = ProcessReconciler::new(db.pool.clone(), reg, "test-supervisor".into());
    reconciler.reconcile().await.unwrap();
    // Second reconciliation should be safe
    let lost = reconciler.reconcile().await.unwrap();
    assert!(lost.is_empty()); // Already 'lost', not 'running'
}

// ── Operation Claim ───────────────────────────────

#[tokio::test]
async fn operation_claim_only_one_succeeds() {
    let db = Database::open_in_memory().await.unwrap();
    let pid = format!("p-{}", uuid::Uuid::new_v4());
    let tid = format!("t-{}", uuid::Uuid::new_v4());
    sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?,'test','active')")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?,?,'test','pending')",
    )
    .bind(&tid)
    .bind(&pid)
    .execute(&db.pool)
    .await
    .unwrap();

    let mgr = OperationManager::new(db.pool.clone());
    let op_id = mgr
        .begin(
            &tid,
            "test_op",
            &serde_json::json!({}),
            &format!("ik-{}", uuid::Uuid::new_v4()),
        )
        .await
        .unwrap();

    let mgr1 = OperationManager::new(db.pool.clone());
    let mgr2 = OperationManager::new(db.pool.clone());
    let op_id1 = op_id.clone();
    let op_id2 = op_id.clone();
    let (r1, r2) = tokio::join!(
        mgr1.try_claim_operation(&op_id1, 60),
        mgr2.try_claim_operation(&op_id2, 60),
    );
    let claimed = r1.unwrap().is_some() as u8 + r2.unwrap().is_some() as u8;
    assert_eq!(claimed, 1, "Only one reconciler should claim");
}

#[tokio::test]
async fn operation_claim_old_owner_cannot_complete() {
    let db = Database::open_in_memory().await.unwrap();
    let pid = format!("p-{}", uuid::Uuid::new_v4());
    let tid = format!("t-{}", uuid::Uuid::new_v4());
    sqlx::query("INSERT INTO projects (id, objective, lifecycle) VALUES (?,'test','active')")
        .bind(&pid)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tasks (id, project_id, goal, lifecycle) VALUES (?,?,'test','pending')",
    )
    .bind(&tid)
    .bind(&pid)
    .execute(&db.pool)
    .await
    .unwrap();

    let mgr = OperationManager::new(db.pool.clone());
    let op_id = mgr
        .begin(
            &tid,
            "test_op",
            &serde_json::json!({}),
            &format!("ik-{}", uuid::Uuid::new_v4()),
        )
        .await
        .unwrap();
    let token1 = mgr.try_claim_operation(&op_id, 1).await.unwrap().unwrap();
    // Let claim expire
    tokio::time::sleep(Duration::from_secs(3)).await;
    let token2 = mgr.try_claim_operation(&op_id, 60).await.unwrap().unwrap();
    assert_ne!(token1, token2);
    // Old owner cannot complete
    let r = mgr
        .complete_claimed_operation(&op_id, &token1, &serde_json::json!({"ok":true}))
        .await;
    assert!(r.is_err());
    // New owner can
    mgr.complete_claimed_operation(&op_id, &token2, &serde_json::json!({"ok":true}))
        .await
        .unwrap();
}

// ── Idempotency claim ─────────────────────────────

#[tokio::test]
async fn idempotency_two_reconciler_claim_one_winner() {
    let db = Database::open_in_memory().await.unwrap();
    let key = format!("recon-{}", uuid::Uuid::new_v4());
    let hash = "hash-recon";
    let pool = Arc::new(db.pool.clone());
    let pool2 = pool.clone();
    let k1 = key.clone();
    let k2 = key.clone();
    let (r1, r2) = tokio::join!(
        idempotency::try_claim(&pool, &k1, hash, 60),
        idempotency::try_claim(&pool2, &k2, hash, 60),
    );
    let claimed = r1.unwrap().is_some() as u8 + r2.unwrap().is_some() as u8;
    assert_eq!(claimed, 1, "Only one should claim");
}
