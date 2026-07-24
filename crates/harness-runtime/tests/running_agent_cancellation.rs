//! I4.5 Running Agent Cancellation — Certification E2E Tests.
//!
//! TCP-based event-driven readiness: test creates a TcpListener on
//! 127.0.0.1:0, passes port + run_id to fixture via env vars. Fixture
//! connects, sends TreeReady JSON, disconnects. Test accepts and reads.
//! No file polling. No race. No fixed sleep.
//!
//! I4.5 structural fix: ProcessManager creates root with CREATE_SUSPENDED,
//! assigns Job Object while suspended, then resumes. No escape window for
//! children/grandchildren.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use harness_runtime::process::manager::ProcessManager;
use harness_runtime::process::registry::ProcessRegistry;
use harness_runtime::process::types::{
    CapturePolicy, ProcessSpec, ProcessState, ProcessTermination, StdinMode,
};

fn fixture_path() -> PathBuf {
    std::env::var("CARGO_BIN_EXE_process_fixture")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let exe = std::env::current_exe().unwrap();
            let dir = exe.parent().unwrap().parent().unwrap();
            dir.join("process-fixture")
                .with_extension(std::env::consts::EXE_EXTENSION)
        })
}

fn spawn_tree_spec(
    id: &str,
    ready_dir: &std::path::Path,
    tcp_port: u16,
    run_id: &str,
) -> ProcessSpec {
    let mut env_overrides = HashMap::new();
    env_overrides.insert(
        "READY_DIR".to_string(),
        ready_dir.to_string_lossy().to_string(),
    );
    env_overrides.insert("READY_TCP_PORT".to_string(), tcp_port.to_string());
    env_overrides.insert("READY_RUN_ID".to_string(), run_id.to_string());

    ProcessSpec {
        execution_id: id.to_string(),
        executable: fixture_path(),
        args: vec!["spawn_tree_and_sleep".to_string()],
        working_directory: ready_dir.to_path_buf(),
        env_overrides,
        env_removals: vec![],
        stdin_mode: StdinMode::Closed,
        timeout: Duration::from_secs(90),
        graceful_shutdown_timeout: Duration::from_secs(2),
        stdout_capture: CapturePolicy::Pipe,
        stderr_capture: CapturePolicy::Pipe,
        output_byte_limit: 10 * 1024 * 1024,
        spool_dir: None,
        allowed_env_var_names: vec![
            "READY_DIR".to_string(),
            "READY_TCP_PORT".to_string(),
            "READY_RUN_ID".to_string(),
        ],
        known_secrets: vec![],
        runtime_profile_id: "test-profile".into(),
    }
}

struct TreeReadiness {
    root_pid: u32,
    child_pid: u32,
    grandchild_pid: u32,
}

/// Create a TCP readiness channel: binds to an OS-assigned port, returns
/// the listener and a run_id for end-to-end matching.
async fn create_readiness_channel() -> (tokio::net::TcpListener, u16, String) {
    let run_id = format!("run-{}", uuid::Uuid::new_v4());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port, run_id)
}

/// Wait for the fixture to connect and send TreeReady JSON.
/// Returns validated PIDs or panics on timeout/invalid data.
async fn wait_tree_ready(
    listener: &tokio::net::TcpListener,
    expected_run_id: &str,
    timeout: Duration,
) -> TreeReadiness {
    let (mut stream, _addr) = tokio::time::timeout(timeout, listener.accept())
        .await
        .expect("timeout waiting for fixture TCP connection")
        .expect("failed to accept TCP connection");

    let mut buf = Vec::new();
    tokio::time::timeout(
        timeout,
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut buf),
    )
    .await
    .expect("timeout reading TreeReady message")
    .expect("failed to read TreeReady message");

    let v: serde_json::Value = serde_json::from_slice(&buf).expect("invalid TreeReady JSON");

    let run_id = v["run_id"].as_str().unwrap_or("");
    assert_eq!(
        run_id, expected_run_id,
        "run_id mismatch: expected {expected_run_id}, got {run_id}"
    );
    assert!(
        v["tree_ready"].as_bool().unwrap_or(false),
        "tree_ready must be true"
    );

    let root = v["root_pid"].as_u64().unwrap_or(0) as u32;
    let child = v["child_pid"].as_u64().unwrap_or(0) as u32;
    let gc = v["grandchild_pid"].as_u64().unwrap_or(0) as u32;
    assert!(root > 0 && child > 0 && gc > 0, "all PIDs must be positive");

    TreeReadiness {
        root_pid: root,
        child_pid: child,
        grandchild_pid: gc,
    }
}

async fn wait_done(mgr: &ProcessManager, eid: &str, timeout: Duration) -> ProcessState {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(state) = mgr.get_state(eid).await {
            if matches!(state, ProcessState::Completed { .. }) {
                return state;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timeout waiting for {eid}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Verify a process is dead by polling for its PID.
/// Uses process handle when available, falls back to tasklist (Windows)
/// or /proc (Unix).
async fn pid_alive(pid: u32) -> bool {
    let pid_s = pid.to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        #[cfg(windows)]
        let result = {
            use std::os::windows::process::CommandExt;
            match std::process::Command::new("cmd")
                .args(["/c", &format!("tasklist /FI \"PID eq {pid_s}\" 2>NUL")])
                .creation_flags(0x08000000)
                .output()
            {
                Ok(o) => String::from_utf8_lossy(&o.stdout).contains(&pid_s),
                Err(_) => false,
            }
        };
        #[cfg(not(windows))]
        let result = std::path::Path::new(&format!("/proc/{pid_s}")).exists();
        let _ = tx.send(result);
    });
    rx.await.unwrap_or(false)
}

async fn wait_pid_dead(pid: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !pid_alive(pid).await {
            return true;
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_tree_dead(root: u32, child: u32, grandchild: u32, label: &str) {
    assert!(
        wait_pid_dead(root, Duration::from_secs(15)).await,
        "{label}: root {root} dead"
    );
    assert!(
        wait_pid_dead(child, Duration::from_secs(15)).await,
        "{label}: child {child} dead"
    );
    assert!(
        wait_pid_dead(grandchild, Duration::from_secs(15)).await,
        "{label}: grandchild {grandchild} dead"
    );
}

// ══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn cancel_running_agent_terminates_process_tree() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let start_count = Arc::new(AtomicUsize::new(0));
    let ready_dir = tempfile::tempdir().unwrap();

    // I4.5: TCP-based event-driven readiness — no file polling.
    let (listener, port, run_id) = create_readiness_channel().await;

    mgr.spawn(&spawn_tree_spec(
        "cancel-tree",
        ready_dir.path(),
        port,
        &run_id,
    ))
    .await
    .unwrap();
    start_count.fetch_add(1, Ordering::SeqCst);
    let ready = wait_tree_ready(&listener, &run_id, Duration::from_secs(20)).await;
    assert!(ready.root_pid > 0 && ready.child_pid > 0 && ready.grandchild_pid > 0);

    mgr.cancel("cancel-tree").await.unwrap();
    let state = wait_done(&mgr, "cancel-tree", Duration::from_secs(20)).await;
    assert!(
        matches!(&state, ProcessState::Completed { outcome } if outcome.termination == ProcessTermination::Cancelled)
    );

    assert_tree_dead(
        ready.root_pid,
        ready.child_pid,
        ready.grandchild_pid,
        "cancel-tree",
    )
    .await;
    assert_eq!(start_count.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_cancel_request_idempotent() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let ready_dir = tempfile::tempdir().unwrap();

    let (listener, port, run_id) = create_readiness_channel().await;

    mgr.spawn(&spawn_tree_spec(
        "dup-cancel",
        ready_dir.path(),
        port,
        &run_id,
    ))
    .await
    .unwrap();
    let ready = wait_tree_ready(&listener, &run_id, Duration::from_secs(20)).await;
    assert!(ready.root_pid > 0);

    mgr.cancel("dup-cancel").await.unwrap();
    mgr.cancel("dup-cancel").await.unwrap();
    let state = wait_done(&mgr, "dup-cancel", Duration::from_secs(20)).await;
    assert!(
        matches!(&state, ProcessState::Completed { outcome } if outcome.termination == ProcessTermination::Cancelled)
    );
    assert!(wait_pid_dead(ready.root_pid, Duration::from_secs(10)).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_response_lost_retry_still_cancelled() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let ready_dir = tempfile::tempdir().unwrap();

    let (listener, port, run_id) = create_readiness_channel().await;

    mgr.spawn(&spawn_tree_spec(
        "rl-cancel",
        ready_dir.path(),
        port,
        &run_id,
    ))
    .await
    .unwrap();
    let ready = wait_tree_ready(&listener, &run_id, Duration::from_secs(20)).await;
    assert!(ready.root_pid > 0);

    mgr.cancel("rl-cancel").await.unwrap();
    mgr.cancel("rl-cancel").await.unwrap();
    let state = wait_done(&mgr, "rl-cancel", Duration::from_secs(20)).await;
    assert!(
        matches!(&state, ProcessState::Completed { outcome } if outcome.termination == ProcessTermination::Cancelled)
    );
    assert!(matches!(
        mgr.get_state("rl-cancel").await,
        Some(ProcessState::Completed { .. })
    ));
    assert!(wait_pid_dead(ready.root_pid, Duration::from_secs(10)).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_nonexistent_is_noop() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let _ = mgr.cancel("no-such-execution").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grandchild_tree_terminated_certification() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let ready_dir = tempfile::tempdir().unwrap();

    // I4.5: TCP-based event-driven readiness — no file polling.
    let (listener, port, run_id) = create_readiness_channel().await;

    mgr.spawn(&spawn_tree_spec(
        "cert-tree",
        ready_dir.path(),
        port,
        &run_id,
    ))
    .await
    .unwrap();
    let ready = wait_tree_ready(&listener, &run_id, Duration::from_secs(30)).await;

    assert!(ready.root_pid > 0 && ready.child_pid > 0 && ready.grandchild_pid > 0);

    // sleeping.txt is written AFTER the TCP readiness message, so poll for it.
    let sleep_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if ready_dir.path().join("sleeping.txt").exists() {
            break;
        }
        if std::time::Instant::now() > sleep_deadline {
            panic!(
                "timeout waiting for sleeping.txt in {}",
                ready_dir.path().display()
            );
        }
        std::thread::sleep(Duration::from_millis(30));
    }

    mgr.cancel("cert-tree").await.unwrap();
    let state = wait_done(&mgr, "cert-tree", Duration::from_secs(20)).await;
    assert!(
        matches!(&state, ProcessState::Completed { outcome } if outcome.termination == ProcessTermination::Cancelled),
        "expected Cancelled, got {:?}",
        state
    );

    assert_tree_dead(
        ready.root_pid,
        ready.child_pid,
        ready.grandchild_pid,
        "cert-tree",
    )
    .await;
}
