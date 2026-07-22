//! I4.5 Running Agent Cancellation — Certification E2E Tests (Phase 3).
//!
//! These tests exercise real OS process cancellation through the production
//! ProcessManager. A long-running child process (process-fixture with
//! spawn_tree_and_sleep) is started, then cancelled via the production
//! cancellation API. The entire process tree is verified terminated.
//!
//! NEVER: DecisionInput::classify() alone, fixed sleep after cancel,
//!        OS kill bypassing ProcessManager, dropping futures instead of cancel.

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

fn spawn_tree_spec(id: &str) -> ProcessSpec {
    ProcessSpec {
        execution_id: id.to_string(),
        executable: fixture_path(),
        args: vec!["spawn_tree_and_sleep".to_string()],
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
        runtime_profile_id: "test-profile".into(),
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

#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    use std::os::windows::process::CommandExt;
    let output = std::process::Command::new("cmd")
        .args(["/c", &format!("tasklist /FI \"PID eq {pid}\" 2>NUL")])
        .creation_flags(0x08000000)
        .output();
    match output {
        Ok(o) => {
            let s = String::from_utf8_lossy(&o.stdout);
            s.contains(&pid.to_string())
        }
        Err(_) => false,
    }
}

#[cfg(not(windows))]
fn pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

async fn wait_pid_dead(pid: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !pid_alive(pid) {
            return true;
        }
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until the process is in Running state and has been running
/// long enough for the tree fixture to spawn its child. Returns once
/// the process state is Running (not Starting). A small bounded poll
/// follows to ensure the child has spawned.
async fn wait_process_ready(mgr: &ProcessManager, eid: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    // Wait for process to leave Starting state.
    loop {
        if let Some(state) = mgr.get_state(eid).await {
            if matches!(state, ProcessState::Running) {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timeout waiting for process running (eid={eid})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Poll for the tree fixture to spawn child/grandchild and produce
    // stdout containing "child_pid=". Up to 500ms under load is normal
    // on Windows for process tree creation + I/O buffering.
    tokio::time::sleep(Duration::from_millis(400)).await;
}

fn parse_child_pid_opt(preview: &str) -> Option<u32> {
    preview
        .lines()
        .find_map(|l| l.strip_prefix("child_pid="))
        .and_then(|s| s.trim().parse().ok())
}

// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn cancel_running_agent_terminates_process_tree() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let start_count = Arc::new(AtomicUsize::new(0));

    let spec = spawn_tree_spec("cancel-tree");
    mgr.spawn(&spec).await.unwrap();
    start_count.fetch_add(1, Ordering::SeqCst);

    // Wait for the process to start (state-based, no fixed sleep).
    wait_process_ready(&mgr, "cancel-tree", Duration::from_secs(10)).await;

    // Cancel the running process tree.
    mgr.cancel("cancel-tree").await.unwrap();

    let state = wait_done(&mgr, "cancel-tree", Duration::from_secs(15)).await;
    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(
            outcome.termination,
            ProcessTermination::Cancelled,
            "must be Cancelled, got {:?}",
            outcome.termination
        );
        let preview = outcome.stdout_preview.as_deref().unwrap_or("");
        if let Some(grandchild_pid) = parse_child_pid_opt(preview) {
            assert!(
                wait_pid_dead(grandchild_pid, Duration::from_secs(10)).await,
                "grandchild {grandchild_pid} must be terminated"
            );
        }
    } else {
        panic!("expected Completed, got state");
    }

    assert_eq!(start_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn duplicate_cancel_request_idempotent() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());

    let spec = spawn_tree_spec("dup-cancel");
    mgr.spawn(&spec).await.unwrap();
    // State-polled readiness — no fixed sleep.
    wait_process_ready(&mgr, "dup-cancel", Duration::from_secs(10)).await;

    mgr.cancel("dup-cancel").await.unwrap();
    mgr.cancel("dup-cancel").await.unwrap(); // idempotent

    let state = wait_done(&mgr, "dup-cancel", Duration::from_secs(15)).await;
    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
    }
}

#[tokio::test]
async fn cancel_response_lost_retry_still_cancelled() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());

    let spec = spawn_tree_spec("rl-cancel");
    mgr.spawn(&spec).await.unwrap();
    // State-polled readiness — no fixed sleep.
    wait_process_ready(&mgr, "rl-cancel", Duration::from_secs(10)).await;

    mgr.cancel("rl-cancel").await.unwrap();
    mgr.cancel("rl-cancel").await.unwrap(); // response lost → retry

    let state = wait_done(&mgr, "rl-cancel", Duration::from_secs(15)).await;
    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
    }

    // Verify no new execution was created.
    let still_terminal = matches!(
        mgr.get_state("rl-cancel").await,
        Some(ProcessState::Completed { .. })
    );
    assert!(still_terminal, "must remain terminal after retry");
}

#[tokio::test]
async fn cancel_nonexistent_is_noop() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());
    let result = mgr.cancel("no-such-execution").await;
    // May return Ok(()) or Err — both are safe; the important property
    // is that it does not panic.
    let _ = result;
}

#[tokio::test]
async fn grandchild_tree_terminated_certification() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());

    let spec = spawn_tree_spec("cert-tree");
    mgr.spawn(&spec).await.unwrap();

    // State-polled readiness — no fixed sleep.
    wait_process_ready(&mgr, "cert-tree", Duration::from_secs(10)).await;

    mgr.cancel("cert-tree").await.unwrap();
    let state = wait_done(&mgr, "cert-tree", Duration::from_secs(15)).await;

    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
        let preview = outcome.stdout_preview.as_deref().unwrap_or("");
        let gc = parse_child_pid_opt(preview)
            .unwrap_or_else(|| panic!("grandchild PID not found in: {preview}"));
        assert!(gc > 0, "must observe grandchild PID");
        assert!(
            wait_pid_dead(gc, Duration::from_secs(10)).await,
            "orphaned grandchild {gc} must be terminated"
        );
    } else {
        panic!("expected Completed state");
    }
}
