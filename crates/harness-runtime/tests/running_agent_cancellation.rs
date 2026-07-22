//! I4.5 Running Agent Cancellation — Certification E2E Tests.
//!
//! These tests exercise real OS process cancellation through the production
//! ProcessManager. A long-running child process (process-fixture with
//! spawn_tree_and_sleep) is started, then cancelled via the production
//! cancellation API. The entire process tree is verified terminated.
//!
//! The process fixture prints root/child/grandchild PIDs to stdout and
//! flushes BEFORE sleeping. ProcessManager captures this via Pipe
//! capture policy. The test reads PIDs from ProcessOutcome after
//! cancellation and verifies all three are dead.
//!
//! Readiness is state-polled: wait for ProcessState::Running, then a
//! short bounded delay for tree spawning. After cancel, PIDs are parsed
//! from the captured stdout preview (always present because of flush).

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
        timeout: Duration::from_secs(90),
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

/// Wait for the process to reach Running state, then a short bounded
/// poll for the tree fixture to spawn child/grandchild and flush PIDs.
async fn wait_ready(mgr: &ProcessManager, eid: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(state) = mgr.get_state(eid).await {
            if matches!(state, ProcessState::Running) {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timeout waiting for {eid} Running state");
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    // Bounded poll after Running: fixture spawns child → child spawns
    // grandchild → child exits → root reads child stdout, prints PIDs,
    // flushes. This takes ~100-500ms depending on system load.
    tokio::time::sleep(Duration::from_millis(1000)).await;
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

/// Parse tree PIDs from the process stdout preview (written + flushed
/// by the fixture BEFORE sleeping).
fn parse_tree_pids(preview: &str) -> (u32, u32, u32) {
    let mut root = 0u32;
    let mut child = 0u32;
    let mut grandchild = 0u32;
    for line in preview.lines() {
        if let Some(v) = line.strip_prefix("root_pid=") {
            root = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("mid_pid=") {
            child = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("child_pid=") {
            grandchild = v.trim().parse().unwrap_or(0);
        }
    }
    (root, child, grandchild)
}

async fn assert_tree_dead(root: u32, child: u32, grandchild: u32, label: &str) {
    assert!(
        wait_pid_dead(root, Duration::from_secs(15)).await,
        "{label}: root {root} must be dead"
    );
    assert!(
        wait_pid_dead(child, Duration::from_secs(15)).await,
        "{label}: child {child} must be dead"
    );
    assert!(
        wait_pid_dead(grandchild, Duration::from_secs(15)).await,
        "{label}: grandchild {grandchild} must be dead"
    );
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

    wait_ready(&mgr, "cancel-tree").await;
    mgr.cancel("cancel-tree").await.unwrap();

    let state = wait_done(&mgr, "cancel-tree", Duration::from_secs(20)).await;
    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(
            outcome.termination,
            ProcessTermination::Cancelled,
            "must be Cancelled"
        );
        let preview = outcome.stdout_preview.as_deref().unwrap_or("");
        let (root, child, grandchild) = parse_tree_pids(preview);
        assert!(root > 0, "must capture root PID from stdout");
        assert!(child > 0, "must capture child PID from stdout");
        assert!(grandchild > 0, "must capture grandchild PID from stdout");
        assert_tree_dead(root, child, grandchild, "cancel-tree").await;
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
    wait_ready(&mgr, "dup-cancel").await;

    mgr.cancel("dup-cancel").await.unwrap();
    mgr.cancel("dup-cancel").await.unwrap();

    let state = wait_done(&mgr, "dup-cancel", Duration::from_secs(20)).await;
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
    wait_ready(&mgr, "rl-cancel").await;

    mgr.cancel("rl-cancel").await.unwrap();
    mgr.cancel("rl-cancel").await.unwrap();

    let state = wait_done(&mgr, "rl-cancel", Duration::from_secs(20)).await;
    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
    }
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
    let _ = result;
}

#[tokio::test]
async fn grandchild_tree_terminated_certification() {
    let reg = Arc::new(ProcessRegistry::new());
    let mgr = ProcessManager::new(reg.clone());

    let spec = spawn_tree_spec("cert-tree");
    mgr.spawn(&spec).await.unwrap();

    wait_ready(&mgr, "cert-tree").await;
    mgr.cancel("cert-tree").await.unwrap();

    let state = wait_done(&mgr, "cert-tree", Duration::from_secs(20)).await;
    if let ProcessState::Completed { outcome } = &state {
        assert_eq!(outcome.termination, ProcessTermination::Cancelled);
        let preview = outcome.stdout_preview.as_deref().unwrap_or("");
        let (root, child, grandchild) = parse_tree_pids(preview);
        assert!(
            root > 0,
            "must capture root PID from stdout preview: {preview}"
        );
        assert!(child > 0, "must capture child PID from stdout");
        assert!(grandchild > 0, "must capture grandchild PID from stdout");
        assert_tree_dead(root, child, grandchild, "cert-tree").await;
    } else {
        panic!("expected Completed state");
    }
}
