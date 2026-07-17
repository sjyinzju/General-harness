//! I2B-0 carryover tests: flood/no-deadlock, invalid UTF-8, output limits,
//! spool creation/readability/cleanup, redaction, termination races, and
//! process-tree termination (Job Object).
//!
//! Requires: `cargo build --bin process-fixture` (build.rs-free workspace —
//! `cargo test --workspace` builds bins first).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use harness_runtime::artifact::{ArtifactOutcome, ArtifactRoot, RetentionPolicy};
use harness_runtime::process::{manager::ProcessManager, registry::ProcessRegistry, types::*};

fn fixture_path() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let debug_dir = exe.parent().unwrap().parent().unwrap();
    debug_dir
        .join("process-fixture")
        .with_extension(std::env::consts::EXE_EXTENSION)
}

fn spec(execution_id: &str, args: Vec<&str>) -> ProcessSpec {
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
        output_byte_limit: 64 * 1024 * 1024,
        spool_dir: None,
        allowed_env_var_names: vec![],
        known_secrets: vec![],
        execution_id: execution_id.to_string(),
        runtime_profile_id: "test-profile".into(),
    }
}

fn mgr() -> ProcessManager {
    ProcessManager::new(Arc::new(ProcessRegistry::new()))
}

async fn wait_done(mgr: &ProcessManager, eid: &str, timeout: Duration) -> ProcessOutcome {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(ProcessState::Completed { outcome }) = mgr.get_state(eid).await {
            return outcome;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timeout waiting for {eid}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Is a PID still alive? (Windows: tasklist; Unix: kill -0)
fn pid_alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        let out = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .expect("tasklist");
        String::from_utf8_lossy(&out.stdout).contains(&pid.to_string())
    }
    #[cfg(not(windows))]
    {
        // `kill -0` probes liveness without a signal — avoids unsafe libc.
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
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
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── Flood / deadlock ─────────────────────────────────────────────

#[tokio::test]
async fn flood_stdout_no_deadlock() {
    let m = mgr();
    // ~1.6 MB — far beyond the OS pipe buffer; hangs forever if not drained.
    let s = spec("flood-out", vec!["flood_stdout", "100000"]);
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "flood-out", Duration::from_secs(20)).await;
    assert_eq!(outcome.termination, ProcessTermination::Completed);
    assert!(
        outcome.stdout_bytes > 1_000_000,
        "expected >1MB drained, got {}",
        outcome.stdout_bytes
    );
}

#[tokio::test]
async fn flood_stderr_no_deadlock() {
    let m = mgr();
    let s = spec("flood-err", vec!["flood_stderr", "100000"]);
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "flood-err", Duration::from_secs(20)).await;
    assert_eq!(outcome.termination, ProcessTermination::Completed);
    assert!(outcome.stderr_bytes > 1_000_000);
}

#[tokio::test]
async fn flood_both_no_deadlock() {
    let m = mgr();
    let s = spec("flood-both", vec!["flood_both", "100000"]);
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "flood-both", Duration::from_secs(30)).await;
    assert_eq!(outcome.termination, ProcessTermination::Completed);
    assert!(outcome.stdout_bytes > 500_000);
    assert!(outcome.stderr_bytes > 500_000);
}

// ── Invalid UTF-8 ────────────────────────────────────────────────

#[tokio::test]
async fn invalid_utf8_safe() {
    let m = mgr();
    let s = spec("bad-utf8", vec!["invalid_utf8"]);
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "bad-utf8", Duration::from_secs(10)).await;
    assert_eq!(outcome.termination, ProcessTermination::Completed);
    assert_eq!(outcome.stdout_bytes, 3);
    // Preview must be valid UTF-8 (lossy) and must not panic anywhere.
    let preview = outcome.stdout_preview.unwrap();
    assert!(preview.contains('\u{FFFD}'), "lossy replacement expected");
}

// ── Output limit ─────────────────────────────────────────────────

#[tokio::test]
async fn output_limit_enforced() {
    let tmp = tempfile::tempdir().unwrap();
    let root = ArtifactRoot::open(&tmp.path().join("artifacts")).unwrap();
    let dir = root
        .create_execution_dir("p1", "r1", "limit-e1", "sup-t")
        .unwrap();

    let m = mgr();
    let mut s = spec("limit-e1", vec!["flood_stdout", "100000"]);
    s.stdout_capture = CapturePolicy::Spool {
        max_memory_bytes: 4 * 1024,
    };
    s.spool_dir = Some(dir.path().to_path_buf());
    s.output_byte_limit = 64 * 1024; // total flood is ~1.6MB
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "limit-e1", Duration::from_secs(20)).await;

    assert_eq!(outcome.termination, ProcessTermination::Completed);
    assert!(outcome.stdout_truncated, "limit must mark truncated");
    assert!(
        outcome.stdout_bytes > 1_000_000,
        "drain continues past limit"
    );
    let spool = PathBuf::from(outcome.stdout_ref.unwrap());
    let len = std::fs::metadata(&spool).unwrap().len();
    assert!(
        len <= 64 * 1024,
        "spool file must not exceed byte limit, got {len}"
    );
}

// ── Spool ────────────────────────────────────────────────────────

#[tokio::test]
async fn spool_created() {
    let tmp = tempfile::tempdir().unwrap();
    let root = ArtifactRoot::open(&tmp.path().join("artifacts")).unwrap();
    let dir = root
        .create_execution_dir("p1", "r1", "spool-e1", "sup-t")
        .unwrap();

    let m = mgr();
    let mut s = spec("spool-e1", vec!["flood_stdout", "5000"]);
    s.stdout_capture = CapturePolicy::Spool {
        max_memory_bytes: 1024,
    };
    s.spool_dir = Some(dir.path().to_path_buf());
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "spool-e1", Duration::from_secs(20)).await;

    assert_eq!(outcome.termination, ProcessTermination::Completed);
    let spool_ref = outcome.stdout_ref.expect("spool ref expected");
    assert!(PathBuf::from(&spool_ref).exists(), "spool file must exist");
    assert!(
        dir.path().join("stdout.spool.meta.json").exists(),
        "spool metadata must exist"
    );
    assert!(!outcome.stdout_truncated);
}

#[tokio::test]
async fn spool_reference_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = ArtifactRoot::open(&tmp.path().join("artifacts")).unwrap();
    let dir = root
        .create_execution_dir("p1", "r1", "spool-e2", "sup-t")
        .unwrap();

    let m = mgr();
    let mut s = spec("spool-e2", vec!["flood_stdout", "1000"]);
    s.stdout_capture = CapturePolicy::Spool {
        max_memory_bytes: 64,
    };
    s.spool_dir = Some(dir.path().to_path_buf());
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "spool-e2", Duration::from_secs(20)).await;

    let content = std::fs::read_to_string(outcome.stdout_ref.unwrap()).unwrap();
    assert!(content.starts_with("stdout line 0"));
    assert!(content.contains("stdout line 999"));
    assert_eq!(content.len() as u64, outcome.stdout_bytes);

    let meta: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("stdout.spool.meta.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["execution_id"], "spool-e2");
    assert_eq!(meta["total_bytes"].as_u64().unwrap(), outcome.stdout_bytes);
    assert_eq!(meta["truncated"], false);
}

#[tokio::test]
async fn spool_cleanup_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let root = ArtifactRoot::open(&tmp.path().join("artifacts")).unwrap();

    // Success + KeepOnFailureOnly → deleted.
    let dir_ok = root
        .create_execution_dir("p1", "r1", "clean-ok", "sup-t")
        .unwrap();
    let m = mgr();
    let mut s = spec("clean-ok", vec!["flood_stdout", "2000"]);
    s.stdout_capture = CapturePolicy::Spool {
        max_memory_bytes: 64,
    };
    s.spool_dir = Some(dir_ok.path().to_path_buf());
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "clean-ok", Duration::from_secs(20)).await;
    assert_eq!(outcome.termination, ProcessTermination::Completed);
    let ok_path = dir_ok.path().to_path_buf();
    dir_ok
        .close(RetentionPolicy::KeepOnFailureOnly, ArtifactOutcome::Success)
        .unwrap();
    assert!(!ok_path.exists(), "success artifacts deleted by policy");

    // Failure + KeepOnFailureOnly → kept (spool still readable).
    let dir_fail = root
        .create_execution_dir("p1", "r1", "clean-fail", "sup-t")
        .unwrap();
    let mut s = spec("clean-fail", vec!["flood_stdout", "2000"]);
    s.stdout_capture = CapturePolicy::Spool {
        max_memory_bytes: 64,
    };
    s.spool_dir = Some(dir_fail.path().to_path_buf());
    // flood then non-zero exit is not a fixture mode; treat Completed run as
    // "failed" at the artifact layer — retention decision is caller-owned.
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "clean-fail", Duration::from_secs(20)).await;
    let spool_ref = outcome.stdout_ref.unwrap();
    dir_fail
        .close(RetentionPolicy::KeepOnFailureOnly, ArtifactOutcome::Failure)
        .unwrap();
    assert!(
        PathBuf::from(&spool_ref).exists(),
        "failure artifacts kept by policy"
    );
}

// ── Redaction ────────────────────────────────────────────────────

#[tokio::test]
async fn known_secret_redacted() {
    let m = mgr();
    let secret = "sk-test-supersecret-0451";
    let mut s = spec("redact-e1", vec!["print_env", "HARNESS_SECRET_PROBE"]);
    s.env_overrides
        .insert("HARNESS_SECRET_PROBE".into(), secret.into());
    s.known_secrets = vec![secret.to_string()];
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "redact-e1", Duration::from_secs(10)).await;

    assert_eq!(outcome.termination, ProcessTermination::Completed);
    let preview = outcome.stdout_preview.unwrap();
    assert!(
        !preview.contains(secret),
        "secret value must not appear in preview: {preview}"
    );
    assert!(
        preview.contains("[REDACTED]"),
        "placeholder expected: {preview}"
    );
    assert!(
        preview.contains("HARNESS_SECRET_PROBE"),
        "env NAME may appear"
    );
}

// ── Termination races: exactly one outcome ───────────────────────

async fn assert_single_stable_outcome(
    m: &ProcessManager,
    eid: &str,
    allowed: &[ProcessTermination],
) -> ProcessTermination {
    let first = wait_done(m, eid, Duration::from_secs(15)).await;
    assert!(
        allowed.contains(&first.termination),
        "unexpected termination {:?}",
        first.termination
    );
    // Re-read after a delay: the outcome must not change (produced once).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let Some(ProcessState::Completed { outcome: second }) = m.get_state(eid).await else {
        panic!("state must remain Completed");
    };
    assert_eq!(first, second, "outcome must be produced exactly once");
    first.termination
}

#[tokio::test]
async fn timeout_vs_natural_exit_single_outcome() {
    let m = mgr();
    // Natural runtime ~1s and timeout 1s race each other.
    let mut s = spec("race-tn", vec!["sleep", "1"]);
    s.timeout = Duration::from_secs(1);
    m.spawn(&s).await.unwrap();
    assert_single_stable_outcome(
        &m,
        "race-tn",
        &[ProcessTermination::Timeout, ProcessTermination::Completed],
    )
    .await;
}

#[tokio::test]
async fn cancel_vs_natural_exit_single_outcome() {
    let m = mgr();
    // Process exits almost immediately; cancel lands right around exit.
    let s = spec("race-cn", vec!["print_stdout"]);
    m.spawn(&s).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = m.cancel("race-cn").await;
    assert_single_stable_outcome(
        &m,
        "race-cn",
        &[ProcessTermination::Cancelled, ProcessTermination::Completed],
    )
    .await;
}

#[tokio::test]
async fn timeout_vs_cancel_single_outcome() {
    let m = mgr();
    let mut s = spec("race-tc", vec!["sleep", "60"]);
    s.timeout = Duration::from_secs(1);
    m.spawn(&s).await.unwrap();
    tokio::time::sleep(Duration::from_millis(950)).await;
    let _ = m.cancel("race-tc").await;
    assert_single_stable_outcome(
        &m,
        "race-tc",
        &[ProcessTermination::Cancelled, ProcessTermination::Timeout],
    )
    .await;
}

// ── Process tree termination ─────────────────────────────────────

fn parse_child_pid(preview: &str) -> u32 {
    preview
        .lines()
        .find_map(|l| l.strip_prefix("child_pid="))
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or_else(|| panic!("child_pid not found in preview: {preview:?}"))
}

#[tokio::test]
async fn grandchild_tree_terminated() {
    let m = mgr();
    // Root stays alive; intermediate exits leaving an orphaned grandchild
    // (sleep 10). taskkill /T cannot reach it — the Job Object must.
    let s = spec("tree-kill", vec!["spawn_tree_and_sleep"]);
    m.spawn(&s).await.unwrap();

    // Wait until the grandchild pid is visible on captured stdout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    tokio::time::sleep(Duration::from_millis(800)).await;

    m.cancel("tree-kill").await.unwrap();
    let outcome = wait_done(&m, "tree-kill", Duration::from_secs(15)).await;
    assert_eq!(outcome.termination, ProcessTermination::Cancelled);

    let grandchild = parse_child_pid(outcome.stdout_preview.as_deref().unwrap_or(""));
    assert!(grandchild > 0);
    let _ = deadline;
    assert!(
        wait_pid_dead(grandchild, Duration::from_secs(5)).await,
        "orphaned grandchild {grandchild} must be terminated with the tree"
    );
}

#[tokio::test]
async fn no_residual_descendants() {
    let m = mgr();
    // Root exits naturally, orphaning a `sleep 10` descendant. After the
    // outcome is finalized no descendant may survive (job close reaps it).
    let s = spec("no-residue", vec!["spawn_grandchild"]);
    m.spawn(&s).await.unwrap();
    let outcome = wait_done(&m, "no-residue", Duration::from_secs(15)).await;
    assert_eq!(outcome.termination, ProcessTermination::Completed);

    let orphan = parse_child_pid(outcome.stdout_preview.as_deref().unwrap_or(""));
    assert!(
        wait_pid_dead(orphan, Duration::from_secs(5)).await,
        "residual descendant {orphan} survived the execution"
    );
}
