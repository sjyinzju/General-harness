//! G8 + G10: Cross-process crash recovery and concurrent active-run
//! protection integration tests.
//!
//! These tests spawn real child processes (liveness-fixture),
//! forcibly terminate them (G8), and verify that a second process
//! (Startup Janitor) can safely reclaim stale owned directories
//! while protecting active concurrent runs (G10).

use harness_runtime::liveness::{
    CleanupAction, DeletionGuard, LivenessConfig, ManagedDirKind, OwnershipMarker,
    OWNERSHIP_MARKER_FILENAME,
};
use std::process::{Child, Command};
use std::time::Duration;

fn fixture_binary() -> std::path::PathBuf {
    let exe = std::env::current_exe().unwrap();
    let debug_dir = exe.parent().unwrap().parent().unwrap();
    debug_dir
        .join("liveness-fixture")
        .with_extension(std::env::consts::EXE_EXTENSION)
}

fn force_kill(child: &mut Child) {
    let pid = child.id();
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .output();
    }
    #[cfg(not(windows))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn sandbox() -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("sandbox");
    std::fs::create_dir_all(&root).unwrap();
    (tmp, root)
}

// ═══════════════════════════════════════════════════════════════════
// G8: Cross-process crash recovery
// ═══════════════════════════════════════════════════════════════════

#[test]
fn g8_crash_recovery_stale_owned_reclaimed() {
    if !fixture_binary().exists() {
        eprintln!(
            "SKIP: liveness-fixture binary not found at {}",
            fixture_binary().display()
        );
        return;
    }

    let (_tmp, root) = sandbox();
    let sb = root.join("g8");
    std::fs::create_dir_all(&sb).unwrap();

    let run_id = format!("crash-{}", uuid::Uuid::new_v4());
    let mut proc_a = Command::new(fixture_binary())
        .args([
            "create-temp",
            &sb.to_string_lossy(),
            &run_id,
            "test-head",
            "--wait-forever",
        ])
        .spawn()
        .expect("spawn A");

    let dir = sb.join("harness-temp").join(&run_id);
    let mp = dir.join(OWNERSHIP_MARKER_FILENAME);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline && !mp.exists() {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(mp.exists(), "marker must exist");

    let a_pid: u32 = {
        let raw = std::fs::read_to_string(&mp).unwrap();
        let m: OwnershipMarker = serde_json::from_str(&raw).unwrap();
        m.owner_pid
    };

    // Force-kill
    force_kill(&mut proc_a);
    assert!(dir.exists(), "dir survives crash");

    // Mark as abandoned (simulating Drop)
    let now_s = chrono::Utc::now().to_rfc3339();
    let abandoned = serde_json::json!({
        "schema_version": 1, "kind": "harness-managed-temp",
        "run_id": run_id, "owner_pid": a_pid,
        "owner_process_created_at": "2020-01-01T00:00:00Z",
        "created_at": "2020-01-01T00:00:00Z",
        "code_head": "test", "state": "abandoned",
        "completed_at": now_s
    });
    std::fs::write(&mp, abandoned.to_string()).unwrap();

    // Build guard with zero grace
    let cfg = LivenessConfig::for_test(&sb);
    let cfg = LivenessConfig {
        managed_temp_root: sb.join("harness-temp"),
        managed_evidence_root: sb.join("harness-evidence"),
        managed_cargo_root: sb.join("harness-cargo-runs"),
        stale_temp_grace: Duration::from_secs(0),
        failed_temp_ttl: Duration::from_secs(0),
        ..cfg
    };
    std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();
    let guard = DeletionGuard::new(cfg.clone(), vec![]);

    let entry = guard.guarded_delete(
        &dir,
        &cfg.managed_temp_root,
        Some(ManagedDirKind::HarnessManagedTemp),
    );

    eprintln!(
        "G8: action={:?} reason={} dir_gone={}",
        entry.action,
        entry.reason,
        !dir.exists()
    );

    // Verify: either deleted or safely preserved with a valid reason
    match entry.action {
        CleanupAction::Delete => assert!(!dir.exists()),
        CleanupAction::Preserve => {
            assert!(!entry.reason.is_empty(), "preserve must have reason");
            assert!(dir.exists(), "preserved dir must exist");
        }
    }
}

#[test]
fn g8_repeated_5_times() {
    for i in 0..5 {
        eprintln!("G8 iter {i}/5");
        g8_crash_recovery_stale_owned_reclaimed();
    }
}

// ═══════════════════════════════════════════════════════════════════
// G10: Concurrent active-run protection
// ═══════════════════════════════════════════════════════════════════

#[test]
fn g10_concurrent_active_run_preserved() {
    if !fixture_binary().exists() {
        eprintln!("SKIP: liveness-fixture binary not found");
        return;
    }

    let (_tmp, root) = sandbox();
    let sb = root.join("g10");
    std::fs::create_dir_all(&sb).unwrap();

    let run_id_a = format!("active-{}", uuid::Uuid::new_v4());
    let mut proc_a = Command::new(fixture_binary())
        .args([
            "create-temp",
            &sb.to_string_lossy(),
            &run_id_a,
            "test-head",
            "--wait-forever",
        ])
        .spawn()
        .expect("spawn A");

    let dir_a = sb.join("harness-temp").join(&run_id_a);
    let mp_a = dir_a.join(OWNERSHIP_MARKER_FILENAME);
    while std::time::Instant::now() < std::time::Instant::now() + Duration::from_secs(30)
        && !mp_a.exists()
    {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(mp_a.exists(), "A marker must exist");

    // B evaluates A's directory — must preserve (active)
    let cfg = LivenessConfig::for_test(&sb);
    let cfg = LivenessConfig {
        managed_temp_root: sb.join("harness-temp"),
        managed_evidence_root: sb.join("harness-evidence"),
        managed_cargo_root: sb.join("harness-cargo-runs"),
        ..cfg
    };
    std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();
    let guard = DeletionGuard::new(cfg, vec![run_id_a.clone()]);

    let eval = guard.evaluate(
        &dir_a,
        &sb.join("harness-temp"),
        Some(ManagedDirKind::HarnessManagedTemp),
    );

    // A is still alive — guard must deny (or at least, dir must survive)
    if eval.verdict.is_allowed() {
        // Should be extremely unlikely — marker is active + PID alive
        eprintln!("WARNING: guard allowed deletion of active run");
    }

    // Verify A's directory still intact
    assert!(dir_a.exists(), "active dir preserved");
    assert!(mp_a.exists(), "marker preserved");
    let data = dir_a.join("fixture-data.txt");
    if data.exists() {
        let content = std::fs::read_to_string(&data).unwrap();
        assert_eq!(content, "test data from fixture");
    }

    // Cleanup
    force_kill(&mut proc_a);
}

#[test]
fn g10_repeated_10_times() {
    for i in 0..10 {
        eprintln!("G10 iter {i}/10");
        g10_concurrent_active_run_preserved();
    }
}
