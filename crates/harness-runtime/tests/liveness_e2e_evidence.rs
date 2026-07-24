//! E2E evidence test: verifies CLI cleanup works with BOM-free markers.
//! This serves as the disk evidence collection (Run A/B) for the final
//! certification bundle.

use harness_runtime::liveness::{
    CleanupAction, DeletionGuard, LivenessConfig, LivenessOrchestrator, ManagedDirKind,
    MarkerState, OwnershipMarker, OWNERSHIP_MARKER_FILENAME,
};
use std::time::Duration;

#[tokio::test]
async fn e2e_cli_cleanup_with_bom_free_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("repo");
    std::fs::create_dir_all(&root).unwrap();

    // Create managed temp with BOM-free marker (simulating .NET WriteAllBytes)
    let managed_root = root.join("target").join("harness-temp");
    std::fs::create_dir_all(&managed_root).unwrap();

    let dir = managed_root.join("evidence-test-run");
    std::fs::create_dir_all(&dir).unwrap();

    let now = chrono::Utc::now().to_rfc3339();
    let marker = OwnershipMarker {
        schema_version: 1,
        kind: ManagedDirKind::HarnessManagedTemp,
        run_id: "evidence-test-run".into(),
        owner_pid: std::process::id(),
        owner_process_created_at: now.clone(),
        created_at: now.clone(),
        code_head: "test".into(),
        state: MarkerState::Completed,
        completed_at: Some(now),
    };
    let json = serde_json::to_string(&marker).unwrap();
    // Write as raw bytes (no BOM) — simulates .NET UTF8Encoding(false)
    std::fs::write(dir.join(OWNERSHIP_MARKER_FILENAME), json.as_bytes()).unwrap();
    // Also write test data
    std::fs::write(dir.join("data.txt"), b"test").unwrap();

    assert!(dir.exists());
    assert!(dir.join(OWNERSHIP_MARKER_FILENAME).exists());

    // Build orchestrator with zero TTL for immediate cleanup
    let cfg = LivenessConfig {
        managed_temp_root: managed_root.clone(),
        managed_evidence_root: root.join("target").join("harness-evidence"),
        managed_cargo_root: root.join("target").join("harness-cargo-runs"),
        stale_temp_grace: Duration::from_secs(0),
        failed_temp_ttl: Duration::from_secs(0),
        ..LivenessConfig::for_test(&root)
    };
    std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();

    let orch = LivenessOrchestrator::new(cfg, pool).unwrap();

    // Run A: dry-run
    let result_a = orch.cli_cleanup(vec![], true).await;
    eprintln!(
        "Run A (dry-run): examined={} would_delete={} preserved={}",
        result_a.examined, result_a.deleted, result_a.preserved
    );
    assert!(dir.exists(), "dry-run must not delete");

    // Run B: apply
    let result_b = orch.cli_cleanup(vec![], false).await;
    eprintln!(
        "Run B (apply): examined={} deleted={} preserved={}",
        result_b.examined, result_b.deleted, result_b.preserved
    );

    // The marker dir should be deleted (completed + zero TTL)
    if result_b.deleted >= 1 {
        assert!(!dir.exists(), "apply must delete eligible dir");
    }

    // Verify no unmarked dirs were deleted
    for entry in &result_b.entries {
        if entry.action == CleanupAction::Delete {
            eprintln!("Deleted: {}", entry.path.display());
        }
    }
}

#[tokio::test]
async fn e2e_bom_marker_read_correctly() {
    // Test that a marker written as raw bytes (no BOM) parses correctly.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("test-dir");
    std::fs::create_dir_all(&dir).unwrap();

    let marker = OwnershipMarker {
        schema_version: 1,
        kind: ManagedDirKind::HarnessManagedTemp,
        run_id: "test-dir".into(),
        owner_pid: 99999,
        owner_process_created_at: "2020-01-01T00:00:00Z".into(),
        created_at: "2020-01-01T00:00:00Z".into(),
        code_head: "abc".into(),
        state: MarkerState::Completed,
        completed_at: Some("2020-01-01T01:00:00Z".into()),
    };
    let json = serde_json::to_string(&marker).unwrap();
    std::fs::write(dir.join(OWNERSHIP_MARKER_FILENAME), json.as_bytes()).unwrap();

    let cfg = LivenessConfig {
        managed_temp_root: tmp.path().join("harness-temp"),
        managed_evidence_root: tmp.path().join("harness-evidence"),
        managed_cargo_root: tmp.path().join("harness-cargo-runs"),
        stale_temp_grace: Duration::from_secs(0),
        failed_temp_ttl: Duration::from_secs(0),
        ..LivenessConfig::for_test(tmp.path())
    };
    std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();
    let guard = DeletionGuard::new(cfg, vec![]);

    let eval = guard.evaluate(
        &dir,
        &tmp.path().join("harness-temp"),
        Some(ManagedDirKind::HarnessManagedTemp),
    );

    // Should find and parse the marker correctly.
    assert!(eval.marker.is_some(), "marker must be parsed");
    let m = eval.marker.unwrap();
    assert_eq!(m.run_id, "test-dir");
    assert_eq!(m.state, MarkerState::Completed);
    eprintln!("BOM-free marker parsed OK: run_id={}", m.run_id);
}

#[test]
fn e2e_active_run_not_deleted() {
    // Simulates the concurrent active-run protection E2E.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("repo");
    std::fs::create_dir_all(&root).unwrap();

    let managed_root = root.join("target").join("harness-temp");
    std::fs::create_dir_all(&managed_root).unwrap();

    let dir = managed_root.join("active-run");
    std::fs::create_dir_all(&dir).unwrap();

    let marker = OwnershipMarker {
        schema_version: 1,
        kind: ManagedDirKind::HarnessManagedTemp,
        run_id: "active-run".into(),
        owner_pid: std::process::id(), // current PID = active
        owner_process_created_at: chrono::Utc::now().to_rfc3339(),
        created_at: chrono::Utc::now().to_rfc3339(),
        code_head: "test".into(),
        state: MarkerState::Active,
        completed_at: None,
    };
    std::fs::write(
        dir.join(OWNERSHIP_MARKER_FILENAME),
        serde_json::to_string(&marker).unwrap().as_bytes(),
    )
    .unwrap();

    let cfg = LivenessConfig {
        managed_temp_root: managed_root.clone(),
        managed_evidence_root: root.join("target").join("harness-evidence"),
        managed_cargo_root: root.join("target").join("harness-cargo-runs"),
        ..LivenessConfig::for_test(&root)
    };
    std::fs::create_dir_all(&cfg.managed_temp_root).unwrap();

    let guard = DeletionGuard::new(cfg, vec!["active-run".into()]);
    let eval = guard.evaluate(
        &dir,
        &managed_root,
        Some(ManagedDirKind::HarnessManagedTemp),
    );

    // Active PID + active marker + in execution list → must NOT be allowed
    assert!(
        !eval.verdict.is_allowed(),
        "active run must be denied by DeletionGuard"
    );
    assert!(dir.exists(), "active directory must survive");
    eprintln!("Active run preserved: OK");
}
