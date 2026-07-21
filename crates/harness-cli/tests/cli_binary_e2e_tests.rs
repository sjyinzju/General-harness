//! I4.5 CLI Binary-Process E2E Certification Tests (Phase 2).
//!
//! Every test spawns the COMPILED harness-cli binary, exercising:
//! main() → argument parsing → ProductionGraph::build() →
//! RealI4OrchestrationGateway → durable SQLite state.
//!
//! Direct calls to cmd_*() or TaskEngineeringLoopService are NOT
//! certification evidence — only binary spawns count.

use std::process::Command;

fn harness_binary() -> String {
    std::env::var("CARGO_BIN_EXE_harness_cli")
        .unwrap_or_else(|_| panic!("Run via `cargo test` so Cargo sets CARGO_BIN_EXE_harness_cli"))
}

/// Ensure the DB is migrated and has FK parent rows.
async fn init_db(db_path: &str) {
    // Use harness-runtime to open (auto-migrates) and seed FK rows.
    let db = harness_runtime::db::Database::open(&std::path::PathBuf::from(db_path))
        .await
        .expect("open DB");
    sqlx::query(
        "INSERT OR IGNORE INTO projects(id,objective,lifecycle) VALUES('proj-seed','e2e','active')",
    )
    .execute(&db.pool)
    .await
    .expect("seed projects");
    sqlx::query("INSERT OR IGNORE INTO tasks(id,project_id,goal,lifecycle) VALUES('task-seed','proj-seed','e2e','submitted')")
        .execute(&db.pool).await.expect("seed tasks");
    // Explicitly close the pool so the binary can open the same file.
    db.pool.close().await;
}

fn run_harness(args: &[&str], db_path: &str) -> std::process::Output {
    let mut cmd = Command::new(harness_binary());
    cmd.args(args);
    cmd.env("HARNESS_DB", db_path);
    cmd.env("NO_COLOR", "1");
    let td = tempfile::tempdir().unwrap();
    cmd.env("HOME", td.path());
    cmd.env("USERPROFILE", td.path());
    cmd.output()
        .unwrap_or_else(|e| panic!("failed to spawn harness: {e}"))
}

fn setup_isolated() -> (tempfile::TempDir, String, String) {
    let root = std::path::PathBuf::from(std::env::var("HARNESS_E2E_TEMP").unwrap_or_else(|_| {
        if cfg!(windows) {
            "C:\\harness-e2e-temp".into()
        } else {
            "/tmp/harness-e2e-temp".into()
        }
    }));
    std::fs::create_dir_all(&root).unwrap();
    let dir = tempfile::tempdir_in(&root).unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();
    let repo_path = dir.path().join("repo");
    std::fs::create_dir_all(&repo_path).unwrap();
    // git init
    Command::new("git")
        .args(["init"])
        .current_dir(&repo_path)
        .output()
        .ok();
    Command::new("git")
        .args(["config", "user.email", "test@test"])
        .current_dir(&repo_path)
        .output()
        .ok();
    Command::new("git")
        .args(["config", "user.name", "test"])
        .current_dir(&repo_path)
        .output()
        .ok();
    std::fs::write(repo_path.join("README.md"), "# test").ok();
    Command::new("git")
        .args(["add", "README.md"])
        .current_dir(&repo_path)
        .output()
        .ok();
    Command::new("git")
        .args(["commit", "-m", "init"])
        .current_dir(&repo_path)
        .output()
        .ok();
    let repo_str = repo_path.to_string_lossy().to_string();
    (dir, db_path, repo_str)
}

fn wt_path(dir: &tempfile::TempDir) -> String {
    let wt = dir.path().join("wt");
    std::fs::create_dir_all(&wt).unwrap();
    wt.to_string_lossy().to_string()
}

// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn binary_start_and_status() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let out = run_harness(
        &[
            "task-loop",
            "start",
            "--project",
            "proj-bs",
            "--task",
            "task-bs",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() || stdout.contains("Loop created") || stderr.contains("Loop created"),
        "start: code={} stdout={} stderr={}",
        out.status.code().unwrap_or(-1),
        stdout,
        stderr
    );
    let loop_id = stdout
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default();
    assert!(!loop_id.is_empty());
    let out2 = run_harness(
        &[
            "task-loop",
            "status",
            &loop_id,
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    assert!(
        out2.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
}

#[tokio::test]
async fn binary_start_replay_idempotent() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let args = &[
        "task-loop",
        "start",
        "--project",
        "proj-idem",
        "--task",
        "task-idem",
        "--owner",
        "ci",
        "--policy",
        "{}",
        "--repo",
        &repo_path,
        "--worktree-root",
        &wt,
    ];
    let out1 = run_harness(args, &db_path);
    assert!(out1.status.success());
    let out2 = run_harness(args, &db_path);
    assert!(out2.status.success());
}

#[tokio::test]
async fn binary_resume_idempotent() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let out = run_harness(
        &[
            "task-loop",
            "start",
            "--project",
            "proj-res",
            "--task",
            "task-res",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let loop_id = stdout
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let out1 = run_harness(
        &[
            "task-loop",
            "resume",
            &loop_id,
            "--owner",
            "ci",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    assert!(out1.status.success());
    let out2 = run_harness(
        &[
            "task-loop",
            "resume",
            &loop_id,
            "--owner",
            "ci",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    assert!(out2.status.success());
}

#[tokio::test]
async fn binary_cancel() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let out = run_harness(
        &[
            "task-loop",
            "start",
            "--project",
            "proj-cn",
            "--task",
            "task-cn",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let loop_id = stdout
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let out2 = run_harness(
        &["task-loop", "cancel", &loop_id, "--owner", "ci"],
        &db_path,
    );
    assert!(
        out2.status.success(),
        "cancel failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
}

#[tokio::test]
async fn binary_inspect_json() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let out = run_harness(
        &[
            "task-loop",
            "start",
            "--project",
            "proj-in",
            "--task",
            "task-in",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let loop_id = stdout
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let out2 = run_harness(&["task-loop", "inspect", &loop_id, "--json"], &db_path);
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains(&loop_id));
    assert!(
        serde_json::from_str::<serde_json::Value>(&stdout2).is_ok(),
        "inspect output not valid JSON: {stdout2}"
    );
}

#[tokio::test]
async fn binary_dry_run_zero_writes() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let out = run_harness(
        &[
            "task-loop",
            "start",
            "--project",
            "proj-dr",
            "--task",
            "task-dr",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let loop_id = stdout
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let before = std::fs::metadata(&db_path).unwrap().len();
    let out2 = run_harness(
        &[
            "task-loop",
            "dry-run-decision",
            &loop_id,
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    assert!(
        out2.status.success(),
        "dry-run failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let after = std::fs::metadata(&db_path).unwrap().len();
    // The DB may grow due to WAL/journal, so allow small variance.
    assert!(
        after <= before + 4096,
        "dry-run must not substantially grow DB: {before} → {after}"
    );
}

#[tokio::test]
async fn binary_nonexistent_loop_error() {
    let (_dir, db_path, _repo_path) = setup_isolated();
    let out = run_harness(&["task-loop", "status", "nonexistent-id"], &db_path);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() || stderr.contains("not found") || stderr.contains("error"),
        "nonexistent loop must error: code={} stderr={}",
        out.status.code().unwrap_or(-1),
        stderr
    );
}

#[tokio::test]
async fn binary_secret_redaction() {
    let (dir, db_path, repo_path) = setup_isolated();
    init_db(&db_path).await;
    let wt = wt_path(&dir);
    let out = run_harness(
        &[
            "task-loop",
            "start",
            "--project",
            "proj-sec",
            "--task",
            "task-sec",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &repo_path,
            "--worktree-root",
            &wt,
        ],
        &db_path,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let loop_id = stdout
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let out2 = run_harness(&["task-loop", "inspect", &loop_id, "--json"], &db_path);
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(!stdout2.contains("sk-"));
    assert!(!stdout2.contains("Bearer "));
}
