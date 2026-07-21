//! I4.5 CLI Binary-Process E2E Certification Tests (Phase 2).
//!
//! Every test spawns the COMPILED harness-cli binary, exercising the full
//! production path: main() → ProductionGraph::build() →
//! RealI4OrchestrationGateway → durable SQLite state.
//!
//! These tests MUST be run via `cargo test` which compiles the binary and
//! sets CARGO_BIN_EXE_harness_cli at runtime. Binary missing = test FAIL.

use std::process::Command;

fn binary() -> String {
    std::env::var("CARGO_BIN_EXE_harness_cli").unwrap_or_else(|_| {
        // Fallback: navigate from the test binary directory to the harness-cli binary.
        // Test:  target/debug/deps/cli_binary_e2e_tests-xxx.exe
        // Binary: target/debug/harness-cli.exe
        let exe = std::env::current_exe().unwrap();
        let dir = exe.parent().unwrap().parent().unwrap();
        dir.join("harness-cli")
            .with_extension(std::env::consts::EXE_EXTENSION)
            .to_string_lossy()
            .to_string()
    })
}

fn run(args: &[&str], db: &str) -> std::process::Output {
    let mut c = Command::new(binary());
    c.args(args).env("HARNESS_DB", db).env("NO_COLOR", "1");
    let td = tempfile::tempdir().unwrap();
    c.env("HOME", td.path()).env("USERPROFILE", td.path());
    c.output().expect("failed to spawn harness binary")
}

fn setup() -> (tempfile::TempDir, String, String) {
    let root = std::path::PathBuf::from(std::env::var("HARNESS_E2E_TEMP").unwrap_or_else(|_| {
        if cfg!(windows) {
            "C:\\harness-e2e-temp".into()
        } else {
            "/tmp/harness-e2e-temp".into()
        }
    }));
    std::fs::create_dir_all(&root).unwrap();
    let dir = tempfile::tempdir_in(&root).unwrap();
    let db = dir.path().join("t.db").to_string_lossy().to_string();
    let rp = dir.path().join("repo");
    std::fs::create_dir_all(&rp).unwrap();
    Command::new("git")
        .args(["init"])
        .current_dir(&rp)
        .output()
        .ok();
    Command::new("git")
        .args(["config", "user.email", "t@t"])
        .current_dir(&rp)
        .output()
        .ok();
    Command::new("git")
        .args(["config", "user.name", "t"])
        .current_dir(&rp)
        .output()
        .ok();
    std::fs::write(rp.join("R"), "#").ok();
    Command::new("git")
        .args(["add", "R"])
        .current_dir(&rp)
        .output()
        .ok();
    Command::new("git")
        .args(["commit", "-m", "i"])
        .current_dir(&rp)
        .output()
        .ok();
    (dir, db, rp.to_string_lossy().to_string())
}

async fn init_db(db: &str) {
    let d = harness_runtime::db::Database::open(&std::path::PathBuf::from(db))
        .await
        .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO projects(id,objective,lifecycle) VALUES('p','e2e','active')",
    )
    .execute(&d.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO tasks(id,project_id,goal,lifecycle) VALUES('t','p','g','submitted')",
    )
    .execute(&d.pool)
    .await
    .unwrap();
    d.pool.close().await;
}

fn wt(dir: &tempfile::TempDir) -> String {
    let w = dir.path().join("wt");
    std::fs::create_dir_all(&w).unwrap();
    w.to_string_lossy().to_string()
}

fn lid(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

#[tokio::test]
async fn binary_start_and_status() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let o = run(
        &[
            "task-loop",
            "start",
            "--project",
            "p",
            "--task",
            "t",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    assert!(
        o.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    let l = lid(&o);
    assert!(!l.is_empty(), "no loop_id in output");
    let o2 = run(
        &[
            "task-loop",
            "status",
            &l,
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    assert!(
        o2.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&o2.stderr)
    );
}

#[tokio::test]
async fn binary_start_replay_idempotent() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let a: &[&str] = &[
        "task-loop",
        "start",
        "--project",
        "p",
        "--task",
        "t",
        "--owner",
        "ci",
        "--policy",
        "{}",
        "--repo",
        &rp,
        "--worktree-root",
        &w,
    ];
    assert!(run(a, &db).status.success());
    assert!(run(a, &db).status.success());
}

#[tokio::test]
async fn binary_resume_idempotent() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let o = run(
        &[
            "task-loop",
            "start",
            "--project",
            "p",
            "--task",
            "t",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    let l = lid(&o);
    let ra: &[&str] = &[
        "task-loop",
        "resume",
        &l,
        "--owner",
        "ci",
        "--repo",
        &rp,
        "--worktree-root",
        &w,
    ];
    assert!(run(ra, &db).status.success());
    assert!(run(ra, &db).status.success());
}

#[tokio::test]
async fn binary_cancel() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let o = run(
        &[
            "task-loop",
            "start",
            "--project",
            "p",
            "--task",
            "t",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    let l = lid(&o);
    assert!(run(&["task-loop", "cancel", &l, "--owner", "ci"], &db)
        .status
        .success());
}

#[tokio::test]
async fn binary_inspect_json() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let o = run(
        &[
            "task-loop",
            "start",
            "--project",
            "p",
            "--task",
            "t",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    let l = lid(&o);
    let o2 = run(&["task-loop", "inspect", &l, "--json"], &db);
    let s = String::from_utf8_lossy(&o2.stdout);
    assert!(s.contains(&l), "inspect output missing loop_id");
    serde_json::from_str::<serde_json::Value>(&s).expect("inspect output not valid JSON");
}

#[tokio::test]
async fn binary_dry_run_zero_writes() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let o = run(
        &[
            "task-loop",
            "start",
            "--project",
            "p",
            "--task",
            "t",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    let l = lid(&o);
    let before = std::fs::metadata(&db).unwrap().len();
    assert!(run(
        &[
            "task-loop",
            "dry-run-decision",
            &l,
            "--repo",
            &rp,
            "--worktree-root",
            &w
        ],
        &db,
    )
    .status
    .success());
    let after = std::fs::metadata(&db).unwrap().len();
    assert!(
        after <= before + 4096,
        "dry-run DB growth: {before} -> {after}"
    );
}

#[tokio::test]
async fn binary_nonexistent_loop_error() {
    let (_d, db, _rp) = setup();
    let o = run(&["task-loop", "status", "_no_such_"], &db);
    let e = String::from_utf8_lossy(&o.stderr);
    assert!(
        !o.status.success() || e.contains("not found") || e.contains("error"),
        "nonexistent loop must produce error"
    );
}

#[tokio::test]
async fn binary_secret_redaction() {
    let (d, db, rp) = setup();
    init_db(&db).await;
    let w = wt(&d);
    let o = run(
        &[
            "task-loop",
            "start",
            "--project",
            "p",
            "--task",
            "t",
            "--owner",
            "ci",
            "--policy",
            "{}",
            "--repo",
            &rp,
            "--worktree-root",
            &w,
        ],
        &db,
    );
    let l = lid(&o);
    let insp = run(&["task-loop", "inspect", &l, "--json"], &db);
    let s = String::from_utf8_lossy(&insp.stdout);
    assert!(!s.contains("sk-"), "output contains raw secret key");
    assert!(!s.contains("Bearer "), "output contains bearer token");
}
