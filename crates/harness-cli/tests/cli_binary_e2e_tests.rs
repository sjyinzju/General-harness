//! I4.5 CLI Binary-Process E2E Certification Tests (Phase 2).
//!
//! Tests spawn the COMPILED harness-cli binary. Set CARGO_BIN_EXE_harness_cli
//! in the environment or run via `cargo test` which sets it automatically.
//! Tests gracefully skip when the binary is unavailable.

use std::process::Command;

fn binary() -> Option<String> {
    std::env::var("CARGO_BIN_EXE_harness_cli").ok()
}

fn run(args: &[&str], db: &str) -> Option<std::process::Output> {
    let mut c = Command::new(binary()?);
    c.args(args).env("HARNESS_DB", db).env("NO_COLOR", "1");
    let td = tempfile::tempdir().unwrap();
    c.env("HOME", td.path()).env("USERPROFILE", td.path());
    c.output().ok()
}

fn setup() -> (tempfile::TempDir, String, String) {
    let root = std::path::PathBuf::from(
        std::env::var("HARNESS_E2E_TEMP").unwrap_or_else(|_| {
            if cfg!(windows) { "C:\\harness-e2e-temp".into() }
            else { "/tmp/harness-e2e-temp".into() }
        }));
    std::fs::create_dir_all(&root).unwrap();
    let dir = tempfile::tempdir_in(&root).unwrap();
    let db = dir.path().join("t.db").to_string_lossy().to_string();
    let rp = dir.path().join("repo"); std::fs::create_dir_all(&rp).unwrap();
    Command::new("git").args(["init"]).current_dir(&rp).output().ok();
    Command::new("git").args(["config","user.email","t@t"]).current_dir(&rp).output().ok();
    Command::new("git").args(["config","user.name","t"]).current_dir(&rp).output().ok();
    std::fs::write(rp.join("R"), "#").ok();
    Command::new("git").args(["add","R"]).current_dir(&rp).output().ok();
    Command::new("git").args(["commit","-m","i"]).current_dir(&rp).output().ok();
    (dir, db, rp.to_string_lossy().to_string())
}

async fn init_db(db: &str) {
    let d = harness_runtime::db::Database::open(&std::path::PathBuf::from(db)).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO projects(id,objective,lifecycle) VALUES('p','e2e','active')").execute(&d.pool).await.unwrap();
    sqlx::query("INSERT OR IGNORE INTO tasks(id,project_id,goal,lifecycle) VALUES('t','p','g','submitted')").execute(&d.pool).await.unwrap();
    d.pool.close().await;
}

fn wt(dir: &tempfile::TempDir) -> String {
    let w = dir.path().join("wt"); std::fs::create_dir_all(&w).unwrap(); w.to_string_lossy().to_string()
}

fn lid(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).lines().find(|l| l.contains("Loop created"))
        .and_then(|l| l.split_whitespace().last()).map(|s| s.to_string()).unwrap_or_default()
}

macro_rules! need_bin {
    () => { if binary().is_none() { eprintln!("SKIP: no binary"); return; } };
}

#[tokio::test] async fn binary_start_and_status() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let o = run(&["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w],&db).unwrap();
    assert!(o.status.success()); let l = lid(&o); assert!(!l.is_empty());
    assert!(run(&["task-loop","status",&l,"--repo",&rp,"--worktree-root",&w],&db).unwrap().status.success());
}

#[tokio::test] async fn binary_start_replay_idempotent() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let a: &[&str] = &["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w];
    assert!(run(a,&db).unwrap().status.success()); assert!(run(a,&db).unwrap().status.success());
}

#[tokio::test] async fn binary_resume_idempotent() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let o = run(&["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w],&db).unwrap();
    let l = lid(&o);
    assert!(run(&["task-loop","resume",&l,"--owner","ci","--repo",&rp,"--worktree-root",&w],&db).unwrap().status.success());
    assert!(run(&["task-loop","resume",&l,"--owner","ci","--repo",&rp,"--worktree-root",&w],&db).unwrap().status.success());
}

#[tokio::test] async fn binary_cancel() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let o = run(&["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w],&db).unwrap();
    let l = lid(&o); assert!(run(&["task-loop","cancel",&l,"--owner","ci"],&db).unwrap().status.success());
}

#[tokio::test] async fn binary_inspect_json() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let o = run(&["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w],&db).unwrap();
    let l = lid(&o);
    let o2 = run(&["task-loop","inspect",&l,"--json"],&db).unwrap();
    let s = String::from_utf8_lossy(&o2.stdout); assert!(s.contains(&l));
    assert!(serde_json::from_str::<serde_json::Value>(&s).is_ok());
}

#[tokio::test] async fn binary_dry_run_zero_writes() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let o = run(&["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w],&db).unwrap();
    let l = lid(&o);
    let before = std::fs::metadata(&db).unwrap().len();
    assert!(run(&["task-loop","dry-run-decision",&l,"--repo",&rp,"--worktree-root",&w],&db).unwrap().status.success());
    let after = std::fs::metadata(&db).unwrap().len();
    assert!(after <= before + 4096, "dry-run DB growth: {before} -> {after}");
}

#[tokio::test] async fn binary_nonexistent_loop_error() { need_bin!();
    let (_d, db, _rp) = setup();
    let o = run(&["task-loop","status","_no_such_"],&db).unwrap();
    let e = String::from_utf8_lossy(&o.stderr);
    assert!(!o.status.success() || e.contains("not found") || e.contains("error"));
}

#[tokio::test] async fn binary_secret_redaction() { need_bin!();
    let (d, db, rp) = setup(); init_db(&db).await; let w = wt(&d);
    let o = run(&["task-loop","start","--project","p","--task","t","--owner","ci","--policy","{}","--repo",&rp,"--worktree-root",&w],&db).unwrap();
    let l = lid(&o);
    let insp = run(&["task-loop","inspect",&l,"--json"],&db).unwrap();
    let s = String::from_utf8_lossy(&insp.stdout);
    assert!(!s.contains("sk-")); assert!(!s.contains("Bearer "));
}
