//! F1 diagnostic: minimal test to debug "goal_not_recovered" issue.
//! Run: cargo run --bin f1-diagnostic

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use harness_runtime::goal::failpoint;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = std::env::temp_dir().join("f1-diag");
    let _ = std::fs::create_dir_all(&tmp);
    let db_path = tmp.join("harness.db");
    let test_repo = tmp.join("repo");
    let wt_root = std::env::temp_dir().join("f1-diag-wt");
    let state_dir = "f1-diag-shared";
    let harness_bin = PathBuf::from("target/debug/harness-cli.exe");

    // Cleanup
    let _ = std::fs::remove_file(&db_path);
    failpoint::cleanup_all_failpoints();

    // Setup repo
    std::fs::create_dir_all(&test_repo)?;
    std::fs::create_dir_all(test_repo.join("src"))?;
    std::fs::write(test_repo.join("src/lib.rs"), b"// diag\n")?;
    std::fs::write(
        test_repo.join("Cargo.toml"),
        b"[package]\nname=\"diag\"\nversion=\"0.1.0\"\nedition=\"2021\"\n",
    )?;
    run_git(&["init", "."], &test_repo)?;
    run_git(&["add", "."], &test_repo)?;
    run_git(
        &[
            "-c",
            "user.name=Diag",
            "-c",
            "user.email=d@t",
            "commit",
            "-m",
            "init",
        ],
        &test_repo,
    )?;

    // Init DB
    println!("[1/7] Initializing DB...");
    let db = harness_runtime::db::Database::open(&db_path).await?;
    let rc = std::sync::Arc::new(harness_runtime::liveness::RunContext::create(
        &tmp, "diag", true,
    )?);
    let _g = harness_runtime::production_graph::ProductionGraph::build(
        db.pool.clone(),
        &wt_root,
        &test_repo,
        rc,
    );
    drop(db);
    println!("  DB initialized at {:?}", db_path);

    // Start Supervisor A
    println!("[2/7] Starting Supervisor A...");
    let mut child_a = Command::new(&harness_bin)
        .args([
            "supervisor",
            "run",
            "--state-dir",
            state_dir,
            "--db",
            &db_path.to_string_lossy(),
            "--repo",
            &test_repo.to_string_lossy(),
            "--worktree-root",
            &wt_root.to_string_lossy(),
            "--code-head",
            "diag",
        ])
        .env("HARNESS_FAILPOINT_ENABLE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    println!("  Supervisor A PID: {}", child_a.id());

    // Wait for A Ready
    let start = Instant::now();
    let mut token_a: i64 = 0;
    while start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(t)) = check_ready(&db_path, state_dir).await {
            token_a = t;
            break;
        }
        if child_a.try_wait().map(|s| s.is_some()).unwrap_or(false) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("  Supervisor A Ready: token={}", token_a);

    // Create goal via standalone CLI
    println!("[3/7] Creating goal via standalone CLI...");
    let goal_id = "g-f1-diag-001";
    let spec = serde_json::json!({
        "goal_id": goal_id,
        "revision": 1, "title": "F1 Diag", "objective": "Test objective",
        "repository_id": "diag", "target_ref": "refs/heads/main",
        "initial_base_head": "abc123", "success_criteria": [{
            "criterion_id": "c1", "description": "Test", "evidence_policy": "task_terminal_result",
            "verification_policy": "existence_only", "subjectivity": "objective", "required": true
        }], "constraints": [], "non_goals": [],
        "budget": { "max_plan_revisions": 2, "max_total_tasks": 1, "max_active_tasks": 1,
            "max_consecutive_failures": 3, "max_no_progress_iterations": 5,
            "max_total_agent_invocations": 10, "max_planner_invocations": 2,
            "max_evaluator_invocations": 2, "max_elapsed_seconds": 600 },
        "approval_policy": { "require_initial_plan_approval": false,
            "require_high_risk_task_approval": false, "require_scope_change_approval": false,
            "require_budget_increase_approval": false, "require_completion_approval": false,
            "require_resume_after_no_progress_approval": false, "approval_timeout_secs": 3600 },
        "created_by": {"user": {"user_id":"diag","user_name":"Diag"}},
        "created_at": "2026-08-05T00:00:00Z"
    });
    let spec_path = tmp.join("goal-spec.json");
    std::fs::write(&spec_path, serde_json::to_string_pretty(&spec)?)?;

    let mut child_cli = Command::new(&harness_bin)
        .args([
            "goal",
            "create",
            "--standalone",
            "--spec-file",
            &spec_path.to_string_lossy(),
            "--db",
            &db_path.to_string_lossy(),
            "--worktree-root",
            &wt_root.to_string_lossy(),
            "--repo",
            &test_repo.to_string_lossy(),
        ])
        .env("HARNESS_FAILPOINT_ENABLE", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    println!("  CLI PID: {}", child_cli.id());

    // Wait for F1 hit
    println!("[4/7] Waiting for F1 failpoint...");
    let fp_start = Instant::now();
    let mut fp_hit = false;
    while fp_start.elapsed() < Duration::from_secs(60) {
        if failpoint::is_failpoint_hit("f1_after_goal_persisted_before_planning") {
            fp_hit = true;
            break;
        }
        // Check if CLI already exited
        if let Ok(Some(s)) = child_cli.try_wait() {
            println!("  CLI exited early: {:?}", s);
            let mut stderr = String::new();
            if let Some(mut r) = child_cli.stderr.take() {
                std::io::Read::read_to_string(&mut r, &mut stderr).ok();
            }
            println!("  CLI stderr: {}", stderr);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("  F1 hit: {}", fp_hit);

    // Verify goal in DB BEFORE killing A
    println!("[4b] Verifying goal in DB before crash...");
    let db_check = harness_runtime::db::Database::open(&db_path).await?;
    let pre_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM goals WHERE goal_id = ?")
        .bind(goal_id)
        .fetch_one(&db_check.pool)
        .await?;
    println!("  Goal count before crash: {}", pre_count);
    drop(db_check);

    // Kill A
    println!("[5/7] Killing Supervisor A...");
    child_a.kill()?;
    child_a.wait()?;
    println!("  A terminated");

    // Release failpoint + wait
    tokio::time::sleep(Duration::from_secs(5)).await;
    failpoint::release_failpoint("f1_after_goal_persisted_before_planning");
    println!("  Failpoint released");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Wait for CLI to finish
    let cli_status = child_cli.wait()?;
    println!("  CLI exited: {:?}", cli_status);

    // Verify goal in DB AFTER CLI completes
    let db_check2 = harness_runtime::db::Database::open(&db_path).await?;
    let post_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM goals WHERE goal_id = ?")
        .bind(goal_id)
        .fetch_one(&db_check2.pool)
        .await?;
    println!("  Goal count after CLI: {}", post_count);
    drop(db_check2);

    // Start B
    println!("[6/7] Starting Supervisor B...");
    let mut child_b = Command::new(&harness_bin)
        .args([
            "supervisor",
            "run",
            "--state-dir",
            state_dir,
            "--db",
            &db_path.to_string_lossy(),
            "--repo",
            &test_repo.to_string_lossy(),
            "--worktree-root",
            &wt_root.to_string_lossy(),
            "--code-head",
            "diag",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    println!("  B PID: {}", child_b.id());

    let b_start = Instant::now();
    let mut token_b: i64 = -1;
    tokio::time::sleep(Duration::from_secs(5)).await;
    while b_start.elapsed() < Duration::from_secs(30) {
        if let Ok(Some(t)) = check_ready(&db_path, state_dir).await {
            token_b = t;
            break;
        }
        if child_b.try_wait().map(|s| s.is_some()).unwrap_or(false) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("  B token: {}", token_b);

    // Final check
    println!("[7/7] Final verification...");
    let db_final = harness_runtime::db::Database::open(&db_path).await?;
    let final_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM goals WHERE goal_id = ?")
        .bind(goal_id)
        .fetch_one(&db_final.pool)
        .await?;
    println!("  Final goal count: {}", final_count);

    let state: Option<(String,)> = sqlx::query_as("SELECT state FROM goals WHERE goal_id = ?")
        .bind(goal_id)
        .fetch_optional(&db_final.pool)
        .await?;
    println!("  Goal state: {:?}", state);
    drop(db_final);

    // Results
    println!();
    println!("=== RESULTS ===");
    println!("  F1 hit:           {}", fp_hit);
    println!("  Pre-crash count:  {}", pre_count);
    println!("  Post-CLI count:   {}", post_count);
    println!("  Final count:      {}", final_count);
    println!("  Token A:          {}", token_a);
    println!("  Token B:          {}", token_b);
    println!("  B > A:            {}", token_b > token_a);

    let _ = child_b.kill();
    let _ = child_b.wait();
    failpoint::cleanup_all_failpoints();

    if fp_hit && final_count > 0 && token_b > token_a {
        println!("\nF1 DIAGNOSTIC: PASS");
    } else {
        println!("\nF1 DIAGNOSTIC: FAIL");
    }

    Ok(())
}

fn run_git(args: &[&str], cwd: &std::path::Path) -> Result<(), String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("git: {}", e))?;
    if !output.status.success() {
        return Err(format!(
            "git error: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

async fn check_ready(db_path: &std::path::Path, state_dir: &str) -> Result<Option<i64>, String> {
    let db = harness_runtime::db::Database::open(db_path)
        .await
        .map_err(|e| format!("db: {}", e))?;
    let repo = harness_runtime::supervisor::repo::SupervisorRepo::new(db.pool.clone());
    match repo.get_active_instance_for_dir(state_dir).await {
        Ok(Some(inst)) => {
            let state_str = format!("{:?}", inst.state);
            if state_str.contains("Ready") {
                Ok(Some(inst.fencing_token))
            } else {
                Ok(None)
            }
        }
        Ok(None) => Ok(None),
        Err(e) => Err(format!("check: {}", e)),
    }
}
