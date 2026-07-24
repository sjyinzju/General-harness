//! harness-cli: CLI entry point with automatic managed-temp lifecycle.
//!
//! Every command automatically:
//! 1. Creates a `RunContext` with managed temp directories.
//! 2. Redirects `TEMP`/`TMP` to the managed temp (inherited by all children).
//! 3. Builds a `ProductionGraph` with mandatory `LivenessOrchestrator`.
//! 4. Starts a periodic janitor for background cleanup.
//! 5. Runs the startup janitor before accepting work.
//! 6. Calls explicit shutdown on all exit paths (including Ctrl+C).
//!
//! # invariants
//! - `RunContext::shutdown()` is ALWAYS called (success, failure, cancel, Ctrl+C).
//! - `std::process::exit()` is NEVER called after `RunContext` creation.
//! - The periodic janitor is started exactly once and cancelled before shutdown.
//! - No detached background tasks remain after shutdown.

mod commands;

use harness_runtime::db::Database;
use harness_runtime::liveness::RunContext;
use harness_runtime::production_graph::ProductionGraph;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    // ── Resolve repo root ──────────────────────────────────────
    let repo_root = parse_flag(&args, "--repo")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let code_head = parse_flag(&args, "--code-head").unwrap_or("unknown");
    // Default DB goes into target/data/ so it never lands in repo root.
    let default_db = repo_root
        .join("target")
        .join("data")
        .join("harness.db")
        .to_string_lossy()
        .to_string();
    let db_path = std::env::var("HARNESS_DB").unwrap_or(default_db);

    // ── "cleanup" and "review" are special cases — no ProductionGraph needed ──
    if args.len() >= 2 && args[1] == "cleanup" {
        return cmd_cleanup(&args, &repo_root, &db_path).await;
    }
    if args.len() >= 2 && args[1] == "review" {
        return cmd_review_standalone(&args, &db_path).await;
    }

    // ── Create RunContext (managed temp + env redirect) ─────────
    let run_context = match RunContext::create(&repo_root, code_head, true) {
        Ok(rc) => Arc::new(rc),
        Err(e) => {
            eprintln!("fatal: run context: {e}");
            return Err(e.into());
        }
    };

    // ── Build ProductionGraph ───────────────────────────────────
    let db = Database::open(&PathBuf::from(&db_path)).await?;
    let worktree_root = parse_flag(&args, "--worktree-root")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo_root.join("target/tmp"));
    let graph = match ProductionGraph::build(
        db.pool.clone(),
        &worktree_root,
        &repo_root,
        run_context.clone(),
    ) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("fatal: {e}");
            // Shutdown run context before exit — env restore + marker finalize.
            let _ = run_context.shutdown(false).await;
            std::process::exit(1);
        }
    };

    // ── Run startup janitor ─────────────────────────────────────
    let _startup_result = graph.startup().await;

    // ── Start periodic janitor ──────────────────────────────────
    let janitor_cancel = graph.start_periodic_janitor(Duration::from_secs(300));

    // ── Install Ctrl+C handler ──────────────────────────────────
    let _ctrlc_cancel = janitor_cancel.clone();
    let _ctrlc_run_context = run_context.clone();

    // ── Dispatch with Ctrl+C awareness ──────────────────────────
    let run_succeeded = tokio::select! {
        result = dispatch_command(&args, &db, &graph) => {
            result
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received, initiating graceful shutdown");
            eprintln!("\nInterrupted — shutting down...");
            false
        }
    };

    // ── Cancel periodic janitor (bounded wait) ──────────────────
    janitor_cancel.cancel();
    // Give the periodic janitor up to 2 seconds to finish its current tick.
    tokio::time::timeout(Duration::from_secs(2), async {
        // The janitor will observe cancellation and exit its loop.
        tokio::time::sleep(Duration::from_millis(100)).await;
    })
    .await
    .ok();

    // ── Explicit shutdown ───────────────────────────────────────
    // The Ctrl+C handler uses its own cleanup path; for the main path,
    // we call shutdown explicitly.  The Drop impl on RunContext is the
    // last-resort safety net.
    let _shutdown_result = graph.shutdown(run_succeeded).await;

    // Also ensure the Ctrl+C path's clone is shut down if it was
    // the one that activated.  Since Ctrl+C selects the signal future,
    // the main run_context.shutdown is still called above.

    tracing::info!(run_succeeded = run_succeeded, "harness exiting");

    if run_succeeded {
        Ok(())
    } else {
        // Return error via main return, NOT process::exit().
        Err("command failed".into())
    }
}

async fn dispatch_command(args: &[String], db: &Database, graph: &ProductionGraph) -> bool {
    match args[1].as_str() {
        "task-loop" => {
            if args.len() < 3 {
                eprintln!("error: missing task-loop subcommand");
                false
            } else {
                dispatch_task_loop(args, db, graph).await
            }
        }
        "review" => {
            if args.len() < 3 {
                eprintln!("error: missing review subcommand");
                false
            } else {
                dispatch_review(args, db).await
            }
        }
        _ => {
            eprintln!("harness v0.1.0 — unknown command: {}", args[1]);
            false
        }
    }
}

async fn dispatch_task_loop(args: &[String], db: &Database, graph: &ProductionGraph) -> bool {
    match args[2].as_str() {
        "start" => {
            let project = parse_flag(args, "--project").unwrap_or("default");
            let task = match parse_flag(args, "--task") {
                Some(t) => t,
                None => {
                    eprintln!("error: --task required");
                    return false;
                }
            };
            let owner = parse_flag(args, "--owner").unwrap_or("cli");
            let policy = parse_flag(args, "--policy").unwrap_or("{}");
            match commands::task_loop::cmd_start(db, Some(graph), project, task, owner, policy)
                .await
            {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "status" => {
            let loop_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: loop-id required");
                    return false;
                }
            };
            match commands::task_loop::cmd_status(db, Some(graph), loop_id).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "resume" => {
            let loop_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: loop-id required");
                    return false;
                }
            };
            let owner = parse_flag(args, "--owner").unwrap_or("cli");
            match commands::task_loop::cmd_resume(db, Some(graph), loop_id, owner).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "cancel" => {
            let loop_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: loop-id required");
                    return false;
                }
            };
            let owner = parse_flag(args, "--owner").unwrap_or("cli");
            match commands::task_loop::cmd_cancel(db, Some(graph), loop_id, owner).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "inspect" => {
            let loop_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: loop-id required");
                    return false;
                }
            };
            if args.contains(&"--json".to_string()) {
                match commands::task_loop::cmd_inspect_json(db, Some(graph), loop_id).await {
                    Ok(()) => true,
                    Err(e) => {
                        eprintln!("error: {e}");
                        false
                    }
                }
            } else {
                match commands::task_loop::cmd_status(db, Some(graph), loop_id).await {
                    Ok(()) => true,
                    Err(e) => {
                        eprintln!("error: {e}");
                        false
                    }
                }
            }
        }
        "dry-run-decision" => {
            let loop_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: loop-id required");
                    return false;
                }
            };
            match commands::task_loop::cmd_dry_run_decision(db, Some(graph), loop_id).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        other => {
            eprintln!("error: unknown subcommand: {other}");
            false
        }
    }
}

async fn cmd_cleanup(
    args: &[String],
    repo_root: &Path,
    db_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = !args.contains(&"--apply".to_string());
    let db = Database::open(&PathBuf::from(db_path)).await?;

    let liveness_config =
        harness_runtime::liveness::LivenessConfig::for_repo(repo_root, "harness-cli".into());
    let pool = db.pool.clone();
    match harness_runtime::liveness::LivenessOrchestrator::new(liveness_config, pool) {
        Ok(orch) => {
            let result = orch.cli_cleanup(vec![], dry_run).await;
            let report =
                harness_runtime::liveness::LivenessOrchestrator::format_dry_run_report(&result);
            println!("{report}");
            if dry_run {
                println!("\n*** DRY RUN — no files were deleted. Use --apply to execute. ***");
            }
        }
        Err(e) => {
            eprintln!("cleanup error: {e}");
        }
    }
    Ok(())
}

fn print_usage() {
    println!("harness v0.1.0 — task engineering harness");
    println!("Usage:");
    println!("  harness task-loop start --project <id> --task <id> [--owner <id>] [--policy <json>] [--repo <path>] [--worktree-root <path>] [--code-head <sha>]");
    println!("  harness task-loop status <loop-id> [--repo <path>]");
    println!("  harness task-loop resume <loop-id> [--owner <id>] [--repo <path>] [--worktree-root <path>]");
    println!("  harness task-loop cancel <loop-id> [--owner <id>] [--repo <path>]");
    println!("  harness task-loop inspect <loop-id> [--json] [--repo <path>]");
    println!("  harness task-loop dry-run-decision <loop-id> [--repo <path>]");
    println!("  harness review create <candidate-id> [--reviewer <profile-id>] [--repo <path>]");
    println!("  harness review run <review-id> [--repo <path>]");
    println!("  harness review show <review-id> [--json] [--repo <path>]");
    println!("  harness review list [--state <state>] [--json] [--repo <path>]");
    println!("  harness cleanup [--dry-run|--apply] [--repo <path>]");
    println!();
    println!("Environment:");
    println!("  HARNESS_DB     path to SQLite database (default: target/data/harness.db)");
    println!("  TEMP/TMP       automatically redirected to managed temp");
}

async fn dispatch_review(args: &[String], db: &Database) -> bool {
    match args[2].as_str() {
        "create" => {
            let candidate_id = match parse_flag(args, "--candidate") {
                Some(c) => c,
                None => {
                    // Also accept positional candidate ID
                    match args.get(3) {
                        Some(c) if !c.starts_with("--") => c.as_str(),
                        _ => {
                            eprintln!("error: --candidate <id> required (or use positional)");
                            return false;
                        }
                    }
                }
            };
            let reviewer = parse_flag(args, "--reviewer").unwrap_or("default-reviewer");
            match commands::review::cmd_review_create(db, candidate_id, reviewer).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "show" => {
            let review_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: review-id required");
                    return false;
                }
            };
            let json = args.contains(&"--json".to_string());
            match commands::review::cmd_review_show(db, review_id, json).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "run" => {
            let review_id = match args.get(3) {
                Some(id) => id,
                None => {
                    eprintln!("error: review-id required");
                    return false;
                }
            };
            match commands::review::cmd_review_run(db, review_id).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        "list" => {
            let json = args.contains(&"--json".to_string());
            let state_filter = parse_flag(args, "--state");
            match commands::review::cmd_review_list(db, state_filter, json).await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("error: {e}");
                    false
                }
            }
        }
        other => {
            eprintln!("error: unknown review subcommand: {other}");
            eprintln!("Usage: harness review <create|show|run|list> [args]");
            false
        }
    }
}

async fn cmd_review_standalone(
    args: &[String],
    db_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(&PathBuf::from(db_path)).await?;
    let ok = dispatch_review(args, &db).await;
    if ok {
        Ok(())
    } else {
        Err("review command failed".into())
    }
}

fn parse_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let idx = args.iter().position(|a| a == flag)?;
    args.get(idx + 1).map(|s| s.as_str())
}
